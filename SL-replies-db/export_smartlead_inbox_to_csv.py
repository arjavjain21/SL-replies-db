#!/usr/bin/env python3
"""
Smartlead Master Inbox Replies → Clean CSV Exporter

Features:
- Async pagination with global RPS limiter (default 5 RPS)
- Tenacity-based retries with exponential backoff and Retry-After support
- Rich progress bars and final summary
- Streams tidy CSV rows (one per email_history item) without large buffers

Python: 3.11+
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import json
import logging
import os
import random
import signal
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup
from tenacity import AsyncRetrying, RetryCallState, retry_if_exception, stop_after_attempt
from tenacity.wait import wait_base
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, TaskID
from rich.table import Table
import platform


BASE_URL = "https://server.smartlead.ai/api/v1/master-inbox"
ENDPOINT = "/inbox-replies"


# ------------------------------- CSV schema ------------------------------- #

CSV_HEADERS: List[str] = [
    # Lead-level
    "lead_category_id",
    "last_sent_time",
    "last_reply_time",
    "has_new_unread_email",
    "email_account_id",
    "revenue",
    "is_pushed_to_sub_sequence",
    "lead_first_name",
    "lead_last_name",
    "lead_email",
    "email_lead_id",
    "email_lead_map_id",
    "lead_status",
    "current_sequence_number",
    "sub_sequence_id",
    "old_replaced_lead_data",
    "lead_next_timestamp_to_send",
    "email_campaign_seq_id",
    "is_important",
    "is_archived",
    "is_snoozed",
    "team_member_id",
    "is_ooo_automated_push_lead",
    "email_campaign_id",
    "email_campaign_name",
    "client_id",
    "belongs_to_sub_sequence",
    "campaign_sending_tz",
    "campaign_sending_days",
    "campaign_sending_startHour",
    "campaign_sending_endHour",
    "sequence_count",
    # Email-history (prefixed with history_)
    "history_stats_id",
    "history_from",
    "history_to",
    "history_type",
    "history_message_id",
    "history_time",
    "history_subject",
    "history_email_seq_number",
    "history_open_count",
    "history_click_count",
    "history_click_details_json",
    "history_cc_joined",
    "email_body_html",
    "email_body_text",
    # Derived/metadata
    "thread_key",
    "thread_lead_id",
    "history_index",
    "history_count",
    "api_fetched_at",
    "source_endpoint",
    "api_sort_by",
    "api_filters_json",
    "page_offset",
    "page_limit",
]


# ------------------------------ Helper types ------------------------------ #


class HttpRetryableError(Exception):
    """Raised when an HTTP response indicates a retryable condition."""

    def __init__(self, status_code: int, message: str, retry_after_seconds: Optional[float] = None):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after_seconds = retry_after_seconds


class RetryAfterOrExponential(wait_base.WaitBase):
    """Tenacity wait strategy that prefers Retry-After header when available,
    otherwise uses exponential backoff with full jitter.
    """

    def __init__(self, multiplier: float = 0.5, max_sleep: float = 30.0):
        self.multiplier = multiplier
        self.max_sleep = max_sleep

    def __call__(self, retry_state: RetryCallState) -> float:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, HttpRetryableError) and exc.retry_after_seconds is not None:
            try:
                return min(float(exc.retry_after_seconds), self.max_sleep)
            except Exception:
                return self.max_sleep
        # full jitter exponential
        attempt = max(1, retry_state.attempt_number)
        expo = self.multiplier * (2 ** (attempt - 1))
        jitter = random.uniform(0, expo)
        return min(jitter, self.max_sleep)


class RPSController:
    """Global async RPS controller using a sliding window over 2 seconds."""

    def __init__(self, rps: float, window_seconds: float = 2.0):
        self.rps = max(0.1, rps)
        self.window_seconds = window_seconds
        self.capacity = int(self.rps * self.window_seconds)
        if self.capacity < 1:
            self.capacity = 1
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()
        self.rate_limit_sleeps: int = 0

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = asyncio.get_running_loop().time()
                # drop old timestamps
                cutoff = now - self.window_seconds
                while self._timestamps and self._timestamps[0] < cutoff:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.capacity:
                    self._timestamps.append(now)
                    return
                # need to wait until the earliest timestamp expires
                sleep_for = self.window_seconds - (now - self._timestamps[0])
                if sleep_for < 0:
                    sleep_for = 0
                self.rate_limit_sleeps += 1
                await asyncio.sleep(sleep_for)


@dataclass
class Stats:
    requests: int = 0
    retries: int = 0
    http_429s: int = 0
    pages_fetched: int = 0
    leads_processed: int = 0
    rows_written: int = 0
    rate_limit_sleeps: int = 0

    def snapshot(self) -> Dict[str, int]:
        return {
            "requests": self.requests,
            "retries": self.retries,
            "http_429s": self.http_429s,
            "pages_fetched": self.pages_fetched,
            "leads_processed": self.leads_processed,
            "rows_written": self.rows_written,
            "rate_limit_sleeps": self.rate_limit_sleeps,
        }


# ------------------------------- CLI parsing ------------------------------ #


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export Smartlead Master Inbox replies to a tidy CSV (async, rate-limited)")
    parser.add_argument("--out", dest="out_path", default=None, help="Output CSV path (default: ./exports/smartlead_inbox_export_<timestamp>.csv)")
    parser.add_argument("--limit-per-page", type=int, default=20, help="Page size 1-20 (default 20)")
    parser.add_argument("--sort-by", choices=["REPLY_TIME_DESC", "SENT_TIME_DESC"], default="REPLY_TIME_DESC", help="API sort order (default REPLY_TIME_DESC)")
    parser.add_argument("--filters-json", dest="filters_json", help="Path to JSON file with API filters (overrides defaults)")
    parser.add_argument("--no-default-filters", action="store_true", help="Disable default OOO filters")
    parser.add_argument("--reply-start", help="ISO8601 start for replyTimeBetween")
    parser.add_argument("--reply-end", help="ISO8601 end for replyTimeBetween")
    parser.add_argument("--campaign-id", action="append", type=int, dest="campaign_ids", help="Filter: campaign id (repeatable)")
    parser.add_argument("--email-account-id", action="append", type=int, dest="email_account_ids", help="Filter: email account id (repeatable)")
    parser.add_argument("--max-concurrency", type=int, default=10, help="Max concurrent workers (default 10)")
    parser.add_argument("--rps", type=float, default=5.0, help="Global requests per second (default 5.0)")
    parser.add_argument("--max-retries", type=int, default=6, help="Max retries on 429/5xx/network (default 6)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Logging level")
    parser.add_argument("--dry-run", action="store_true", help="Do not write CSV; just run and count")
    parser.add_argument("--max-pages", type=int, default=None, help=argparse.SUPPRESS)
    return parser


def default_filters() -> Dict[str, Any]:
    return {"leadCategories": {"categoryIdsIn": [6]}}


def load_effective_filters(args: argparse.Namespace) -> Optional[Dict[str, Any]]:
    if args.filters_json:
        with open(args.filters_json, "r", encoding="utf-8") as f:
            base = json.load(f)
    else:
        base = None if args.no_default_filters else default_filters()

    filters: Dict[str, Any] = {} if base is None else dict(base)

    # Convenience flags
    if args.reply_start or args.reply_end:
        filters["replyTimeBetween"] = {"start": args.reply_start or "", "end": args.reply_end or ""}
    if args.campaign_ids:
        filters["campaignIdsIn"] = list({int(x) for x in args.campaign_ids})
    if args.email_account_ids:
        filters["emailAccountIdsIn"] = list({int(x) for x in args.email_account_ids})

    if not filters:
        return None
    return filters


def build_default_output_path() -> Path:
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("./exports")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"smartlead_inbox_export_{ts}.csv"


# ------------------------------- HTTP client ------------------------------ #


def parse_retry_after(header_val: Optional[str]) -> Optional[float]:
    if not header_val:
        return None
    try:
        return float(header_val)
    except Exception:
        # try HTTP-date format — fallback to None
        try:
            retry_time = dt.datetime.strptime(header_val, "%a, %d %b %Y %H:%M:%S GMT")
            now = dt.datetime.utcnow()
            delta = (retry_time - now).total_seconds()
            return max(0.0, delta)
        except Exception:
            return None


async def fetch_page(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    rps: RPSController,
    stats: Stats,
    logger: logging.Logger,
    limit: int,
    offset: int,
    sort_by: str,
    filters: Optional[Dict[str, Any]],
    max_retries: int,
) -> Dict[str, Any]:
    url = f"{BASE_URL}{ENDPOINT}"
    params = {"api_key": api_key, "fetch_message_history": "true"}
    payload: Dict[str, Any] = {"limit": int(limit), "offset": int(offset), "sortBy": str(sort_by)}
    if filters is not None:
        payload["filters"] = filters

    wait_strategy = RetryAfterOrExponential(multiplier=0.5, max_sleep=30.0)

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(max_retries),
        retry=retry_if_exception(lambda e: isinstance(e, HttpRetryableError) or isinstance(e, httpx.TransportError)),
        wait=wait_strategy,
        reraise=True,
        before=lambda s: _before_attempt(stats),
        before_sleep=lambda s: _before_sleep_log(s, logger, stats),
    ):
        with attempt:
            await rps.acquire()
            try:
                resp = await client.post(url, params=params, json=payload)
            except httpx.TransportError as te:
                raise te

            if resp.status_code in (429,) or resp.status_code >= 500:
                retry_after = parse_retry_after(resp.headers.get("Retry-After"))
                if resp.status_code == 429:
                    stats.http_429s += 1
                raise HttpRetryableError(resp.status_code, f"HTTP {resp.status_code} on offset {offset}", retry_after)

            resp.raise_for_status()
            return resp.json()

    # Unreachable due to reraise=True, but keeps type checkers happy
    raise RuntimeError("fetch_page retry loop exhausted unexpectedly")


def _before_attempt(stats: Stats) -> None:
    stats.requests += 1


def _before_sleep_log(state: RetryCallState, logger: logging.Logger, stats: Stats) -> None:
    stats.retries += 1
    exc = state.outcome.exception() if state.outcome else None
    wait_obj = RetryAfterOrExponential()
    wait_seconds = wait_obj(state)
    if isinstance(exc, HttpRetryableError):
        logger.warning(
            "Retrying due to HTTP error: status=%s attempt=%s wait=%.2fs", exc.status_code, state.attempt_number, wait_seconds
        )
    else:
        logger.warning("Retrying due to network error: attempt=%s wait=%.2fs", state.attempt_number, wait_seconds)


# ---------------------------- Flattening utilities ---------------------------- #


def _get(d: Dict[str, Any], key: str) -> str:
    val = d.get(key, "")
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return json.dumps(val, separators=(",", ":"))
    return str(val)


def _compact_json(v: Any) -> str:
    if v in (None, ""):
        return ""
    try:
        return json.dumps(v, separators=(",", ":"))
    except Exception:
        return str(v)


def _extract_campaign_sending(lead: Dict[str, Any]) -> Tuple[str, str, str, str]:
    schedule = lead.get("campaign_sending_schedule") or {}
    tz = schedule.get("timezone") or lead.get("campaign_sending_tz") or ""
    days = schedule.get("days") or lead.get("campaign_sending_days") or []
    if isinstance(days, list):
        days_joined = ",".join(str(x) for x in days)
    else:
        days_joined = str(days) if days else ""
    start_hour = schedule.get("startHour") or lead.get("campaign_sending_startHour") or ""
    end_hour = schedule.get("endHour") or lead.get("campaign_sending_endHour") or ""
    return str(tz), str(days_joined), str(start_hour), str(end_hour)


def _extract_history_time(item: Dict[str, Any]) -> Any:
    for k in ("time", "sent_time", "reply_time", "history_time", "timestamp"):
        if k in item and item[k]:
            return item[k]
    return ""


def _strip_html_to_text(html: str) -> str:
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator=" ", strip=True)
    except Exception:
        return ""


def flatten_rows_for_lead(
    lead: Dict[str, Any],
    *,
    api_fetched_at_iso: str,
    sort_by: str,
    filters_json_compact: str,
    page_offset: int,
    page_limit: int,
) -> Iterable[Dict[str, Any]]:
    tz, days, start_hour, end_hour = _extract_campaign_sending(lead)

    base_row = {
        # Lead-level
        "lead_category_id": _get(lead, "lead_category_id"),
        "last_sent_time": _get(lead, "last_sent_time"),
        "last_reply_time": _get(lead, "last_reply_time"),
        "has_new_unread_email": _get(lead, "has_new_unread_email"),
        "email_account_id": _get(lead, "email_account_id"),
        "revenue": _get(lead, "revenue"),
        "is_pushed_to_sub_sequence": _get(lead, "is_pushed_to_sub_sequence"),
        "lead_first_name": _get(lead, "lead_first_name"),
        "lead_last_name": _get(lead, "lead_last_name"),
        "lead_email": _get(lead, "lead_email"),
        "email_lead_id": _get(lead, "email_lead_id"),
        "email_lead_map_id": _get(lead, "email_lead_map_id"),
        "lead_status": _get(lead, "lead_status"),
        "current_sequence_number": _get(lead, "current_sequence_number"),
        "sub_sequence_id": _get(lead, "sub_sequence_id"),
        "old_replaced_lead_data": _get(lead, "old_replaced_lead_data"),
        "lead_next_timestamp_to_send": _get(lead, "lead_next_timestamp_to_send"),
        "email_campaign_seq_id": _get(lead, "email_campaign_seq_id"),
        "is_important": _get(lead, "is_important"),
        "is_archived": _get(lead, "is_archived"),
        "is_snoozed": _get(lead, "is_snoozed"),
        "team_member_id": _get(lead, "team_member_id"),
        "is_ooo_automated_push_lead": _get(lead, "is_ooo_automated_push_lead"),
        "email_campaign_id": _get(lead, "email_campaign_id"),
        "email_campaign_name": _get(lead, "email_campaign_name"),
        "client_id": _get(lead, "client_id"),
        "belongs_to_sub_sequence": _get(lead, "belongs_to_sub_sequence"),
        "campaign_sending_tz": tz,
        "campaign_sending_days": days,
        "campaign_sending_startHour": start_hour,
        "campaign_sending_endHour": end_hour,
        "sequence_count": _get(lead, "sequence_count"),
    }

    email_history = lead.get("email_history") or lead.get("emailHistory") or []
    if not isinstance(email_history, list):
        email_history = []

    # sort ascending by time
    def sort_key(item: Dict[str, Any]) -> Tuple[int, str]:
        t = _extract_history_time(item)
        # try numeric
        try:
            return (0, f"{int(t):020d}")
        except Exception:
            return (1, str(t))

    email_history_sorted = sorted(email_history, key=sort_key)
    history_count = len(email_history_sorted)

    if history_count == 0:
        row = {
            **base_row,
            "history_stats_id": "",
            "history_from": "",
            "history_to": "",
            "history_type": "",
            "history_message_id": "",
            "history_time": "",
            "history_subject": "",
            "history_email_seq_number": "",
            "history_open_count": "",
            "history_click_count": "",
            "history_click_details_json": "",
            "history_cc_joined": "",
            "email_body_html": "",
            "email_body_text": "",
            "thread_key": _get(lead, "email_lead_map_id"),
            "thread_lead_id": _get(lead, "email_lead_id"),
            "history_index": "0",
            "history_count": "0",
            "api_fetched_at": api_fetched_at_iso,
            "source_endpoint": ENDPOINT,
            "api_sort_by": sort_by,
            "api_filters_json": filters_json_compact,
            "page_offset": str(page_offset),
            "page_limit": str(page_limit),
        }
        yield row
        return

    for idx, item in enumerate(email_history_sorted, start=1):
        body_html = (
            item.get("body")
            or item.get("htmlBody")
            or item.get("email_body_html")
            or item.get("emailBodyHtml")
            or ""
        )
        body_text = _strip_html_to_text(str(body_html))
        cc_val = item.get("cc") or item.get("cc_list") or []
        if isinstance(cc_val, list):
            cc_joined = ";".join(str(x) for x in cc_val)
        else:
            cc_joined = str(cc_val) if cc_val else ""

        click_details = item.get("click_details") or item.get("clickDetails") or ""
        row = {
            **base_row,
            "history_stats_id": _get(item, "stats_id") or _get(item, "id"),
            "history_from": _get(item, "from"),
            "history_to": _get(item, "to"),
            "history_type": _get(item, "type"),
            "history_message_id": _get(item, "message_id"),
            "history_time": str(_extract_history_time(item)),
            "history_subject": _get(item, "subject"),
            "history_email_seq_number": _get(item, "email_seq_number"),
            "history_open_count": _get(item, "open_count"),
            "history_click_count": _get(item, "click_count"),
            "history_click_details_json": _compact_json(click_details),
            "history_cc_joined": cc_joined,
            "email_body_html": str(body_html) if body_html is not None else "",
            "email_body_text": body_text,
            "thread_key": _get(lead, "email_lead_map_id"),
            "thread_lead_id": _get(lead, "email_lead_id"),
            "history_index": str(idx),
            "history_count": str(history_count),
            "api_fetched_at": api_fetched_at_iso,
            "source_endpoint": ENDPOINT,
            "api_sort_by": sort_by,
            "api_filters_json": filters_json_compact,
            "page_offset": str(page_offset),
            "page_limit": str(page_limit),
        }
        yield row


# ------------------------------ Pipeline runner ------------------------------ #


class GracefulExit(SystemExit):
    pass


def _install_signal_handlers(loop: asyncio.AbstractEventLoop):
    def _handler():
        raise GracefulExit(1)

    try:
        loop.add_signal_handler(signal.SIGINT, _handler)
        loop.add_signal_handler(signal.SIGTERM, _handler)
    except NotImplementedError:
        pass


async def producer(
    *,
    limit: int,
    max_pages: Optional[int],
    offsets_q: asyncio.Queue[Optional[int]],
    found_tail_event: asyncio.Event,
    tail_offset_holder: Dict[str, int],
    num_workers: int,
) -> None:
    offset = 0
    produced = 0
    while True:
        if max_pages is not None and produced >= max_pages:
            break
        if found_tail_event.is_set() and offset > tail_offset_holder.get("tail_offset", 0):
            break
        await offsets_q.put(offset)
        produced += 1
        offset += limit

    # signal workers to stop
    for _ in range(num_workers):
        await offsets_q.put(None)


async def worker(
    *,
    client: httpx.AsyncClient,
    api_key: str,
    rps: RPSController,
    stats: Stats,
    logger: logging.Logger,
    limit: int,
    sort_by: str,
    filters: Optional[Dict[str, Any]],
    max_retries: int,
    offsets_q: asyncio.Queue[Optional[int]],
    rows_q: asyncio.Queue[Optional[Dict[str, Any]]],
    found_tail_event: asyncio.Event,
    tail_offset_holder: Dict[str, int],
    api_fetched_at_iso: str,
) -> None:
    filters_compact = json.dumps(filters or {}, separators=(",", ":"), sort_keys=True)
    while True:
        offset = await offsets_q.get()
        if offset is None:
            offsets_q.task_done()
            return
        try:

            page_json = await fetch_page(
                client=client,
                api_key=api_key,
                rps=rps,
                stats=stats,
                logger=logger,
                limit=limit,
                offset=offset,
                sort_by=sort_by,
                filters=filters,
                max_retries=max_retries,
            )

            leads = (
                page_json.get("data")
                or page_json.get("leads")
                or page_json.get("results")
                or page_json.get("items")
                or page_json
            )
            if isinstance(leads, dict):
                # if response wraps leads in a known key
                for k in ("data", "leads", "results", "items"):
                    if isinstance(leads.get(k), list):
                        leads = leads[k]
                        break
            if not isinstance(leads, list):
                leads = []

            stats.pages_fetched += 1
            stats.leads_processed += len(leads)

            if len(leads) < limit:
                tail_offset_holder["tail_offset"] = offset
                found_tail_event.set()

            for lead in leads:
                for row in flatten_rows_for_lead(
                    lead,
                    api_fetched_at_iso=api_fetched_at_iso,
                    sort_by=sort_by,
                    filters_json_compact=filters_compact,
                    page_offset=offset,
                    page_limit=limit,
                ):
                    await rows_q.put(row)
        finally:
            offsets_q.task_done()


async def writer(
    *,
    rows_q: asyncio.Queue[Optional[Dict[str, Any]]],
    output_path: Optional[Path],
    stats: Stats,
    progress: Progress,
    rows_task: TaskID,
    dry_run: bool,
) -> None:
    csv_file = None
    csv_writer: Optional[csv.DictWriter] = None
    try:
        if not dry_run and output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            # Use BOM + CRLF on Windows for best Excel compatibility
            is_windows = platform.system() == "Windows"
            file_encoding = "utf-8-sig" if is_windows else "utf-8"
            line_ending = "\r\n" if is_windows else "\n"
            csv_file = open(output_path, "w", encoding=file_encoding, newline="")
            csv_writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS, lineterminator=line_ending, quoting=csv.QUOTE_ALL)
            csv_writer.writeheader()

        while True:
            item = await rows_q.get()
            try:
                if item is None:
                    rows_q.task_done()
                    return
                if csv_writer is not None:
                    # Ensure all headers exist
                    row = {h: item.get(h, "") for h in CSV_HEADERS}
                    csv_writer.writerow(row)
                stats.rows_written += 1
                progress.update(rows_task, advance=1)
            finally:
                rows_q.task_done()
    finally:
        if csv_file is not None:
            csv_file.flush()
            csv_file.close()


async def run_async(args: argparse.Namespace) -> int:
    console = Console()

    # Configure logging
    log_level = getattr(logging, args.log_level, logging.INFO)
    logging.basicConfig(level=log_level, format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("smartlead_export")

    api_key = os.environ.get("SMARTLEAD_API_KEY")
    if not api_key:
        logger.error("SMARTLEAD_API_KEY environment variable is required")
        return 2

    if not (1 <= args.limit_per_page <= 20):
        logger.error("--limit-per-page must be between 1 and 20")
        return 2

    effective_filters = load_effective_filters(args)
    api_fetched_at_iso = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

    output_path = Path(args.out_path) if args.out_path else build_default_output_path()
    if args.dry_run:
        logger.info("Dry-run: not writing CSV. Output path would be: %s", output_path)

    rps = RPSController(args.rps)
    stats = Stats()

    # Queues
    offsets_q: asyncio.Queue[Optional[int]] = asyncio.Queue(maxsize=args.max_concurrency * 5)
    rows_q: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue(maxsize=args.max_concurrency * 100)
    found_tail_event = asyncio.Event()
    tail_offset_holder: Dict[str, int] = {"tail_offset": 0}

    timeout = httpx.Timeout(30.0, connect=10.0)
    start_time_monotonic = asyncio.get_running_loop().time()
    async with httpx.AsyncClient(timeout=timeout) as client:
        progress = Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            TextColumn("ETA"),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        )
        with progress:
            pages_task = progress.add_task("Pages fetched", total=None)
            leads_task = progress.add_task("Leads processed", total=None)
            rows_task = progress.add_task("Rows written", total=None)

            # background task to mirror counters to progress bars
            async def progress_refresher():
                while True:
                    progress.update(pages_task, completed=stats.pages_fetched)
                    progress.update(leads_task, completed=stats.leads_processed)
                    progress.update(rows_task, completed=stats.rows_written)
                    await asyncio.sleep(0.25)

            refresher_task = asyncio.create_task(progress_refresher())

            writer_task = asyncio.create_task(
                writer(
                    rows_q=rows_q,
                    output_path=None if args.dry_run else output_path,
                    stats=stats,
                    progress=progress,
                    rows_task=rows_task,
                    dry_run=bool(args.dry_run),
                )
            )

            producer_task = asyncio.create_task(
                producer(
                    limit=args.limit_per_page,
                    max_pages=args.max_pages,
                    offsets_q=offsets_q,
                    found_tail_event=found_tail_event,
                    tail_offset_holder=tail_offset_holder,
                    num_workers=args.max_concurrency,
                )
            )

            workers = [
                asyncio.create_task(
                    worker(
                        client=client,
                        api_key=api_key,
                        rps=rps,
                        stats=stats,
                        logger=logger,
                        limit=args.limit_per_page,
                        sort_by=args.sort_by,
                        filters=effective_filters,
                        max_retries=args.max_retries,
                        offsets_q=offsets_q,
                        rows_q=rows_q,
                        found_tail_event=found_tail_event,
                        tail_offset_holder=tail_offset_holder,
                        api_fetched_at_iso=api_fetched_at_iso,
                    )
                )
                for _ in range(args.max_concurrency)
            ]

            try:
                await producer_task
                await offsets_q.join()  # wait for all offsets to be processed
                # workers should exit after consuming sentinel Nones
                await asyncio.gather(*workers, return_exceptions=True)
                # stop writer
                await rows_q.put(None)
                await writer_task
            except GracefulExit:
                logger.warning("Received termination signal, shutting down gracefully...")
                for w in workers:
                    w.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
                await rows_q.put(None)
                await writer_task
                refresher_task.cancel()
            finally:
                refresher_task.cancel()

    stats.rate_limit_sleeps = rps.rate_limit_sleeps

    # final summary
    elapsed_seconds = asyncio.get_running_loop().time() - start_time_monotonic
    table = Table(title="Smartlead Inbox Export Summary")
    table.add_column("Metric")
    table.add_column("Value")
    for k, v in [
        ("Elapsed", f"{elapsed_seconds:.2f}s"),
        ("Pages", stats.pages_fetched),
        ("Leads", stats.leads_processed),
        ("Rows", stats.rows_written),
        ("HTTP Requests", stats.requests),
        ("Retries", stats.retries),
        ("429s", stats.http_429s),
        ("Rate-limit sleeps", stats.rate_limit_sleeps),
        ("Output", str(output_path if not args.dry_run else "(dry-run)")),
    ]:
        table.add_row(k, str(v))
    console.print(table)

    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)
    try:
        return loop.run_until_complete(run_async(args))
    finally:
        loop.close()


if __name__ == "__main__":
    # For Python 3.8 compatibility, use asyncio.run
    sys.exit(main())

