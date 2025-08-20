#!/usr/bin/env python3
"""
Smartlead Master Inbox Replies → Clean CSV Exporter

Features:
- Async pagination over /inbox-replies with fetch_message_history=true
- Global RPS limiter (default 5 RPS) with precise spacing
- Exponential backoff retries with jitter for 429/5xx/network errors
- Respects Retry-After when present
- Concurrent workers (default 10), single CSV writer streaming rows
- Progress bars for pages, leads, rows; final summary table
- CLI options to tune filters, concurrency, RPS, retries, output path, etc.

Python: 3.11+
Dependencies: httpx, tenacity, rich, beautifulsoup4
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import signal
import sys
from collections import deque
from contextlib import asynccontextmanager
import contextlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

import httpx
from bs4 import BeautifulSoup
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TaskID, TextColumn, TimeElapsedColumn
from rich.table import Table
from tenacity import RetryCallState, AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_random_exponential


SMARTLEAD_BASE_URL = "https://server.smartlead.ai/api/v1/master-inbox"
INBOX_REPLIES_ENDPOINT = "/inbox-replies"


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
    # Email-history
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


class ExporterError(Exception):
    """Base error for exporter."""


class HTTPRetryableError(ExporterError):
    """Error that indicates the request should be retried."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class RPSController:
    """Global RPS controller using precise request spacing.

    Ensures that across all workers, requests are spaced by at least
    1 / rps seconds. This is strict and simple, and provides a global
    rate limit regardless of local concurrency.
    """

    def __init__(self, rps: float):
        if rps <= 0:
            raise ValueError("RPS must be > 0")
        self.min_interval = 1.0 / rps
        self._lock = asyncio.Lock()
        self._next_available_time = 0.0
        self.total_sleep_seconds = 0.0
        self.sleep_events = 0

    async def acquire(self) -> None:
        now = asyncio.get_running_loop().time()
        async with self._lock:
            now = asyncio.get_running_loop().time()
            wait_for = self._next_available_time - now
            if wait_for > 0:
                self.total_sleep_seconds += wait_for
                self.sleep_events += 1
                await asyncio.sleep(wait_for)
                now = asyncio.get_running_loop().time()
            # Advance next available slot
            self._next_available_time = max(self._next_available_time, now) + self.min_interval


@dataclass
class Metrics:
    requests_made: int = 0
    retries: int = 0
    rate_limit_429: int = 0


def _compact_json(data: Any) -> str:
    try:
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        return ""


def _parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    if not header_value:
        return None
    # Retry-After can be seconds or HTTP date
    try:
        secs = float(header_value)
        if secs < 0:
            return None
        return secs
    except Exception:
        pass
    try:
        dt = datetime.strptime(header_value, "%a, %d %b %Y %H:%M:%S %Z")
        return max(0.0, (dt.replace(tzinfo=timezone.utc) - datetime.now(timezone.utc)).total_seconds())
    except Exception:
        return None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _strip_html_to_text(html: str) -> str:
    try:
        soup = BeautifulSoup(html or "", "html.parser")
        return soup.get_text(separator=" ", strip=True)
    except Exception:
        return ""


def _safe_get(d: Dict[str, Any], key: str) -> str:
    val = d.get(key, "")
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return _compact_json(val)
    return str(val)


def _extract_history_time(item: Dict[str, Any]) -> str:
    # Try common keys
    for k in (
        "history_time",
        "time",
        "timestamp",
        "date",
        "reply_time",
        "sent_time",
        "created_at",
        "createdAt",
    ):
        v = item.get(k)
        if v:
            return str(v)
    return ""


def _extract_email_body_html(item: Dict[str, Any]) -> str:
    for k in (
        "email_body_html",
        "email_body",
        "html_body",
        "body_html",
        "body",
        "message_body",
        "reply_body",
        "content_html",
    ):
        v = item.get(k)
        if v:
            return str(v)
    return ""


def _join_list(values: Any, sep: str = ",") -> str:
    if isinstance(values, list):
        return sep.join(str(x) for x in values)
    return ""


def _campaign_sending_fields(lead: Dict[str, Any]) -> tuple[str, str, str, str]:
    tz = _safe_get(lead, "campaign_sending_tz")
    days = ""
    start_hour = _safe_get(lead, "campaign_sending_startHour")
    end_hour = _safe_get(lead, "campaign_sending_endHour")

    # Try nested schedule if present
    sched = lead.get("campaign_sending_schedule") or lead.get("sending_schedule")
    if isinstance(sched, dict):
        days_val = sched.get("days") or sched.get("sending_days")
        if isinstance(days_val, list):
            days = ",".join(str(d) for d in days_val)
        start_hour = start_hour or str(sched.get("startHour", ""))
        end_hour = end_hour or str(sched.get("endHour", ""))
        tz = tz or str(sched.get("tz", ""))
    return tz, days, start_hour, end_hour


def flatten_rows_for_lead(
    lead: Dict[str, Any],
    *,
    api_context: Dict[str, Any],
) -> Iterable[Dict[str, str]]:
    history = lead.get("email_history") or lead.get("emailHistory") or []
    fetched_at = api_context.get("api_fetched_at", _iso_now())

    tz, days, start_hour, end_hour = _campaign_sending_fields(lead)

    base_row: Dict[str, str] = {
        # Lead-level
        "lead_category_id": _safe_get(lead, "lead_category_id"),
        "last_sent_time": _safe_get(lead, "last_sent_time"),
        "last_reply_time": _safe_get(lead, "last_reply_time"),
        "has_new_unread_email": _safe_get(lead, "has_new_unread_email"),
        "email_account_id": _safe_get(lead, "email_account_id"),
        "revenue": _safe_get(lead, "revenue"),
        "is_pushed_to_sub_sequence": _safe_get(lead, "is_pushed_to_sub_sequence"),
        "lead_first_name": _safe_get(lead, "lead_first_name"),
        "lead_last_name": _safe_get(lead, "lead_last_name"),
        "lead_email": _safe_get(lead, "lead_email"),
        "email_lead_id": _safe_get(lead, "email_lead_id"),
        "email_lead_map_id": _safe_get(lead, "email_lead_map_id"),
        "lead_status": _safe_get(lead, "lead_status"),
        "current_sequence_number": _safe_get(lead, "current_sequence_number"),
        "sub_sequence_id": _safe_get(lead, "sub_sequence_id"),
        "old_replaced_lead_data": _safe_get(lead, "old_replaced_lead_data"),
        "lead_next_timestamp_to_send": _safe_get(lead, "lead_next_timestamp_to_send"),
        "email_campaign_seq_id": _safe_get(lead, "email_campaign_seq_id"),
        "is_important": _safe_get(lead, "is_important"),
        "is_archived": _safe_get(lead, "is_archived"),
        "is_snoozed": _safe_get(lead, "is_snoozed"),
        "team_member_id": _safe_get(lead, "team_member_id"),
        "is_ooo_automated_push_lead": _safe_get(lead, "is_ooo_automated_push_lead"),
        "email_campaign_id": _safe_get(lead, "email_campaign_id"),
        "email_campaign_name": _safe_get(lead, "email_campaign_name"),
        "client_id": _safe_get(lead, "client_id"),
        "belongs_to_sub_sequence": _safe_get(lead, "belongs_to_sub_sequence"),
        "campaign_sending_tz": tz,
        "campaign_sending_days": days,
        "campaign_sending_startHour": start_hour,
        "campaign_sending_endHour": end_hour,
        "sequence_count": _safe_get(lead, "sequence_count"),
        # Derived
        "thread_key": _safe_get(lead, "email_lead_map_id"),
        "thread_lead_id": _safe_get(lead, "email_lead_id"),
        "api_fetched_at": str(fetched_at),
        "source_endpoint": "/inbox-replies",
        "api_sort_by": str(api_context.get("api_sort_by", "")),
        "api_filters_json": str(api_context.get("api_filters_json", "")),
        "page_offset": str(api_context.get("page_offset", "")),
        "page_limit": str(api_context.get("page_limit", "")),
    }

    if not history:
        row = base_row.copy()
        row.update(
            {
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
                "history_index": "0",
                "history_count": "0",
            }
        )
        yield row
        return

    # Sort by time ascending if possible
    def _sort_key(it: Dict[str, Any]) -> str:
        return _extract_history_time(it)

    sorted_history = sorted(history, key=_sort_key)
    total = len(sorted_history)

    for idx, item in enumerate(sorted_history, start=1):
        body_html = _extract_email_body_html(item)
        row = base_row.copy()
        row.update(
            {
                "history_stats_id": _safe_get(item, "history_stats_id") or _safe_get(item, "stats_id"),
                "history_from": _safe_get(item, "history_from") or _safe_get(item, "from"),
                "history_to": _safe_get(item, "history_to") or _safe_get(item, "to"),
                "history_type": _safe_get(item, "history_type") or _safe_get(item, "type"),
                "history_message_id": _safe_get(item, "history_message_id") or _safe_get(item, "message_id"),
                "history_time": _extract_history_time(item),
                "history_subject": _safe_get(item, "history_subject") or _safe_get(item, "subject"),
                "history_email_seq_number": _safe_get(item, "history_email_seq_number") or _safe_get(item, "email_seq_number"),
                "history_open_count": _safe_get(item, "history_open_count") or _safe_get(item, "open_count"),
                "history_click_count": _safe_get(item, "history_click_count") or _safe_get(item, "click_count"),
                "history_click_details_json": _compact_json(item.get("history_click_details") or item.get("click_details") or item.get("clicks")),
                "history_cc_joined": _join_list(item.get("cc") or item.get("history_cc"), sep=";"),
                "email_body_html": body_html,
                "email_body_text": _strip_html_to_text(body_html),
                "history_index": str(idx),
                "history_count": str(total),
            }
        )
        yield row


def _build_default_filters() -> Dict[str, Any]:
    return {"leadCategories": {"categoryIdsIn": [6]}}


def _merge_convenience_filters(
    base_filters: Dict[str, Any],
    *,
    reply_start: Optional[str],
    reply_end: Optional[str],
    campaign_ids: List[int],
    email_account_ids: List[int],
) -> Dict[str, Any]:
    filters = dict(base_filters) if base_filters else {}
    if reply_start or reply_end:
        filters["replyTimeBetween"] = {"from": reply_start or "", "to": reply_end or ""}
    if campaign_ids:
        filters["emailCampaignIdIn"] = campaign_ids
    if email_account_ids:
        filters["emailAccountIdIn"] = email_account_ids
    return filters


def _output_path_default() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("./exports") / f"smartlead_inbox_export_{ts}.csv"


def _validate_limit_per_page(value: int) -> int:
    if not (1 <= value <= 20):
        raise argparse.ArgumentTypeError("--limit-per-page must be between 1 and 20")
    return value


def _validate_sort_by(value: str) -> str:
    allowed = {"REPLY_TIME_DESC", "SENT_TIME_DESC"}
    if value not in allowed:
        raise argparse.ArgumentTypeError(f"--sort-by must be one of {allowed}")
    return value


def _positive_float(value: str) -> float:
    try:
        v = float(value)
    except Exception:
        raise argparse.ArgumentTypeError("must be a float")
    if v <= 0:
        raise argparse.ArgumentTypeError("must be > 0")
    return v


def _non_negative_int(value: str) -> int:
    try:
        v = int(value)
    except Exception:
        raise argparse.ArgumentTypeError("must be an integer")
    if v < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return v


def _load_filters_json(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _env_api_key() -> str:
    api_key = os.environ.get("SMARTLEAD_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("SMARTLEAD_API_KEY environment variable is required")
    return api_key


def _make_params(api_key: str) -> Dict[str, Any]:
    return {"api_key": api_key, "fetch_message_history": "true"}


def _http_client() -> httpx.AsyncClient:
    timeout = httpx.Timeout(connect=30.0, read=60.0, write=30.0, pool=60.0)
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=60.0)
    return httpx.AsyncClient(base_url=SMARTLEAD_BASE_URL, timeout=timeout, limits=limits)


def _tenacity_before_sleep(retry_state: RetryCallState) -> None:
    attempt = retry_state.attempt_number
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    if exc is not None:
        console = Console(stderr=True)
        console.log(f"Retrying after attempt {attempt} due to: {exc}")


async def _respect_retry_after(response: httpx.Response) -> None:
    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
    if retry_after is not None and retry_after > 0:
        await asyncio.sleep(retry_after)


def _is_retryable_status(status: int) -> bool:
    return status == 429 or status >= 500


def _normalize_leads_payload(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    # Some APIs might nest under 'leads'
    leads = payload.get("leads")
    if isinstance(leads, list):
        return [x for x in leads if isinstance(x, dict)]
    return []


def _compact_filters_for_api(filters: Optional[Dict[str, Any]]) -> str:
    return _compact_json(filters or {})


async def _fetch_page_once(
    *,
    client: httpx.AsyncClient,
    rps: RPSController,
    params: Dict[str, Any],
    offset: int,
    limit: int,
    sort_by: str,
    filters: Dict[str, Any],
    metrics: Metrics,
) -> List[Dict[str, Any]]:
    await rps.acquire()
    metrics.requests_made += 1

    try:
        response = await client.post(
            INBOX_REPLIES_ENDPOINT,
            params=params,
            json={
                "offset": offset,
                "limit": limit,
                "sortBy": sort_by,
                "filters": filters or {},
            },
        )
    except httpx.HTTPError as e:
        raise HTTPRetryableError(f"Network error: {e}") from e

    if _is_retryable_status(response.status_code):
        if response.status_code == 429:
            metrics.rate_limit_429 += 1
        await _respect_retry_after(response)
        raise HTTPRetryableError(f"HTTP {response.status_code}", status_code=response.status_code)

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        # Non-retryable client error
        raise ExporterError(f"HTTP error: {e}") from e

    try:
        payload = response.json()
    except Exception as e:
        raise ExporterError(f"Failed to parse JSON: {e}") from e

    return _normalize_leads_payload(payload)


async def fetch_page(
    *,
    client: httpx.AsyncClient,
    rps: RPSController,
    params: Dict[str, Any],
    offset: int,
    limit: int,
    sort_by: str,
    filters: Dict[str, Any],
    metrics: Metrics,
    max_retries: int,
) -> List[Dict[str, Any]]:
    attempts = max(1, max_retries)

    def _before_sleep(rs: RetryCallState) -> None:
        metrics.retries += 1
        _tenacity_before_sleep(rs)

    async for attempt in AsyncRetrying(
        retry=retry_if_exception_type(HTTPRetryableError),
        wait=wait_random_exponential(multiplier=0.5, max=30.0),
        stop=stop_after_attempt(attempts),
        reraise=True,
        before_sleep=_before_sleep,
    ):
        with attempt:
            return await _fetch_page_once(
                client=client,
                rps=rps,
                params=params,
                offset=offset,
                limit=limit,
                sort_by=sort_by,
                filters=filters,
                metrics=metrics,
            )
    return []


@asynccontextmanager
async def _csv_writer(path: Path, headers: List[str]) -> AsyncIterator[csv.DictWriter | None]:
    if path is None:
        yield None
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    # newline='' is important for csv
    f = path.open("w", encoding="utf-8", newline="")
    try:
        writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_ALL, lineterminator="\n")
        writer.writeheader()
        yield writer
    finally:
        f.flush()
        f.close()


async def producer(
    *,
    page_queue: asyncio.Queue[int],
    limit: int,
    prefetch: int,
    tail_event: asyncio.Event,
    max_pages: Optional[int],
    console: Console,
) -> None:
    offset = 0
    pages_enqueued = 0
    try:
        while True:
            if tail_event.is_set():
                break
            # Keep a small buffer to avoid overrun after tail
            while page_queue.qsize() < prefetch and (max_pages is None or pages_enqueued < max_pages):
                await page_queue.put(offset)
                offset += limit
                pages_enqueued += 1
            if max_pages is not None and pages_enqueued >= max_pages:
                break
            await asyncio.sleep(0.05)
    except asyncio.CancelledError:
        console.log("Producer cancelled")
        raise


async def worker(
    *,
    worker_id: int,
    page_queue: asyncio.Queue[int],
    row_queue: asyncio.Queue[Dict[str, str]],
    client: httpx.AsyncClient,
    rps: RPSController,
    params: Dict[str, Any],
    limit: int,
    sort_by: str,
    filters: Dict[str, Any],
    tail_event: asyncio.Event,
    metrics: Metrics,
    max_retries: int,
    progress_pages: Progress,
    pages_task: TaskID,
    progress_leads: Progress,
    leads_task: TaskID,
) -> None:
    console = Console(stderr=True)
    while True:
        try:
            offset = await page_queue.get()
        except asyncio.CancelledError:
            break
        if offset is None:  # Sentinel
            page_queue.task_done()
            break
        try:
            leads = await fetch_page(
                client=client,
                rps=rps,
                params=params,
                offset=offset,
                limit=limit,
                sort_by=sort_by,
                filters=filters,
                metrics=metrics,
                max_retries=max_retries,
            )
            # Note: we can't pass args into worker signature easily; adjust by using a closure or set attribute.
            progress_pages.advance(pages_task, 1)
            if len(leads) < limit:
                tail_event.set()
            progress_leads.advance(leads_task, len(leads))

            api_context = {
                "api_fetched_at": _iso_now(),
                "api_sort_by": sort_by,
                "api_filters_json": _compact_filters_for_api(filters),
                "page_offset": offset,
                "page_limit": limit,
            }
            for lead in leads:
                for row in flatten_rows_for_lead(lead, api_context=api_context):
                    await row_queue.put(row)
        except HTTPRetryableError:
            metrics.retries += 1
            # Re-queue the same offset to retry later via producer flow.
            # However, because tenacity already retried within fetch_page,
            # at this point it actually means we exhausted retries; propagate.
            console.log(f"Worker {worker_id}: exhausted retries for offset={offset}")
            tail_event.set()
            # Do not requeue; break to stop worker
            break
        except ExporterError as e:
            console.log(f"Worker {worker_id}: fatal error at offset={offset}: {e}")
            tail_event.set()
            break
        finally:
            page_queue.task_done()


async def writer_consumer(
    *,
    row_queue: asyncio.Queue[Dict[str, str]],
    writer: Optional[csv.DictWriter],
    progress_rows: Progress,
    rows_task: TaskID,
    dry_run: bool,
) -> int:
    rows_written = 0
    while True:
        row = await row_queue.get()
        if row is None:  # Sentinel
            row_queue.task_done()
            break
        if not dry_run:
            try:
                writer and writer.writerow(row)
            except Exception:
                # Skip malformed row but continue
                pass
        rows_written += 1
        progress_rows.advance(rows_task, 1)
        row_queue.task_done()
    return rows_written


async def run_async(args: argparse.Namespace) -> int:
    console = Console()

    api_key = _env_api_key()
    params = _make_params(api_key)

    # Build filters
    if args.filters_json:
        effective_filters = _load_filters_json(args.filters_json) or {}
    else:
        effective_filters = {} if args.no_default_filters else _merge_convenience_filters(_build_default_filters(), reply_start=None, reply_end=None, campaign_ids=[], email_account_ids=[])
    # Apply convenience flags (compose into effective_filters)
    effective_filters = _merge_convenience_filters(
        effective_filters,
        reply_start=args.reply_start,
        reply_end=args.reply_end,
        campaign_ids=args.campaign_id or [],
        email_account_ids=args.email_account_id or [],
    )

    out_path: Optional[Path] = None if args.dry_run else Path(args.out)

    rps = RPSController(args.rps)
    metrics = Metrics()

    page_queue: asyncio.Queue[int] = asyncio.Queue()
    row_queue: asyncio.Queue[Dict[str, str]] = asyncio.Queue(maxsize=1000)
    tail_event = asyncio.Event()

    # Progress bars
    progress_columns = (
        TextColumn("{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
    )
    progress_pages = Progress(*progress_columns, console=console)
    progress_leads = Progress(*progress_columns, console=console)
    progress_rows = Progress(*progress_columns, console=console)

    total_pages_unknown = 0
    pages_task = progress_pages.add_task("Pages fetched", total=None)
    leads_task = progress_leads.add_task("Leads processed", total=None)
    rows_task = progress_rows.add_task("Rows written", total=None)

    async with _http_client() as client:
        with progress_pages, progress_leads, progress_rows:
            @asynccontextmanager
            async def _null_writer_cm():
                yield None

            writer_cm = _csv_writer(out_path, CSV_HEADERS) if not args.dry_run else _null_writer_cm()
            async with writer_cm as writer:
                # Start background tasks
                prod_task = asyncio.create_task(
                    producer(
                        page_queue=page_queue,
                        limit=args.limit_per_page,
                        prefetch=max(1, args.max_concurrency * 2),
                        tail_event=tail_event,
                        max_pages=args.max_pages,
                        console=console,
                    )
                )

                worker_tasks = [
                    asyncio.create_task(
                        worker(
                            worker_id=i + 1,
                            page_queue=page_queue,
                            row_queue=row_queue,
                            client=client,
                            rps=rps,
                            params=params,
                            limit=args.limit_per_page,
                            sort_by=args.sort_by,
                            filters=effective_filters,
                            tail_event=tail_event,
                            metrics=metrics,
                            max_retries=args.max_retries,
                            progress_pages=progress_pages,
                            pages_task=pages_task,
                            progress_leads=progress_leads,
                            leads_task=leads_task,
                        )
                    )
                    for i in range(args.max_concurrency)
                ]

                writer_task = asyncio.create_task(
                    writer_consumer(
                        row_queue=row_queue,
                        writer=writer,
                        progress_rows=progress_rows,
                        rows_task=rows_task,
                        dry_run=args.dry_run,
                    )
                )

                # Graceful shutdown on SIGINT
                stop_event = asyncio.Event()

                def _handle_sigint():
                    stop_event.set()

                try:
                    loop = asyncio.get_running_loop()
                    loop.add_signal_handler(signal.SIGINT, _handle_sigint)
                except NotImplementedError:
                    pass

                try:
                    # Wait for tail or stop
                    tail_wait = asyncio.create_task(tail_event.wait())
                    stop_wait = asyncio.create_task(stop_event.wait())
                    done, pending = await asyncio.wait({tail_wait, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
                    for p in pending:
                        p.cancel()
                except asyncio.CancelledError:
                    pass

                # Stop producing new pages
                if not prod_task.done():
                    prod_task.cancel()
                    with contextlib.suppress(Exception):  # type: ignore
                        await prod_task

                # Drain the page queue and signal workers to finish
                await page_queue.join()
                for _ in worker_tasks:
                    await page_queue.put(None)  # type: ignore
                await asyncio.gather(*worker_tasks, return_exceptions=True)

                # Signal writer to finish
                await row_queue.put(None)  # type: ignore
                rows_written = await writer_task

    # Summary
    table = Table(title="Export Summary")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    rows_done = int(progress_rows.get_task(rows_task).completed or 0)
    leads_done = int(progress_leads.get_task(leads_task).completed or 0)
    pages_done = int(progress_pages.get_task(pages_task).completed or 0)
    table.add_row("Rows", str(rows_done))
    table.add_row("Leads", str(leads_done))
    table.add_row("Pages", str(pages_done))
    table.add_row("Requests", str(metrics.requests_made))
    table.add_row("Retries", str(metrics.retries))
    table.add_row("429 Hits", str(metrics.rate_limit_429))
    table.add_row("Rate-limit sleeps", f"{rps.sleep_events} ({rps.total_sleep_seconds:.2f}s)")
    if out_path:
        table.add_row("Output", str(out_path))
    console.print(table)

    return 0


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Smartlead Master Inbox replies to CSV (streaming, async)")
    parser.add_argument("--out", default=str(_output_path_default()), help="Output CSV path (default: ./exports/smartlead_inbox_export_<timestamp>.csv)")
    parser.add_argument("--limit-per-page", type=_validate_limit_per_page, default=20, help="Page size 1-20 (default 20)")
    parser.add_argument("--sort-by", type=_validate_sort_by, default="REPLY_TIME_DESC", choices=["REPLY_TIME_DESC", "SENT_TIME_DESC"], help="Sort by (default REPLY_TIME_DESC)")
    parser.add_argument("--filters-json", default=None, help="Path to JSON file with API filters (overrides defaults)")
    parser.add_argument("--no-default-filters", action="store_true", help="Disable default OOO filters (fetch everything)")
    parser.add_argument("--reply-start", default=None, help="ISO8601 start for replyTimeBetween")
    parser.add_argument("--reply-end", default=None, help="ISO8601 end for replyTimeBetween")
    parser.add_argument("--campaign-id", type=int, action="append", help="Campaign ID filter (repeatable)")
    parser.add_argument("--email-account-id", type=int, action="append", help="Email account ID filter (repeatable)")
    parser.add_argument("--max-concurrency", type=_non_negative_int, default=10, help="Max async workers (default 10)")
    parser.add_argument("--rps", type=_positive_float, default=5.0, help="Global requests per second (default 5.0)")
    parser.add_argument("--max-retries", type=_non_negative_int, default=6, help="Max retries on retryable errors (default 6)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Log level")
    parser.add_argument("--dry-run", action="store_true", help="Do not write CSV; only count")
    parser.add_argument("--max-pages", type=int, default=None, help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    # Allow overriding max retries in tenacity via env/arg by monkeypatching decorator? We used a constant in decorator.
    # For simplicity, keep decorator default of 6 attempts; align with CLI default.
    try:
        return asyncio.run(run_async(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())

