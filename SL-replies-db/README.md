## Smartlead Master Inbox Replies → Clean CSV Exporter

Async Python 3.11 tool to export all Smartlead Master Inbox replies as a tidy, analysis-ready CSV. It handles large inboxes with global rate limiting (default 5 RPS), exponential backoff retries, and streaming CSV writing with rich progress reporting.

### What it does
- Calls `POST /inbox-replies?fetch_message_history=true` and paginates through all results
- Flattens each lead's `email_history` so each email (SENT/REPLY) becomes a separate CSV row
- Duplicates lead-level fields on every row for easy analysis
- Includes both raw HTML body and a text-stripped version
- Enforces a global RPS limit across all workers with exponential backoff + Retry-After support
- Streams rows to CSV; no large in-memory buffers

### Setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export SMARTLEAD_API_KEY=your_api_key_here
```

### Usage

Default (OOO only):
```bash
python export_smartlead_inbox_to_csv.py
```

Disable defaults (fetch everything):
```bash
python export_smartlead_inbox_to_csv.py --no-default-filters
```

Custom filters from file:
```bash
python export_smartlead_inbox_to_csv.py --filters-json filters.json
```

Higher concurrency but same RPS (safe):
```bash
python export_smartlead_inbox_to_csv.py --max-concurrency 16 --rps 5
```

Other examples:
```bash
# limit per page 10, sort by sent time desc
python export_smartlead_inbox_to_csv.py --limit-per-page 10 --sort-by SENT_TIME_DESC

# filter by reply time
python export_smartlead_inbox_to_csv.py --reply-start 2024-01-01T00:00:00Z --reply-end 2024-12-31T23:59:59Z

# filter by campaign and email account (repeatable)
python export_smartlead_inbox_to_csv.py --campaign-id 123 --campaign-id 456 --email-account-id 789

# dry-run (no CSV written; just counts)
python export_smartlead_inbox_to_csv.py --dry-run
```

### Defaults and configuration
- API base: `https://server.smartlead.ai/api/v1/master-inbox`
- Endpoint: `POST /inbox-replies?api_key=API_KEY&fetch_message_history=true`
- Default filters (can be disabled): `{ "leadCategories": { "categoryIdsIn": [6] } }` (Out Of Office)
- Pagination: `limit=20` by default; `offset=0, 20, 40, ...` until a page returns fewer than `limit`
- Concurrency: `--max-concurrency 10` by default, governed by global RPS limiter
- Global rate limit: `--rps 5.0` (≤ 10 requests per 2s total across all workers)
- Retries: `--max-retries 6`, exponential backoff with full jitter, Retry-After honored
- Output: `./exports/smartlead_inbox_export_<YYYYMMDD_HHMMSS>.csv`

### CSV schema
One row per email in `email_history`, sorted by time ascending per lead. Missing fields are empty strings. Headers include duplicate lead-level fields, history fields, and derived metadata. See `export_smartlead_inbox_to_csv.py` (`CSV_HEADERS`).

### Notes on rate limiting and tuning
- You can safely increase `--max-concurrency`; the RPS controller ensures global throughput never exceeds the configured RPS.
- If you see frequent 429s, lower `--rps` a bit. The tool already backs off and honors `Retry-After`.

### Running as a module
```bash
python -m sl_replies_exporter
```

### License
MIT. See `LICENSE`.

