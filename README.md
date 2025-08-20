Smartlead Master Inbox Replies → Clean CSV Exporter

What it does
This tool asynchronously fetches all Smartlead Master Inbox replies via the /inbox-replies endpoint (with fetch_message_history=true), flattens each lead's email_history into tidy rows, and streams them to a CSV. It is designed for large inboxes with: global rate limiting (default 5 RPS), exponential backoff retries, and rich progress reporting.

Setup
1) Create and activate a virtual environment:
```bash
python -m venv .venv && source .venv/bin/activate
```
2) Install dependencies:
```bash
pip install -r requirements.txt
```
3) Set your API key:
```bash
export SMARTLEAD_API_KEY=YOUR_API_KEY
```

Usage
Default (OOO only):
```bash
python export_smartlead_inbox_to_csv.py
```

Disable defaults (fetch all):
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

CLI Options
- --out PATH (default: ./exports/smartlead_inbox_export_<YYYYMMDD_HHMMSS>.csv)
- --limit-per-page INT (1–20; default 20)
- --sort-by {REPLY_TIME_DESC,SENT_TIME_DESC} (default REPLY_TIME_DESC)
- --filters-json PATH (optional; overrides defaults)
- --no-default-filters (drop default OOO filter)
- --reply-start ISO8601, --reply-end ISO8601 (compose replyTimeBetween)
- --campaign-id INT (repeatable)
- --email-account-id INT (repeatable)
- --max-concurrency INT (default 10)
- --rps FLOAT (default 5.0)
- --max-retries INT (default 6)
- --log-level {DEBUG,INFO,WARNING,ERROR} (default INFO)
- --dry-run (don’t write CSV; just count)
- --max-pages INT (hidden/dev)

CSV Schema
One row per email in the thread, sorted ascending by time, with lead-level fields duplicated on each row. Also includes both HTML and text bodies. See implementation headers in export_smartlead_inbox_to_csv.py for exact order.

Notes
- Global rate limit is enforced across all workers. Increase --max-concurrency to improve parallelism while keeping the same RPS.
- Retries use exponential backoff with jitter and honor Retry-After when present.
- Progress bars show pages fetched, leads processed, and rows written. A final summary table prints key metrics.
- Ctrl+C is handled gracefully; in-flight tasks are stopped and partial results are preserved.

Run as a module
```bash
python -m export_smartlead_inbox_to_csv --dry-run
```

License
MIT
