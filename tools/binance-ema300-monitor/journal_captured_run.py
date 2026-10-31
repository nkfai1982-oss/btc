"""Validate and synchronize the three portfolio CSVs after a monitor run.

The monitor owns signal and portfolio calculations. This helper copies only
validated per-strategy logs into the repository and, when pre-run snapshots are
provided, summarizes new rows for the existing change-only chat workflow.
"""
from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile

from binance_ema300_monitor import DAILY_INTERVAL_MS, INTERVAL_MS, utc_timestamp
from portfolio_csv import PORTFOLIO_FIELDS

BASE = Path("/workspace/binance-ema300-monitor")
REPO = Path("/workspace/btc/tools/binance-ema300-monitor")
LOGS = {
    "ema300-8h": "ema300_portfolio.csv",
    "donchian-8h-20-10": "donchian_20_10_portfolio.csv",
    "daily-ema6-24": "daily_ema6_24_portfolio.csv",
}


def read_log(path: Path, strategy: str) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Required portfolio CSV is missing: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != PORTFOLIO_FIELDS:
            raise ValueError(f"Unexpected portfolio CSV schema: {path}")
        rows = list(reader)
    if not rows or rows[0]["record_type"] != "STARTING_BALANCE":
        raise ValueError(f"Portfolio CSV has no starting-balance row: {path}")
    prior_time = None
    seen = set()
    for row in rows:
        if row["strategy"] != strategy or row["symbol"] != "BTCUSDT":
            raise ValueError(f"Unexpected strategy or market in {path}")
        key = (row["record_type"], row["candle_close_utc"])
        if key in seen:
            raise ValueError(f"Duplicate portfolio row in {path}: {key}")
        seen.add(key)
        if prior_time is not None and row["candle_close_utc"] <= prior_time:
            raise ValueError(f"Portfolio rows are out of order in {path}")
        prior_time = row["candle_close_utc"]
    return rows


def sync_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as temporary:
            temporary_name = temporary.name
            with source.open("rb") as input_file:
                shutil.copyfileobj(input_file, temporary)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, destination)
        temporary_name = None
    finally:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass


def report_row(row: dict[str, str]) -> dict[str, str]:
    result = dict(row)
    result["target"] = row["signal_target"]
    result["close_price_usdt"] = row["close_usdt"]
    for name_key, value_key in (("rule_metric_1_name", "rule_metric_1_value"),
                                ("rule_metric_2_name", "rule_metric_2_value")):
        if row[name_key]:
            result[row[name_key]] = row[value_key]
    return result


def main() -> int:
    run = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    rows_by_strategy = {}
    for strategy, filename in LOGS.items():
        source = BASE / filename
        rows = read_log(source, strategy)
        rows_by_strategy[strategy] = rows
        sync_file(source, REPO / filename)

    result = {
        "ema_rows_added": [],
        "new_additional_rows": [],
        "transitions": [],
        "latest_additional": {},
        "ticker_errors": [],
        "ticker_available": False,
        "historical_catch_up": False,
        "portfolio_row_counts": {key: len(rows) for key, rows in rows_by_strategy.items()},
        "portfolio_files_synced": list(LOGS.values()),
        "window_available": False,
    }

    if run is not None:
        run.mkdir(parents=True, exist_ok=True)
        ema_before_path = run / "ema_before.json"
        additional_before_path = run / "additional_before.json"
        if ema_before_path.exists() and additional_before_path.exists():
            with ema_before_path.open(encoding="utf-8") as handle:
                ema_before = json.load(handle)
            with additional_before_path.open(encoding="utf-8") as handle:
                additional_before = json.load(handle)
            thresholds = {
                "ema300-8h": int(ema_before["last_processed_open_time_ms"]) + INTERVAL_MS - 1,
                "donchian-8h-20-10": int(additional_before["last_8h_open_time_ms"]) + INTERVAL_MS - 1,
                "daily-ema6-24": int(additional_before["last_daily_open_time_ms"]) + DAILY_INTERVAL_MS - 1,
            }
            new_rows = {}
            for strategy, rows in rows_by_strategy.items():
                new_rows[strategy] = [
                    report_row(row) for row in rows
                    if row["record_type"] == "COMPLETED_CANDLE"
                    and row["candle_close_utc"] > utc_timestamp(thresholds[strategy])
                ]
            result["window_available"] = True
            result["ema_rows_added"] = new_rows["ema300-8h"]
            result["new_additional_rows"] = (
                new_rows["donchian-8h-20-10"] + new_rows["daily-ema6-24"]
            )
            result["transitions"] = [
                row for strategy in LOGS
                for row in new_rows[strategy]
                if row["signal_changed"] == "true"
            ]
            result["historical_catch_up"] = any(len(group) > 1 for group in new_rows.values())
            result["latest_additional"] = {
                strategy: report_row(rows[-1])
                for strategy, rows in rows_by_strategy.items()
                if strategy != "ema300-8h"
            }
        (run / "ticker.json").write_text(
            json.dumps({"current_spot_price_usdt": "", "spot_ticker_observed_utc": "", "errors": []}, indent=2) + "\n",
            encoding="utf-8",
        )
        (run / "journal_result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
