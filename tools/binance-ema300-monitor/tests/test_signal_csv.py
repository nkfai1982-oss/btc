import csv
import io
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import binance_ema300_monitor as monitor_module
from binance_ema300_monitor import (
    AdditionalSignalEvent,
    AdditionalSignalMonitor,
    AdditionalSignalState,
    Candle,
    DAILY_INTERVAL_MS,
    INTERVAL_MS,
    load_additional_state,
    make_additional_config,
    save_additional_state,
    utc_timestamp,
)
from signal_csv import FIELDS, append_evaluations, evaluation_row


class SignalCsvTests(unittest.TestCase):
    def row(self, strategy="rule-a", symbol="BTCUSDT", close="2025-01-01T00:00:00Z", **overrides):
        row = dict.fromkeys(FIELDS, "")
        row.update(
            strategy=strategy,
            symbol=symbol,
            interval="8h",
            candle_close_utc=close,
            close_usdt="100.00000000",
            previous_target="CASH",
            target="BTC",
            signal_changed="true",
            effective_from_utc="2025-01-01T00:00:00.001Z",
            record_type="COMPLETED_CANDLE",
        )
        row.update(overrides)
        return row

    def test_append_deduplicates_strategy_symbol_close_and_preserves_existing_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "evaluations.csv"
            original = self.row()
            self.assertEqual(append_evaluations(path, [original]), 1)
            changed_retry = self.row(target="CASH", signal_changed="false", close_usdt="99.00000000")
            self.assertEqual(append_evaluations(path, [changed_retry]), 0)
            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows, [original])

    def test_same_timestamp_for_distinct_strategies_is_not_deduplicated(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "evaluations.csv"
            rows = [self.row(strategy="rule-a"), self.row(strategy="rule-b")]
            self.assertEqual(append_evaluations(path, rows), 2)
            self.assertEqual(append_evaluations(path, rows), 0)
            with path.open(newline="", encoding="utf-8") as handle:
                saved = list(csv.DictReader(handle))
            self.assertEqual([row["strategy"] for row in saved], ["rule-a", "rule-b"])

    def test_schema_rejection_does_not_change_existing_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "evaluations.csv"
            path.write_text("wrong,header\nold,value\n", encoding="utf-8")
            before = path.read_bytes()
            with self.assertRaisesRegex(ValueError, "Unexpected CSV schema"):
                append_evaluations(path, [self.row()])
            self.assertEqual(path.read_bytes(), before)

    def test_evaluation_row_formats_thresholds_and_effective_timestamp(self):
        candle = Candle(1_700_000_000_000, 123.456789012, 1_700_000_000_999)
        event = AdditionalSignalEvent(
            strategy="donchian-8h-20-10",
            interval="8h",
            candle=candle,
            previous_target="CASH",
            target="BTC",
            details={"highest_close_prior20": 120.5, "lowest_close_prior10": 90.25},
        )
        row = evaluation_row(event, "BTCUSDT", utc_timestamp)
        self.assertEqual(set(row), set(FIELDS))
        self.assertEqual(row["close_usdt"], "123.45678901")
        self.assertEqual(row["highest_close_prior20_usdt"], "120.50000000")
        self.assertEqual(row["lowest_close_prior10_usdt"], "90.25000000")
        self.assertEqual(row["signal_changed"], "true")
        self.assertEqual(row["candle_close_utc"], utc_timestamp(candle.close_time_ms))
        self.assertEqual(row["effective_from_utc"], utc_timestamp(candle.close_time_ms + 1))


class AdditionalSignalCsvIntegrationTests(unittest.TestCase):
    def setUp(self):
        now_ms = int(time.time() * 1000)
        current_8h = (now_ms // INTERVAL_MS) * INTERVAL_MS
        current_day = (now_ms // DAILY_INTERVAL_MS) * DAILY_INTERVAL_MS
        self.latest_8h = current_8h - INTERVAL_MS
        self.latest_day = current_day - DAILY_INTERVAL_MS
        self.previous_8h = self.latest_8h - INTERVAL_MS
        self.previous_day = self.latest_day - DAILY_INTERVAL_MS

    def make_state(self):
        return AdditionalSignalState(
            config=make_additional_config("BTCUSDT", "https://api.binance.com"),
            donchian_target="CASH",
            last_8h_open_time_ms=self.previous_8h,
            recent_8h_closes=[100.0] * 20,
            daily_ema6=100.0,
            daily_ema24=100.0,
            daily_target="CASH",
            last_daily_open_time_ms=self.previous_day,
        )

    def make_monitor(self, temp_dir, client):
        root = Path(temp_dir)
        state_path = root / "state.json"
        save_additional_state(self.make_state(), state_path)
        return AdditionalSignalMonitor(
            "BTCUSDT", "https://api.binance.com", state_path, client,
            stdout=io.StringIO(), stderr=io.StringIO(), results_path=root / "results.csv",
        ), state_path

    def test_journals_both_rules_and_repeat_check_adds_no_duplicates(self):
        latest_8h, latest_day = self.latest_8h, self.latest_day

        class FixtureClient:
            def fetch_all(self, symbol, start_time_ms=0, *, allow_historical_short_rows=False, interval="8h"):
                open_time = latest_8h if interval == "8h" else latest_day
                duration = INTERVAL_MS if interval == "8h" else DAILY_INTERVAL_MS
                if start_time_ms > open_time:
                    return []
                return [Candle(open_time, 101.0, open_time + duration - 1)]

        with tempfile.TemporaryDirectory() as tmp:
            monitor, _ = self.make_monitor(tmp, FixtureClient())
            events = monitor.run_once()
            self.assertEqual([event.strategy for event in events], ["donchian-8h-20-10", "daily-ema6-24"])
            csv_path = Path(tmp) / "results.csv"
            with csv_path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual({row["strategy"] for row in rows}, {"donchian-8h-20-10", "daily-ema6-24"})
            self.assertEqual(monitor.run_once(), [])
            with csv_path.open(newline="", encoding="utf-8") as handle:
                self.assertEqual(len(list(csv.DictReader(handle))), 2)

    def test_csv_failure_does_not_advance_json_state(self):
        latest_8h, latest_day = self.latest_8h, self.latest_day

        class FixtureClient:
            def fetch_all(self, symbol, start_time_ms=0, *, allow_historical_short_rows=False, interval="8h"):
                open_time = latest_8h if interval == "8h" else latest_day
                duration = INTERVAL_MS if interval == "8h" else DAILY_INTERVAL_MS
                return [Candle(open_time, 101.0, open_time + duration - 1)]

        with tempfile.TemporaryDirectory() as tmp:
            monitor, state_path = self.make_monitor(tmp, FixtureClient())
            before = load_additional_state(state_path, monitor.config)
            with patch.object(monitor_module, "append_evaluations", side_effect=OSError("disk unavailable")):
                with self.assertRaisesRegex(monitor_module.MonitorError, "state not advanced"):
                    monitor.run_once()
            after = load_additional_state(state_path, monitor.config)
            self.assertIsNotNone(before)
            self.assertIsNotNone(after)
            assert before is not None and after is not None
            self.assertEqual(after.last_8h_open_time_ms, before.last_8h_open_time_ms)
            self.assertEqual(after.last_daily_open_time_ms, before.last_daily_open_time_ms)
            self.assertEqual(after.donchian_target, before.donchian_target)
            self.assertEqual(after.daily_target, before.daily_target)


if __name__ == "__main__":
    unittest.main()
