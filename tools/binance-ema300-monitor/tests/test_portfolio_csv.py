import csv
import io
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from binance_ema300_monitor import (
    AdditionalSignalState,
    Candle,
    DAILY_INTERVAL_MS,
    INTERVAL_MS,
    Monitor,
    MonitorState,
    make_additional_config,
    make_config,
    save_additional_state,
    save_state,
)
from portfolio_csv import PORTFOLIO_FIELDS, STRATEGIES, PortfolioJournal


class PortfolioJournalTests(unittest.TestCase):
    def make_journal(self, root, capital=1000.0):
        root = Path(root)
        return PortfolioJournal.alongside(
            root / "binance_ema300_state.json",
            portfolio_state_file=root / "portfolio_state.json",
            ema300_file=root / "ema300.csv",
            donchian_file=root / "donchian.csv",
            daily_ema_file=root / "daily.csv",
            starting_capital_usdt=capital,
        )

    def start(self, journal, targets=None, price=100.0, time_ms=0, allow=True):
        targets = targets or {
            "ema300-8h": "LONG",
            "donchian-8h-20-10": "CASH",
            "daily-ema6-24": "CASH",
        }
        journal.ensure_started(
            "BTCUSDT",
            {
                "ema300-8h": "8h",
                "donchian-8h-20-10": "8h",
                "daily-ema6-24": "1d",
            },
            targets,
            price,
            time_ms,
            allow_initialize=allow,
        )
        return targets

    def event(self, open_ms, close_ms, open_price, close_price, previous, target, details=None):
        return SimpleNamespace(
            candle=Candle(open_ms, close_price, close_ms, open_price),
            previous_target=previous,
            target=target,
            details=details or {},
        )

    def rows(self, path):
        with Path(path).open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            self.assertEqual(reader.fieldnames, PORTFOLIO_FIELDS)
            return list(reader)

    def test_baseline_creates_three_independent_1000_usdt_ledgers_from_saved_targets(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = self.make_journal(tmp)
            self.start(journal)
            for strategy in STRATEGIES:
                path = journal.csv_paths[strategy]
                rows = self.rows(path)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["record_type"], "STARTING_BALANCE")
                self.assertEqual(rows[0]["starting_capital_usdt"], "1000.00000000")
                self.assertEqual(rows[0]["portfolio_value_usdt"], "1000.00000000")
            ema = self.rows(journal.csv_paths["ema300-8h"])[0]
            self.assertEqual(ema["signal_target"], "LONG")
            self.assertEqual(ema["position_at_close"], "BTC")
            self.assertEqual(ema["btc_units"], "10.000000000000")
            cash = self.rows(journal.csv_paths["donchian-8h-20-10"])[0]
            self.assertEqual(cash["position_at_close"], "CASH")
            self.assertEqual(cash["cash_usdt"], "1000.00000000")

    def test_requires_explicit_initialization_and_refuses_to_rebaseline_missing_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = self.make_journal(tmp)
            with self.assertRaisesRegex(ValueError, "Initialize explicitly"):
                self.start(journal, allow=False)
            self.assertFalse(journal.state_path.exists())
            self.start(journal)
            journal.csv_paths["ema300-8h"].unlink()
            reloaded = self.make_journal(tmp)
            with self.assertRaisesRegex(ValueError, "missing its original baseline"):
                self.start(reloaded, allow=False)
            self.assertFalse(journal.csv_paths["ema300-8h"].exists())

    def test_signal_change_at_close_executes_at_next_open_and_tracks_cumulative_return(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = self.make_journal(tmp)
            targets = {
                "ema300-8h": "FLAT",
                "donchian-8h-20-10": "CASH",
                "daily-ema6-24": "CASH",
            }
            self.start(journal, targets, price=100.0)
            first = self.event(1, INTERVAL_MS, 110.0, 121.0, "FLAT", "LONG")
            self.assertTrue(journal.record_evaluation("ema300-8h", "BTCUSDT", "8h", first))
            row1 = self.rows(journal.csv_paths["ema300-8h"])[-1]
            self.assertEqual(row1["signal_target"], "LONG")
            self.assertEqual(row1["position_at_close"], "CASH")
            self.assertEqual(row1["execution_action"], "HOLD")
            self.assertEqual(row1["portfolio_value_usdt"], "1000.00000000")

            second = self.event(INTERVAL_MS + 1, 2 * INTERVAL_MS, 100.0, 110.0, "LONG", "LONG")
            self.assertTrue(journal.record_evaluation("ema300-8h", "BTCUSDT", "8h", second))
            row2 = self.rows(journal.csv_paths["ema300-8h"])[-1]
            self.assertEqual(row2["execution_action"], "BUY_BTC")
            self.assertEqual(row2["execution_price_usdt"], "100.00000000")
            self.assertEqual(row2["position_at_close"], "BTC")
            self.assertEqual(row2["btc_units"], "10.000000000000")
            self.assertEqual(row2["portfolio_value_usdt"], "1100.00000000")
            self.assertEqual(row2["profit_loss_usdt"], "100.00000000")
            self.assertEqual(row2["total_return_pct"], "10.00000000")
            self.assertEqual(row2["period_return_pct"], "10.00000000")

    def test_open_gap_and_intracandle_move_are_marked_for_existing_long_position(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = self.make_journal(tmp)
            self.start(journal, price=100.0)
            event = self.event(1, INTERVAL_MS, 110.0, 121.0, "LONG", "LONG")
            journal.record_evaluation("ema300-8h", "BTCUSDT", "8h", event)
            row = self.rows(journal.csv_paths["ema300-8h"])[-1]
            self.assertEqual(row["position_at_close"], "BTC")
            self.assertEqual(row["portfolio_value_usdt"], "1210.00000000")
            self.assertEqual(row["total_return_pct"], "21.00000000")

    def test_daily_baseline_inside_candle_does_not_count_prebaseline_open_to_baseline_return(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = self.make_journal(tmp)
            targets = {
                "ema300-8h": "LONG",
                "donchian-8h-20-10": "CASH",
                "daily-ema6-24": "BTC",
            }
            baseline = 8 * 60 * 60 * 1000 - 1
            self.start(journal, targets, price=100.0, time_ms=baseline)
            first = self.event(0, DAILY_INTERVAL_MS - 1, 90.0, 110.0, "BTC", "CASH")
            journal.record_evaluation("daily-ema6-24", "BTCUSDT", "1d", first)
            row1 = self.rows(journal.csv_paths["daily-ema6-24"])[-1]
            self.assertEqual(row1["execution_action"], "HOLD")
            self.assertEqual(row1["portfolio_value_usdt"], "1100.00000000")
            self.assertEqual(row1["total_return_pct"], "10.00000000")
            self.assertEqual(row1["position_at_close"], "BTC")

            next_day = self.event(DAILY_INTERVAL_MS, 2 * DAILY_INTERVAL_MS - 1, 120.0, 120.0, "CASH", "CASH")
            journal.record_evaluation("daily-ema6-24", "BTCUSDT", "1d", next_day)
            row2 = self.rows(journal.csv_paths["daily-ema6-24"])[-1]
            self.assertEqual(row2["execution_action"], "SELL_TO_CASH")
            self.assertEqual(row2["execution_price_usdt"], "120.00000000")
            self.assertEqual(row2["portfolio_value_usdt"], "1200.00000000")
            self.assertEqual(row2["total_return_pct"], "20.00000000")

    def test_recovery_from_csv_row_committed_before_state_and_retry_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = self.make_journal(tmp)
            targets = {
                "ema300-8h": "FLAT",
                "donchian-8h-20-10": "CASH",
                "daily-ema6-24": "CASH",
            }
            self.start(journal, targets)
            event = self.event(1, INTERVAL_MS, 100.0, 105.0, "FLAT", "LONG")
            with patch.object(journal, "_save_state", side_effect=OSError("simulated crash")):
                with self.assertRaisesRegex(OSError, "simulated crash"):
                    journal.record_evaluation("ema300-8h", "BTCUSDT", "8h", event)
            self.assertEqual(len(self.rows(journal.csv_paths["ema300-8h"])), 2)

            recovered = self.make_journal(tmp)
            self.start(recovered, targets, allow=False)
            self.assertFalse(recovered.record_evaluation("ema300-8h", "BTCUSDT", "8h", event))
            payload = recovered._load_state("BTCUSDT")
            self.assertEqual(payload["strategies"]["ema300-8h"]["pending_signal_target"], "LONG")
            self.assertEqual(len(self.rows(recovered.csv_paths["ema300-8h"])), 2)

    def test_conflicting_duplicate_and_missing_interval_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = self.make_journal(tmp)
            targets = {
                "ema300-8h": "FLAT",
                "donchian-8h-20-10": "CASH",
                "daily-ema6-24": "CASH",
            }
            self.start(journal, targets)
            event = self.event(1, INTERVAL_MS, 100.0, 101.0, "FLAT", "LONG")
            journal.record_evaluation("ema300-8h", "BTCUSDT", "8h", event)
            conflicting = self.event(1, INTERVAL_MS, 100.0, 102.0, "FLAT", "CASH")
            with self.assertRaisesRegex(ValueError, "Conflicting duplicate"):
                journal.record_evaluation("ema300-8h", "BTCUSDT", "8h", conflicting)

            gap = self.event(3 * INTERVAL_MS + 1, 4 * INTERVAL_MS, 102.0, 103.0, "LONG", "LONG")
            with self.assertRaisesRegex(ValueError, "Missing or out-of-order portfolio candle"):
                journal.record_evaluation("ema300-8h", "BTCUSDT", "8h", gap)

    def test_malformed_csv_schema_is_not_reset(self):
        with tempfile.TemporaryDirectory() as tmp:
            journal = self.make_journal(tmp)
            self.start(journal)
            path = journal.csv_paths["ema300-8h"]
            path.write_text("wrong,header\nold,row\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Unexpected portfolio CSV schema"):
                self.start(self.make_journal(tmp), allow=False)


class MonitorPortfolioIntegrationTests(unittest.TestCase):
    def test_explicit_monitor_initialization_preserves_signal_states_and_creates_all_logs(self):
        now_ms = int(time.time() * 1000)
        current_8h = (now_ms // INTERVAL_MS) * INTERVAL_MS
        current_day = (now_ms // DAILY_INTERVAL_MS) * DAILY_INTERVAL_MS
        saved_8h = current_8h - INTERVAL_MS
        saved_day = current_day - DAILY_INTERVAL_MS

        class EmptyClient:
            def fetch_all(self, symbol, start_time_ms=0, *, allow_historical_short_rows=False, interval="8h"):
                return []

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            signal_path = root / "binance_ema300_state.json"
            additional_path = root / "binance_additional_signals_state.json"
            primary_state = MonitorState(
                config=make_config("BTCUSDT", "https://api.binance.com"),
                ema=99.0,
                target="LONG",
                last_processed_open_time_ms=saved_8h,
                processed_candles=1000,
                last_close=100.0,
            )
            additional_state = AdditionalSignalState(
                config=make_additional_config("BTCUSDT", "https://data-api.binance.vision"),
                donchian_target="CASH",
                last_8h_open_time_ms=saved_8h,
                recent_8h_closes=[100.0] * 20,
                daily_ema6=90.0,
                daily_ema24=100.0,
                daily_target="CASH",
                last_daily_open_time_ms=saved_day,
            )
            save_state(primary_state, signal_path)
            save_additional_state(additional_state, additional_path)
            primary_before = signal_path.read_bytes()
            additional_before = additional_path.read_bytes()
            monitor = Monitor(
                "BTCUSDT",
                "https://api.binance.com",
                signal_path,
                EmptyClient(),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
                additional_state_path=additional_path,
                additional_client=EmptyClient(),
                additional_base_url="https://data-api.binance.vision",
                initialize_portfolio=True,
            )
            monitor.run_once()

            self.assertEqual(signal_path.read_bytes(), primary_before)
            self.assertEqual(additional_path.read_bytes(), additional_before)
            self.assertEqual(
                {path.name for path in root.glob("*_portfolio.csv")},
                {"ema300_portfolio.csv", "donchian_20_10_portfolio.csv", "daily_ema6_24_portfolio.csv"},
            )
            self.assertTrue((root / "binance_portfolio_state.json").exists())


if __name__ == "__main__":
    unittest.main()
