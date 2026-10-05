import io
import tempfile
import time
import unittest
from pathlib import Path

from binance_ema300_monitor import (
    Candle,
    INTERVAL_MS,
    Monitor,
    MonitorError,
    MonitorState,
    completed_candles,
    ensure_contiguous,
    load_state,
    make_config,
    next_target,
    save_state,
    update_ema,
)


class SignalRuleTests(unittest.TestCase):
    def test_hysteresis_transitions_and_deadband_retains_previous_target(self):
        ema = 100.0
        self.assertEqual(next_target(102.01, ema, "FLAT"), "LONG")
        self.assertEqual(next_target(97.0, ema, "LONG"), "FLAT")
        self.assertEqual(next_target(100.0, ema, "LONG"), "LONG")
        self.assertEqual(next_target(100.0, ema, "FLAT"), "FLAT")
        # The upper trigger is strict, while the lower trigger is inclusive.
        self.assertEqual(next_target(102.0, ema, "FLAT"), "FLAT")
        self.assertEqual(next_target(97.0, ema, "LONG"), "FLAT")

    def test_ema_recursive_adjust_false_seeded_by_first_close(self):
        first = update_ema(None, 10.0, period=3)
        second = update_ema(first, 14.0, period=3)
        third = update_ema(second, 12.0, period=3)
        self.assertAlmostEqual(first, 10.0)
        self.assertAlmostEqual(second, 12.0)  # alpha = 2 / (3 + 1)
        self.assertAlmostEqual(third, 12.0)


class CandleAndStateTests(unittest.TestCase):
    def test_incomplete_candle_is_excluded_until_its_close_timestamp_is_past(self):
        candles = [
            Candle(open_time_ms=0, close_price=100.0, close_time_ms=99),
            Candle(open_time_ms=100, close_price=101.0, close_time_ms=199),
            Candle(open_time_ms=200, close_price=102.0, close_time_ms=299),
        ]
        self.assertEqual(completed_candles(candles, now_ms=199), candles[:1])
        self.assertEqual(completed_candles(candles, now_ms=200), candles[:2])
        self.assertEqual(completed_candles(candles, now_ms=300), candles)

    def test_contiguity_check_rejects_a_missing_candle(self):
        with self.assertRaisesRegex(MonitorError, "Missing or out-of-order"):
            ensure_contiguous(
                [Candle(open_time_ms=2 * INTERVAL_MS, close_price=102.0, close_time_ms=3 * INTERVAL_MS - 1)],
                first_expected_open=INTERVAL_MS,
            )

    def test_state_round_trip_preserves_config_and_signal_fields(self):
        config = make_config("BTCUSDT", "https://api.binance.com")
        original = MonitorState(
            config=config,
            ema=67234.125,
            target="LONG",
            last_processed_open_time_ms=1_700_000_000_000,
            processed_candles=1234,
            last_close=68000.5,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.json"
            save_state(original, path)
            restored = load_state(path, config)
        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(restored.config, original.config)
        self.assertEqual(restored.ema, original.ema)
        self.assertEqual(restored.target, original.target)
        self.assertEqual(restored.last_processed_open_time_ms, original.last_processed_open_time_ms)
        self.assertEqual(restored.processed_candles, original.processed_candles)
        self.assertEqual(restored.last_close, original.last_close)

    def test_first_run_reconstructs_completed_fixture_candles_only(self):
        now_ms = int(time.time() * 1000)
        current_open = (now_ms // INTERVAL_MS) * INTERVAL_MS
        opens = [current_open - 3 * INTERVAL_MS, current_open - 2 * INTERVAL_MS,
                 current_open - INTERVAL_MS, current_open]
        prices = [100.0, 103.0, 100.0, 110.0]
        fixture_candles = [
            Candle(open_time_ms=open_time, close_price=price,
                   close_time_ms=open_time + INTERVAL_MS - 1)
            for open_time, price in zip(opens, prices)
        ]

        class FixtureClient:
            def fetch_all(self, symbol, start_time_ms):
                return fixture_candles

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            stdout = io.StringIO()
            monitor = Monitor(
                "BTCUSDT",
                "https://api.binance.com",
                state_path,
                FixtureClient(),
                stdout=stdout,
                stderr=io.StringIO(),
            )
            monitor.run_once()
            restored = load_state(
                state_path,
                make_config("BTCUSDT", "https://api.binance.com"),
            )

        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(restored.processed_candles, 3)
        self.assertEqual(restored.last_processed_open_time_ms, current_open - INTERVAL_MS)
        self.assertEqual(restored.target, "LONG")
        self.assertIn("replayed 3 completed candles", stdout.getvalue())

    def test_polling_does_not_repeat_the_startup_banner(self):
        now_ms = int(time.time() * 1000)
        current_open = (now_ms // INTERVAL_MS) * INTERVAL_MS
        saved_open = current_open - INTERVAL_MS
        config = make_config("BTCUSDT", "https://api.binance.com")
        existing = MonitorState(
            config=config,
            ema=100.0,
            target="FLAT",
            last_processed_open_time_ms=saved_open,
            processed_candles=100,
            last_close=100.0,
        )

        class FixtureClient:
            def fetch_all(self, symbol, start_time_ms):
                self.last_request = (symbol, start_time_ms)
                return [
                    Candle(
                        open_time_ms=current_open,
                        close_price=100.5,
                        close_time_ms=current_open + INTERVAL_MS - 1,
                    )
                ]

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            save_state(existing, state_path)
            stdout = io.StringIO()
            monitor = Monitor(
                "BTCUSDT",
                "https://api.binance.com",
                state_path,
                FixtureClient(),
                stdout=stdout,
                stderr=io.StringIO(),
            )
            monitor.run_once(once=False)
            monitor.run_once(once=False)

        self.assertEqual(stdout.getvalue().count("[startup]"), 1)


if __name__ == "__main__":
    unittest.main()
