import io
import tempfile
import time
import unittest
from pathlib import Path

from binance_ema300_monitor import (
    BinancePublicClient,
    Candle,
    EMA_PERIOD,
    INTERVAL_MS,
    Monitor,
    MonitorError,
    MonitorState,
    PartialDurationCandleError,
    completed_candles,
    ensure_contiguous,
    historical_gap_ranges,
    load_state,
    make_config,
    next_target,
    parse_candle,
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
    def test_parse_rejects_short_duration_row_as_not_a_full_bar(self):
        open_time = 1_518_048_000_000
        row = [open_time, "1", "1", "1", "7784.02", "1", open_time + 1_694_788]
        with self.assertRaises(PartialDurationCandleError):
            parse_candle(row)

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

    def test_historical_gap_summary_counts_but_does_not_fill_missing_intervals(self):
        candles = [
            Candle(0, 100.0, INTERVAL_MS - 1),
            Candle(3 * INTERVAL_MS, 101.0, 4 * INTERVAL_MS - 1),
        ]
        self.assertEqual(historical_gap_ranges(candles), [(0, 3 * INTERVAL_MS, 2)])
        with self.assertRaisesRegex(MonitorError, "Missing or out-of-order"):
            ensure_contiguous(candles)

    def test_public_client_skips_only_early_close_rows_during_initial_history(self):
        open_a = 1_518_019_200_000
        open_short = open_a + INTERVAL_MS
        open_b = open_short + 3 * INTERVAL_MS

        def row(open_time, close, close_time=None):
            return [
                open_time,
                "1",
                "1",
                "1",
                str(close),
                "1",
                close_time if close_time is not None else open_time + INTERVAL_MS - 1,
            ]

        short_close = open_short + 1_694_788

        class FixtureClient(BinancePublicClient):
            def fetch_page(self, symbol, start_time_ms, limit=1000):
                self.request = (symbol, start_time_ms, limit)
                return [
                    row(open_a, 100.0),
                    row(open_short, 101.0, short_close),
                    row(open_b, 102.0),
                ]

        stderr = io.StringIO()
        client = FixtureClient("https://api.binance.com", stderr=stderr)
        candles = client.fetch_all("BTCUSDT", allow_historical_short_rows=True)
        self.assertEqual([c.open_time_ms for c in candles], [open_a, open_b])
        self.assertEqual(client.last_skipped_historical_rows, 1)
        self.assertIn("not treated as a completed full bar", stderr.getvalue())
        self.assertEqual(historical_gap_ranges(candles), [(open_a, open_b, 3)])
        with self.assertRaises(PartialDurationCandleError):
            client.fetch_all("BTCUSDT", allow_historical_short_rows=False)

    def test_recent_short_row_is_not_skipped_even_during_initial_history(self):
        now_ms = int(time.time() * 1000)
        current_open = (now_ms // INTERVAL_MS) * INTERVAL_MS
        row = [current_open, "1", "1", "1", "100", "1", current_open + 1000]

        class FixtureClient(BinancePublicClient):
            def fetch_page(self, symbol, start_time_ms, limit=1000):
                return [row]

        with self.assertRaises(PartialDurationCandleError):
            FixtureClient("https://api.binance.com").fetch_all(
                "BTCUSDT", allow_historical_short_rows=True
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
        opens = [
            open_time
            for open_time in range(
                current_open - (EMA_PERIOD + 1) * INTERVAL_MS,
                current_open + 1,
                INTERVAL_MS,
            )
            if open_time != current_open - 150 * INTERVAL_MS
        ]
        prices = [100.0] * len(opens)
        prices[-3:] = [100.0, 103.0, 110.0]
        fixture_candles = [
            Candle(open_time_ms=open_time, close_price=price,
                   close_time_ms=open_time + INTERVAL_MS - 1)
            for open_time, price in zip(opens, prices)
        ]

        class FixtureClient:
            def fetch_all(self, symbol, start_time_ms, *, allow_historical_short_rows=False):
                self.allow_historical_short_rows = allow_historical_short_rows
                return fixture_candles

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            stdout = io.StringIO()
            client = FixtureClient()
            monitor = Monitor(
                "BTCUSDT",
                "https://api.binance.com",
                state_path,
                client,
                stdout=stdout,
                stderr=io.StringIO(),
            )
            monitor.run_once()
            restored = load_state(
                state_path,
                make_config("BTCUSDT", "https://api.binance.com"),
            )

        self.assertTrue(client.allow_historical_short_rows)
        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(restored.processed_candles, EMA_PERIOD)
        self.assertEqual(restored.last_processed_open_time_ms, current_open - INTERVAL_MS)
        self.assertEqual(restored.target, "LONG")
        self.assertIn(f"replayed {EMA_PERIOD} completed candles", stdout.getvalue())
        self.assertIn("preserve gaps without synthesizing candles", stdout.getvalue())
        self.assertIn("1 missing 8-hour interval", stdout.getvalue())

    def test_polling_does_not_repeat_the_startup_banner_and_keeps_updates_strict(self):
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
            def fetch_all(self, symbol, start_time_ms, *, allow_historical_short_rows=False):
                self.allow_historical_short_rows = allow_historical_short_rows
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
            client = FixtureClient()
            monitor = Monitor(
                "BTCUSDT",
                "https://api.binance.com",
                state_path,
                client,
                stdout=stdout,
                stderr=io.StringIO(),
            )
            monitor.run_once(once=False)
            monitor.run_once(once=False)

        self.assertFalse(client.allow_historical_short_rows)
        self.assertEqual(stdout.getvalue().count("[startup]"), 1)

    def test_first_run_with_fewer_than_ema_period_bars_does_not_create_state(self):
        now_ms = int(time.time() * 1000)
        current_open = (now_ms // INTERVAL_MS) * INTERVAL_MS
        fixture_candles = [
            Candle(open_time, 100.0, open_time + INTERVAL_MS - 1)
            for open_time in range(
                current_open - (EMA_PERIOD - 1) * INTERVAL_MS,
                current_open + 1,
                INTERVAL_MS,
            )
        ]

        class FixtureClient:
            def fetch_all(self, symbol, start_time_ms, *, allow_historical_short_rows=False):
                return fixture_candles

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            monitor = Monitor(
                "BTCUSDT",
                "https://api.binance.com",
                state_path,
                FixtureClient(),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            with self.assertRaisesRegex(MonitorError, "at least 300 are required"):
                monitor.run_once()
            self.assertFalse(state_path.exists())

    def test_saved_state_update_gap_is_rejected_without_mutating_state(self):
        now_ms = int(time.time() * 1000)
        current_open = (now_ms // INTERVAL_MS) * INTERVAL_MS
        config = make_config("BTCUSDT", "https://api.binance.com")
        existing = MonitorState(
            config=config,
            ema=100.0,
            target="FLAT",
            last_processed_open_time_ms=current_open - 2 * INTERVAL_MS,
            processed_candles=500,
            last_close=100.0,
        )

        class FixtureClient:
            def fetch_all(self, symbol, start_time_ms, *, allow_historical_short_rows=False):
                self.allow_historical_short_rows = allow_historical_short_rows
                return [Candle(current_open, 101.0, current_open + INTERVAL_MS - 1)]

        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            save_state(existing, state_path)
            client = FixtureClient()
            monitor = Monitor(
                "BTCUSDT",
                "https://api.binance.com",
                state_path,
                client,
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            with self.assertRaisesRegex(MonitorError, "Missing or out-of-order"):
                monitor.run_once()
            restored = load_state(state_path, config)

        self.assertFalse(client.allow_historical_short_rows)
        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(restored, existing)


if __name__ == "__main__":
    unittest.main()
