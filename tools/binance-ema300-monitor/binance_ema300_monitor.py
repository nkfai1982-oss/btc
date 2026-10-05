#!/usr/bin/env python3
"""Signal-only Binance Spot 8h EMA300 monitor; never places orders."""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlsplit

INTERVAL = "8h"
INTERVAL_MS = 8 * 60 * 60 * 1000
EMA_PERIOD = 300
LONG_MULTIPLIER = 1.02
FLAT_MULTIPLIER = 0.97
API_PAGE_LIMIT = 1000
STATE_VERSION = 1
DEFAULT_BASE_URL = "https://api.binance.com"
USER_AGENT = "binance-ema300-signal-monitor/1.0"


class MonitorError(Exception):
    """A safe-to-display configuration, data, or network error."""


@dataclass(frozen=True)
class Candle:
    open_time_ms: int
    close_price: float
    close_time_ms: int


@dataclass
class MonitorState:
    config: dict[str, Any]
    ema: float | None
    target: str
    last_processed_open_time_ms: int | None
    processed_candles: int
    last_close: float | None


@dataclass(frozen=True)
class CandleEvent:
    candle: Candle
    ema: float
    distance_pct: float
    previous_target: str
    target: str


def utc_timestamp(epoch_ms: int) -> str:
    precision = "milliseconds" if epoch_ms % 1000 else "seconds"
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat(
        timespec=precision
    ).replace("+00:00", "Z")


def normalize_base_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("base URL must be an http(s) origin, e.g. https://api.binance.com")
    return f"{parsed.scheme}://{parsed.netloc}"


def make_config(symbol: str, base_url: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "interval": INTERVAL,
        "ema_period": EMA_PERIOD,
        "ema_method": "recursive adjust=False",
        "ema_seed": "first observed completed close",
        "long_trigger_multiple": LONG_MULTIPLIER,
        "flat_trigger_multiple": FLAT_MULTIPLIER,
        "initial_target": "FLAT",
        "execution_convention": "next observed open",
        "base_url": base_url,
    }


def update_ema(previous_ema: float | None, close: float, period: int = EMA_PERIOD) -> float:
    """Apply adjust=False EMA recursion, seeding from the first close."""
    if isinstance(period, bool) or not isinstance(period, int) or period < 1:
        raise ValueError("EMA period must be a positive integer")
    if not math.isfinite(close) or close <= 0:
        raise ValueError("close must be a finite positive number")
    if previous_ema is None:
        return close
    if not math.isfinite(previous_ema) or previous_ema <= 0:
        raise ValueError("previous EMA must be a finite positive number")
    alpha = 2.0 / (period + 1.0)
    return alpha * close + (1.0 - alpha) * previous_ema


def next_target(close: float, ema: float, previous_target: str) -> str:
    """Apply the frozen long/flat hysteresis thresholds."""
    if previous_target not in {"LONG", "FLAT"}:
        raise ValueError("previous target must be LONG or FLAT")
    if not math.isfinite(close) or close <= 0 or not math.isfinite(ema) or ema <= 0:
        raise ValueError("close and EMA must be finite positive numbers")
    if close > LONG_MULTIPLIER * ema:
        return "LONG"
    if close <= FLAT_MULTIPLIER * ema:
        return "FLAT"
    return previous_target


def parse_candle(row: Any) -> Candle:
    if not isinstance(row, list) or len(row) < 7:
        raise MonitorError("Binance returned a malformed kline row (expected at least 7 fields).")
    try:
        if isinstance(row[0], bool) or isinstance(row[6], bool):
            raise ValueError("boolean timestamp")
        open_time = int(row[0])
        close = float(row[4])
        close_time = int(row[6])
    except (TypeError, ValueError, OverflowError) as exc:
        raise MonitorError(f"Binance returned an invalid kline value: {exc}") from exc
    if open_time < 0 or close_time < open_time:
        raise MonitorError("Binance returned invalid kline timestamps.")
    if open_time % INTERVAL_MS != 0:
        raise MonitorError(
            f"Kline open time {open_time} is not aligned to an 8-hour UTC boundary."
        )
    if close_time != open_time + INTERVAL_MS - 1:
        raise MonitorError(
            f"Kline at {utc_timestamp(open_time)} has an unexpected close timestamp; refusing to process it."
        )
    if not math.isfinite(close) or close <= 0:
        raise MonitorError("Binance returned a non-positive or non-finite close price.")
    return Candle(open_time, close, close_time)


def completed_candles(candles: Iterable[Candle], now_ms: int) -> list[Candle]:
    """Return only candles whose inclusive close timestamp is strictly in the past."""
    return [candle for candle in candles if candle.close_time_ms < now_ms]


def expected_latest_completed_open(now_ms: int) -> int:
    current_open = (now_ms // INTERVAL_MS) * INTERVAL_MS
    return current_open - INTERVAL_MS


def validate_freshness(candles: list[Candle], now_ms: int) -> None:
    if not candles:
        raise MonitorError("Binance returned no klines; no state was changed.")
    newest_open = candles[-1].open_time_ms
    current_open = (now_ms // INTERVAL_MS) * INTERVAL_MS
    expected_latest = current_open - INTERVAL_MS
    if newest_open > current_open:
        raise MonitorError(
            f"Binance returned a future 8-hour candle ({utc_timestamp(newest_open)}); no state was changed."
        )
    if newest_open < expected_latest:
        raise MonitorError(
            "Binance kline data appears stale: newest open is "
            f"{utc_timestamp(newest_open)}, while at least {utc_timestamp(expected_latest)} "
            "is expected to be available. No state was changed."
        )


def ensure_contiguous(candles: Iterable[Candle], first_expected_open: int | None = None) -> None:
    previous_open = first_expected_open - INTERVAL_MS if first_expected_open is not None else None
    for candle in candles:
        if previous_open is not None and candle.open_time_ms != previous_open + INTERVAL_MS:
            raise MonitorError(
                "Missing or out-of-order 8-hour klines between "
                f"{utc_timestamp(previous_open)} and {utc_timestamp(candle.open_time_ms)}; "
                "no signal state was advanced past the gap."
            )
        previous_open = candle.open_time_ms


def empty_state(config: dict[str, Any]) -> MonitorState:
    return MonitorState(
        config=dict(config),
        ema=None,
        target="FLAT",
        last_processed_open_time_ms=None,
        processed_candles=0,
        last_close=None,
    )


def apply_candle(state: MonitorState, candle: Candle) -> CandleEvent:
    if state.last_processed_open_time_ms is not None:
        expected = state.last_processed_open_time_ms + INTERVAL_MS
        if candle.open_time_ms != expected:
            raise MonitorError(
                "New candle is not contiguous with saved state: expected "
                f"{utc_timestamp(expected)}, received {utc_timestamp(candle.open_time_ms)}."
            )
    previous_target = state.target
    state.ema = update_ema(state.ema, candle.close_price)
    state.target = next_target(candle.close_price, state.ema, previous_target)
    state.last_processed_open_time_ms = candle.open_time_ms
    state.processed_candles += 1
    state.last_close = candle.close_price
    distance_pct = (candle.close_price / state.ema - 1.0) * 100.0
    return CandleEvent(candle, state.ema, distance_pct, previous_target, state.target)


def state_to_dict(state: MonitorState) -> dict[str, Any]:
    if (
        state.ema is None
        or state.last_processed_open_time_ms is None
        or state.last_close is None
        or state.processed_candles < 1
    ):
        raise MonitorError("Refusing to persist an uninitialized signal state.")
    return {
        "version": STATE_VERSION,
        "config": state.config,
        "ema": state.ema,
        "target": state.target,
        "last_processed_open_time_ms": state.last_processed_open_time_ms,
        "processed_candles": state.processed_candles,
        "last_close": state.last_close,
    }


def save_state(state: MonitorState, path: Path) -> None:
    """Atomically replace local JSON state after each newly processed candle."""
    path = Path(path).expanduser()
    parent = path.parent
    temp_name: str | None = None
    try:
        parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temp_name = temporary.name
            json.dump(state_to_dict(state), temporary, indent=2, sort_keys=True, allow_nan=False)
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temp_name, path)
        temp_name = None
        try:
            directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # Atomic rename has already succeeded; directory fsync is best-effort.
            pass
    except (OSError, TypeError, ValueError) as exc:
        raise MonitorError(f"Could not atomically write state file {path}: {exc}") from exc
    finally:
        if temp_name is not None:
            try:
                os.unlink(temp_name)
            except OSError:
                pass


def load_state(path: Path, expected_config: dict[str, Any]) -> MonitorState | None:
    path = Path(path).expanduser()
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise MonitorError(
            f"Cannot read state file {path}: {exc}. It was not reset; inspect or restore it explicitly."
        ) from exc
    if not isinstance(payload, dict) or payload.get("version") != STATE_VERSION:
        raise MonitorError(
            f"State file {path} has an unsupported or invalid format. It was not reset."
        )
    if payload.get("config") != expected_config:
        raise MonitorError(
            "Saved state configuration does not match the requested symbol, rule, or base URL. "
            "No state was reset. If you intentionally want a fresh reconstruction, archive the "
            f"state file and start with a new one: {path}"
        )
    try:
        ema = float(payload["ema"])
        last_close = float(payload["last_close"])
        last_open = payload["last_processed_open_time_ms"]
        count = payload["processed_candles"]
        target = payload["target"]
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise MonitorError(f"State file {path} is incomplete or invalid; it was not reset.") from exc
    if (
        not math.isfinite(ema)
        or ema <= 0
        or not math.isfinite(last_close)
        or last_close <= 0
        or isinstance(last_open, bool)
        or not isinstance(last_open, int)
        or last_open < 0
        or isinstance(count, bool)
        or not isinstance(count, int)
        or count < 1
        or target not in {"LONG", "FLAT"}
    ):
        raise MonitorError(f"State file {path} contains invalid values; it was not reset.")
    return MonitorState(
        config=dict(expected_config),
        ema=ema,
        target=target,
        last_processed_open_time_ms=last_open,
        processed_candles=count,
        last_close=last_close,
    )


def retry_after_seconds(header_value: str | None) -> float | None:
    if not header_value:
        return None
    try:
        return max(0.0, float(header_value.strip()))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(header_value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


class BinancePublicClient:
    """Read-only client for the public Spot klines endpoint."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 20.0,
        sleep: Callable[[float], None] = time.sleep,
        stderr: Any = sys.stderr,
    ) -> None:
        self.base_url = normalize_base_url(base_url)
        self.timeout = timeout
        self.sleep = sleep
        self.stderr = stderr

    def fetch_page(self, symbol: str, start_time_ms: int, limit: int = API_PAGE_LIMIT) -> list[Any]:
        query = urllib.parse.urlencode(
            {"symbol": symbol, "interval": INTERVAL, "limit": limit, "startTime": start_time_ms}
        )
        url = f"{self.base_url}/api/v3/klines?{query}"
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            method="GET",
        )
        rate_limit_attempt = 0
        while True:
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                if not isinstance(payload, list):
                    raise MonitorError("Binance returned a non-list klines response.")
                return payload
            except urllib.error.HTTPError as exc:
                if exc.code in {418, 429}:
                    rate_limit_attempt += 1
                    exponential = min(2.0 ** min(rate_limit_attempt - 1, 8), 300.0)
                    server_wait = retry_after_seconds(exc.headers.get("Retry-After"))
                    wait_seconds = max(exponential, server_wait or 0.0)
                    print(
                        f"[rate-limit] Binance HTTP {exc.code}; retrying after "
                        f"{wait_seconds:g}s (Retry-After honored when supplied).",
                        file=self.stderr,
                        flush=True,
                    )
                    self.sleep(wait_seconds)
                    continue
                body = ""
                try:
                    body = exc.read(500).decode("utf-8", errors="replace").strip()
                except OSError:
                    pass
                raise MonitorError(
                    f"Binance klines request failed with HTTP {exc.code} {exc.reason}"
                    + (f": {body}" if body else "")
                ) from exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                raise MonitorError(f"Could not fetch public Binance klines: {exc}") from exc
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise MonitorError(f"Binance returned invalid JSON: {exc}") from exc

    def fetch_all(self, symbol: str, start_time_ms: int = 0) -> list[Candle]:
        cursor = start_time_ms
        candles: list[Candle] = []
        while True:
            rows = self.fetch_page(symbol, cursor, API_PAGE_LIMIT)
            if not rows:
                break
            page = [parse_candle(row) for row in rows]
            if len(page) > API_PAGE_LIMIT:
                raise MonitorError("Binance returned more klines than the requested page limit.")
            if any(page[index].open_time_ms >= page[index + 1].open_time_ms for index in range(len(page) - 1)):
                raise MonitorError("Binance returned duplicate or out-of-order klines.")
            if page[0].open_time_ms < cursor:
                raise MonitorError("Binance returned klines before the requested start time.")
            if candles and page[0].open_time_ms <= candles[-1].open_time_ms:
                raise MonitorError("Kline pagination did not advance monotonically.")
            candles.extend(page)
            if len(page) < API_PAGE_LIMIT:
                break
            cursor = page[-1].open_time_ms + 1
            if page[-1].close_time_ms >= int(time.time() * 1000):
                break
        return candles


class Monitor:
    def __init__(
        self,
        symbol: str,
        base_url: str,
        state_path: Path,
        client: BinancePublicClient,
        stdout: Any = sys.stdout,
        stderr: Any = sys.stderr,
    ) -> None:
        self.symbol = symbol
        self.base_url = normalize_base_url(base_url)
        self.state_path = Path(state_path).expanduser()
        self.client = client
        self.stdout = stdout
        self.stderr = stderr
        self.config = make_config(symbol, self.base_url)
        self.last_no_data_notice_ms: int | None = None
        self.startup_announced = False

    def _say(self, message: str) -> None:
        print(message, file=self.stdout, flush=True)

    def _startup_status(self, state: MonitorState) -> None:
        assert state.ema is not None and state.last_processed_open_time_ms is not None
        self._say(
            f"[startup] {self.symbol} Spot {INTERVAL} UTC | target={state.target} | "
            f"EMA{EMA_PERIOD}={state.ema:.8f} | last close={state.last_close:.8f} | "
            f"last processed candle open={utc_timestamp(state.last_processed_open_time_ms)} | "
            "signal-only (no account access or orders)."
        )

    def _report_no_data(self, now_ms: int, once: bool) -> None:
        if (
            once
            or self.last_no_data_notice_ms is None
            or now_ms - self.last_no_data_notice_ms >= 30 * 60 * 1000
        ):
            last_open = "unknown"
            try:
                saved = load_state(self.state_path, self.config)
                if saved and saved.last_processed_open_time_ms is not None:
                    last_open = utc_timestamp(saved.last_processed_open_time_ms)
            except MonitorError:
                pass
            print(
                "[no-new-data] Binance returned no newer klines; signal state was not changed. "
                f"Last saved candle open={last_open}.",
                file=self.stderr,
                flush=True,
            )
            self.last_no_data_notice_ms = now_ms

    def _print_event(self, event: CandleEvent, catch_up: bool) -> None:
        transition = (
            f"transition {event.previous_target}->{event.target}"
            if event.previous_target != event.target
            else "target retained"
        )
        delayed = " | HISTORICAL CATCH-UP (not a live-at-close alert)" if catch_up else ""
        self._say(
            f"[candle close {utc_timestamp(event.candle.close_time_ms)}] "
            f"close price={event.candle.close_price:.8f} | EMA{EMA_PERIOD}={event.ema:.8f} | "
            f"distance={event.distance_pct:+.3f}% | target={event.target} ({transition}) | "
            "determined at candle close; next observed open is the execution convention; "
            f"signal only, no orders.{delayed}"
        )

    def run_once(self, once: bool = True) -> None:
        state = load_state(self.state_path, self.config)
        if state is None:
            if not self.startup_announced:
                self._say(
                    f"[startup] {self.symbol} Spot {INTERVAL} UTC EMA{EMA_PERIOD}; no local state found. "
                    "Reconstructing from all available completed public klines (this may take several requests)."
                )
                self.startup_announced = True
            start_time = 0
        else:
            if not self.startup_announced:
                self._startup_status(state)
                self.startup_announced = True
            assert state.last_processed_open_time_ms is not None
            start_time = state.last_processed_open_time_ms + 1

        fetched = self.client.fetch_all(self.symbol, start_time)
        now_ms = int(time.time() * 1000)
        if not fetched:
            if state is None:
                raise MonitorError("No historical klines were returned; initialization did not occur.")
            self._report_no_data(now_ms, once)
            return
        validate_freshness(fetched, now_ms)
        if state is not None:
            assert state.last_processed_open_time_ms is not None
            ensure_contiguous(
                fetched,
                first_expected_open=state.last_processed_open_time_ms + INTERVAL_MS,
            )
        finished = completed_candles(fetched, now_ms)

        if state is None:
            if not finished:
                raise MonitorError("No completed historical candles are available; initialization did not occur.")
            ensure_contiguous(finished)
            state = empty_state(self.config)
            for candle in finished:
                apply_candle(state, candle)
            save_state(state, self.state_path)
            self._say(
                f"[initialized] replayed {len(finished)} completed candles from "
                f"{utc_timestamp(finished[0].open_time_ms)} through "
                f"{utc_timestamp(finished[-1].open_time_ms)}; seed=first completed close; "
                f"initial target=FLAT; reconstructed target={state.target}."
            )
            self._startup_status(state)
            return

        if not finished:
            # A currently forming candle was returned and passed the freshness check;
            # there is simply no completed signal to process yet.
            return

        assert state.last_processed_open_time_ms is not None
        if finished[0].open_time_ms <= state.last_processed_open_time_ms:
            raise MonitorError(
                "Binance returned a completed candle at or before the saved timestamp; "
                "no state was changed."
            )
        ensure_contiguous(finished, first_expected_open=state.last_processed_open_time_ms + INTERVAL_MS)
        latest_expected = expected_latest_completed_open(now_ms)
        for candle in finished:
            event = apply_candle(state, candle)
            save_state(state, self.state_path)
            self._print_event(event, catch_up=candle.open_time_ms < latest_expected)

    def run_forever(self, poll_seconds: float) -> None:
        while True:
            try:
                self.run_once(once=False)
            except MonitorError as exc:
                print(f"[error] {exc} No unvalidated candle was advanced.", file=self.stderr, flush=True)
            time.sleep(poll_seconds)


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return parsed


def symbol_value(value: str) -> str:
    normalized = value.upper()
    if not re.fullmatch(r"[A-Z0-9]{1,30}", normalized):
        raise argparse.ArgumentTypeError("symbol must contain 1-30 letters or digits, e.g. BTCUSDT")
    return normalized


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor the frozen Binance Spot 8h EMA300 long/flat target using public klines only. "
            "This script never connects to an account or places orders."
        )
    )
    parser.add_argument("--symbol", type=symbol_value, default="BTCUSDT", help="Spot symbol (default: BTCUSDT)")
    parser.add_argument("--poll-seconds", type=positive_float, default=60.0, help="poll interval (default: 60)")
    parser.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="local JSON state path (default: binance_ema300_state.json beside this script)",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Binance API origin (default: %(default)s)")
    parser.add_argument("--timeout", type=positive_float, default=20.0, help="HTTP request timeout in seconds")
    parser.add_argument("--once", action="store_true", help="fetch/process once, then exit")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        base_url = normalize_base_url(args.base_url)
    except ValueError as exc:
        parser.error(str(exc))
    state_path = args.state_file or Path(__file__).resolve().with_name("binance_ema300_state.json")
    client = BinancePublicClient(base_url=base_url, timeout=args.timeout)
    monitor = Monitor(args.symbol, base_url, state_path, client)
    try:
        if args.once:
            monitor.run_once(once=True)
        else:
            monitor.run_forever(args.poll_seconds)
    except KeyboardInterrupt:
        print("\nStopped; local signal state is preserved.", file=sys.stderr, flush=True)
        return 130
    except MonitorError as exc:
        print(f"[error] {exc}", file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
