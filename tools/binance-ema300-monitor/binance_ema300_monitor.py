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

from signal_csv import append_evaluations, evaluation_row

INTERVAL = "8h"
INTERVAL_MS = 8 * 60 * 60 * 1000
DAILY_INTERVAL = "1d"
DAILY_INTERVAL_MS = 24 * 60 * 60 * 1000
INTERVAL_MILLISECONDS = {INTERVAL: INTERVAL_MS, DAILY_INTERVAL: DAILY_INTERVAL_MS}
EMA_PERIOD = 300
MIN_RECONSTRUCTION_CANDLES = EMA_PERIOD
LONG_MULTIPLIER = 1.02
FLAT_MULTIPLIER = 0.97
API_PAGE_LIMIT = 1000
STATE_VERSION = 1
ADDITIONAL_STATE_VERSION = 1
DEFAULT_BASE_URL = "https://api.binance.com"
DEFAULT_MARKET_DATA_BASE_URL = "https://data-api.binance.vision"
USER_AGENT = "binance-ema300-signal-monitor/1.0"


class MonitorError(Exception):
    """A safe-to-display configuration, data, or network error."""


class PartialDurationCandleError(MonitorError):
    """A kline closes before its nominal interval ends."""

    def __init__(
        self,
        open_time_ms: int,
        close_time_ms: int,
        expected_close_time_ms: int,
        interval_label: str = "8-hour",
    ):
        self.open_time_ms = open_time_ms
        self.close_time_ms = close_time_ms
        self.expected_close_time_ms = expected_close_time_ms
        super().__init__(
            f"Kline at {utc_timestamp(open_time_ms)} has an early close timestamp "
            f"{utc_timestamp(close_time_ms)}; a full {interval_label} bar would close at "
            f"{utc_timestamp(expected_close_time_ms)}."
        )


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


@dataclass
class AdditionalSignalState:
    config: dict[str, Any]
    donchian_target: str
    last_8h_open_time_ms: int
    recent_8h_closes: list[float]
    daily_ema6: float
    daily_ema24: float
    daily_target: str
    last_daily_open_time_ms: int


@dataclass(frozen=True)
class AdditionalSignalEvent:
    strategy: str
    interval: str
    candle: Candle
    previous_target: str
    target: str
    details: dict[str, float]


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


def interval_milliseconds(interval: str) -> int:
    try:
        return INTERVAL_MILLISECONDS[interval]
    except KeyError as exc:
        raise ValueError(f"unsupported Binance candle interval: {interval}") from exc


def parse_candle(row: Any, interval: str = INTERVAL) -> Candle:
    if not isinstance(row, list) or len(row) < 7:
        raise MonitorError("Binance returned a malformed kline row (expected at least 7 fields).")
    interval_ms = interval_milliseconds(interval)
    interval_label = "8-hour" if interval == INTERVAL else "daily"
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
    if open_time % interval_ms != 0:
        raise MonitorError(
            f"Kline open time {open_time} is not aligned to a {interval_label} UTC boundary."
        )
    expected_close_time = open_time + interval_ms - 1
    if close_time < expected_close_time:
        raise PartialDurationCandleError(
            open_time,
            close_time,
            expected_close_time,
            interval_label=interval_label,
        )
    if close_time > expected_close_time:
        raise MonitorError(
            f"Kline at {utc_timestamp(open_time)} has an unexpected {interval_label} close timestamp; "
            "refusing to process it."
        )
    if not math.isfinite(close) or close <= 0:
        raise MonitorError("Binance returned a non-positive or non-finite close price.")
    return Candle(open_time, close, close_time)


def completed_candles(candles: Iterable[Candle], now_ms: int) -> list[Candle]:
    """Return only candles whose inclusive close timestamp is strictly in the past."""
    return [candle for candle in candles if candle.close_time_ms < now_ms]


def expected_latest_completed_open(now_ms: int, interval: str = INTERVAL) -> int:
    interval_ms = interval_milliseconds(interval)
    current_open = (now_ms // interval_ms) * interval_ms
    return current_open - interval_ms


def validate_freshness(candles: list[Candle], now_ms: int, interval: str = INTERVAL) -> None:
    if not candles:
        raise MonitorError("Binance returned no klines; no state was changed.")
    newest_open = candles[-1].open_time_ms
    interval_ms = interval_milliseconds(interval)
    current_open = (now_ms // interval_ms) * interval_ms
    expected_latest = current_open - interval_ms
    interval_label = "8-hour" if interval == INTERVAL else "daily"
    if newest_open > current_open:
        raise MonitorError(
            f"Binance returned a future {interval_label} candle ({utc_timestamp(newest_open)}); "
            "no state was changed."
        )
    if newest_open < expected_latest:
        raise MonitorError(
            f"Binance {interval_label} kline data appears stale: newest open is "
            f"{utc_timestamp(newest_open)}, while at least {utc_timestamp(expected_latest)} "
            "is expected to be available. No state was changed."
        )


def ensure_contiguous(
    candles: Iterable[Candle],
    first_expected_open: int | None = None,
    interval_ms: int = INTERVAL_MS,
) -> None:
    previous_open = first_expected_open - interval_ms if first_expected_open is not None else None
    for candle in candles:
        if previous_open is not None and candle.open_time_ms != previous_open + interval_ms:
            raise MonitorError(
                "Missing or out-of-order klines between "
                f"{utc_timestamp(previous_open)} and {utc_timestamp(candle.open_time_ms)}; "
                "no signal state was advanced past the gap."
            )
        previous_open = candle.open_time_ms


def historical_gap_ranges(
    candles: Iterable[Candle], interval_ms: int = INTERVAL_MS
) -> list[tuple[int, int, int]]:
    """Return (previous open, next open, missing intervals) without filling gaps."""
    gaps: list[tuple[int, int, int]] = []
    previous_open: int | None = None
    for candle in candles:
        current_open = candle.open_time_ms
        if previous_open is not None:
            delta = current_open - previous_open
            if delta <= 0:
                raise MonitorError("Historical klines are duplicate or out of order; no state was changed.")
            if delta % interval_ms:
                raise MonitorError("Historical klines are not separated by whole candle intervals; no state was changed.")
            missing = delta // interval_ms - 1
            if missing:
                gaps.append((previous_open, current_open, missing))
        previous_open = current_open
    return gaps


def empty_state(config: dict[str, Any]) -> MonitorState:
    return MonitorState(
        config=dict(config),
        ema=None,
        target="FLAT",
        last_processed_open_time_ms=None,
        processed_candles=0,
        last_close=None,
    )


def apply_candle(
    state: MonitorState,
    candle: Candle,
    *,
    require_contiguous: bool = True,
) -> CandleEvent:
    if require_contiguous and state.last_processed_open_time_ms is not None:
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


def make_additional_config(symbol: str, base_url: str) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "base_url": base_url,
        "donchian_entry_lookback": 20,
        "donchian_exit_lookback": 10,
        "daily_ema_fast": 6,
        "daily_ema_slow": 24,
        "ema_method": "recursive adjust=False, seeded by first available completed close",
    }


def additional_state_to_dict(state: AdditionalSignalState) -> dict[str, Any]:
    return {
        "version": ADDITIONAL_STATE_VERSION,
        "config": state.config,
        "donchian_target": state.donchian_target,
        "last_8h_open_time_ms": state.last_8h_open_time_ms,
        "recent_8h_closes": state.recent_8h_closes,
        "daily_ema6": state.daily_ema6,
        "daily_ema24": state.daily_ema24,
        "daily_target": state.daily_target,
        "last_daily_open_time_ms": state.last_daily_open_time_ms,
    }


def save_additional_state(state: AdditionalSignalState, path: Path) -> None:
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
            json.dump(
                additional_state_to_dict(state),
                temporary,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            temporary.write("\n")
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temp_name, path)
        temp_name = None
    except (OSError, TypeError, ValueError) as exc:
        raise MonitorError(f"Could not atomically write additional signal state {path}: {exc}") from exc
    finally:
        if temp_name is not None:
            try:
                os.unlink(temp_name)
            except OSError:
                pass


def load_additional_state(
    path: Path, expected_config: dict[str, Any]
) -> AdditionalSignalState | None:
    path = Path(path).expanduser()
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise MonitorError(
            f"Cannot read additional signal state {path}: {exc}. It was not reset; inspect or restore it explicitly."
        ) from exc
    if not isinstance(payload, dict) or payload.get("version") != ADDITIONAL_STATE_VERSION:
        raise MonitorError(f"Additional signal state {path} has an unsupported or invalid format; it was not reset.")
    if payload.get("config") != expected_config:
        raise MonitorError(
            "Saved additional signal state does not match the requested symbol, rules, or API origin. "
            f"No state was reset: {path}"
        )
    try:
        recent_closes = [float(value) for value in payload["recent_8h_closes"]]
        state = AdditionalSignalState(
            config=dict(expected_config),
            donchian_target=payload["donchian_target"],
            last_8h_open_time_ms=payload["last_8h_open_time_ms"],
            recent_8h_closes=recent_closes,
            daily_ema6=float(payload["daily_ema6"]),
            daily_ema24=float(payload["daily_ema24"]),
            daily_target=payload["daily_target"],
            last_daily_open_time_ms=payload["last_daily_open_time_ms"],
        )
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise MonitorError(f"Additional signal state {path} is incomplete or invalid; it was not reset.") from exc
    if (
        not isinstance(state.donchian_target, str)
        or state.donchian_target not in {"BTC", "CASH"}
        or not isinstance(state.daily_target, str)
        or state.daily_target not in {"BTC", "CASH"}
        or isinstance(state.last_8h_open_time_ms, bool)
        or not isinstance(state.last_8h_open_time_ms, int)
        or state.last_8h_open_time_ms < 0
        or isinstance(state.last_daily_open_time_ms, bool)
        or not isinstance(state.last_daily_open_time_ms, int)
        or state.last_daily_open_time_ms < 0
        or len(state.recent_8h_closes) > 20
        or any(not math.isfinite(close) or close <= 0 for close in state.recent_8h_closes)
        or not math.isfinite(state.daily_ema6)
        or state.daily_ema6 <= 0
        or not math.isfinite(state.daily_ema24)
        or state.daily_ema24 <= 0
    ):
        raise MonitorError(f"Additional signal state {path} contains invalid values; it was not reset.")
    return state


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
        self.last_skipped_historical_rows = 0

    def fetch_page(
        self,
        symbol: str,
        start_time_ms: int,
        limit: int = API_PAGE_LIMIT,
        interval: str = INTERVAL,
    ) -> list[Any]:
        query = urllib.parse.urlencode(
            {"symbol": symbol, "interval": interval, "limit": limit, "startTime": start_time_ms}
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
                if exc.code == 451 and self.base_url == DEFAULT_BASE_URL:
                    fallback_url = url.replace(DEFAULT_BASE_URL, DEFAULT_MARKET_DATA_BASE_URL, 1)
                    fallback_request = urllib.request.Request(
                        fallback_url,
                        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
                        method="GET",
                    )
                    try:
                        with urllib.request.urlopen(fallback_request, timeout=self.timeout) as response:
                            fallback_payload = json.loads(response.read().decode("utf-8"))
                    except (
                        urllib.error.URLError,
                        TimeoutError,
                        OSError,
                        UnicodeDecodeError,
                        json.JSONDecodeError,
                    ) as fallback_exc:
                        raise MonitorError(
                            "Binance api.binance.com returned HTTP 451 and the public market-data mirror "
                            f"fallback failed: {fallback_exc}"
                        ) from fallback_exc
                    if not isinstance(fallback_payload, list):
                        raise MonitorError("Binance market-data mirror returned a non-list klines response.")
                    print(
                        "[endpoint-fallback] api.binance.com returned HTTP 451; using Binance's "
                        "public market-data mirror for the same read-only klines request.",
                        file=self.stderr,
                        flush=True,
                    )
                    return fallback_payload
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

    def fetch_all(
        self,
        symbol: str,
        start_time_ms: int = 0,
        *,
        allow_historical_short_rows: bool = False,
        interval: str = INTERVAL,
    ) -> list[Candle]:
        cursor = start_time_ms
        candles: list[Candle] = []
        previous_raw_open: int | None = None
        skipped_historical_rows = 0
        self.last_skipped_historical_rows = 0
        reconstruction_cutoff = (
            expected_latest_completed_open(int(time.time() * 1000), interval)
            if allow_historical_short_rows
            else None
        )
        while True:
            if interval == INTERVAL:
                rows = self.fetch_page(symbol, cursor, API_PAGE_LIMIT)
            else:
                rows = self.fetch_page(symbol, cursor, API_PAGE_LIMIT, interval)
            if not rows:
                break
            if len(rows) > API_PAGE_LIMIT:
                raise MonitorError("Binance returned more klines than the requested page limit.")
            last_row_close_time: int | None = None
            for row in rows:
                candle: Candle | None
                try:
                    candle = parse_candle(row, interval)
                except PartialDurationCandleError as exc:
                    row_open_time = exc.open_time_ms
                    if (
                        reconstruction_cutoff is None
                        or row_open_time >= reconstruction_cutoff
                    ):
                        raise
                    candle = None
                    skipped_historical_rows += 1
                    self.last_skipped_historical_rows = skipped_historical_rows
                    interval_label = "8-hour" if interval == INTERVAL else "daily"
                    row_noun = "bar" if interval == INTERVAL else "candle"
                    print(
                        f"[history-warning] skipped short-duration historical {interval_label} kline at "
                        f"{utc_timestamp(exc.open_time_ms)}: closeTime="
                        f"{utc_timestamp(exc.close_time_ms)}, expected="
                        f"{utc_timestamp(exc.expected_close_time_ms)}; it is not treated as a "
                        f"completed full {row_noun}.",
                        file=self.stderr,
                        flush=True,
                    )
                else:
                    row_open_time = candle.open_time_ms
                row_close_time = int(row[6])
                if row_open_time < cursor:
                    raise MonitorError("Binance returned klines before the requested start time.")
                if previous_raw_open is not None and row_open_time <= previous_raw_open:
                    raise MonitorError("Binance returned duplicate or out-of-order klines.")
                previous_raw_open = row_open_time
                last_row_close_time = row_close_time
                if candle is not None:
                    candles.append(candle)
            if len(rows) < API_PAGE_LIMIT:
                break
            assert previous_raw_open is not None
            cursor = previous_raw_open + 1
            if last_row_close_time is not None and last_row_close_time >= int(time.time() * 1000):
                break
        return candles


def next_donchian_target(
    close: float, prior_closes: list[float], previous_target: str
) -> tuple[str, float | None, float | None]:
    if previous_target not in {"BTC", "CASH"}:
        raise ValueError("previous Donchian target must be BTC or CASH")
    if not math.isfinite(close) or close <= 0:
        raise ValueError("close must be a finite positive number")
    if len(prior_closes) < 20:
        return previous_target, None, None
    highest_prior20 = max(prior_closes[-20:])
    lowest_prior10 = min(prior_closes[-10:])
    if close > highest_prior20:
        return "BTC", highest_prior20, lowest_prior10
    if close < lowest_prior10:
        return "CASH", highest_prior20, lowest_prior10
    return previous_target, highest_prior20, lowest_prior10


def next_daily_ema_target(ema6: float, ema24: float) -> str:
    if not math.isfinite(ema6) or ema6 <= 0 or not math.isfinite(ema24) or ema24 <= 0:
        raise ValueError("daily EMA values must be finite and positive")
    return "BTC" if ema6 > ema24 else "CASH"


class AdditionalSignalMonitor:
    """Tracks Donchian 20/10 on 8h candles and EMA6/24 on UTC daily candles."""

    def __init__(
        self,
        symbol: str,
        base_url: str,
        state_path: Path,
        client: BinancePublicClient,
        stdout: Any = sys.stdout,
        stderr: Any = sys.stderr,
        results_path: Path | None = None,
    ) -> None:
        self.symbol = symbol
        self.base_url = normalize_base_url(base_url)
        self.state_path = Path(state_path).expanduser()
        self.client = client
        self.stdout = stdout
        self.stderr = stderr
        self.config = make_additional_config(symbol, self.base_url)
        self.results_path = results_path or self.state_path.with_name("additional_signal_results.csv")

    def _say(self, message: str) -> None:
        print(message, file=self.stdout, flush=True)

    def _initialize(self, now_ms: int) -> AdditionalSignalState:
        history8h = self.client.fetch_all(
            self.symbol, 0, allow_historical_short_rows=True, interval=INTERVAL
        )
        daily_history = self.client.fetch_all(
            self.symbol,
            0,
            allow_historical_short_rows=True,
            interval=DAILY_INTERVAL,
        )
        validate_freshness(history8h, now_ms, INTERVAL)
        validate_freshness(daily_history, now_ms, DAILY_INTERVAL)
        completed8h = completed_candles(history8h, now_ms)
        completed_daily = completed_candles(daily_history, now_ms)
        if len(completed8h) < 21:
            raise MonitorError("At least 21 completed 8-hour candles are required to initialize Donchian 20/10.")
        if len(completed_daily) < 24:
            raise MonitorError("At least 24 completed UTC daily candles are required to initialize EMA6/24.")

        target8h = "CASH"
        recent_closes: list[float] = []
        for candle in completed8h:
            target8h, _, _ = next_donchian_target(candle.close_price, recent_closes, target8h)
            recent_closes = (recent_closes + [candle.close_price])[-20:]

        ema6: float | None = None
        ema24: float | None = None
        for candle in completed_daily:
            ema6 = update_ema(ema6, candle.close_price, period=6)
            ema24 = update_ema(ema24, candle.close_price, period=24)
        assert ema6 is not None and ema24 is not None
        state = AdditionalSignalState(
            config=self.config,
            donchian_target=target8h,
            last_8h_open_time_ms=completed8h[-1].open_time_ms,
            recent_8h_closes=recent_closes,
            daily_ema6=ema6,
            daily_ema24=ema24,
            daily_target=next_daily_ema_target(ema6, ema24),
            last_daily_open_time_ms=completed_daily[-1].open_time_ms,
        )
        save_additional_state(state, self.state_path)
        print(
            f"[additional-signals-baseline] Donchian20/10={state.donchian_target}; "
            f"daily EMA6/24={state.daily_target} (EMA6={ema6:.8f}, EMA24={ema24:.8f}); "
            "historical signals were used only to initialize state and were not reported.",
            file=self.stderr,
            flush=True,
        )
        return state

    def _report_event(self, event: AdditionalSignalEvent) -> None:
        action = "ENTER BTC" if event.target == "BTC" else "EXIT TO CASH"
        effective_open = event.candle.close_time_ms + 1
        detail_text = " | ".join(f"{key}={value:.8f}" for key, value in event.details.items())
        self._say(
            f"[new-signal][{event.strategy}] {action} | "
            f"candle close UTC={utc_timestamp(event.candle.close_time_ms)} | "
            f"close={event.candle.close_price:.8f} | "
            f"target {event.previous_target}->{event.target} | {detail_text} | "
            f"effective from {utc_timestamp(effective_open)} UTC; long/cash only, no leverage or orders."
        )

    def run_once(self) -> list[AdditionalSignalEvent]:
        now_ms = int(time.time() * 1000)
        state = load_additional_state(self.state_path, self.config)
        if state is None:
            self._initialize(now_ms)
            return []

        fetched8h = self.client.fetch_all(
            self.symbol,
            state.last_8h_open_time_ms + 1,
            interval=INTERVAL,
        )
        fetched_daily = self.client.fetch_all(
            self.symbol,
            state.last_daily_open_time_ms + 1,
            interval=DAILY_INTERVAL,
        )
        if fetched8h:
            validate_freshness(fetched8h, now_ms, INTERVAL)
        if fetched_daily:
            validate_freshness(fetched_daily, now_ms, DAILY_INTERVAL)
        new8h = completed_candles(fetched8h, now_ms)
        new_daily = completed_candles(fetched_daily, now_ms)
        if new8h:
            ensure_contiguous(
                new8h,
                first_expected_open=state.last_8h_open_time_ms + INTERVAL_MS,
                interval_ms=INTERVAL_MS,
            )
        if new_daily:
            ensure_contiguous(
                new_daily,
                first_expected_open=state.last_daily_open_time_ms + DAILY_INTERVAL_MS,
                interval_ms=DAILY_INTERVAL_MS,
            )

        events: list[AdditionalSignalEvent] = []
        evaluations: list[AdditionalSignalEvent] = []
        target8h = state.donchian_target
        recent_closes = list(state.recent_8h_closes)
        for candle in new8h:
            previous_target = target8h
            target8h, highest_prior20, lowest_prior10 = next_donchian_target(
                candle.close_price, recent_closes, target8h
            )
            assert highest_prior20 is not None and lowest_prior10 is not None
            evaluation = AdditionalSignalEvent(
                strategy="donchian-8h-20-10", interval=INTERVAL, candle=candle,
                previous_target=previous_target, target=target8h,
                details={"highest_close_prior20": highest_prior20,
                         "lowest_close_prior10": lowest_prior10},
            )
            evaluations.append(evaluation)
            if target8h != previous_target:
                events.append(evaluation)
            recent_closes = (recent_closes + [candle.close_price])[-20:]

        ema6 = state.daily_ema6
        ema24 = state.daily_ema24
        target_daily = state.daily_target
        for candle in new_daily:
            ema6 = update_ema(ema6, candle.close_price, period=6)
            ema24 = update_ema(ema24, candle.close_price, period=24)
            previous_target = target_daily
            target_daily = next_daily_ema_target(ema6, ema24)
            evaluation = AdditionalSignalEvent(
                strategy="daily-ema6-24", interval=DAILY_INTERVAL, candle=candle,
                previous_target=previous_target, target=target_daily,
                details={"ema6": ema6, "ema24": ema24},
            )
            evaluations.append(evaluation)
            if target_daily != previous_target:
                events.append(evaluation)

        if new8h or new_daily:
            updated = AdditionalSignalState(
                config=self.config,
                donchian_target=target8h,
                last_8h_open_time_ms=(new8h[-1].open_time_ms if new8h else state.last_8h_open_time_ms),
                recent_8h_closes=recent_closes,
                daily_ema6=ema6,
                daily_ema24=ema24,
                daily_target=target_daily,
                last_daily_open_time_ms=(new_daily[-1].open_time_ms if new_daily else state.last_daily_open_time_ms),
            )
            try:
                append_evaluations(self.results_path, [
                    evaluation_row(event, self.symbol, utc_timestamp) for event in evaluations
                ])
            except (OSError, ValueError) as exc:
                raise MonitorError(f"Could not journal additional signals: {exc}; state not advanced") from exc
            save_additional_state(updated, self.state_path)
        for event in events:
            self._report_event(event)
        return events


class Monitor:
    def __init__(
        self,
        symbol: str,
        base_url: str,
        state_path: Path,
        client: BinancePublicClient,
        stdout: Any = sys.stdout,
        stderr: Any = sys.stderr,
        additional_state_path: Path | None = None,
        enable_additional_signals: bool = True,
        additional_client: BinancePublicClient | None = None,
        additional_base_url: str | None = None,
        additional_results_path: Path | None = None,
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
        self.additional_monitor = (
            AdditionalSignalMonitor(
                symbol,
                normalize_base_url(additional_base_url or self.base_url),
                additional_state_path
                or self.state_path.with_name("binance_additional_signals_state.json"),
                additional_client or client,
                stdout=stdout,
                stderr=stderr,
                results_path=additional_results_path,
            )
            if enable_additional_signals
            else None
        )

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
        ema_error: MonitorError | None = None
        additional_error: MonitorError | None = None
        try:
            self._run_ema300_once(once)
        except MonitorError as exc:
            ema_error = exc
        if self.additional_monitor is not None:
            try:
                self.additional_monitor.run_once()
            except MonitorError as exc:
                additional_error = exc
        if ema_error is not None and additional_error is not None:
            raise MonitorError(
                f"EMA300 check failed: {ema_error}; additional signal check failed: {additional_error}"
            ) from ema_error
        if ema_error is not None:
            raise ema_error
        if additional_error is not None:
            raise MonitorError(f"Additional signal check failed: {additional_error}") from additional_error

    def _run_ema300_once(self, once: bool = True) -> None:
        state = load_state(self.state_path, self.config)
        if state is None:
            if not self.startup_announced:
                self._say(
                    f"[startup] {self.symbol} Spot {INTERVAL} UTC EMA{EMA_PERIOD}; no local state found. "
                    "Reconstructing from all available completed public klines (this may take several requests)."
                )
                self._say(
                    "[history-policy] first run only: skip historical klines with an early closeTime "
                    "(never process them as full bars); preserve gaps without synthesizing candles; "
                    f"require at least {MIN_RECONSTRUCTION_CANDLES} valid completed bars. "
                    "Saved-state updates remain strict and contiguous."
                )
                self.startup_announced = True
            start_time = 0
        else:
            if not self.startup_announced:
                self._startup_status(state)
                self.startup_announced = True
            assert state.last_processed_open_time_ms is not None
            start_time = state.last_processed_open_time_ms + 1

        fetched = self.client.fetch_all(
            self.symbol,
            start_time,
            allow_historical_short_rows=state is None,
        )
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
            if len(finished) < MIN_RECONSTRUCTION_CANDLES:
                raise MonitorError(
                    f"Only {len(finished)} valid full-duration completed 8-hour candles are available; "
                    f"at least {MIN_RECONSTRUCTION_CANDLES} are required for EMA{EMA_PERIOD} history. "
                    "No state was changed."
                )
            gaps = historical_gap_ranges(finished)
            skipped = getattr(self.client, "last_skipped_historical_rows", 0)
            if skipped:
                self._say(
                    f"[history-warning] omitted {skipped} early-close historical row(s); none was used "
                    "as a candle or included in the EMA."
                )
            if gaps:
                missing_total = sum(gap[2] for gap in gaps)
                self._say(
                    f"[history-warning] {missing_total} missing 8-hour interval(s) across {len(gaps)} "
                    "historical gap(s); no candles were synthesized. The EMA uses only valid returned bars."
                )
                for previous_open, next_open, missing in gaps[:10]:
                    self._say(
                        f"[history-gap] {missing} missing open(s) between "
                        f"{utc_timestamp(previous_open)} and {utc_timestamp(next_open)}."
                    )
                if len(gaps) > 10:
                    self._say(f"[history-gap] {len(gaps) - 10} additional gap(s) not listed individually.")
            state = empty_state(self.config)
            for candle in finished:
                apply_candle(state, candle, require_contiguous=False)
            save_state(state, self.state_path)
            self._say(
                f"[initialized] replayed {len(finished)} completed candles from "
                f"{utc_timestamp(finished[0].open_time_ms)} through "
                f"{utc_timestamp(finished[-1].open_time_ms)}; seed=first completed close; "
                f"initial target=FLAT; reconstructed target={state.target}; "
                f"valid bars={len(finished)}; gaps={len(gaps)}."
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
            "Monitor three independent Binance Spot BTCUSDT signal rules using public klines only. "
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
    parser.add_argument(
        "--additional-state-file",
        type=Path,
        default=None,
        help="state path for Donchian 20/10 and daily EMA6/24 (default: alongside EMA300 state)",
    )
    parser.add_argument(
        "--additional-results-file", type=Path, default=None,
        help="CSV journal of all new Donchian/daily evaluations (default: beside additional state)",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Binance API origin (default: %(default)s)")
    parser.add_argument(
        "--additional-base-url",
        default=None,
        help="Binance public market-data origin for Donchian/daily rules (default: data-api mirror)",
    )
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
    additional_state_path = args.additional_state_file or state_path.with_name(
        "binance_additional_signals_state.json"
    )
    client = BinancePublicClient(base_url=base_url, timeout=args.timeout)
    additional_base_url = normalize_base_url(
        args.additional_base_url
        or (DEFAULT_MARKET_DATA_BASE_URL if base_url == DEFAULT_BASE_URL else base_url)
    )
    additional_client = BinancePublicClient(
        base_url=additional_base_url,
        timeout=args.timeout,
    )
    monitor = Monitor(
        args.symbol,
        base_url,
        state_path,
        client,
        additional_state_path=additional_state_path,
        additional_client=additional_client,
        additional_base_url=additional_base_url,
        additional_results_path=args.additional_results_file,
    )
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
