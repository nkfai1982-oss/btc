"""Per-strategy simulated portfolio tracking for completed Binance candles.

The ledger models a long-or-cash BTCUSDT strategy, starts with a fixed USDT
balance, and applies a signal produced at one candle close at the next candle
open. It is a simulation only: it does not inspect accounts or place orders.
"""
from __future__ import annotations

import csv
import fcntl
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

PORTFOLIO_FIELDS = [
    "record_type",
    "strategy",
    "symbol",
    "interval",
    "candle_open_utc",
    "candle_close_utc",
    "close_usdt",
    "previous_target",
    "signal_target",
    "signal_changed",
    "effective_from_utc",
    "execution_action",
    "execution_price_usdt",
    "position_at_close",
    "cash_usdt",
    "btc_units",
    "portfolio_value_usdt",
    "starting_capital_usdt",
    "profit_loss_usdt",
    "total_return_pct",
    "period_return_pct",
    "rule_metric_1_name",
    "rule_metric_1_value",
    "rule_metric_2_name",
    "rule_metric_2_value",
]

STRATEGIES = (
    "ema300-8h",
    "donchian-8h-20-10",
    "daily-ema6-24",
)
STATE_VERSION = 1


def utc_timestamp(epoch_ms: int) -> str:
    precision = "milliseconds" if epoch_ms % 1000 else "seconds"
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat(
        timespec=precision
    ).replace("+00:00", "Z")


def _timestamp_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Portfolio timestamps must include a timezone")
    return int(parsed.timestamp() * 1000)


def _interval_ms(interval: str) -> int:
    if interval == "8h":
        return 8 * 60 * 60 * 1000
    if interval == "1d":
        return 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported portfolio interval: {interval}")


def _positive_finite(value: Any, label: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} must be a finite positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{label} must be a finite positive number")
    return parsed


def _exposure(target: str) -> str:
    if target in {"LONG", "BTC"}:
        return "BTC"
    if target in {"FLAT", "CASH"}:
        return "CASH"
    raise ValueError(f"Unsupported signal target for portfolio tracking: {target!r}")


def _fmt(value: float, places: int = 8) -> str:
    if not math.isfinite(value):
        raise ValueError("Refusing to write a non-finite portfolio value")
    return f"{value:.{places}f}"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != PORTFOLIO_FIELDS:
            raise ValueError(f"Unexpected portfolio CSV schema in {path}; refusing to overwrite")
        return list(reader)


def append_portfolio_rows(path: Path, rows: list[dict[str, str]]) -> int:
    """Append idempotently under a file lock and atomically replace the CSV."""
    if not rows:
        return 0
    path = Path(path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_name("." + path.name + ".lock").open("a", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        existing = _read_csv(path)
        key = lambda row: (row["record_type"], row["strategy"], row["candle_close_utc"])
        existing_by_key = {key(row): row for row in existing}
        added: list[dict[str, str]] = []
        for row in rows:
            if set(row) != set(PORTFOLIO_FIELDS):
                raise ValueError("Portfolio row does not match CSV schema")
            row_key = key(row)
            saved = existing_by_key.get(row_key)
            if saved is not None:
                if saved != row:
                    raise ValueError(
                        f"Conflicting portfolio row for {row['strategy']} at {row['candle_close_utc']}"
                    )
                continue
            existing_by_key[row_key] = row
            added.append(row)
        if not added:
            return 0
        temporary_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                newline="",
                encoding="utf-8",
                dir=path.parent,
                delete=False,
            ) as handle:
                temporary_name = handle.name
                writer = csv.DictWriter(handle, fieldnames=PORTFOLIO_FIELDS, lineterminator="\n")
                writer.writeheader()
                writer.writerows(existing + added)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, path)
            temporary_name = None
        finally:
            if temporary_name is not None:
                try:
                    os.unlink(temporary_name)
                except OSError:
                    pass
        return len(added)


class PortfolioJournal:
    """Own three independent portfolio states and write one CSV per strategy."""

    def __init__(
        self,
        state_path: Path,
        csv_paths: Mapping[str, Path],
        starting_capital_usdt: float = 1000.0,
    ) -> None:
        self.state_path = Path(state_path).expanduser()
        self.csv_paths = {name: Path(path).expanduser() for name, path in csv_paths.items()}
        if set(self.csv_paths) != set(STRATEGIES):
            raise ValueError(f"Exactly these strategy CSVs are required: {', '.join(STRATEGIES)}")
        self.starting_capital_usdt = _positive_finite(starting_capital_usdt, "starting capital")
        self._state: dict[str, Any] | None = None

    @classmethod
    def alongside(
        cls,
        state_file: Path,
        *,
        portfolio_state_file: Path | None = None,
        ema300_file: Path | None = None,
        donchian_file: Path | None = None,
        daily_ema_file: Path | None = None,
        starting_capital_usdt: float = 1000.0,
    ) -> "PortfolioJournal":
        directory = Path(state_file).expanduser().parent
        return cls(
            portfolio_state_file or directory / "binance_portfolio_state.json",
            {
                "ema300-8h": ema300_file or directory / "ema300_portfolio.csv",
                "donchian-8h-20-10": donchian_file or directory / "donchian_20_10_portfolio.csv",
                "daily-ema6-24": daily_ema_file or directory / "daily_ema6_24_portfolio.csv",
            },
            starting_capital_usdt,
        )

    @property
    def is_initialized(self) -> bool:
        return self._state is not None or self.state_path.exists()

    def _load_state(self, symbol: str) -> dict[str, Any]:
        try:
            with self.state_path.open(encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Cannot read portfolio state {self.state_path}: {exc}") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("version") != STATE_VERSION
            or payload.get("symbol") != symbol
            or payload.get("starting_capital_usdt") != self.starting_capital_usdt
            or not isinstance(payload.get("strategies"), dict)
            or set(payload["strategies"]) != set(STRATEGIES)
        ):
            raise ValueError(f"Portfolio state {self.state_path} is invalid or has mismatched settings")
        for strategy in STRATEGIES:
            item = payload["strategies"].get(strategy)
            if not isinstance(item, dict):
                raise ValueError(f"Portfolio state for {strategy} is invalid")
            _positive_finite(item.get("last_price_usdt"), f"{strategy} last price")
            _positive_finite(item.get("equity_usdt"), f"{strategy} equity")
            cash = float(item.get("cash_usdt", -1))
            units = float(item.get("btc_units", -1))
            if not math.isfinite(cash) or cash < 0 or not math.isfinite(units) or units < 0:
                raise ValueError(f"Portfolio balances for {strategy} are invalid")
            if not math.isclose(
                cash + units * float(item["last_price_usdt"]),
                float(item["equity_usdt"]),
                rel_tol=1e-9,
                abs_tol=1e-6,
            ):
                raise ValueError(f"Portfolio balances and equity disagree for {strategy}")
            if item.get("position_target") not in {"BTC", "CASH"}:
                raise ValueError(f"Portfolio state position for {strategy} is invalid")
            if item.get("pending_signal_target") not in {"LONG", "FLAT", "BTC", "CASH"}:
                raise ValueError(f"Portfolio state pending signal for {strategy} is invalid")
        return payload

    def _save_state(self, payload: dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.state_path.parent,
                prefix=f".{self.state_path.name}.", suffix=".tmp", delete=False
            ) as handle:
                temporary_name = handle.name
                json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, self.state_path)
            temporary_name = None
        except (OSError, TypeError, ValueError) as exc:
            raise ValueError(f"Could not atomically write portfolio state {self.state_path}: {exc}") from exc
        finally:
            if temporary_name is not None:
                try:
                    os.unlink(temporary_name)
                except OSError:
                    pass

    def _baseline_row(
        self,
        strategy: str,
        symbol: str,
        interval: str,
        target: str,
        price: float,
        time_ms: int,
        cash: float,
        btc_units: float,
        exposure: str,
    ) -> dict[str, str]:
        capital = self.starting_capital_usdt
        return dict(zip(PORTFOLIO_FIELDS, [
            "STARTING_BALANCE",
            strategy,
            symbol,
            interval,
            "",
            utc_timestamp(time_ms),
            _fmt(price),
            "",
            target,
            "false",
            utc_timestamp(time_ms + 1),
            "INITIALIZE_BTC" if exposure == "BTC" else "INITIALIZE_CASH",
            _fmt(price) if exposure == "BTC" else "",
            exposure,
            _fmt(cash),
            _fmt(btc_units, 12),
            _fmt(capital),
            _fmt(capital),
            _fmt(0.0),
            _fmt(0.0),
            "",
            "",
            "",
            "",
            "",
        ]))

    def ensure_started(
        self,
        symbol: str,
        interval_by_strategy: Mapping[str, str],
        targets: Mapping[str, str],
        baseline_price_usdt: float,
        baseline_time_ms: int,
        *,
        allow_initialize: bool = False,
    ) -> None:
        """Load an existing ledger or create a clean 1000-USDT baseline for all rules."""
        price = _positive_finite(baseline_price_usdt, "baseline price")
        if set(interval_by_strategy) != set(STRATEGIES) or set(targets) != set(STRATEGIES):
            raise ValueError("Portfolio initialization must include all three strategies")
        if self.state_path.exists():
            self._state = self._load_state(symbol)
            for strategy in STRATEGIES:
                path = self.csv_paths[strategy]
                rows = _read_csv(path)
                if not rows or rows[0] != self._state["baseline_rows"][strategy]:
                    raise ValueError(
                        f"Portfolio CSV {path} is missing its original baseline or has been altered; "
                        "refusing to reset or rebaseline it"
                    )
                previous_row_time: int | None = None
                for row in rows:
                    if row["strategy"] != strategy or row["symbol"] != symbol:
                        raise ValueError(f"Portfolio CSV {path} contains a different strategy or market")
                    row_time = _timestamp_ms(row["candle_close_utc"])
                    if previous_row_time is not None and row_time <= previous_row_time:
                        raise ValueError(f"Portfolio CSV {path} is duplicated or out of order")
                    previous_row_time = row_time
                last = rows[-1]
                item = self._state["strategies"][strategy]
                last_time = _timestamp_ms(last["candle_close_utc"])
                state_time = int(item["last_mark_time_ms"])
                strategy_recovered = last_time > state_time
                if last_time < state_time:
                    raise ValueError(
                        f"Portfolio CSV {path} is behind its saved state; refusing to guess missing history"
                    )
                if strategy_recovered:
                    # CSV append precedes JSON state persistence, so an ahead row is a recoverable retry.
                    if last["position_at_close"] not in {"BTC", "CASH"}:
                        raise ValueError(f"Recovered CSV position is invalid for {strategy}")
                    if last["interval"] != item["interval"]:
                        raise ValueError(f"Recovered CSV interval disagrees for {strategy}")
                    _exposure(last["signal_target"])
                    close_price = _positive_finite(last["close_usdt"], f"{strategy} recovered close")
                    cash = _positive_finite(last["cash_usdt"], f"{strategy} recovered cash") if last["cash_usdt"] else 0.0
                    units = float(last["btc_units"])
                    if not math.isfinite(units) or units < 0:
                        raise ValueError(f"Recovered CSV BTC units are invalid for {strategy}")
                    equity = _positive_finite(last["portfolio_value_usdt"], f"{strategy} recovered equity")
                    if not math.isclose(cash + units * close_price, equity, rel_tol=1e-8, abs_tol=1e-5):
                        raise ValueError(f"Recovered CSV balances and equity disagree for {strategy}")
                    item.update({
                        "position_target": last["position_at_close"],
                        "pending_signal_target": last["signal_target"],
                        "cash_usdt": cash,
                        "btc_units": units,
                        "equity_usdt": equity,
                        "last_price_usdt": close_price,
                        "last_mark_time_ms": last_time,
                        "last_candle_open_time_ms": _timestamp_ms(last["candle_open_utc"]),
                    })
                elif (
                    not math.isclose(float(last["portfolio_value_usdt"]), float(item["equity_usdt"]), rel_tol=1e-8, abs_tol=1e-5)
                    or last["position_at_close"] != item["position_target"]
                    or last["signal_target"] != item["pending_signal_target"]
                ):
                    raise ValueError(f"Portfolio CSV and saved state disagree for {strategy}")
                if not strategy_recovered and item["pending_signal_target"] != targets[strategy]:
                    raise ValueError(f"Portfolio pending target disagrees with saved signal state for {strategy}")
            self._save_state(self._state)
            return

        if not allow_initialize:
            raise ValueError(
                f"Portfolio state {self.state_path} is missing. Initialize explicitly with --initialize-portfolio; "
                "existing history will not be reset automatically."
            )
        for strategy, path in self.csv_paths.items():
            existing_rows = _read_csv(path)
            if existing_rows:
                raise ValueError(
                    f"Portfolio CSV {path} already contains history; initialization would overwrite its baseline"
                )

        exposures = {strategy: _exposure(targets[strategy]) for strategy in STRATEGIES}
        strategies: dict[str, dict[str, Any]] = {}
        baseline_rows: dict[str, dict[str, str]] = {}
        for strategy in STRATEGIES:
            exposure = exposures[strategy]
            cash = 0.0 if exposure == "BTC" else self.starting_capital_usdt
            units = self.starting_capital_usdt / price if exposure == "BTC" else 0.0
            strategies[strategy] = {
                "interval": interval_by_strategy[strategy],
                "position_target": exposure,
                "pending_signal_target": targets[strategy],
                "cash_usdt": cash,
                "btc_units": units,
                "equity_usdt": self.starting_capital_usdt,
                "last_price_usdt": price,
                "last_mark_time_ms": int(baseline_time_ms),
                "last_candle_open_time_ms": None,
            }
            baseline_rows[strategy] = self._baseline_row(
                strategy, symbol, interval_by_strategy[strategy], targets[strategy],
                price, int(baseline_time_ms), cash, units, exposure
            )
        payload = {
            "version": STATE_VERSION,
            "symbol": symbol,
            "starting_capital_usdt": self.starting_capital_usdt,
            "baseline_price_usdt": price,
            "baseline_time_ms": int(baseline_time_ms),
            "baseline_rows": baseline_rows,
            "strategies": strategies,
        }
        # Keep initialization explicit: data files are written before the state marker.
        for strategy in STRATEGIES:
            append_portfolio_rows(self.csv_paths[strategy], [baseline_rows[strategy]])
        self._save_state(payload)
        self._state = payload

    def record_evaluation(
        self,
        strategy: str,
        symbol: str,
        interval: str,
        event: Any,
        details: Mapping[str, float] | None = None,
    ) -> bool:
        """Account for an evaluation and its close-to-next-open execution lag."""
        if not self.is_initialized:
            return False
        if strategy not in STRATEGIES:
            raise ValueError(f"Unknown portfolio strategy: {strategy}")
        payload = self._load_state(symbol)
        if self._state is not None:
            self._state = payload
        item = payload["strategies"][strategy]
        candle = event.candle
        close_time_ms = int(candle.close_time_ms)
        last_mark_time = int(item["last_mark_time_ms"])
        if close_time_ms <= last_mark_time:
            replayed = next((
                row for row in _read_csv(self.csv_paths[strategy])
                if row["record_type"] == "COMPLETED_CANDLE"
                and row["candle_close_utc"] == utc_timestamp(close_time_ms)
            ), None)
            if (
                replayed is not None
                and replayed["previous_target"] == event.previous_target
                and replayed["signal_target"] == event.target
                and math.isclose(float(replayed["close_usdt"]), float(candle.close_price), rel_tol=1e-10, abs_tol=1e-8)
            ):
                return False
            if close_time_ms < last_mark_time:
                raise ValueError(f"Unjournaled out-of-order candle for {strategy}")
            raise ValueError(f"Conflicting duplicate portfolio candle for {strategy}")
        if event.previous_target != item["pending_signal_target"]:
            raise ValueError(
                f"Portfolio signal state disagrees for {strategy}: expected "
                f"{item['pending_signal_target']}, received {event.previous_target}"
            )

        prior_open = item.get("last_candle_open_time_ms")
        interval_ms = _interval_ms(interval)
        if prior_open is not None:
            expected_open = int(prior_open) + interval_ms
            if int(candle.open_time_ms) != expected_open:
                raise ValueError(
                    f"Missing or out-of-order portfolio candle for {strategy}: expected "
                    f"{utc_timestamp(expected_open)}, received {utc_timestamp(int(candle.open_time_ms))}"
                )
        elif strategy == "daily-ema6-24":
            in_baseline_candle = int(candle.open_time_ms) <= last_mark_time < close_time_ms
            starts_after_baseline = int(candle.open_time_ms) == last_mark_time + 1
            if not (in_baseline_candle or starts_after_baseline):
                raise ValueError("First daily portfolio candle does not align with its baseline")
        elif int(candle.open_time_ms) != last_mark_time + 1:
            raise ValueError(f"First portfolio candle for {strategy} does not follow its baseline")

        prior_equity = float(item["equity_usdt"])
        cash = float(item["cash_usdt"])
        btc_units = float(item["btc_units"])
        position = item["position_target"]
        pending_target = item["pending_signal_target"]
        execution_action = "HOLD"
        execution_price = ""

        if int(candle.open_time_ms) > last_mark_time:
            open_price = _positive_finite(
                getattr(candle, "open_price", None) or item["last_price_usdt"],
                "candle open price",
            )
            value_at_open = cash + btc_units * open_price
            wanted_position = _exposure(pending_target)
            if wanted_position != position:
                execution_price = _fmt(open_price)
                if wanted_position == "BTC":
                    btc_units = value_at_open / open_price
                    cash = 0.0
                    execution_action = "BUY_BTC"
                else:
                    cash = value_at_open
                    btc_units = 0.0
                    execution_action = "SELL_TO_CASH"
                position = wanted_position
        elif not (int(candle.open_time_ms) <= last_mark_time < close_time_ms):
            raise ValueError(f"Out-of-order candle for {strategy}: {utc_timestamp(close_time_ms)}")
        elif _exposure(pending_target) != position:
            raise ValueError(
                f"Cannot execute a pre-baseline pending target for {strategy}; refusing to guess a fill"
            )

        close_price = _positive_finite(candle.close_price, "candle close price")
        equity = cash + btc_units * close_price
        if not math.isfinite(equity) or equity <= 0:
            raise ValueError(f"Invalid portfolio equity for {strategy}")
        signal_target = event.target
        rule_details = dict(details or getattr(event, "details", {}) or {})
        metric_items = list(rule_details.items())[:2]
        metric1_name, metric1_value = metric_items[0] if metric_items else ("", "")
        metric2_name, metric2_value = metric_items[1] if len(metric_items) > 1 else ("", "")
        total_return_pct = (equity / self.starting_capital_usdt - 1.0) * 100.0
        period_return_pct = (equity / prior_equity - 1.0) * 100.0
        row = dict(zip(PORTFOLIO_FIELDS, [
            "COMPLETED_CANDLE",
            strategy,
            symbol,
            interval,
            utc_timestamp(int(candle.open_time_ms)),
            utc_timestamp(close_time_ms),
            _fmt(close_price),
            event.previous_target,
            signal_target,
            str(event.previous_target != signal_target).lower(),
            utc_timestamp(close_time_ms + 1),
            execution_action,
            execution_price,
            position,
            _fmt(cash),
            _fmt(btc_units, 12),
            _fmt(equity),
            _fmt(self.starting_capital_usdt),
            _fmt(equity - self.starting_capital_usdt),
            _fmt(total_return_pct),
            _fmt(period_return_pct),
            str(metric1_name),
            _fmt(float(metric1_value)) if metric1_value != "" else "",
            str(metric2_name),
            _fmt(float(metric2_value)) if metric2_value != "" else "",
        ]))

        # Write the CSV before advancing the portfolio state; retries are idempotent.
        append_portfolio_rows(self.csv_paths[strategy], [row])
        item.update({
            "position_target": position,
            "pending_signal_target": signal_target,
            "cash_usdt": cash,
            "btc_units": btc_units,
            "equity_usdt": equity,
            "last_price_usdt": close_price,
            "last_mark_time_ms": close_time_ms,
            "last_candle_open_time_ms": int(candle.open_time_ms),
        })
        self._save_state(payload)
        self._state = payload
        return True
