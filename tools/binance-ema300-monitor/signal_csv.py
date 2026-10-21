"""Durable, idempotent evaluation journal (not an order or returns ledger)."""
from __future__ import annotations

import csv
import fcntl
import os
import tempfile
from pathlib import Path

FIELDS = [
    "strategy", "symbol", "interval", "candle_close_utc", "close_usdt",
    "previous_target", "target", "signal_changed", "highest_close_prior20_usdt",
    "lowest_close_prior10_usdt", "ema6_usdt", "ema24_usdt",
    "effective_from_utc", "record_type",
]


def append_evaluations(path: Path, rows: list[dict]) -> int:
    """Commit rows before advancing signal state; retries deduplicate by candle/rule.

    A lock serializes writers; atomic replacement protects the CSV against a crash.
    State persistence is deliberately separate: a failed state save can safely replay.
    """
    if not rows:
        return 0
    path = Path(path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_name('.' + path.name + '.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        existing = []
        if path.exists():
            with path.open(newline='', encoding='utf-8') as handle:
                reader = csv.DictReader(handle)
                if reader.fieldnames != FIELDS:
                    raise ValueError(f'Unexpected CSV schema in {path}; refusing to overwrite')
                existing = list(reader)
        key = lambda row: (row['strategy'], row['symbol'], row['candle_close_utc'])
        keys = {key(row) for row in existing}
        added = []
        for row in rows:
            if set(row) != set(FIELDS):
                raise ValueError('Evaluation row does not match CSV schema')
            if key(row) not in keys:
                keys.add(key(row))
                added.append(row)
        if not added:
            return 0
        name = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', newline='', encoding='utf-8',
                                             dir=path.parent, delete=False) as handle:
                name = handle.name
                writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator='\n')
                writer.writeheader()
                writer.writerows(existing + added)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(name, path)
            name = None
        finally:
            if name is not None:
                os.unlink(name)
        return len(added)


def evaluation_row(event, symbol: str, utc_timestamp, record_type='COMPLETED_CANDLE') -> dict:
    details = event.details
    return dict(zip(FIELDS, [
        event.strategy, symbol, event.interval, utc_timestamp(event.candle.close_time_ms),
        f'{event.candle.close_price:.8f}', event.previous_target, event.target,
        str(event.previous_target != event.target).lower(),
        f"{details['highest_close_prior20']:.8f}" if 'highest_close_prior20' in details else '',
        f"{details['lowest_close_prior10']:.8f}" if 'lowest_close_prior10' in details else '',
        f"{details['ema6']:.8f}" if 'ema6' in details else '',
        f"{details['ema24']:.8f}" if 'ema24' in details else '',
        utc_timestamp(event.candle.close_time_ms + 1), record_type,
    ]))
