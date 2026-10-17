# Binance Spot BTC Signal Monitor

A small Python 3.10+ **signal-only** monitor for three independent Binance Spot BTCUSDT rules. It reads public klines and reports target portfolio-state changes; it never connects to a Binance account, requests API keys, places an order, trades, or auto-executes anything. It uses only Python's standard library.

## Frozen rule

After each **completed 8-hour UTC candle**, update the recursive EMA(300) with `adjust=False`, seeded from the first completed close:

```text
EMA_t = close_t                                      (first observed close)
EMA_t = (2 / 301) * close_t + (299 / 301) * EMA_(t-1) (thereafter)
```

Then update the target using the candle close and the updated EMA:

- If `close > 1.02 × EMA`, target **LONG**.
- Else if `close <= 0.97 × EMA`, target **FLAT**.
- Otherwise retain the previous target.

The historical backtest convention is next-observed-open execution. Each new-candle message says the target was determined at that candle close and that the next observed open is the execution convention. This describes the signal/backtest timing only; it is not a promise of an available price or fill.

## Additional independent signals

The original EMA300 strategy remains unchanged. Two additional long-or-cash signals are evaluated independently; their state is not combined with the EMA300 target, and they print alerts only when their own target changes.

### 8-hour Donchian 20/10

After each completed 8-hour UTC candle, compare its close with the preceding completed candles, excluding the current candle:

- Enter **BTC** only when the close is strictly greater than the highest close of the prior 20 completed 8-hour candles.
- Exit to **CASH** only when the close is strictly less than the lowest close of the prior 10 completed 8-hour candles.
- Otherwise retain the previous Donchian target.

The resulting signal applies from the next 8-hour interval. The monitor reports only target transitions.

### UTC daily EMA(6/24)

After each completed UTC daily candle, calculate recursive `adjust=False` EMAs from completed daily closes, seeded with the first available completed close:

- Target **BTC** when EMA(6) is greater than EMA(24).
- Target **CASH** when EMA(6) is equal to or below EMA(24).

The daily target applies to the following day's return. The monitor reports only target transitions. This strategy is independent from the 8-hour Donchian state and the EMA300 state.

The separate state file defaults to `binance_additional_signals_state.json` beside the EMA300 state file. On first initialization it reconstructs the current positions from completed historical candles without emitting old historical transitions. The Donchian replay starts from **CASH** because no earlier saved position exists. Later checks process only newly completed candles and save state atomically. The additional signals use Binance's public market-data mirror (`data-api.binance.vision`) by default; the existing EMA300 endpoint setting is retained, with the official mirror used as a read-only fallback if Binance's main API returns HTTP 451.

## Setup and run

From the repository root:

```bash
cd tools/binance-ema300-monitor
python3 --version
python3 -m unittest discover -s tests -v
```

No package installation is required. Run a single fetch/reconstruction pass:

```bash
python3 binance_ema300_monitor.py --once
```

Run continuously with the default 60-second polling interval (the process runs only while you keep it running):

```bash
python3 binance_ema300_monitor.py
```

Optional examples:

```bash
python3 binance_ema300_monitor.py --symbol BTCUSDT --poll-seconds 60
python3 binance_ema300_monitor.py --once --state-file ./state/btcusdt.json
python3 binance_ema300_monitor.py --base-url https://api.binance.com --timeout 20
python3 binance_ema300_monitor.py --additional-base-url https://data-api.binance.vision
```

`--symbol` changes the Spot market being observed. The original EMA300 interval, period, thresholds, and semantics remain frozen. The EMA300 state file is `binance_ema300_state.json`; the additional-strategy state file defaults to `binance_additional_signals_state.json` beside it and can be overridden with `--additional-state-file`. Both JSON state files are atomically replaced.

## First run and restart behavior

If there is no state file, the monitor requests Binance's available 8-hour history in pages of at most 1,000 klines and excludes the currently forming candle. On first-run reconstruction only, a historical kline with an early `closeTime` is explicitly warned about and skipped; it is never treated as a full candle. Gaps in EMA300 history are reported and are not filled with synthetic candles. At least 300 valid completed bars are required before EMA300 state can be initialized. The monitor seeds EMA from the first valid completed close and replays the hysteresis rule in timestamp order, starting the target at **FLAT** because there is no earlier saved target. It prints the replay range and final reconstructed target; it does not print an alert for every historical candle in that initial replay. The additional strategies initialize independently: Donchian replays completed 8-hour closes and the daily EMAs replay completed UTC daily closes, without reporting historical transitions.

On later starts it validates that the saved configurations match the selected symbol, rules, and API origins, then fetches candles after the saved open times and processes newly completed candles in order. Historical short rows may be skipped only during initial reconstruction; short rows and missing/out-of-order intervals during saved-state updates are rejected, and state is not advanced. A corrupt, mismatched, stale, incomplete, or non-contiguous data/state condition is reported rather than silently resetting or advancing. HTTP 418/429 responses honor `Retry-After` when supplied and otherwise use exponential backoff. If you deliberately want to reconstruct from scratch, stop the process and **archive or remove the relevant state file**; that will cause a fresh baseline. Do not do this if you need to preserve the current signal state.

All market data comes from Binance Spot's public `GET /api/v3/klines` endpoint, using UTC `8h` and `1d` intervals and no credentials. The two added strategies use the public `data-api.binance.vision` mirror by default; the existing EMA300 client falls back to that mirror if `api.binance.com` returns HTTP 451. See Binance's [Spot market API catalog](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market) and [Spot REST API documentation](https://developers.binance.com/en/docs/products/spot/rest-api).

## Reading alerts and important caveats

The EMA300 rule continues to report each newly processed completed 8-hour candle. The Donchian and daily EMA rules print only new target transitions, with their close time, threshold/EMA values, and effective next interval/day. A target is only that rule's computed desired state: it may differ from your actual holdings, open orders, or account state. The program does not inspect any of them.

This is a local script, not a hosted or always-on service. It monitors only while the process is running and has network access. It does not guarantee delivery if the computer is off, the connection fails, or Binance is unavailable. It is not investment advice and does not submit, stage, or automate trades.

## Tests

The offline unit tests cover EMA recursion, LONG/FLAT hysteresis and deadband retention, strict Donchian thresholds, daily EMA equality exits, simultaneous independent signal changes, exclusion of incomplete candles, JSON state round-trip, startup-only short-row handling, gap reporting without synthesis, and the minimum history requirement. They use local fixtures and make no network requests.
