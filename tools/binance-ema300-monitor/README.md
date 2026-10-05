# Binance EMA300 Signal Monitor

A small Python 3.10+ **signal-only** monitor for a frozen Binance Spot rule. It reads public klines and reports the target portfolio state; it never connects to a Binance account, requests API keys, places an order, trades, or auto-executes anything. It uses only Python's standard library.

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

## Setup and run

From the repository root, enter the monitor directory:

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
```

`--symbol` changes the Spot market being observed; the 8-hour interval, EMA period, thresholds, and signal semantics remain frozen. The default state file is `binance_ema300_state.json` beside the script. The JSON state is atomically replaced and records the configuration, EMA, LONG/FLAT target, last processed candle open time, last close, and count of processed candles. Each later completed candle is saved before its event is printed.

## First run and restart behavior

If there is no state file, the monitor requests Binance's available 8-hour history in pages of at most 1,000 klines, excludes the currently forming candle, seeds EMA from the first completed close, and replays the hysteresis rule in timestamp order. For deterministic reconstruction where there is no earlier saved target, it starts the target at **FLAT** before replaying the earliest returned completed candle. It prints the replay range and final reconstructed target; it does not print an alert for every historical candle in that initial replay.

On later starts it validates that the saved configuration matches the selected symbol, rule, and API origin, then fetches candles after the saved open time and processes all newly completed candles in order. A corrupt, mismatched, stale, incomplete, or non-contiguous data/state condition is reported rather than silently resetting or advancing. HTTP 418/429 responses honor `Retry-After` when supplied and otherwise use exponential backoff. If you deliberately want to reconstruct from scratch, stop the process and **archive or remove the state file yourself**; that will cause a full replay. Do not do this if you need to preserve the current monitor state.

The default endpoint is Binance Spot's public `GET /api/v3/klines` endpoint, with `symbol`, `interval=8h`, and `startTime` parameters. It uses UTC interval boundaries and does not send credentials. See Binance's [Spot market API catalog](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market) and [Spot REST API documentation](https://developers.binance.com/en/docs/products/spot/rest-api).

## Reading alerts and important caveats

Each processed completed candle produces one concise line with its UTC candle timestamp, close, EMA, percent distance to EMA, target, and whether the target changed or was retained. If a restart requires replaying old completed candles, those events are marked **HISTORICAL CATCH-UP (not a live-at-close alert)**. A target is only the rule's computed desired state: it may differ from your actual holdings, open orders, or account state. The program does not inspect any of them.

This is a local script, not a hosted or always-on service. It monitors only while the process is running and has network access. It does not guarantee delivery if the computer is off, the connection fails, or Binance is unavailable. It is not investment advice and does not submit, stage, or automate trades.

## Tests

The offline unit tests cover EMA recursion, LONG/FLAT hysteresis and deadband retention, exclusion of incomplete candles, and JSON state round-trip. They use local fixtures and make no network requests.
