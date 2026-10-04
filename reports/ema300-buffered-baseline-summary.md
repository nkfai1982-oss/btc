# Frozen EMA(300) buffered baseline

## Frozen rule

On each completed 8-hour candle, enter long when the close is greater than 1.02 × EMA(300), exit to cash when the close is less than or equal to 0.97 × EMA(300), and otherwise retain the current position. EMA is the existing recursive calculation (`adjust=False`); signals execute at the next observed open. Exposure is capped at 100% with no leverage. Trading costs are 0.15% or 0.30% per side.

## Backtest comparison

Each run starts with 1,000 USDT and runs through the final dataset close, **2026-09-27 15:59:59.999 UTC**. The comparison is against the former unbuffered close > EMA(300) baseline.

| Start date | Entry open (USDT/BTC) | Cost per side | Rule | ROI | Max drawdown | Daily Sharpe |
|---|---:|---:|---|---:|---:|---:|
| 2020-01-01 | — | 0.15% | Buffered +2%/−3% | 2,300.94% | −34.46% | 1.108 |
| 2020-01-01 | — | 0.15% | Unbuffered baseline | 1,851.17% | −35.66% | 1.060 |
| 2020-01-01 | — | 0.30% | Buffered +2%/−3% | 2,171.03% | −35.24% | 1.088 |
| 2020-01-01 | — | 0.30% | Unbuffered baseline | 1,469.00% | −37.19% | 0.980 |
| 2021-04-01 | 58,739.50 | 0.15% | Buffered +2%/−3% | 227.46% | −34.46% | 0.590 |
| 2021-04-01 | 58,739.50 | 0.15% | Unbuffered baseline | 186.75% | −35.66% | 0.541 |
| 2021-04-01 | 58,739.50 | 0.30% | Buffered +2%/−3% | 212.08% | −35.24% | 0.565 |
| 2021-04-01 | 58,739.50 | 0.30% | Unbuffered baseline | 141.58% | −37.19% | 0.452 |

## Interpretation and validation

The +2%/−3% pair ranked first or second in ROI among 36 entry/exit pairs tested at integer percentage steps from 0% to 5%, and beat the no-buffer baseline in all four date/cost cases. However, those pairs were selected on reused historical data; the two account windows overlap, and the fee scenarios use the same price history. There is no independent holdout, so this is not proof of improvement and does not rule out overfitting. The dataset tail ends **2026-09-27**, roughly a week before the comparison run. Freeze this pair and validate it only on genuinely untouched future data or with a forward paper test, without further tuning on this history.

These are hypothetical backtests, not a trading recommendation.
