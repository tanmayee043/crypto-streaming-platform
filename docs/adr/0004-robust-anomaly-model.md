# 4. Robust anomaly model per symbol

**Status:** accepted

## Context

The first model used raw prices with mean and standard deviation over 30 trades. It flagged 4.63% of 60,000 real trades and caught 72.2% of planted price spikes. One big move also inflates a standard deviation, which can hide the next move.

## Decision

Compare each trade with the previous 100 trades of the same symbol, excluding itself:

- **Price:** log return more than 6 robust standard deviations from the median, with a scale floor.
- **Size:** at least 50× the window's median trade size.

A variant using a trimmed scale was rejected after it did worse on the hold-out set.

## Consequences

- On the same data, the model flags 2.89% and catches 95.2% of planted price spikes. On a separate hold-out set it flags 1.55% and catches 90.7%.
- Real trades have no labels, so precision is unknown. Detection is only measured on planted spikes.
- Thresholds were tuned on historical data and should be re-checked as markets change.
- One large order that fills in many parts can be flagged several times.
