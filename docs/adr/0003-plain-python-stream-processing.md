# 3. Plain Python for stream processing

**Status:** accepted

## Context

Live Binance.US traffic is typically under 3 trades/s across four symbols, with short spikes around 6. The processing is simple: one rolling window per symbol, plus the delivery guarantee.

## Decision

Write the detector and storage consumer as plain Python services using `confluent-kafka`, rather than adopting Flink or Spark Structured Streaming.

## Consequences

- The delivery guarantee stays visible in a small amount of code, and each step can be tested directly.
- A 60,000-trade burst ran at about 5,850 trades/s on a laptop, far above live traffic.
- The detector is single-threaded, so scaling means more partitions and one detector per partition.
- Window state, recovery and offsets are this project's own code to maintain and test.
