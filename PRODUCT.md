# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

Plain HTML/CSS/JavaScript in `static/index.html` with Chart.js from a CDN, served by FastAPI (no build step). Chosen by the owner for Week 3 so every line stays explainable; a React app shared across the three portfolio projects is a possible later step.

## Users

- **Primary: a market-surveillance analyst** at an exchange, watching live trading for abnormal activity and needing to show compliance that no trades were silently dropped. Works in long sessions, scans fast, clicks into a flagged trade to understand it.
- **Secondary: a recruiter or interviewer** opening the portfolio link. Must understand what the system does within about 30 seconds, without reading the README.

## Product Purpose

Real-time market surveillance on Binance.US trades: flag unusual price moves and unusually large trades per symbol as they happen, explain why each was flagged, and prove the pipeline is complete and healthy (every trade captured, low latency, consumers keeping up). Success: an analyst sees an anomaly within seconds and understands it in one click; a visitor sees the detector working even when the live pipeline is off.

## Positioning

Completeness you can prove: exchange trade IDs are sequential, so the system measures exactly which trades it missed, and its delivery guarantee is demonstrated by repeatable crash tests (100 forced crashes, zero lost or duplicated trades) rather than claimed.

## Capabilities

- Live trades and anomalies pushed to the browser (Server-Sent Events).
- Pipeline health: trades/s, end-to-end latency, completeness %, consumer lag, against stated SLOs (completeness ≥ 99.9%, p95 latency ≤ 2 s, lag < 1,000 messages).
- Anomaly detail: the trade, the surrounding trades, the scores and plain-English reasons.
- Anomaly history with filters (symbol, type, date range).
- Replay mode: recorded real trades replayed at 10×/100×/1000× through the same model.
- Alerts: browser notifications and a Discord webhook.

## Constraints

- When the live pipeline is offline, say so plainly and offer a one-click replay of recorded real trades instead of an empty screen.
- Never invent data: every number shown comes from the pipeline, the database or a replay that is labelled as a replay.
- Accessible: keyboard navigation, visible focus, sufficient contrast, charts that don't rely on colour alone, reduced-motion support; usable at 375 px wide.

## Terminology

- **Price anomaly:** log return more than 6 robust standard deviations from the window's median.
- **Size (volume) anomaly:** trade at least 50× the window's median size.
- **Completeness:** stored trade IDs ÷ expected trade IDs for a symbol over a window.
