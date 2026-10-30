# Design system: Surveillance dashboard

Adapted from the Kraken reference (`design-references/awesome-design-md/design-md/kraken`), chosen by the owner. Kraken's site is light; analysts work long sessions on trading screens, so this system keeps Kraken's palette for a light theme and derives a dark theme from its near-black and cool greys. The page follows the operating system's setting, with a manual toggle.

## Principles

- **Operate, don't decorate.** The analyst is mid-task. Density is fine; every element earns its place by answering "is something unusual happening, and is the pipeline healthy?"
- **Purple means action or state.** Kraken purple marks primary actions, the selected item and focus, never decoration.
- **Never colour alone.** Anomaly types differ by marker shape and label as well as colour (price ▲ triangle, size ◆ diamond, both ★ star); health states carry a word (OK, Breach, Unknown) next to the dot.
- **Replay is always labelled.** Recorded data is shown under a persistent "Replay" banner with its date, so it can never be mistaken for live data.

## Colour tokens

| Token | Light | Dark | Use |
|---|---|---|---|
| `--bg` | `#ffffff` | `#101114` | page |
| `--surface` | `#f6f6f9` | `#16171c` | second neutral layer: panels, drawer, table header |
| `--raised` | `#ffffff` | `#1d1e25` | inputs, hovered rows |
| `--border` | `#dedee5` | `#2a2c36` | dividers |
| `--text` | `#101114` | `#ececf1` | primary text |
| `--text-2` | `#5c5f75` | `#9a9db0` | secondary text (≥ 4.5:1 on `--bg` and `--surface`) |
| `--accent` | `#7132f5` | `#9b7bff` | actions, selection, focus, links |
| `--accent-strong` | `#5741d8` | `#b39cff` | pressed / hover |
| `--accent-subtle` | `rgba(133,91,251,0.14)` | `rgba(155,123,255,0.18)` | selected backgrounds |
| `--up` | `#0f7a4a` | `#3ccf8e` | price up, OK |
| `--down` | `#c4283d` | `#ff6b7d` | price down, breach |
| `--warn` | `#9a5a00` | `#f2b14a` | degraded, unknown |
| `--price` | `#c4283d` | `#ff6b7d` | price anomaly marker |
| `--size` | `#5741d8` | `#b39cff` | size anomaly marker |
| `--both` | `#9a5a00` | `#f2b14a` | price + size marker |

## Type

- **IBM Plex Sans** (Kraken's own listed fallback) for all UI text: 13 px labels, 14 px body, 16 px section titles, 20 px page title. Weights 400 / 500 / 600. Ratio about 1.15 per step, fixed rem sizes.
- **IBM Plex Mono** only for numbers that are compared or scanned (prices, z-scores, ratios, IDs, latencies), with `font-variant-numeric: tabular-nums`.

## Shape and depth

- Radius: 12 px buttons and inputs (Kraken: rounded, never pills), 8 px badges, 16 px drawer corners on mobile.
- Elevation: borders do most of the work. The drawer uses Kraken's whisper shadow `0 4px 24px rgba(0,0,0,0.08)` (dark: `rgba(0,0,0,0.5)`).

## Layout

- Top bar: title, mode switch (Live / Replay), symbol selector, alerts toggle, theme toggle.
- Health strip: one row of five measures against their SLO targets.
- Main: price chart (wide) beside the anomaly feed; below, tabs for Anomaly history and Pipeline health.
- Drawer: fixed to the right on desktop (420 px), a full-height sheet at ≤ 768 px. Non-modal: Esc closes it, focus returns to the row that opened it.
- Breakpoints: 768 px (feed moves under the chart, drawer becomes a sheet), 480 px (strip wraps to two columns).

## Motion

150–200 ms ease-out for state changes (drawer slide, row highlight on arrival). The live dot pulses only when motion is allowed (`prefers-reduced-motion: no-preference`). No page-load choreography.

## States

Every data region has a skeleton loading state, an empty state that says what would make data appear, and an error state that names the problem and offers a retry. When the live feed is offline the chart area offers a one-click replay of recorded trades.
