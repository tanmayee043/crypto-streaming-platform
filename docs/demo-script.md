# Demo video script (2–3 minutes)

Record with OBS Studio (free) or the Windows Game Bar (Win + G). Use a 1080p window, hide bookmarks and notifications, and zoom the browser to about 110%. Speak over it live or add narration afterwards. Cut any waiting.

**Before you record:** make sure the deployed site is up, the crash replay is selected at 1000×, Grafana is open through the SSH tunnel, and a terminal is ready in the repo. Never show the contents of `.env`, or the Grafana password.

| Time | On screen | Say (roughly) |
|---|---|---|
| 0:00–0:15 | The live dashboard at your public HTTPS address | "This is a real-time market surveillance system on live Binance.US trades. An exchange's surveillance team needs to see abnormal trading as it happens, and prove to compliance that no trades were silently dropped. Most demos do the first part. This one is built around the second." |
| 0:15–0:35 | Point at the health strip and live chart. Open the **Pipeline health** tab | "The strip shows throughput, latency, completeness and consumer lag against their targets. Exchange trade IDs are sequential, so the system knows exactly which trades it missed." (Read out the current completeness figure and how many days it covers.) |
| 0:35–1:05 | **Replay** → 10 Oct 2025 crash → **1000×** → **Start**. Let anomalies stream in | "This is the real crash of 10 October 2025: 10,507 trades, replayed through the same model. On ordinary days it flags 1.5 to 3 percent of trades. Here, 7." |
| 1:05–1:30 | Click an anomaly to open the drawer. Point at the reasons and mini chart | "Every flag explains itself: how far the move was from normal, compared with the last 100 trades of that symbol, and the trades around it. An analyst can judge it in one click." |
| 1:30–2:05 | Terminal: show the end of a crash test, or run `python scripts/crash_test.py --kills 10` beforehand and show the result | "The original consumer used Kafka's auto-commit. Restarting Postgres once lost 67 percent of trades, silently. Now offsets are committed only after the write, and upserts make redelivery harmless. This test hard-kills the consumer 100 times during a stream, then compares the database with every trade Kafka accepted: zero missing, zero duplicates." |
| 2:05–2:30 | Grafana: SLO row, "Where the time goes", consumer lag | "Every service exports metrics. Grafana shows the service levels and where the latency goes, per stage." (Read out the p95 you see.) |
| 2:30–2:50 | GitHub Actions run list; then the README's architecture and metrics | "Every push runs the tests and crash tests in CI, then builds the ARM image and deploys it, rolling back if it's unhealthy. It all runs 24/7 on a free Oracle VM, with only HTTPS exposed." |
| 2:50–3:00 | Back to the live dashboard | "Link's in the description. Thanks for watching." |

**Say only what's true on the day you record.** Quote completeness with the number of days it really covers, and don't say "30 days at 99.9%" until it has actually run that long.

**For a README GIF (about 10 seconds):** the replay at 1000× with anomalies streaming in, plus one click into the drawer. ScreenToGif is free. Keep it under about 10 MB and save it as `docs/demo.gif`, then replace the "Demo video coming soon" line at the top of the README.
