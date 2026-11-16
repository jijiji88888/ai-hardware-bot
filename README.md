# AI Hardware & Deep-Tech News Aggregator (`ai-hardware-bot`)

## Distinct Role in the Two-Repository Architecture

To prevent duplicate scraping and separate data collection from longitudinal trend intelligence, the pipeline is split into two focused repositories:

1. **`jijiji88888/ai-hardware-bot` (This Repo — Pure Ingestion, Normalization & Time-Series Storage)**:
   - Polls and scrapes all external sources (RSS/Atom feeds, arXiv preprints, Hacker News, Reddit, TrendForce, Semiconductor Engineering, ServeTheHome, OpenCompute, etc.).
   - Deduplicates URLs and content fingerprints.
   - Persists normalized items and point-in-time engagement snapshots into SQLite (`agi_hardware_trends.db`) with **WAL mode** enabled (`aggregator_bridge.py`).
   - Supports `PURE_AGGREGATOR_MODE=true` so it focuses strictly on fast, resilient 24/7 collection without blocking on inline LLM scoring.

2. **[`ajsfgu9/tech-trend-researcher`](https://github.com/ajsfgu9/tech-trend-researcher) (Downstream Consumer — Longitudinal Trend Discovery & Editorial Briefs)**:
   - Performs **zero** duplicate web scraping.
   - Reads `agi_hardware_trends.db` in read-only WAL mode (`?mode=ro`).
   - Studies **real longitudinal trends** over time (CUSUM/EWMA entity & bottleneck burst detection, cross-layer lead-lag propagation from `Primary Engineering` $\rightarrow$ `Debate` $\rightarrow$ `Supply Chain` $\rightarrow$ `Mainstream`, and emerging technical bottleneck co-occurrence shifts), reserving item scoring as a secondary sub-component.
   - Outputs structured **Automated Content Briefs** (`content_briefs.json`) for the downstream News Writer.

## Quickstart

```bash
python -c "from aggregator_bridge import upgrade_aggregator_db_schema; upgrade_aggregator_db_schema('/app/data/agi_hardware_trends.db')"
docker compose up -d --build
```
