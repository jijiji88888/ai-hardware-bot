# AI Hardware & Deep-Tech News Aggregator (`ai-hardware-bot`)

## Role in the Two-Repository Pipeline

1. **`jijiji88888/ai-hardware-bot` (Broad Internet Fetcher & Relevance Filter)**:
   - Fetches candidate articles, filings, and preprints from the broad internet (`arXiv`, `SEC EDGAR EFTS`, `Semiconductor Engineering`, `ServeTheHome`, `The Register HPC`, `OCP`, `DIGITIMES`, `TrendForce`, `Techmeme`, `Hacker News`).
   - Uses its OpenRouter LLM classifier (`classify_batch` in `app.py`) to evaluate domain relevance and extract structured fields (`hardware_domain`, `bottleneck_addressed`, `specific_tech`, `relevance_score`, `why_it_matters`, `ai_revolution_thesis`, `raw_summary`) into `agi_hardware_trends.db` (SQLite WAL mode).

2. **[`ajsfgu9/tech-trend-researcher`](https://github.com/ajsfgu9/tech-trend-researcher) (Social Trend Researcher & Viral Feature Matcher)**:
   - Monitors broad social & developer discussion channels (`Reddit`, `Hacker News`, `GitHub`, social tech clusters) to discover what is currently trending and what is likely to go viral next.
   - Cross-compares live social trends against the previously sorted items in `agi_hardware_trends.db` to produce a two-part output:
     - **Output 1 (`trending_now`)**: What is trending right now across the research pipeline and why it is poised to gain traction.
     - **Output 2 (`database_viral_matches`)**: Whether any previously sorted item in `agi_hardware_trends.db` shares identical technical/narrative features (`bottleneck_addressed`, `specific_tech`, `hardware_domain`, information asymmetry, and controversy triggers) to go viral online—listing the exact matching database items if `YES`, or outputting `NO` if none match.
