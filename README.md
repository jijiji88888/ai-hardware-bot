# AI Hardware & Deep-Tech News Aggregator (`ai-hardware-bot`)

## Role in the Two-Repository Pipeline

1. **`jijiji88888/ai-hardware-bot` (Broad Internet Fetcher & Relevance Filter)**:
   - Driven by `.env` switches (`ENABLE_*`), per-source caps (`*_MAX_ITEMS*`), `ARXIV_CATEGORIES`, and direct `Label=URL` feed/scraper specifications parsed via `get_feed_specs()` in `app.py` (with zero inefficient Google News or SEC EDGAR bulk keyword search queries).
   - Fetches candidate articles and preprints from direct specialist and supply-chain sources (`arXiv` combined category feed across `cs.AR`, `cs.DC`, `cs.ET`, `cs.NI`, `cs.RO`, `cs.LG`, `eess.SP`, `eess.SY`, `eess.IV`, `physics.optics`, `physics.app-ph`, `cond-mat.mtrl-sci`, `physics.flu-dyn`; `Semiconductor Engineering`; `The Register HPC`; `ServeTheHome`; `OCP Blog`; `Data Center Dynamics`; `Power Electronics News`; `EE Times`; `Blocks and Files`; `Chips and Cheese`; `HPCwire`; `DIGITIMES Asia`; `TrendForce`; `Techmeme`; and `Hacker News`).
   - Deterministically tags and prioritizes candidates across the **6 Priority Frontier Hardware Pillars** (`match_frontier_topics` / `enrich_item_with_frontier_topics`) in memory before queue capping or LLM batching.
   - Uses its OpenRouter LLM classifier (`classify_batch` in `app.py`) to evaluate domain relevance and extract structured fields (`hardware_domain`, `bottleneck_addressed`, `specific_tech`, `frontier_topic`, `frontier_subtopics`, `relevance_score`, `why_it_matters`, `ai_revolution_thesis`, `raw_summary`) into `agi_hardware_trends.db` (SQLite WAL mode).

2. **[`ajsfgu9/tech-trend-researcher`](https://github.com/ajsfgu9/tech-trend-researcher) (Social Trend Researcher & Viral Feature Matcher)**:
   - Monitors broad social & developer discussion channels (`Reddit`, `Hacker News`, `GitHub`, social tech clusters) to discover what is currently trending and what is likely to go viral next.
   - Cross-compares live social trends against the previously sorted items in `agi_hardware_trends.db` to produce a two-part output:
     - **Output 1 (`trending_now`)**: What is trending right now across the research pipeline and why it is poised to gain traction.
     - **Output 2 (`database_viral_matches`)**: Whether any previously sorted item in `agi_hardware_trends.db` shares identical technical/narrative features (`bottleneck_addressed`, `specific_tech`, `hardware_domain`, `frontier_topic`, information asymmetry, and controversy triggers) to go viral online—listing the exact matching database items if `YES`, or outputting `NO` if none match.

## 6 Priority Frontier Hardware Pillars & Direct Source Mapping

1. **Advanced Thermal Management & Liquid/Immersion Cooling Systems**
   - *Direct Sources*: `Data Center Dynamics`, `ServeTheHome`, `OCP Blog`, `HPCwire`, `The Register HPC`, `arXiv` (`physics.flu-dyn`, `physics.app-ph`, `cs.AR`, `cs.DC`).
   - *Core sub-topics*: Direct-to-Chip (DLC) microchannel cold plates & manifold hydraulics; single-phase & two-phase liquid immersion cooling (synthetic dielectric fluids); Liquid-to-Liquid & Liquid-to-Air CDUs; in-silicon backside microfluidic cooling channels.
2. **Glass Substrates & Panel-Level Packaging (PLP)**
   - *Direct Sources*: `Semiconductor Engineering`, `EE Times`, `DIGITIMES Asia`, `TrendForce`, `Chips and Cheese`, `arXiv` (`cond-mat.mtrl-sci`, `physics.app-ph`, `cs.AR`, `cs.ET`).
   - *Core sub-topics*: Through-Glass Vias (TGV) with ultra-high aspect ratios & sub-10 µm pitch; Fan-Out Panel-Level Packaging (FOPLP) on rectangular panels; CTE matching & ABF warpage mitigation.
3. **Next-Generation Memory Hierarchies & Near-Memory Compute**
   - *Direct Sources*: `Blocks and Files`, `Semiconductor Engineering`, `Chips and Cheese`, `TrendForce`, `EE Times`, `ServeTheHome`, `arXiv` (`cs.AR`, `cs.DC`, `cs.ET`, `eess.SY`).
   - *Core sub-topics*: HBM4 & HBM4e logic base dies (TSMC N4/N3) & 2,048-bit wide interfaces; Compute Express Link (CXL 3.0 / 3.1) memory pooling, fabric switches & disaggregation; Processing-in-Memory (PIM) & Near-Memory Compute.
4. **Rack-Scale High-Voltage Power Delivery & Point-of-Load (PoL) Conversion**
   - *Direct Sources*: `Power Electronics News`, `Data Center Dynamics`, `OCP Blog`, `ServeTheHome`, `EE Times`, `arXiv` (`eess.SY`, `physics.app-ph`, `cs.AR`).
   - *Core sub-topics*: 400V / 800V DC high-voltage server rack busbar architectures; Vertical Power Delivery (VPD) & on-package Integrated Voltage Regulators (IVR); GaN & SiC point-of-load converter stages at multi-MHz switching frequencies.
5. **Ultra-Wide Bandgap Materials & Synthetic Diamond Thermal Spreaders**
   - *Direct Sources*: `Power Electronics News`, `Semiconductor Engineering`, `EE Times`, `DIGITIMES Asia`, `arXiv` (`cond-mat.mtrl-sci`, `physics.app-ph`).
   - *Core sub-topics*: CVD diamond substrates & heat spreaders (~2,000 W/m·K) bonded directly to silicon; Gallium Oxide ($\text{Ga}_2\text{O}_3$) & Aluminum Nitride (AlN) high-voltage, high-heat power electronics.
6. **All-Optical Circuit Switching (OCS) & Co-Packaged Switch Fabrics**
   - *Direct Sources*: `Semiconductor Engineering`, `ServeTheHome`, `HPCwire`, `EE Times`, `OCP Blog`, `Chips and Cheese`, `TrendForce`, `arXiv` (`physics.optics`, `physics.app-ph`, `cs.NI`, `cs.AR`, `cs.DC`).
   - *Core sub-topics*: Direct-drive optical circuit switches (OCS) using MEMS 2D/3D mirror arrays; 51.2T and 102.4T switch ASICs co-packaged with optical engines (CPO).
