# AI Hardware & Deep-Tech News Aggregator (`ai-hardware-bot`)

## Role in the Two-Repository Pipeline

1. **`jijiji88888/ai-hardware-bot` (Broad Internet Fetcher & Relevance Filter)**:
   - Fetches candidate articles, filings, and preprints from the broad internet (`arXiv`, `SEC EDGAR EFTS`, `Semiconductor Engineering`, `ServeTheHome`, `The Register HPC`, `OCP`, `Data Center Dynamics`, `Chips and Cheese`, `DIGITIMES`, `TrendForce`, `Techmeme`, `Hacker News`, and targeted Frontier Topic RSS streams).
   - Deterministically tags and prioritizes candidates across the **6 Priority Frontier Hardware Pillars** (`match_frontier_topics` / `enrich_item_with_frontier_topics`) before queue capping or LLM batching.
   - Uses its OpenRouter LLM classifier (`classify_batch` in `app.py`) to evaluate domain relevance and extract structured fields (`hardware_domain`, `bottleneck_addressed`, `specific_tech`, `frontier_topic`, `frontier_subtopics`, `relevance_score`, `why_it_matters`, `ai_revolution_thesis`, `raw_summary`) into `agi_hardware_trends.db` (SQLite WAL mode).

2. **[`ajsfgu9/tech-trend-researcher`](https://github.com/ajsfgu9/tech-trend-researcher) (Social Trend Researcher & Viral Feature Matcher)**:
   - Monitors broad social & developer discussion channels (`Reddit`, `Hacker News`, `GitHub`, social tech clusters) to discover what is currently trending and what is likely to go viral next.
   - Cross-compares live social trends against the previously sorted items in `agi_hardware_trends.db` to produce a two-part output:
     - **Output 1 (`trending_now`)**: What is trending right now across the research pipeline and why it is poised to gain traction.
     - **Output 2 (`database_viral_matches`)**: Whether any previously sorted item in `agi_hardware_trends.db` shares identical technical/narrative features (`bottleneck_addressed`, `specific_tech`, `hardware_domain`, `frontier_topic`, information asymmetry, and controversy triggers) to go viral online—listing the exact matching database items if `YES`, or outputting `NO` if none match.

## 6 Priority Frontier Hardware Pillars Tracked Across All Stages

1. **Advanced Thermal Management & Liquid/Immersion Cooling Systems**
   - *Why it complies*: Compute accelerator TDPs have exceeded 1,000 W per socket and are moving toward 2,000 W+, rendering air cooling physically unviable. Heat dissipation at the cold plate and facility loop is now a primary governor of cluster density.
   - *Core sub-topics*:
     - Direct-to-Chip (DLC) microchannel cold plates and manifold hydraulics.
     - Single-phase and two-phase liquid immersion cooling using synthetic dielectric fluids.
     - Liquid-to-Liquid and Liquid-to-Air Coolant Distribution Units (CDUs).
     - In-silicon microfluidic cooling channels etched directly into the back of active compute dies.
2. **Glass Substrates & Panel-Level Packaging (PLP)**
   - *Why it complies*: As multi-die reticle sizes expand (e.g., dual compute dies surrounded by 8–12 HBM stacks), standard organic Ajinomoto Build-up Film (ABF) substrates suffer from thermal warpage, trace density limits, and electrical parasitic losses. Glass core substrates represent the next physical substrate transition.
   - *Core sub-topics*:
     - Through-Glass Vias (TGV) with ultra-high aspect ratios and sub-10 µm pitch.
     - Fan-Out Panel-Level Packaging (FOPLP) moving packaging economics from 300 mm round wafers to large rectangular panels.
     - Coefficient of Thermal Expansion (CTE) matching between silicon dies, glass interposers, and board assemblies.
3. **Next-Generation Memory Hierarchies & Near-Memory Compute**
   - *Why it complies*: Frontier LLM inference is fundamentally memory-bandwidth bound (FLOP-to-byte imbalances). The transition to custom base dies and memory-pooling interconnects is shifting memory from a passive commodity to active compute silicon.
   - *Core sub-topics*:
     - HBM4 & HBM4e: Migration from standard DRAM base dies to advanced logic nodes (such as TSMC N4/N3) for 2,048-bit wide memory interfaces.
     - Compute Express Link (CXL 3.0 / 3.1): Memory pooling, fabric switches, and cache-coherent hardware resource disaggregation across server nodes.
     - Processing-in-Memory (PIM) & Near-Memory Compute: Integrating arithmetic logic directly into DRAM banks or buffer dies to reduce data movement latency.
4. **Rack-Scale High-Voltage Power Delivery & Point-of-Load (PoL) Conversion**
   - *Why it complies*: Delivering thousands of amperes at sub-1.0V to silicon packages creates unsustainable resistive ($I^2R$) heat losses across traditional 12V and 48V copper busbars. Power delivery network (PDN) design inside the rack is as critical as power delivery to the building.
   - *Core sub-topics*:
     - 400V / 800V DC high-voltage server rack busbar architectures.
     - Vertical Power Delivery (VPD) and on-package Integrated Voltage Regulators (IVR) delivering power directly underneath the die.
     - Gallium Nitride (GaN) and Silicon Carbide (SiC) point-of-load converter stages operating at multi-megahertz switching frequencies.
5. **Ultra-Wide Bandgap Materials & Synthetic Diamond Thermal Spreaders**
   - *Why it complies*: Silicon's natural thermal conductivity (~150 W/m·K) forms a thermal bottleneck at the junction layer. Advanced materials integrated into the wafer stack allow higher sustained clock frequencies and current densities without thermal throttling.
   - *Core sub-topics*:
     - Chemical Vapor Deposition (CVD) diamond substrates and heat spreaders (~2,000 W/m·K) bonded directly to silicon.
     - Gallium Oxide ($\text{Ga}_2\text{O}_3$) and aluminum nitride (AlN) for extreme high-voltage, high-heat power electronics.
6. **All-Optical Circuit Switching (OCS) & Co-Packaged Switch Fabrics**
   - *Why it complies*: Clusters scaling to 100,000+ accelerators spend 15–20% of their power purely on optical transceivers, electrical SerDes, and retimers. Eliminating electrical-optical-electrical (E-O-E) conversions in switching fabrics is mandatory to sustain network scaling.
   - *Core sub-topics*:
     - Direct-drive optical circuit switches (OCS) using micro-electro-mechanical system (MEMS) 2D/3D mirror arrays.
     - 51.2T and 102.4T switch ASICs co-packaged with optical engines (CPO).

## Optional `.env` Overrides

All 6 Frontier Hardware Pillars are enabled by default in `app.py` (even if your existing `.env` is unchanged). You can optionally tune their behavior in `.env`:

```ini
# Targeted arXiv keyword search across the 6 Frontier Hardware Pillars
ENABLE_ARXIV_FRONTIER_SEARCH=true
ARXIV_FRONTIER_MAX_RESULTS_PER_TOPIC=8
ARXIV_CATEGORIES=cs.AR,cs.DC,cs.ET,cs.NI,cs.RO,cs.AI,cs.LG,eess.SY,eess.SP,eess.IV,physics.optics,physics.app-ph,cond-mat.mtrl-sci,physics.flu-dyn

# Targeted Frontier Hardware RSS streams
ENABLE_FRONTIER_TOPIC_RSS=true
FRONTIER_TOPIC_RSS_MAX_ITEMS_PER_FEED=8

# Topic-scoped SEC EDGAR EFTS full-text search expansion across all 6 pillars
ENABLE_SEC_FRONTIER_EXPANSION=true
```
