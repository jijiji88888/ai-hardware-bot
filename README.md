# AI Hardware & Deep-Tech News Aggregator (`ai-hardware-bot`)

## Role in the Two-Repository Pipeline

1. **`jijiji88888/ai-hardware-bot` (Broad Internet Fetcher & Relevance Filter)**:
   - Driven by `.env` switches (`ENABLE_*`), per-source caps (`*_MAX_ITEMS*`), `ARXIV_CATEGORIES`, and direct `Label=URL` feed/scraper specifications parsed via `get_feed_specs()` in `app.py`.
   - Fetches candidate articles and preprints with **equal source treatment** (`fair_cap_items` round-robins across `arXiv`, `Hacker News`, `Techmeme`, `Semiconductor Engineering`, `The Register HPC`, `ServeTheHome`, `OCP Blog`, `Power Electronics News`, `EE Times`, `Blocks and Files`, `Chips and Cheese`, `Quantum Computing Report`, `DIGITIMES Asia`, `TrendForce`, `Data Center Dynamics`, and `HPCwire`).
   - Uses a 4-stage 403-resilient HTML scraper (`scrape_single_html_target`) for Cloudflare-protected sites (`TrendForce News`, `Data Center Dynamics`, `HPCwire`): (1) Chrome navigation headers + JSON-LD/DOM extraction, (2) WordPress REST API (`/wp-json/wp/v2/posts`), (3) Wayback Machine raw `id_` snapshots, and (4) domain-indexed fallback.
   - Uses a unified in-memory signal scanner (`HARDWARE_DOMAIN_TAXONOMY` / `match_hardware_signals`) and the OpenRouter LLM classifier (`classify_batch` in `app.py`) to evaluate relevance equally across **all 8 physical hardware domains** into `agi_hardware_trends.db` (SQLite WAL mode).

2. **[`ajsfgu9/tech-trend-researcher`](https://github.com/ajsfgu9/tech-trend-researcher) (Social Trend Researcher & Viral Feature Matcher)**:
   - Monitors broad social & developer discussion channels (`Reddit`, `Hacker News`, `GitHub`, social tech clusters) to discover what is currently trending and what is likely to go viral next.
   - Cross-compares live social trends against the previously sorted items in `agi_hardware_trends.db` to produce a two-part output:
     - **Output 1 (`trending_now`)**: What is trending right now across the research pipeline and why it is poised to gain traction.
     - **Output 2 (`database_viral_matches`)**: Whether any previously sorted item in `agi_hardware_trends.db` shares identical technical/narrative features (`bottleneck_addressed`, `specific_tech`, `hardware_domain`, information asymmetry, and controversy triggers) to go viral online—listing the exact matching database items if `YES`, or outputting `NO` if none match.

## Unified Hardware Layers (Old + New Topics Integrated Equally)

- **AI Accelerators**: GPU, TPU, NPU, custom ASIC, wafer-scale engines, training & inference chips.
- **Memory & Near-Memory Compute**: HBM, HBM4 & HBM4e (custom logic base dies on TSMC N4/N3, 2,048-bit wide interfaces), DRAM, SRAM, MRAM/ReRAM, Compute Express Link (CXL 3.0 / 3.1 memory pooling, fabric switches & resource disaggregation), memory wall, Processing-in-Memory (PIM) & near-memory/in-memory compute.
- **Interconnect & Optical Switching Fabrics**: NVLink/NVSwitch, Ethernet/InfiniBand hardware, optical interconnect, silicon photonics, Co-Packaged Optics (CPO, including 51.2T and 102.4T switch ASICs co-packaged with optical engines), All-Optical Circuit Switching (direct-drive OCS using MEMS 2D/3D mirror arrays eliminating E-O-E conversions), pluggable optics, transceivers, SerDes, retimers, switches.
- **Advanced Packaging & Substrates**: Chiplets, 2.5D/3D packaging, CoWoS, interposers, glass core substrates & Through-Glass Vias (TGV with ultra-high aspect ratios & sub-10 µm pitch), Fan-Out Panel-Level Packaging (FOPLP on rectangular panels), Coefficient of Thermal Expansion (CTE) matching & organic ABF warpage mitigation, hybrid bonding, RDL.
- **Semiconductor Manufacturing & Wafer Materials**: Lithography (EUV/High-NA), deposition, etch, metrology, advanced nodes, compound & Ultra-Wide Bandgap (UWBG) semiconductors (Gallium Oxide / $\text{Ga}_2\text{O}_3$ and Aluminum Nitride / AlN for high-voltage/high-heat power electronics), Chemical Vapor Deposition (CVD) synthetic diamond substrates & thermal spreaders (~2,000 W/m·K bonded to silicon), wafer capacity, yield, EDA & physical design.
- **Power Delivery & Thermal Management**: Datacenter power delivery, Rack-Scale High-Voltage Power (400V / 800V DC busbar architectures), Vertical Power Delivery (VPD) & on-package Integrated Voltage Regulators (IVR), multi-MHz GaN & SiC Point-of-Load (PoL) converters, Direct-to-Chip (DLC) microchannel cold plates & manifold hydraulics, single-phase & two-phase dielectric liquid immersion cooling, Liquid-to-Liquid & Liquid-to-Air CDUs, and in-silicon backside microfluidic cooling channels.
- **Robotics Hardware**: Sensors, actuators, harmonic drives, robot hands, embodied AI hardware, embedded & edge autonomy compute.
- **Quantum, Neuromorphic & Optical/Photonic Compute**:
  - **Neuromorphic Computing**: Mimics biological neural spikes in silicon (spiking neural networks / SNNs, event-based continuous sensory processors, memristive/analog synaptic crossbars) to process continuous sensory data at fractional milliwatt power budgets.
  - **Quantum Computing**: Uses superposition and entanglement (superconducting, trapped-ion, neutral-atom, photonic, silicon-spin, and topological QPUs, fault-tolerant logical qubits, QEC surface/LDPC codes, cryo-CMOS control) to solve specific combinatorial, quantum chemistry, and cryptographic problems intractable for classical supercomputers.
  - **Optical & Photonic Computing**: Replaces electrons with photons for matrix-vector multiplication (Mach-Zehnder interferometer / MZI meshes, optical tensor cores, WDM photonic processors) and photonic die-to-die interconnects, bypassing copper thermal and bandwidth bottlenecks.
