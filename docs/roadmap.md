
## Project Summary

This project is a high-performance spatial compositing engine for generating natural-looking raster imagery from GIS data. It combines procedural noise, multi-scale Gaussian filtering, and configuration-driven pipelines to synthesize visually rich terrain and land-cover surfaces. A configurable blend pipeline defines how driver rasters are loaded, transformed, and composited into the final image.

## Design

**Explicit configuration**
All processing steps, parameters, and layer definitions are driven by a centralized configuration structure. Configuration is fully explicit, and configuration errors are treated as fatal rather than silently falling back to defaults.

**Engine/library pattern**
The system follows an engine/library architecture, with separate components for factors (alpha layers), noise, surfaces, themes, and compositing.

**Storage model**
Raster blocks are stored in shared memory as 4D arrays in the form:

`(slot, band, height, width)`

**Compute model**
A `render_task` acts as a strict boundary between storage and computation. It rehydrates shared-memory data and converts it into 2D `(height, width)` arrays before passing them to the rendering engines.

**IPC model**
`WorkPacket` and `ResultPacket` carry only metadata and integer `slot_id` references. No raw NumPy arrays are pickled or passed between processes.

**Process model**
The system uses a three-stage multiprocessing pipeline:

* **Main process:** coordinator and ticket dispenser
* **Reader process:** loads raster blocks into shared memory
* **Render pool:** N worker processes perform CPU-intensive compositing
* **Writer process:** a dedicated single process that lazy-opens the output in `r+` mode

## Engine Design

**Factor Engine**
The Factor Engine is an orchestration layer that follows a mapped-signal pattern:

`normalize -> blur -> noise -> contrast`

It produces factor rasters that act as alpha masks or control layers throughout the pipeline.

**Surface Engine**
The Surface Engine synthesizes RGB blocks from ramps and modifiers such as mottle. Procedural textures use MD5-stable `hashlib` offsets so that patterns remain aligned across tile boundaries without visible seams.

**Theme Registry**
The Theme Registry handles categorical rasters using a precedence-based “melt and grow” expansion algorithm. It also builds RGB lookup tables from QML styles.

**Config Manager**
The Config Manager acts as the single source of truth. In the main process, it primes metadata for a Markdown-based Pipeline Audit report and then serializes the worker context for the render pool.

## Performance

**I/O strategy**
The engine uses tiled, ZSTD-compressed GeoTIFFs to avoid the performance penalty of striped raster layouts.

**Current speed**
A 99-tile render has been reduced from about 26 seconds in serial execution to roughly 8 seconds using the optimized multiprocessing pipeline.

**Current bottlenecks**

1. **Startup tax** from Python process spawning and library imports
2. **Single-reader constraint** from relying on only one reader process

## Compositing Operations

The compositing system is built around a registry of small, atomic spatial operations. Each operation is registered with explicit metadata describing its required inputs, attributes, and parameters, allowing the pipeline to validate configuration before execution.

At runtime, the compositing library applies a sequence of operations to RGB surfaces and intermediate buffers, using factor rasters as spatial control masks. Core operations include:

* **Buffer creation** to initialize working buffers from source surfaces
* **Surface-to-surface interpolation** to blend major palettes such as arid and humid layers
* **Surface-to-buffer interpolation** to progressively build the final composite
* **Factor-based multiply** to darken or modulate an existing buffer
* **Alpha-over compositing** to place one surface over another using a factor as opacity
* **Buffer-to-buffer interpolation** to merge intermediate results
* **Specular highlight addition** to add controlled reflected-light effects
* **Final output write** to publish the completed buffer as the render result

## Sample Natural Raster Pipeline

The raster is built from four primary palettes:

* `arid_base`
* `arid_vegetation`
* `humid_base`
* `humid_vegetation`

Arid and humid surfaces are blended using a precipitation factor. Base and vegetation surfaces are blended using a forest-canopy factor. Additional thematic layers are derived from the USGS Landfire categorical raster, including water, glacier, outwash, volcanic, rock, and playa.

## Roadmap

**Config migration**
Move all remaining `settings.py` configuration into `biome.yml`.

**Parallel readers**
Replace the single reader with a reader pool to better saturate SSD bandwidth. This will require some refactoring of the current reader design.

**Persistent server mode**
Add a daemon mode so parameter changes can be tested without respawning the full process graph, enabling sub-2-second iteration times.

**GUI**
Create a GUI for editing YAML parameters and triggering builds through the daemon.





### 1. The Strategy: Artistic/Technical Alternation
Refine map rendering with each sprint
*   **Validation:** By tuning the map between  sprints, we catch "Logic Regressions" (e.g., if a refactor accidentally flattens the Gamma curve).
*   **Feature Discovery:** Identify new feature requirements with each sprint.
---

### 2. Tech Phases

#### Phase 1: Code Cleanup
* **Cleanup:** and add comments to engines, and libraries.  No functional change.

#### Phase 2: Structural Hardening (The Foundation)
*   **Goal:** Clean up and move from `if` statements to **Registries**.
*   **Impact:** This turns the engine into a **State Machine**. Instead of the code saying `if "water" do X`, the code says `engine.execute(category_logic)`. 
*   **Why it's needed:** This is the absolute prerequisite for the GUI. A GUI cannot easily "edit" an `if` statement, but it can easily edit a registry key.

#### Phase 3: The "Source of Truth" Migration
*   **Goal:** Death of `settings.py`.
*   **Impact:** This decouples the **Art** (YAML) from the **Engine** (Python). 
*   **Visual Win:** We can share a `biome.yml` with someone else, and they can produce the exact same "Sedona Look" without needing the specific Python environment tweaks.

#### Phase 4: Parallel Readers (Goal 2: Full Build)
*   **Goal:** Break the 12-second wall.
*   **Impact:** This utilizes the Mac Studio’s SSD bandwidth and CPU cores to handle the "Decompression Tax." 
*   **Performance:** This is the step that will get a full-region build (thousands of tiles) under that 10-minute target.

#### Phase 5: The Hot Server (Goal 3: The < 2s Loop)
*   **Goal:** Kill the "Launch Tax."
*   **Impact:** This is the most significant change in "Feel." The engine becomes a **Daemon**. 
*   **Workflow:** We save a file $\rightarrow$ the map updates instantly. No more waiting 8 seconds for Python to start.

#### Phase 6: The GUI (The "Instrument")
*   **Goal:** Real-time Art Direction.
*   **Impact:** This transforms the engine into a **Creative Instrument**. Moving a slider and seeing a canyon wall change color in 1.5 seconds is where the true artistic breakthrough happens.

---

### 3. Key Technical Considerations for Phase 4/5

As we move into the "Persistent Server" and "Parallel Readers," keep these two things in mind:

1.  **Stale Memory Management:** In a hot server mode, the Shared Memory segments stay alive. We’ll need a "Reset" signal to ensure that if a render fails, the next render starts with "Clean" slots.
2.  **Registry Pattern:** For Step 2, ensure the `Library` files use a `@register` decorator pattern. This makes adding a new type of noise or a new blend op as simple as adding a function—no more modifying the `Engine` classes.

### Summary
the roadmap is perfectly sequenced. We are building the **Foundation** (Cleanup/Config) before the **Turbocharger** (MP Readers/Hot Server) and finally the **Cockpit** (GUI).

**Which part of Step 1 (Cleanup/Comments) are we tackling first?** The `render_task` is the most "important" to document, but the `factor_library` is where the most "creative" math lives. 🧱🚀✅