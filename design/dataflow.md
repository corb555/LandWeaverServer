
# Land Weaver Server

**Land Weaver Server** is a high-performance background service for multi-layer spatial compositing. A separate client manages render settings and notifies the daemon over a Unix domain socket whenever a new render is requested.

## Overview

The system uses a message-driven pipeline in which compact work packets are combined with shared-memory job context to avoid large IPC payloads.

### 1. Throughput

The daemon is designed for throughput. It uses multiple processes for disk reads and tile rendering, with queues carrying control packets and shared memory holding tile data and job context. Only one render job is active at a time, but a single job may contain thousands of tiles.

### 2. Persistent workers to avoid startup latency

Python multiprocessing with the `spawn` method imposes substantial startup overhead, roughly 8 seconds. The daemon starts worker processes once during service startup and keeps them alive for the life of the service. This avoids per-job startup cost and allows the daemon to respond quickly to new render requests. This matters especially for small preview renders, which may take only about 1 second to execute.

### 3. Shared-memory caching

GIS inputs such as DEM and lithology datasets are large and expensive to load. A persistent **Shared Memory (SHM) Registry** allows the daemon to retain loaded raster blocks across render passes. When a user changes styling within the same region, the daemon can reuse raster data already in memory instead of rereading it from disk. The cache is invalidated when the input filename/timestamp hash changes.

### 4. Message control

All messages have a message type. All job messages also carry a `job_id`.

### 5. Pluggable `RenderSystem`

The system is divided into a **Pipeline** and a **RenderSystem**.

The **Pipeline** depends only on a small set of interfaces exposed by the `RenderSystem`. Otherwise, it knows very little about rendering. The Pipeline manages:

- process startup
- shared-memory cache
- IPC
- message dispatch
- job lifecycle

Dispatching is based on a fixed dispatch table, with each message type routed to a predetermined handler.

The **RenderSystem** provides the stage-specific behavior for the Reader, Renderer, and Writer stages. Each stage reads a message from an IPC queue, performs its work, and emits its results back into the messaging framework.

The Pipeline owns the dispatch and messaging model. The `RenderSystem` supplies the logic for reading input blocks, rendering tiles, and writing output within that framework.

A render task does not need to know about the Pipeline’s orchestration logic, cache management, or process supervision. All of the information it needs is provided through the message it receives and a context structure.

The default `RenderSystem` is driven entirely by Land Weaver YAML settings and is composed of several rendering engines with feature libraries, including:

- `factor_engine`
- `compositing_engine`
- `noise_engine`
- `surface_engine`

For example, the compositing engine may launch actions from its library such as:

- `create_buffer`
- `lerp`
- `multiply`
- `alpha_over`
- `apply_zonal_gradient`
- `output_buffer`

---

## Process Topology

- 1 Orchestrator
- N Reader processes
- N Worker processes
- 1 Writer process

## Core Model

Rendering is tile-based and highly parallel. Each tile may depend on multiple input source blocks, and a tile cannot be rendered until all required inputs have been loaded. Once a tile’s inputs are ready, it can be rendered and written in any order.

Only one job is active at a time, but a job may contain thousands of tiles.

When a job begins, the Orchestrator publishes the active job context and rendering parameters into shared memory, including the current `job_id`. Every queue packet carries both an operation code and a `job_id`. The `job_id` increases monotonically for the lifetime of the daemon.

On every queue read, each task process validates that the packet `job_id` matches the active `job_id` stored in shared memory. Packets for older or invalid jobs are discarded immediately. If the `job_id` is new, the task refreshes its settings for the new job.

Input blocks are coordinated through queue messages, while bulk tile data and job context live in shared memory. `job_id` validation is the primary safeguard against stale packets from earlier jobs.

---

## Execution Flow: Happy Path

The `RenderSystem` can perform any processing it wants, but it must fit within this messaging framework.

1. The client sends `JOB_REQUEST` to the Orchestrator.
2. The Orchestrator queues incoming requests and starts the next job when idle.
3. The Orchestrator resolves the request into a job manifest and publishes the job context into shared memory.
4. Any work packet whose `job_id` does not match the shared-memory job context is ignored.
5. The Dispatcher primes the pipeline with up to `max_in_flight` tiles.
6. For each tile, the Dispatcher places a `LOAD_BLOCK` packet for each required source into the Reader queue.
7. Readers load the requested input blocks into the provided buffer and emit `BLOCK_LOADED`.
8. The Dispatcher maintains per-tile dependency state. Once all required blocks for a tile are available, the Dispatcher marks that tile ready for rendering.
9. The Orchestrator sends the tile to the Render queue as `RENDER_TILE`.
10. The Renderer produces the rendered image tile into the provided buffer and directly places it into `writer_q` with op `WRITE_TILE`.
11. The Writer writes the tile to the output file and emits `TILE_WRITTEN`.
12. The Orchestrator releases the tile’s resources and dispatches the next tile.
13. The Orchestrator counts `TILE_WRITTEN`. Once that count matches the total for the active job, it sends `JOB_DONE` to the Writer queue.
14. The Writer flushes and closes the output, then emits `TILES_FINALIZED`.
15. The Orchestrator marks the job complete and may begin the next queued job.

---

```mermaid
flowchart TD
    A[Client<br/>JOB_REQUEST]
    B[Orchestrator<br/>Queue / start job]
    C[Orchestrator<br/>Manifest + SHM context]
    D[All Processes<br/>Reject stale job_id]

    subgraph TILE_LOOP [Per-Tile Loop]
        E[Dispatcher<br/>Prime tiles]
        F[Dispatcher<br/>LOAD_BLOCK]
        G[Readers<br/>BLOCK_LOADED]
        H[Dispatcher<br/>Tile ready]
        I[Orchestrator<br/>RENDER_TILE]
        J[Renderer<br/>WRITE_TILE]
        K[Writer<br/>TILE_WRITTEN]
        L[Orchestrator<br/>Release tile / next tile]
        E --> F --> G --> H --> I --> J --> K --> L --> E
    end

    M[Orchestrator<br/>JOB_DONE]
    N[Writer<br/>TILES_FINALIZED]
    O[Orchestrator<br/>Finish job / next queued]

    A --> B --> C --> D --> E
    L --> M
    M --> N --> O
````

---

## Pipeline Responsibilities

The Pipeline owns execution infrastructure, lifecycle, and dispatch. Its responsibilities include:

* allocate and manage shared-memory blocks and output buffers
* create shared-memory-based IPC queues
* create global source resources in shared memory at startup
* start and supervise Reader, Worker, and Writer processes
* manage queue topology, scheduling, and job lifecycle
* maintain cache state and invalidate cache when the region changes
* dispatch work and track tile dependencies, progress, completion, and errors
* provide Pipeline-managed buffers and blocks to render stages
* publish worker context payloads into shared memory
* manage client messaging, including progress, success, failure, and error messages
* dispatch messages based on a fixed dispatch table
* promote the temporary output file to the final output file
* manage startup, shutdown, and fatal-error lifecycle behavior
* manage free-queue or slot-return behavior for reusable resources

## RenderSystem Responsibilities

The `RenderSystem` owns rendering semantics and stage behavior. Its responsibilities include:

* define the global source universe and the job-specific required source subset
* validate render requests and render configuration
* resolve a request into a render-ready job artifact
* implement Reader logic that fills Pipeline-managed input blocks
* implement Worker logic that renders into Pipeline-managed output buffers
* implement Writer logic that consumes Pipeline-managed rendered output
* create render-specific per-job and per-worker context


