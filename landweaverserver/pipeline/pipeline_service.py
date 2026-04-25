from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from queue import Empty
import time
import traceback
from types import SimpleNamespace
from typing import List, Callable, Optional, TypeAlias, Counter

from rasterio.windows import Window

from landweaverserver.common.ipc_packets import (Envelope, Op, JobDonePacket, ErrorPacket,
                                                 BlockLoadedPacket, TileWrittenPacket, SEV_WARNING,
                                                 SEV_CANCEL, SEV_FATAL)
from landweaverserver.pipeline.client_proxy import ClientProxy
from landweaverserver.pipeline.io_manager import IOSystem
from landweaverserver.pipeline.job_control import JobControl, JobManifest
from landweaverserver.pipeline.pipeline_runtime import PipelineRuntime
from landweaverserver.pipeline.system_config import SystemConfig
from landweaverserver.pipeline.tile_dispatcher import TileDispatcher, DispatchResult

from landweaverserver.render.job_resolver import JobResolver
from landweaverserver.render.render_system import RenderSystem

EnvelopeHandler: TypeAlias = Callable[[Envelope], None]


def dbg(msg, id):
    pass


# pipeline_service.py
class PipelineService:
    def __init__(self, system_yml_path: Path, render_system: RenderSystem):
        source_specs = render_system.get_source_specs()
        engine_cfg = SystemConfig.load_engine_specs(system_yml_path, source_specs)
        self.runtime = PipelineRuntime(engine_cfg, source_specs=source_specs)
        io_system = IOSystem()
        self.client_proxy = ClientProxy(
            engine_cfg.get("system.socket_path"), status_q=self.runtime.status_q,
            response_q=self.runtime.response_q,
            request_schema=render_system.get_render_request_schema()
        )
        self.orchestrator = PipelineOrchestrator(
            pipeline_runtime=self.runtime, io_system=io_system, render_system=render_system
        )
        self.render_system = render_system

    def start(self) -> None:
        self.runtime.start()
        self.client_proxy.start()
        self.orchestrator.loop()  # Loop does not return unless error

        # LOOP terminated. Clean up and exit
        self.client_proxy.stop()

MINUTES = 60

class PipelineOrchestrator:
    def __init__(
            self, pipeline_runtime: PipelineRuntime, io_system: IOSystem,
            render_system: RenderSystem, ) -> None:

        self.idle_start_ts = None
        self.idle_seconds_timeout = 120 * MINUTES
        self.previous_ts = None
        self.last_progress_pulse = 0.0
        self.pending_jobs: List[dict] = []

        self.runtime = pipeline_runtime
        self.render_system = render_system
        self.io_system = io_system

        self.dispatcher = TileDispatcher(runtime=self.runtime, max_in_flight=10)
        self.resolver = JobResolver(
            config_loader=lambda p: self.render_system.load(p), render_system=self.render_system
        )
        self.stats = JobTelemetry()
        self.job_control: JobControl = JobControl()

        # Operation dispatch table
        self._op_dispatch_tbl: dict[Op, EnvelopeHandler] = {
            Op.JOB_REQUEST: self._handle_job_request,
            Op.BLOCK_LOADED: self._handle_block_loaded,
            Op.TILE_WRITTEN: self._handle_tile_written,
            Op.TILES_FINALIZED: self._handle_job_finalized,
            Op.ERROR: self._handle_error,
            Op.WRITER_ABORTED: self._handle_wr_abort,
            Op.SHUTDOWN: self._initiate_shutdown,
        }

        self.running = True

    def loop(self) -> None:
        while self.running:
            try:
                # 1.  GET A MESSAGE
                try:
                    if self.idle_start_ts is None:
                        self.idle_start_ts = time.perf_counter()
                    envelope: Envelope = self.runtime.status_q.get(timeout=0.05)
                    self.idle_start_ts = None  # Got a message, clear idle time stamp
                except Empty:
                    # timeout on empty q
                    if self.idle_start_ts is not None:
                        idle_seconds = time.perf_counter() - self.idle_start_ts
                        if idle_seconds > self.idle_seconds_timeout:
                            # IDLE TOO LONG. SHUTDOWN
                            self._initiate_shutdown(f"Idle Timeout - Shutting down")
                            # Exit the loop
                            break
                    # Send progress to client periodically
                    self._pulse_client_progress()
                    continue

                # 2.  DISPATCH MSG TO APPROPRIATE OP HANDLER
                self.update_telemetry(envelope.op)
                handler = self._op_dispatch_tbl.get(envelope.op, self._handle_unknown_op)
                handler(envelope)
                self._pulse_client_progress()

            except KeyboardInterrupt:
                print("🛑 User initiated shutdown (Ctrl+C)")
                self._initiate_shutdown("User Interruption")
                break

            except Exception as e:
                # Something went wrong in the Orchestrator logic itself.
                print(f"\n CRITICAL ORCHESTRATOR FAILURE")
                traceback.print_exc()

                #  Notify the Client
                self._send_client(
                    {
                        "msg": "error", "job_id": "system", "severity": SEV_FATAL,
                        "message": f"render pipeline Crash: {str(e)}"
                    }
                )

                # C. Resource Reclamation
                self._initiate_shutdown(f"System Error: {e}")

                # D. Exit the loop
                break

    def _handle_job_request(self, envelope: Envelope) -> None:
        """Queue a new job request. Start it immedidately if we are idle."""
        data = envelope.payload

        # Queue job
        self.pending_jobs.append(data)

        # Immediately run it if we're not busy.
        if not self.job_control.busy:
            self._start_next_job()
        else:
            # Notify client that the job is queued
            self._send_client(
                {
                    "msg": "error", "job_id": "1", "severity": SEV_WARNING,
                    "message": "Request is queued"
                }
            )

    def _start_next_job(self) -> None:
        """
        Start the next valid queued job.

        Walk through queued requests until a job successfully launches.
        If no job can be launched, the daemon transitions to the idle state.
        """
        while self.pending_jobs:
            # Grab next job
            json_job_req = self.pending_jobs.pop(0)
            job_id = json_job_req.get("job_id", "unknown")

            try:
                # Convert client request to a validated manifest.
                job_manifest = self._prepare_manifest(json_job_req)
            except (ValueError, IOError) as exc:
                # Error in request
                print(f"Job {job_id} error: {exc}")
                self._send_client(
                    {
                        "msg": "error", "job_id": job_id, "severity": SEV_CANCEL,
                        "message": str(exc), "report": "",
                    }
                )
                continue

            # LAUNCH JOB
            self._launch_job(job_manifest)
            return

    def _prepare_manifest(self, json_job_req) -> JobManifest:
        """
        Prepare job manifest
        Return manifest on success or raise exception
        """
        # 1. Parse render config and build the manifest
        job_manifest = self.resolver.create_job_manifest(json_job_req)

        # 2. Verify Engine has the required sources for this job - raises Exception
        self._verify_required_sources(job_manifest)

        # 3. Verify render config - raises Exception
        self._verify_render_config(job_manifest)

        return job_manifest

    def _launch_job(self, job_manifest: JobManifest) -> bool:
        """
        Launch the  job in the  manifests after preparing worker contexts.
        """
        job_id = job_manifest.job_id
        self.showtime("Launch Job")

        try:
            # Reset cache if job is for a different region
            self.runtime.sync_to_geography(job_manifest.region_id)

            # 4. Init render system
            self.render_system.init_render_engines(job_manifest.resources, self.runtime, )
            self.showtime("render system done")

            # 5. Initialize output file
            self._unlink_file_if_exists(job_manifest.temp_out_path)
            self.io_system.initialize_physical_output(
                job_manifest.temp_out_path, job_manifest.profile, )

            # 6. Reset telemetry
            if hasattr(self.runtime, "registry"):
                self.runtime.registry.start_session()

            # 7. Prepare worker contexts
            reader_ctx, worker_ctx, writer_ctx = self.render_system.prepare_job_contexts(
                job_manifest
            )
            self.showtime("Worker Context done")

            # 8. Initialize job control
            win_list = self._generate_job_windows(job_manifest)
            self.job_control = JobControl(
                manifest=job_manifest, total_tiles=len(win_list), )

            # 9. Publish context to workers
            self.runtime.update_context(
                job_id=job_id, reader_data=reader_ctx, worker_data=worker_ctx,
                writer_data=writer_ctx, )
            self.showtime("Job Context published")

            # 10. Initialize Dispatcher
            self.dispatcher.initialize_job(job_manifest, win_list)

            # Prime the pipeline
            candidates = self.dispatcher.get_priming_list(job_id)

            for result in candidates:
                for read_env in result.read_packets:
                    self.send_to_worker("reader_q", read_env)

                if result.render_packet:
                    self.send_to_worker(
                        "worker_q", Envelope(op=Op.RENDER_TILE, payload=result.render_packet), )
            self.showtime("pipeline primed")

            # Return to main  loop and continue rest of work
            return True

        except Exception:
            raise

    def _handle_job_finalized(self, envelope: Envelope) -> None:
        """
        Render Complete
        Publish the finalized temp file and notify the client.
        """
        if self.job_control is None:
            raise ValueError("[ORCHESTRATOR] Received TILES_FINALIZED but no job is active")

        finalized_job_id = envelope.payload
        if finalized_job_id != self.job_control.job_id:
            raise ValueError(
                f"[ORCHESTRATOR] Received TILES_FINALIZED for job '{finalized_job_id}', "
                f"but active job is '{self.job_control.job_id}'"
            )

        try:
            # Publish temp -> final atomically after writer flush/close completes
            self.job_control.temp_out_path.replace(
                self.job_control.final_out_path
            )  # print(f"✅ [Orchestrator] render complete for job: '{self.job_control.job_id}'")
        except Exception as exc:
            print(
                f"❌ [Orchestrator] Failed to publish temp output "
                f"'{self.job_control.temp_out_path}' -> '{self.job_control.final_out_path}': {exc}"
            )
            self._unlink_file_if_exists(self.job_control.temp_out_path)
            self._send_client(
                {
                    "msg": "error", "job_id": self.job_control.job_id,
                    "message": f"publish failure: {exc}",
                }
            )
            self.job_control.clear_job()
            self._start_next_job()
            return

        duration = self.job_control.elapsed
        print(
            f"[Orchestrator] ✅ JOB COMPLETE FOR  '{self.job_control.job_id}' "
            f"| Tiles: {self.job_control.total_tiles} "
            f"| Time: {duration:.3f}s "
            f"({(duration / self.job_control.total_tiles) * 1000:.1f}ms/tile)"
        )

        self._send_client(
            {
                "msg": "complete", "job_id": self.job_control.job_id,
                "path": str(self.job_control.final_out_path),
            }
        )
        self.showtime(f"JOB {self.job_control.job_id} COMPLETE")

        # CACHE stats
        self._print_cache_analysis()
        self.job_control.clear_job()
        self._start_next_job()

    def _handle_block_loaded(self, envelope: Envelope) -> None:
        """Advance a tile after one block finishes loading."""
        packet: BlockLoadedPacket = envelope.payload
        if not self.valid_job_id(packet.job_id):
            return

        render_packet = self.dispatcher.on_source_block_loaded(
            packet.job_id, packet.tile_id, packet.read_duration, )
        if render_packet is not None:
            self.send_to_worker(
                "worker_q", Envelope(op=Op.RENDER_TILE, payload=render_packet), )

    def _handle_tile_written(self, envelope: Envelope) -> None:
        """Release tile resources via the dispatcher and advance job progress."""
        packet: TileWrittenPacket = envelope.payload
        if not self.valid_job_id(packet.job_id):
            return
        if True:
            dbg(
                f" >>> [RECV] Q: {"status":8} | OP: {envelope.op.name:15} | TILE: "
                f"{packet.tile_id:<5} | "
                f"JOB: {packet.job_id}"
                f" [STATE] In-Flight: {"":<3} | Pending Jobs: {"":<2} | "
                f"Progress: ", packet.tile_id
            )

        if not self.dispatcher.on_tile_written(packet.tile_id):
            return

        is_complete = self.job_control.mark_tile_written()
        if is_complete:
            self._finalize_job()
            return

        dispatch_result: DispatchResult = self.dispatcher.dispatch_next_tile(
            self.job_control.job_id
        )
        if dispatch_result.tile_id is None:
            return

        for env in dispatch_result.read_packets:
            self.send_to_worker("reader_q", env)

        if dispatch_result.render_packet is not None:
            self.send_to_worker(
                "worker_q", Envelope(op=Op.RENDER_TILE, payload=dispatch_result.render_packet), )

    def _handle_job_cancel(self, envelope: Envelope) -> None:
        print(f"⚠️ [Orchestrator] Job Cancel : {envelope.op}")
        # 1. Global SHM Flip (Interruption)
        self.runtime.cancel_active_job()

        # 2. pipeline Signaling (Cleanup)
        self.send_to_worker('writer_q', Envelope(op=Op.JOB_CANCEL))

        # 3. Logic cleanup
        self.job_control.clear_job()
        self._start_next_job()

    def _handle_error(self, envelope: Envelope) -> None:
        """
        Handle a pipeline error, cancel output, or shutdown based on severity.
        Severity: 0=Fatal (Shutdown), 1=Cancel Job, 2=Warning (Continue)
        """
        payload: ErrorPacket = envelope.payload
        job_id = payload.job_id or self.job_control.job_id
        sev = payload.severity
        print(f"Err sev={sev}")

        # 1. Log to Orchestrator Console
        sev_label = {0: "FATAL", 1: "CANCEL", 2: "WARNING"}.get(sev)
        print(
            f"pipeline received: Sev: {sev_label} From: {payload.section}   "
            f"Job: '{job_id}' Error: {payload.message}"
        )

        # 2. Forward to Client Proxy
        # We send the raw severity so the client can decide how to color the UI
        self._send_client(
            {
                "msg": "error", "job_id": job_id, "severity": sev,
                "message": f"{payload.section} {sev_label.lower()}: {payload.message}",
            }
        )

        # 3. Action Logic
        if sev == SEV_WARNING:
            # Severity 2: Do nothing else; let the pipeline continue
            return

        if sev == SEV_CANCEL:
            # Severity 1: Stop the current job if it matches the active ID
            if self.job_control.job_id == job_id:
                # Notify Writer to unlink and close
                packet = JobDonePacket(job_id=self.job_control.job_id)
                self.send_to_worker('writer_q', Envelope(op=Op.JOB_CANCEL, payload=packet))

                # Reclaim Shared Memory Slots
                self.dispatcher.abort_job()
                self.runtime.cancel_active_job()

                # Local cleanup and state reset
                self.job_control.clear_job()
                self._start_next_job()

        elif sev == SEV_FATAL:
            # Severity 0: The system is in an unrecoverable state
            print(" FATAL ERROR: Initiating  system shutdown.")
            self._initiate_shutdown(" FATAL ERROR: Initiating  system shutdown.")

    def _handle_wr_abort(self, envelope: Envelope) -> None:
        self._initiate_shutdown(reason="")

    def valid_job_id(self, job_id):
        return job_id == self.job_control.job_id

    def _initiate_shutdown(self, reason: str) -> None:
        """Initiate daemon shutdown and delegate runtime teardown."""
        print(f"\n[Orchestrator] Shutdown Initiated: {reason}")
        self.running = False
        try:
            self.runtime.stop()
        except Exception as exc:
            print(f"   ⚠️ Runtime stop error: {exc}")

        print("✅ [Orchestrator] System Purge Complete. Daemon Halted.")

    def showtime(self, msg):
        wall_start = datetime.now()
        start_ts = wall_start.strftime("%H:%M:%S.%f")[:-3]
        if self.previous_ts is None:
            print(f"{start_ts} {msg}")
        else:
            print(f"{start_ts} {msg}. Elapsed: {wall_start - self.previous_ts}")
        self.previous_ts = wall_start

    def _handle_unknown_op(self, envelope: Envelope) -> None:
        """Fallback handler for unregistered OpCodes."""
        self.stats.unknown_ops += 1
        print(f"⚠️ [Orchestrator] ERROR: Unknown OpCode: {envelope.op}")
        self._initiate_shutdown(" ")

    def _print_cache_analysis(self):
        stats = self.runtime.registry.get_telemetry()

        hwm = stats['transit_hwm']
        limit = stats['transit_max']
        transit_utilization = (hwm / limit * 100) if limit > 0 else 0

        print(f"---  SHM CAPACITY REPORT ---")
        print(f"Static Cache:    {stats['static_used']}/{stats['static_total']} slots used.")
        print(
            f"Transit Highway: {hwm}/{limit} peak slots used ({transit_utilization:.1f}% pressure)."
        )

        print(f"\nAnalysis (Partition Tuning):")

        # RULE 1: Transit is potentially over-provisioned
        if transit_utilization < 30 and stats['static_used'] == stats['static_total']:
            suggested_extra_static = limit - (hwm + 5)  # Keep a small safety buffer of 5
            print(f"    Optimization: Transit potentially overprovisioned for this run.")
            print(f"     Your peak transit load was {hwm} slots.")
            print(f"     You could decrease 'transit_buffer_factor' for this run.")

        # RULE 2: Transit is near-death (Deadlock risk)
        elif transit_utilization > 90:
            print(f"  🛑 Critical: TRANSIT HIGHWAY SATURATED.")
            print(f"     Peak usage {hwm} is too close to limit {limit}.")
            print(
                f"     Danger of pipeline deadlock! Increase 'transit_buffer_factor' or increase "
                f"'input_slots'."
            )

        # RULE 3: Balanced
        else:
            print(f"  ✅ Healthy: Transit highway has sufficient headroom.")

        # RULE 4: Static Cache too small for  region
        if stats['static_used'] == stats['static_total'] and stats['misses'] > stats[
            'static_total']:
            print(
                f"  💡 Notice: Region is larger than Static Cache. Consider increasing "
                f"'input_slots' to fit the whole run in RAM."
            )

    @staticmethod
    def _generate_job_windows(manifest: JobManifest) -> List[Window]:
        """Calculates global windows using a uniform 256x256 grid."""

        if manifest.envelope is not None:
            # Use the existing preview envelope
            target_env = manifest.envelope
        else:
            # FULL RENDER: Create a virtual envelope covering the entire anchor
            meta = manifest.source_metadata[manifest.resources.anchor_key]
            target_env = Window(0, 0, meta['width'], meta['height'])

        tiles = []
        # Step by 256 pixels across the target area
        for r in range(int(target_env.row_off), int(target_env.row_off + target_env.height), 256):
            for c in range(
                    int(target_env.col_off), int(target_env.col_off + target_env.width), 256
            ):
                # Calculate width/height, ensuring we don't go out of bounds
                w = min(256, int(target_env.col_off + target_env.width) - c)
                h = min(256, int(target_env.row_off + target_env.height) - r)
                tiles.append(Window(c, r, w, h))

        return tiles

    def send_to_worker(self, queue_attr: str, envelope: Envelope) -> None:
        """
        Hardened IPC dispatch for worker queues with fault detection.
        """
        # 1. DEFENSIVE QUEUE LOOKUP
        # getattr is safe, but we must ensure the attribute exists and isn't None
        queue = getattr(self.runtime, queue_attr, None)
        if queue is None:
            print(f"⚠️ [SEND_ERROR] Target queue '{queue_attr}' is not initialized.")
            return

        # 2. THE HARDENED 'PUT'
        try:
            # We use a non-blocking put or a very short timeout to detect
            # deadlocked queues, but for this architecture, a standard put
            # wrapped in exception handling is usually the 'Fact-Based' choice.
            queue.put(envelope)
        except (OSError, ValueError, BrokenPipeError) as e:
            # This happens if the queue was closed by another process
            # or the system is halfway through a shutdown.
            print(f"❌ [IPC FAILURE] Cannot send to {queue_attr}: {e}")
            # If the system is supposed to be running, this is a fatal logic error
            if self.running:
                self._initiate_shutdown(f"IPC Channel {queue_attr} collapsed.")
            return

        # 3. DEFENSIVE METADATA EXTRACTION
        # We must assume self.job_control or envelope.payload could be None
        # during edge-case state transitions (like a shutdown).
        payload = envelope.payload
        tile_id = getattr(payload, 'tile_id', '-')

        # Safe Job ID fallback
        job_id = "N/A"
        if payload and hasattr(payload, 'job_id'):
            job_id = payload.job_id
        elif self.job_control:
            job_id = self.job_control.job_id

        # 4. SAFE STATE CAPTURE
        pending_jobs = len(self.pending_jobs)
        in_flight = len(self.dispatcher.active_tiles) if self.dispatcher else 0

        # Calculate progress only if job_control is active
        if self.job_control:
            prog_str = f"{self.job_control.tiles_written}/{self.job_control.total_tiles}"
        else:
            prog_str = "IDLE"

        # 5. VISIBILITY (Using the existing dbg helper)
        dbg(
            f" >>> [SEND] Q: {queue_attr:8} | OP: {envelope.op.name:15} | TILE: {tile_id:<5} | "
            f"JOB: {job_id:10} | "
            f"[STATE] In-Flight: {in_flight:<3} | Pending: {pending_jobs:<2} | "
            f"Progress: {prog_str}", tile_id
        )

    @staticmethod
    def _build_temp_output_path(final_path: Path, job_id: str) -> Path:
        """Return a temp output path in the same directory as the final output."""
        return final_path.with_name(f"{final_path.stem}.{job_id}.tmp{final_path.suffix}")

    @staticmethod
    def _unlink_file_if_exists(path: Optional[Path]) -> None:
        """Best-effort unlink."""
        if path is None:
            return
        try:
            if path.exists():
                path.unlink()
        except Exception as exc:
            print(f"⚠️ [Orchestrator] Failed to unlink temp file '{path}': {exc}")

    def _finalize_job(self) -> None:
        """Begin successful job finalization by asking the writer to flush and close."""
        if self.job_control is None:
            raise ValueError("[ORCHESTRATOR] Finalize Job but no job is active")

        packet = JobDonePacket(job_id=self.job_control.job_id)
        self.send_to_worker('writer_q', Envelope(op=Op.JOB_DONE, payload=packet))

    def _send_client(self, payload: dict) -> None:
        """Send a response payload back to the socket proxy."""
        self.runtime.response_q.put(payload)

    def update_telemetry(self, op):
        self.stats.last_op = op
        self.stats.op_counts[op] += 1
        self.stats.print_report(orchestrator=self, interval=1.0)

    def _pulse_client_progress(self) -> None:
        """Send a progress heartbeat to the client if a job is active."""
        if not self.job_control.busy:
            return

        now = time.perf_counter()
        if now - self.last_progress_pulse < 0.3:
            return

        job = self.job_control

        # Calculate float percentage and clamp to a visible minimum.
        raw_pct = (job.tiles_written / job.total_tiles) * 100.0 if job.total_tiles > 0 else 0.0
        pct = round(min(99.99, max(0.01, raw_pct)), 2)

        self._send_client(
            {
                "msg": "progress", "request_id": job.job_id, "progress": pct, "message": "",
            }
        )
        self.last_progress_pulse = now

    def _verify_required_sources(self, job_manifest) -> None:
        """Verify that all sources required by the render are available.

        Args:
            job_manifest: Resolved manifest for the pending job.

        Raises:
            Exception if not all required sources are available
        """
        job_id = job_manifest.job_id

        required_sources = set(job_manifest.resources.sources.keys())
        allocated_pools = set(self.runtime.pool_map.keys())

        if required_sources.issubset(allocated_pools):
            return

        missing = required_sources - allocated_pools
        missing_sorted = ", ".join(sorted(missing))

        error_msg = (f"⚠️ Job: {job_id} - Missing source configuration\n"
                     f"This render requires source(s) that are not available in the current "
                     f"engine configuration.\n"
                     f"Missing: {missing_sorted}\n"
                     f"To fix this, add the missing source(s) to 'engine_config.yml' under "
                     f"'source_specs', then restart Land Weaver Server.")
        raise ValueError(f"⚠️ {error_msg}")

    def _verify_render_config(self, job_manifest) -> None:
        """Verify that the render pipeline is internally valid.

        Args:
            job_manifest: Resolved manifest for the pending job.

        Raises:
            Exception if the pipeline audit fails.
        """
        audit_ctx = SimpleNamespace(
            render_cfg=job_manifest.render_cfg, eng_resources=self.runtime,
            theme_registry=self.render_system.theme_reg,
            anchor_key=job_manifest.resources.anchor_key, )

        has_errors, report_md, raw_errors = self.render_system.analyze_pipeline(audit_ctx)
        if has_errors:
            error_summary = "\n".join(f"• {err}" for err in raw_errors[:2])

            if len(raw_errors) > 2:
                error_summary += f"\n...and {len(raw_errors) - 2} more errors."

            final_msg = f"⚠️ render config errors:\n{error_summary}"
            raise ValueError(final_msg)


@dataclass
class JobTelemetry:
    job_id: str = "IDLE"
    start_time: float = 0.0
    last_report_time: float = 0.0
    total_tiles: int = 0
    tiles_written: int = 0
    op_counts: Counter = field(default_factory=Counter)
    last_op: Op = Op.TILES_FINALIZED
    unknown_ops: int = 0
    bad_job_ids: int = 0
    pending_dependencies: dict[int, int] = field(default_factory=dict)

    def reset(self, job_id: str, total_tiles: int) -> None:
        """Reset telemetry for a newly started job."""
        self.job_id = job_id
        self.total_tiles = total_tiles
        self.tiles_written = 0
        self.start_time = time.perf_counter()
        self.last_report_time = 0.0
        self.pending_dependencies.clear()

    def print_report(
            self, *, orchestrator: "PipelineOrchestrator", interval: float = 5.0, ) -> None:
        """Print a throttled runtime report for debugging."""
        return
