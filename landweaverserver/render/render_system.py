import hashlib
from pathlib import Path
from typing import Tuple

from landweaverserver.common.keys import RequiredResources
from landweaverserver.pipeline.job_control import JobManifest
from landweaverserver.pipeline.worker_contexts import WriterContext, WorkerContext, ReaderContext
from landweaverserver.render.compositing_engine import CompositingEngine
from landweaverserver.render.factor_engine import FactorEngine
from landweaverserver.render.noise_engine import NoiseEngine
from landweaverserver.render.surface_engine import SurfaceEngine
from landweaverserver.render.theme_registry import ThemeRegistry
from landweaverserver.render.client_message_schema import RENDER_REQUEST_SCHEMA
from landweaverserver.render.render_config import RenderConfig, analyze_pipeline

CONFIG_PATH = Path("config/render.yml")


class RenderSystem:
    """
    Orchestrator-side container for rendering logic.
    Handles persistent engines and packages them into worker contexts.
    """

    def __init__(self):
        # Persistent Math Engines
        self.noise_engine = None
        self.theme_reg = None
        self.factor_eng = None
        self.surface_eng = None
        self.compositor = None
        self.render_cfg = RenderConfig.load(CONFIG_PATH)

    def init_render_engines(self, resources, eng_resources):
        """Initial bootstrap of all render engines."""
        if self.factor_eng is None:
            self.surface_eng = SurfaceEngine(self.render_cfg)
            self.compositor = CompositingEngine()
            self.theme_reg = ThemeRegistry(self.render_cfg)

            # 1. Noise System
            self.noise_engine = NoiseEngine(
                self.render_cfg, profiles=self.render_cfg.noises, create_shm=True
                )
            eng_resources.manage_noise_engine(self.noise_engine)

            # 2. Factor System
            self.factor_eng = FactorEngine(
                self.render_cfg, self.theme_reg, self.noise_engine, self.render_cfg.factors,
                resources, None
            )
            print("FactorEngine initialized")

    @staticmethod
    def analyze_pipeline(ctx):
        return analyze_pipeline(ctx)

    def load(self, pth):
        return RenderConfig.load(CONFIG_PATH)

    def prepare_job_contexts(
            self, manifest: 'JobManifest'
    ) -> Tuple['ReaderContext', 'WorkerContext', 'WriterContext']:
        """
        Synchronizes engines with the manifest and builds the
        serialization-ready contexts for the workers.
        """
        # 1. Logic Sync: Update engines with current job settings
        self.theme_reg.load_metadata(manifest.render_cfg)
        self.factor_eng.cfg = manifest.render_cfg
        self.surface_eng.cfg = manifest.render_cfg

        # 3. Assemble Reader Context
        reader_ctx = ReaderContext(
            render_cfg=manifest.render_cfg, anchor_key=manifest.resources.anchor_key,
            source_paths=manifest.resources.sources, job_id=manifest.job_id
        )

        # 4. Assemble Renderer Context
        worker_ctx = WorkerContext(
            render_cfg=manifest.render_cfg, themes=self.theme_reg, compositor=self.compositor,
            pipeline=manifest.render_cfg.pipeline, anchor_key=manifest.resources.anchor_key,
            surface_inputs=manifest.resources.surface_inputs, resources=manifest.resources,
            noise_registry=self.noise_engine, job_id=manifest.job_id
        )

        # 5. Assemble Writer Context
        writer_ctx = WriterContext(
            output_path=manifest.temp_out_path, output_profile=manifest.profile,
            write_offset_row=manifest.write_offset[0], write_offset_col=manifest.write_offset[1],
            job_id=manifest.job_id
        )

        return reader_ctx, worker_ctx, writer_ctx

    def resolve_job_hashes(self, render_cfg: RenderConfig, resources: RequiredResources):
        """
        Consolidates YAML state and File-System state into buckets.
        """
        # Bucket 1: Geography ( source paths and timestamps)
        geography_hash = self.get_region_hash(resources)

        # Bucket 2: Logic (Topology and math structure from YAML)
        config_hashes = render_cfg.get_hashes()

        # Bucket 3: Style (YAML settings + External Ramp File timestamps)
        # We augment the YAML 'style' hash with the ' asset' hash
        ramp_hash = SurfaceEngine.get_ramp_hash(render_cfg, resources)

        # Append ramp hash to style hash
        style_hash = f"{config_hashes.get('style', '')}_{ramp_hash}"

        return geography_hash, {
            "topology": config_hashes.get("topology"), "logic": config_hashes.get("logic"),
            "style": style_hash
        }

    def get_source_specs(self):
        return self.render_cfg.source_specs

    @staticmethod
    def get_render_request_schema():
        return RENDER_REQUEST_SCHEMA

    @staticmethod
    def get_region_hash(resources) -> str:
        """
        Create a stable hash based on file paths and modification timestamps.

        Args:
            resources: Resolved render resources.

        Returns:
            Stable hash representing the current source-data region context.
        """
        context_parts = []

        for path in sorted(Path(p).resolve() for p in resources.sources.values()):
            stat = path.stat()
            context_parts.append(f"{path}|{stat.st_mtime_ns}")

        raw_context = "|".join(context_parts)
        return hashlib.md5(raw_context.encode("utf-8")).hexdigest()
