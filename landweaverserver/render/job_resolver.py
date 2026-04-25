from pathlib import Path
from typing import Callable, Optional

from rasterio.windows import Window

from landweaverserver.pipeline.io_manager import IOManager
from landweaverserver.pipeline.job_control import JobManifest
from landweaverserver.render.render_config import derive_resources, RenderConfig


class JobResolver:
    def __init__(self, config_loader: Callable[[Path], RenderConfig], render_system):
        self.config_loader = config_loader
        self.job_id = 0
        self.render_system = render_system

        """
        Parse the client json job request and create a job manifest
        {
          "msg": "render_request",
          "request_id": 12,
          "params": {
            "percent": 0.2,
            "row": 0.1,
            "col": 0.9,
            "prefix": "Sedona"
            "output_suffix": "_biome"
            }
        }
        """

    def create_job_manifest(self, json_request: dict) -> JobManifest:
        """
        Parse the client json job request and create a job manifest.
        """
        job_id = str(self.job_id)
        self.job_id += 1

        # 1. Extract and Resolve Base Paths
        params = json_request.get("params", {})

        config_dir = Path("config").expanduser()
        config_path = Path("config", "render.yml").resolve()

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found at: {config_path}")

        region = params.get("prefix")
        build_dir = Path("build", region).resolve()
        suffix = params.get("output_suffix")
        output_file = f"{region}{suffix}.tif"
        output_path = Path("build", region, output_file).resolve()

        print("=" * 60)
        print(f"\n\n[PIPELINE] NEW JOB REQUEST - create_job_manifest for Job '{job_id}'")
        print(f"   - Config: {config_path}")

        # 2. Load and Resolve internal configuration
        try:
            render_cfg = self.config_loader(config_path)
        except Exception as e:
            raise ValueError(f"Render config '{config_path}' - error: {str(e)}")

        try:
            # We pass the pre-calculated config_dir and build_dir Path objects
            render_cfg.resolve_paths(
                prefix=params.get("prefix", ""), config_dir=config_dir, build_dir=build_dir,
                output_path=str(output_path)
            )
        except Exception as e:
            raise ValueError(f"Render Config path resolution error: {str(e)}")

        # 3. Resolve output targets
        final_out_path = Path(render_cfg.files["output"])
        temp_out_path = self.build_temp_output_path(final_out_path, job_id)
        render_cfg.files["output"] = temp_out_path

        # 4. Resolve Resources and Geography
        resources = derive_resources(render_cfg=render_cfg)

        # Setup preview parameters
        percent = float(params.get("percent", 1.0))
        row_focal = float(params.get("row", 0.0))
        col_focal = float(params.get("col", 0.0))
        envelope: Optional[Window] = None
        write_offset = (0, 0)
        source_metadata = {}

        # 5. Hashing
        geography_hash, final_hashes = self.render_system.resolve_job_hashes(render_cfg, resources)

        # Bind the identity to the resources
        resources = resources.with_hashes(
            geography_hash=geography_hash, hashes=final_hashes
        )

        # 6. Path Validation and Profile Building
        try:
            with IOManager(render_cfg, resources.sources, resources.anchor_key) as io:
                profile = self.build_output_profile(io)

                for dkey in resources.sources:
                    try:
                        src = io.sources[dkey]
                        source_metadata[dkey] = {"width": src.width, "height": src.height}
                    except Exception as e:
                        raise IOError(f"IO error opening {dkey}: {str(e)}")

                if 0.0 < percent < 1.0:
                    envelope = self.calculate_preview_window(
                        io.anchor_src, percent=percent, rel_x=col_focal, rel_y=row_focal
                    )
                    if envelope is not None:
                        profile.update(
                            {
                                "width": int(envelope.width), "height": int(envelope.height),
                                "transform": io.anchor_src.window_transform(envelope),
                            }
                        )
                        write_offset = (int(envelope.row_off), int(envelope.col_off))

        except Exception as e:
            raise IOError(f"Resource validation failed: {str(e)}")

        return JobManifest(
            job_id=job_id, render_cfg=render_cfg, resources=resources,
            final_out_path=final_out_path, temp_out_path=temp_out_path, profile=profile,
            region_id=geography_hash, envelope=envelope, write_offset=write_offset,
            render_params=(percent, row_focal, col_focal), source_metadata=source_metadata
        )

    @staticmethod
    def build_temp_output_path(final_path: Path, job_id: str) -> Path:
        """
        Build a temporary output path in the same directory as the final output.

        Args:
            final_path: Final published output path.
            job_id: Active job identifier.

        Returns:
            Temporary render output path.
        """
        return final_path.with_name(f"{final_path.stem}.{job_id}.tmp")

    @staticmethod
    def build_output_profile(io: "IOManager") -> dict:
        """
        Generate the Rasterio profile for the output GeoTIFF.

        Args:
            io: Open IO manager for the anchor dataset.

        Returns:
            Raster output profile dictionary.
        """
        anchor = io.anchor_src
        return {
            "driver": "GTiff", "height": anchor.height, "width": anchor.width, "count": 3,
            "dtype": "uint8", "crs": anchor.crs, "transform": anchor.transform, "tiled": True,
            "blockxsize": 256, "blockysize": 256, "compress": "deflate", "predictor": 2,
            "nodata": None,
        }

    @staticmethod
    def calculate_preview_window(
            src, percent: float, rel_x: float, rel_y: float, ) -> Window:
        """
        Calculate a global preview window using normalized focal coordinates.

        Args:
            src: Anchor Rasterio source.
            percent: Fraction of the full image size to render.
            rel_x: Horizontal focal point in normalized coordinates.
            rel_y: Vertical focal point in normalized coordinates.

        Returns:
            Block-aligned preview window.
        """
        full_w, full_h = src.width, src.height
        target_w, target_h = int(full_w * percent), int(full_h * percent)

        col_off = int(full_w * rel_x) - (target_w // 2)
        row_off = int(full_h * rel_y) - (target_h // 2)

        col_off = (max(0, col_off) // 256) * 256
        row_off = (max(0, row_off) // 256) * 256

        col_off = max(0, min(col_off, full_w - target_w))
        row_off = max(0, min(row_off, full_h - target_h))

        return Window(
            col_off, row_off, min(target_w, full_w - col_off), min(target_h, full_h - row_off), )
