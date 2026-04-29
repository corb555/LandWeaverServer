import argparse

from landweaverserver.pipeline.pipeline_service import PipelineService
from landweaverserver.render.render_config import RenderConfig
from landweaverserver.render.render_system import RenderSystem


def _validate_file_existence(cfg: RenderConfig):
    """
    Since validate_paths was removed from ConfigMgr to keep it logic-only,
     we perform the disk check here before starting the engine.
    """
    if cfg is None:
        raise ValueError("_validate_file_existence: cfg is None")

    for key, path in cfg.files.items():
        if key == "output":
            if not path.parent.exists():
                raise FileNotFoundError(f"Output directory missing: {path.parent}")
            continue

        if not path.exists():
            raise FileNotFoundError(f"Required input file missing: [{key}] -> {path}")


def main():
    parser = argparse.ArgumentParser(description="Land Weaver Server")
    parser.add_argument("--config", required=True, help="Path to the YAML config file")
    parser.add_argument("--describe", action="store_true", help="Generate pipeline description")

    print("Land Weaver Server")
    print("!  " + "NOTICE: THIS IS BETA SOFTWARE. DO NOT USE FOR PRODUCTION.".center(64) + "  !")
    print("!  " + "Features and configuration will change without notice.".center(64) + "  !")
    args = parser.parse_args()
    print(f"System Config File: {args.config}")

    # 2. Initialize Engine
    render_system = RenderSystem()
    pipeline = PipelineService(args.config, render_system)

    # 4. Execute renders
    try:
        pipeline.start()
        print("🛑 Shutdown complete.")
    except Exception as e:
        print(f"\n❌ pipeline error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
