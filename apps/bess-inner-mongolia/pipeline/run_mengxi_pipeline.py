import logging
import os
from datetime import datetime

from services.loader.province_misc_to_db_v2 import main as load_province_misc
from services.common.focused_assets_data import main as build_focused_assets_data

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


def _as_bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y"}


def main() -> None:
    """
    Orchestrates the Inner Mongolia pipeline inside the existing ECS/RDS framework.

    Expected behavior:
    1. refresh province misc datasets
    2. refresh focused company-assets datasets
    3. leave room for Mengxi-specific post-processing later
    """
    started_at = datetime.utcnow()
    logger.info("Starting Mengxi pipeline at %s", started_at.isoformat())

    run_province_misc = _as_bool(os.getenv("RUN_PROVINCE_MISC"), True)
    run_focused_assets = _as_bool(os.getenv("RUN_FOCUSED_ASSETS"), True)

    try:
        if run_province_misc:
            logger.info("Step 1: loading province misc data")
            load_province_misc()
        else:
            logger.info("Skipping province misc step")

        if run_focused_assets:
            logger.info("Step 2: building focused assets data")
            build_focused_assets_data()
        else:
            logger.info("Skipping focused assets step")

        # Placeholder for future Mengxi-specific result generation
        # from services.bess_inner_mongolia.result_loader import refresh_mengxi_results
        # refresh_mengxi_results()

        finished_at = datetime.utcnow()
        logger.info("Mengxi pipeline completed at %s", finished_at.isoformat())

    except Exception:
        logger.exception("Mengxi pipeline failed")
        raise


if __name__ == "__main__":
    main()