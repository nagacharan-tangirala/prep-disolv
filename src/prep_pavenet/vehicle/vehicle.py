from __future__ import annotations

import logging

from prep_pavenet.setup.config import *
from prep_pavenet.vehicle.sumo import SumoConverter

logger = logging.getLogger(__name__)

SUMO = "sumo"


def create_vehicles(config: Config) -> None:
    """Create the vehicle data."""
    logger.debug("Read SUMO output files")
    output_path = config.path / config.get(OUTPUT_SETTINGS)[OUTPUT_PATH]
    if config.get(VEHICLE_SETTINGS)[SIMULATOR] == SUMO:
        sumo_converter = SumoConverter(
            config.get(TRAFFIC_SETTINGS),
            config.get(VEHICLE_SETTINGS),
            config.path,
            output_path,
        )
        sumo_converter.fcd_to_parquet()
