from __future__ import annotations

import logging

from disolv_positions.common.config import *
from disolv_positions.vehicle.sumo import SumoConverter

logger = logging.getLogger(__name__)

SUMO = "sumo"


class VehicleConverter:
    def __init__(self, config: Config) -> None:
        """The constructor of the VehicleConverter class."""
        self.config = config
        self.vehicle_file = None
        self.vehicle_count = 0

    def create_vehicles(self) -> int:
        """Create the vehicle data."""
        logger.debug("Read SUMO output files")
        output_path = self.config.path / self.config.get(OUTPUT_SETTINGS)[OUTPUT_PATH]
        if self.config.get(VEHICLE_SETTINGS)[SIMULATOR] == SUMO:
            sumo_converter = SumoConverter(
                self.config,
                output_path,
            )
            sumo_converter.fcd_to_parquet()
            self.vehicle_count = sumo_converter.get_unique_vehicle_count()
            self.vehicle_file = sumo_converter.get_parquet_file()
        return self.vehicle_count
