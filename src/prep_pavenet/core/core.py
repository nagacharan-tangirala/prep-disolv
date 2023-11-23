from __future__ import annotations

import logging

from prep_pavenet.controller.controller import create_controllers
from prep_pavenet.rsu.rsu import create_rsu
from prep_pavenet.setup.config import LOG_SETTINGS, Config
from prep_pavenet.setup.logger import setup_logging
from prep_pavenet.vehicle.vehicle import create_vehicles

logger = logging.getLogger(__name__)


class Core:
    def __init__(self, config_file: str):
        self.config = Config(config_file)

    def prepare_scenario(self) -> None:
        setup_logging(self.config.path, self.config.settings.get(LOG_SETTINGS))
        logger.debug("Initializing the scenario preparation")
        self._prepare_scenario()

    def _prepare_scenario(self) -> None:
        """Prepare the scenario."""
        total_node_count = 0
        logger.debug("Preparing the Vehicle Data")
        vehicle_count = self._create_vehicle_data()
        total_node_count += vehicle_count
        logger.debug(f"Number of vehicles: {vehicle_count}")

        logger.debug("Preparing RSU data")
        rsu_count = self._create_rsu_data(total_node_count)
        total_node_count += rsu_count
        logger.debug(f"Number of RSUs: {rsu_count}")

        logger.debug("Preparing Controller data")
        self._create_controller_data(total_node_count)

        logger.debug("Scenario is prepared")
        logger.debug("Preparing Base Station data")
        self._create_base_station_data()

    def _create_vehicle_data(self) -> int:
        """Create the vehicle data."""
        return create_vehicles(self.config)

    def _create_rsu_data(self, rsu_init_id: int) -> int:
        """Create the RSU data."""
        return create_rsu(self.config, rsu_init_id)

    def _create_controller_data(self, controller_init_id) -> None:
        """Create the controller data."""
        return create_controllers(self.config, controller_init_id)

    def _create_base_station_data(self) -> None:
        """Create the base station data."""
