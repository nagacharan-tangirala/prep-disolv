from __future__ import annotations

import logging

from prep_pavenet.controller.controller import ControllerConverter
from prep_pavenet.links.all_links import DeviceLinks
from prep_pavenet.rsu.rsu import RsuConverter
from prep_pavenet.common.config import (
    LOG_SETTINGS,
    Config,
    DATA_STEP_SETTINGS,
    VEHICLE_DATA,
    RSU_DATA,
    LINK_DATA,
    BASE_STATION_DATA,
    CONTROLLER_DATA,
)
from prep_pavenet.common.logger import setup_logging
from prep_pavenet.vehicle.vehicle import VehicleConverter

logger = logging.getLogger(__name__)


class Core:
    def __init__(self, config_file: str):
        self.config: Config = Config(config_file)
        self.vehicle_file = None
        self.rsu_file = None
        self.controller_file = None

    def prepare_scenario(self) -> None:
        setup_logging(self.config.path, self.config.settings.get(LOG_SETTINGS))
        logger.info("Initializing the scenario preparation")
        self._prepare_scenario()

    def _prepare_scenario(self) -> None:
        """Prepare the scenario."""
        total_node_count = 0
        if self.config.get(DATA_STEP_SETTINGS).get(VEHICLE_DATA):
            logger.info("Preparing the Vehicle Data")
            vehicle_count = self._create_vehicle_data()
            total_node_count += vehicle_count
            vehicle_msg = f"Number of vehicles: {vehicle_count}"
            logger.info(vehicle_msg)
        else:
            logger.info("Skipping vehicle data")

        if self.config.get(DATA_STEP_SETTINGS).get(RSU_DATA):
            logger.info("Preparing RSU data")
            rsu_count = self._create_rsu_data(total_node_count)
            total_node_count += rsu_count
            rsu_msg = f"Number of RSUs: {rsu_count}"
            logger.info(rsu_msg)
        else:
            logger.info("Skipping RSU data")

        if self.config.get(DATA_STEP_SETTINGS).get(CONTROLLER_DATA):
            logger.info("Preparing Controller data")
            self._create_controller_data(total_node_count)
        else:
            logger.info("Skipping Controller data")

        if self.config.get(DATA_STEP_SETTINGS).get(BASE_STATION_DATA):
            logger.info("Preparing Base Station data")
            self._create_base_station_data()
        else:
            logger.info("Skipping Base Station data")

        if self.config.get(DATA_STEP_SETTINGS).get(LINK_DATA):
            logger.info("Creating the links")
            self._create_links()
        logger.info("Scenario is prepared")

    def _create_vehicle_data(self) -> int:
        """Create the vehicle data."""
        vehicle_converter = VehicleConverter(self.config)
        vehicle_converter.create_vehicles()
        self.vehicle_file = vehicle_converter.vehicle_file
        return vehicle_converter.vehicle_count

    def _create_rsu_data(self, rsu_init_id: int) -> int:
        """Create the RSU data."""
        rsu_converter = RsuConverter(self.config)
        rsu_converter.create_rsu(rsu_init_id)
        self.rsu_file = rsu_converter.rsu_file
        return rsu_converter.rsu_count

    def _create_controller_data(self, controller_init_id) -> None:
        """Create the controller data."""
        controller_converter = ControllerConverter(self.config)
        controller_converter.create_controllers(controller_init_id)
        self.controller_file = controller_converter.controller_file
        return controller_converter.controller_count

    def _create_base_station_data(self) -> None:
        """Create the base station data."""

    def _create_links(self) -> None:
        """Create the links."""
        logger.info("Creating the links")
        assert self.vehicle_file is not None
        assert self.rsu_file is not None
        assert self.controller_file is not None
        device_links = DeviceLinks(
            self.vehicle_file, self.rsu_file, self.controller_file, self.config
        )
        device_links.create_all_links()
