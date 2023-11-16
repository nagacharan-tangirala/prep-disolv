import logging

from prep_pavenet.setup.config import Config, LOG_SETTINGS
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
        """ Prepare the scenario."""
        logger.debug("Preparing the Vehicle Data")
        self._create_vehicle_data()
        logger.debug("Preparing RSU data")
        self._create_rsu_data()
        logger.debug("Preparing Base Station data")
        self._create_base_station_data()
        logger.debug("Preparing Controller data")
        self._create_controller_data()
        logger.debug("Scenario is prepared")

    def _create_vehicle_data(self) -> None:
        """Create the vehicle data."""
        create_vehicles(self.config)

    def _create_rsu_data(self) -> None:
        """Create the RSU data."""

    def _create_controller_data(self) -> None:
        """Create the controller data."""
        pass

    def _create_base_station_data(self) -> None:
        """Create the base station data."""
        pass
