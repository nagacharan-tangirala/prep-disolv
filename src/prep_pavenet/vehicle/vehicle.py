import logging

from prep_pavenet.setup.config import VEHICLE_SETTINGS, Config
from prep_pavenet.vehicle.from_sumo import fcd_to_parquet

logger = logging.getLogger(__name__)

SUMO_FILE = "sumo_output"


def create_vehicles(config: Config) -> None:
    """Create the vehicle data."""
    logger.debug("Read SUMO output files")
    fcd_to_parquet(config.settings.get(VEHICLE_SETTINGS).get(SUMO_FILE), config.path)
