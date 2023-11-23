from __future__ import annotations

import logging

from prep_pavenet.controller.central import CentralControllerPlacer
from prep_pavenet.setup.config import *

logger = logging.getLogger(__name__)

CENTER = "center"


def create_controllers(config: Config, controller_id_init: int) -> int:
    """Create the controller data."""
    logger.debug("Read Controller data")
    output_path = config.path / config.get(OUTPUT_SETTINGS)[OUTPUT_PATH]
    if config.get(CONTROLLER_SETTINGS)[PLACEMENT] == CENTER:
        controller_placer = CentralControllerPlacer(
            config.get(TRAFFIC_SETTINGS),
            config.get(CONTROLLER_SETTINGS),
            config.path,
            output_path,
            controller_id_init,
        )
        controller_placer.create_controller_data()
        return 1
