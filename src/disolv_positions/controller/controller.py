from __future__ import annotations

import logging

from disolv_positions.controller.central import CentralControllerPlacer
from disolv_positions.common.config import (
    CONTROLLER_SETTINGS,
    OUTPUT_PATH,
    OUTPUT_SETTINGS,
    PLACEMENT,
    TRAFFIC_SETTINGS,
    Config,
)

logger = logging.getLogger(__name__)

CENTER = "center"


class ControllerConverter:
    def __init__(self, config: Config) -> None:
        """The constructor of the ControllerConverter class."""
        self.config = config
        self.controller_file = None
        self.controller_count = 0

    def create_controllers(self, controller_id_init: int) -> int:
        """Create the controller data."""
        logger.debug("Read Controller data")
        output_path = self.config.path / self.config.get(OUTPUT_SETTINGS)[OUTPUT_PATH]
        if self.config.get(CONTROLLER_SETTINGS)[PLACEMENT] == CENTER:
            controller_placer = CentralControllerPlacer(
                self.config.get(TRAFFIC_SETTINGS),
                self.config.get(CONTROLLER_SETTINGS),
                self.config.path,
                output_path,
                controller_id_init,
            )
            controller_placer.create_controller_data()
            self.controller_file = controller_placer.get_controller_file()
            self.controller_count = 1
        return self.controller_count
