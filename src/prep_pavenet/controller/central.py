from __future__ import annotations

from pathlib import Path

import pandas as pd

from prep_pavenet.common.columns import (
    ACTIVATION_COLUMNS,
    ACTIVATIONS_FOLDER,
    CONTROLLER_COLUMNS,
    POSITIONS_FOLDER,
)
from prep_pavenet.common.utils import get_center, get_offsets
from prep_pavenet.common.config import END_TIME, ID_INIT, NETWORK_FILE, START_TIME
from prep_pavenet.rsu.junction import get_lat_lon

CENTRE = "center"


class CentralControllerPlacer:
    def __init__(
        self,
        trace_config: dict,
        cont_config: dict,
        config_path: Path,
        output_path: Path,
        controller_id_init: int,
    ) -> None:
        """The constructor of the CentralControllerPlacer class."""
        self.output_path: Path = output_path
        self.config_path: Path = config_path
        self.start_time: int = cont_config[START_TIME]
        self.end_time: int = cont_config[END_TIME]
        self.id_init: int = cont_config[ID_INIT]
        self.sumo_net: Path = config_path / trace_config[NETWORK_FILE]
        self.controller_id_init = controller_id_init
        self.controller_file = (
            self.output_path / POSITIONS_FOLDER / "controllers.parquet"
        )

    def create_controller_data(self):
        """Create the controller data."""
        self._write_activation_data()
        self._write_controller_data()

    def get_controller_file(self) -> Path:
        """Return the controller file."""
        return self.controller_file

    def _write_activation_data(self) -> None:
        """Write the activation data of controller to a file."""
        activation_file = (
            self.output_path / ACTIVATIONS_FOLDER / "controller_activations.parquet"
        )
        controller_id = self.id_init
        activation_df = pd.DataFrame(
            [[controller_id, self.controller_id_init, self.start_time, self.end_time]],
            columns=ACTIVATION_COLUMNS,
        )
        activation_df.to_parquet(activation_file, index=False)

    def _write_controller_data(self) -> None:
        """Write the controller data to a file."""
        controller_id = self.id_init
        centers = get_center(self.sumo_net)
        center_lat, center_lon = get_lat_lon(centers[0], centers[1], self.sumo_net)
        controller_df = pd.DataFrame(
            [
                [
                    0,
                    controller_id,
                    self.controller_id_init,
                    centers[0],
                    centers[1],
                    center_lat,
                    center_lon,
                ]
            ],
            columns=CONTROLLER_COLUMNS,
        )
        controller_df.to_parquet(self.controller_file, index=False)
