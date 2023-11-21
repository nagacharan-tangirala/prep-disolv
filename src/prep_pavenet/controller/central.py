from __future__ import annotations

from pathlib import Path

import pandas as pd

from prep_pavenet.common.columns import (
    ACTIVATION_COLUMNS,
    ACTIVATIONS_FOLDER,
    CONTROLLER_COLUMNS,
    POSITIONS_FOLDER,
)
from prep_pavenet.common.utils import get_center
from prep_pavenet.setup.config import END_TIME, ID_INIT, NETWORK_FILE, START_TIME

CENTRE = "center"


class CentralControllerPlacer:
    def __init__(
        self,
        trace_config: dict,
        cont_config: dict,
        config_path: Path,
        output_path: Path,
    ) -> None:
        """The constructor of the CentralControllerPlacer class."""
        self.output_path = output_path
        self.config_path = config_path
        self.start_time = cont_config[START_TIME]
        self.end_time = cont_config[END_TIME]
        self.id_init = cont_config[ID_INIT]
        self.sumo_net = config_path / trace_config[NETWORK_FILE]

    def create_controller_data(self):
        """Create the controller data."""
        self._write_activation_data()
        self._write_controller_data()

    def _write_activation_data(self) -> None:
        """Write the activation data of controller to a file."""
        activation_file = (
            self.output_path / ACTIVATIONS_FOLDER / "controller_activations.parquet"
        )
        controller_id = self.id_init
        activation_df = pd.DataFrame(
            [[controller_id, self.start_time, self.end_time]],
            columns=ACTIVATION_COLUMNS,
        )
        activation_df.to_parquet(activation_file, index=False)
        activation_df.to_csv(
            self.output_path / ACTIVATIONS_FOLDER / "controller_activations.csv",
            index=False,
            header=True,
        )

    def _write_controller_data(self) -> None:
        """Write the controller data to a file."""
        controller_file = self.output_path / POSITIONS_FOLDER / "controller.parquet"
        controller_id = self.id_init
        centers = get_center(self.sumo_net)
        controller_df = pd.DataFrame(
            [[0, controller_id, centers[0], centers[1]]],
            columns=CONTROLLER_COLUMNS,
        )
        controller_df.to_parquet(controller_file, index=False)
        controller_df.to_csv(
            self.output_path / POSITIONS_FOLDER / "controller.csv",
            index=False,
            header=True,
        )
