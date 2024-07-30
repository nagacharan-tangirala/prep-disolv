from pathlib import Path

import numpy as np
import pandas as pd

from src.prep_disolv.common.columns import POSITIONS_FOLDER, ACTIVATIONS_FOLDER, ACTIVATION_COLUMNS
from src.prep_disolv.common.config import Config, RSU_SETTINGS, START_TIME, SIMULATION_SETTINGS, DURATION, RSU_FILENAME


class RSUData:
    def __init__(self, rsu_id, x, y):
        self.id = rsu_id
        self.x = x
        self.y = y


class InputPlacement:
    def __init__(
        self,
        config: Config,
        output_path: Path,
    ):
        """The constructor of the JunctionPlacement class."""
        self.output_path = output_path
        self.config_path = config.path
        self.start_time = config.get(RSU_SETTINGS)[START_TIME]
        self.end_time = config.get(SIMULATION_SETTINGS)[DURATION]
        self.rsu_file = self.output_path / POSITIONS_FOLDER / config.get(RSU_SETTINGS)[RSU_FILENAME]
        self.rsu_count = 0
        self.parquet_file = (
            self.output_path / POSITIONS_FOLDER / self.rsu_file
        )
        self.rsu_data = []

    def get_unique_rsu_count(self) -> int:
        """Get the unique RSU count."""
        return self.rsu_count

    def get_parquet_file(self) -> Path:
        """Get the parquet file."""
        return self.parquet_file

    def create_rsu_data(self) -> None:
        """Create the RSU data."""
        self._read_given_rsu_data()
        self.rsu_count = len(self.rsu_data)
        self._write_activation_data()

    def _read_given_rsu_data(self) -> None:
        rsu_info = pd.read_parquet(self.rsu_file)
        x = rsu_info['x']
        y = rsu_info['y']
        rsu_ids = rsu_info['agent_id']
        for (rsu_id, x, y) in zip(rsu_ids, x, y):
            self.rsu_data.append(RSUData(rsu_id, x, y))

    def _write_activation_data(self):
        """Read the given RSU data."""
        activation_file = (
                self.output_path / ACTIVATIONS_FOLDER / "rsu_activations.parquet"
        )
        activation_df = pd.DataFrame(columns=ACTIVATION_COLUMNS)
        for rsu_data in self.rsu_data:
            rsu_id_arr = np.array([rsu_data.id] * 1)
            ns3_id_arr = rsu_id_arr
            start_time_arr = np.array(self.start_time)
            end_time_arr = np.array(self.end_time)
            temp_df = pd.DataFrame(
                {
                    ACTIVATION_COLUMNS[0]: rsu_id_arr,
                    ACTIVATION_COLUMNS[1]: ns3_id_arr,
                    ACTIVATION_COLUMNS[2]: start_time_arr,
                    ACTIVATION_COLUMNS[3]: end_time_arr,
                }
            )
            activation_df = (
                temp_df
                if activation_df.empty
                else pd.concat([activation_df, temp_df], ignore_index=True)
            )

        activation_df.to_parquet(activation_file, index=False)
