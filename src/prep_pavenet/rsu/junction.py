from __future__ import annotations

import xml.etree.ElementTree as Et
from pathlib import Path

import pandas as pd

from prep_pavenet.common.columns import *
from prep_pavenet.setup.config import END_TIME, NETWORK_FILE, START_TIME, ID_INIT

JUNCTION = "junction"


class JunctionData:
    def __init__(self, junction_id: int, x: float, y: float) -> None:
        self.id = junction_id
        self.x = x
        self.y = y

    def __repr__(self) -> str:
        return f"JunctionData({self.id}, {self.x}, {self.y})"


class JunctionActivationData:
    def __init__(self, junction_id: int, start_time: int, end_time: int) -> None:
        self.id = junction_id
        self.start_times = start_time
        self.end_times = end_time


class JunctionPlacement:
    def __init__(
        self,
        trace_config: dict,
        rsu_config: dict,
        config_path: Path,
        output_path: Path,
    ):
        """The constructor of the JunctionPlacement class."""
        self.output_path = output_path
        self.config_path = config_path
        self.start_time = rsu_config[START_TIME]
        self.end_time = rsu_config[END_TIME]
        self.id_init = rsu_config[ID_INIT]
        self.sumo_net = config_path / trace_config[NETWORK_FILE]

    def create_rsu_data(self) -> None:
        """Create the RSU data."""
        junctions = self._get_junctions()
        self._write_activation_data(junctions)
        self._write_rsu_data(junctions)

    def _write_activation_data(self, junctions: list[JunctionData]) -> None:
        """Write the activation data to a file."""
        activations = []
        for junction in junctions:
            junction_data = JunctionActivationData(
                junction.id, self.start_time, self.end_time
            )
            activations.append(junction_data)

        activation_file = (
            self.output_path / ACTIVATIONS_FOLDER / "rsu_activations.parquet"
        )
        activation_df = pd.DataFrame(
            [
                [activation.id, activation.start_times, activation.end_times]
                for activation in activations
            ],
            columns=ACTIVATION_COLUMNS,
        )
        activation_df.to_parquet(activation_file, index=False)
        activation_df.to_csv(
            self.output_path / ACTIVATIONS_FOLDER / "rsu_activations.csv",
            index=False,
            header=True,
        )

    def _write_rsu_data(self, junctions: list[JunctionData]) -> None:
        """Write the junction data to a file."""
        junction_file = self.output_path / POSITIONS_FOLDER / "roadside_units.parquet"
        junction_df = pd.DataFrame(
            [[0, junction.id, junction.x, junction.y] for junction in junctions],
            columns=RSU_COLUMNS,
        )
        junction_df.to_parquet(junction_file, index=False)
        junction_df.to_csv(
            self.output_path / POSITIONS_FOLDER / "roadside_units.csv", index=False
        )

    def _get_junctions(self) -> list[JunctionData]:
        """Get all junctions from the SUMO network file."""
        element_iter = Et.iterparse(self.sumo_net, events=("start", "end"))
        junctions = []
        junction_count = 1
        for event, item in element_iter:
            if (
                event == "end"
                and item.tag == JUNCTION
                and item.attrib["type"] == "priority"
            ):
                junction_id = self.id_init + junction_count
                x = float(item.attrib[COORD_X])
                y = float(item.attrib[COORD_Y])
                junctions.append(JunctionData(junction_id, x, y))
                junction_count += 1
        return junctions
