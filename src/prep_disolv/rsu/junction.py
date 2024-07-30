from __future__ import annotations

import xml.etree.ElementTree as Et
from pathlib import Path
from pyproj import Transformer

import numpy as np
import pandas as pd

from src.prep_disolv.common.columns import (
    POSITIONS_FOLDER,
    ACTIVATIONS_FOLDER,
    RSU_COLUMNS,
    ACTIVATION_COLUMNS,
    COORD_X,
    COORD_Y,
)
from src.prep_disolv.common.utils import get_offsets, get_projection
from src.prep_disolv.common.config import ID_INIT, NETWORK_FILE, START_TIME, \
    RSU_SETTINGS, TRAFFIC_SETTINGS, SIMULATION_SETTINGS, Config, DURATION

JUNCTION = "junction"


def get_lat_lon(coord_x: str, coord_y: str, sumo_net: Path) -> tuple[float, float]:
    """Get the latitude and longitude from the coordinates."""
    from_proj = get_projection(sumo_net)
    transformer = Transformer.from_crs(
        crs_from=from_proj,
        crs_to="epsg:4326",
    )
    return transformer.transform(float(coord_x), float(coord_y))


class JunctionData:
    def __init__(
        self, junction_id: int, ns3_id: int, x: float, y: float, lat: float, lon: float
    ) -> None:
        self.ns3_id = ns3_id
        self.id = junction_id
        self.x = x
        self.y = y
        self.lat = lat
        self.lon = lon

    def __repr__(self) -> str:
        return (
            f"JunctionData("
            f"{self.id}, {self.ns3_id}, {self.x}, {self.y}, {self.lat}, {self.lon})"
        )


class JunctionActivationData:
    def __init__(
        self, junction_id: int, junction_ns3_id: int, start_time: int, end_time: int
    ) -> None:
        self.id = junction_id
        self.ns3_id = junction_ns3_id
        self.start_times = start_time
        self.end_times = end_time

    def __repr__(self) -> str:
        return (
            f"JunctionActivationData({self.id}, {self.start_times}, {self.end_times})"
        )


class JunctionPlacement:
    def __init__(
        self,
        config: Config,
        output_path: Path,
        ns3_id_init: int,
    ):
        """The constructor of the JunctionPlacement class."""
        self.output_path = output_path
        self.config_path = config.path
        self.start_time = config.get(RSU_SETTINGS)[START_TIME]
        self.end_time = config.get(SIMULATION_SETTINGS)[DURATION]
        self.id_init = config.get(RSU_SETTINGS)[ID_INIT]
        self.ns3_id_init = ns3_id_init
        self.sumo_net = self.config_path / config.get(TRAFFIC_SETTINGS)[NETWORK_FILE]
        self.rsu_count = 0
        self.parquet_file = (
            self.output_path / POSITIONS_FOLDER / "roadside_units.parquet"
        )

    def create_rsu_data(self) -> None:
        """Create the RSU data."""
        junctions = self._get_junctions()
        self.rsu_count = len(junctions)
        self._write_activation_data(junctions)
        self._write_rsu_data(junctions)

    def get_unique_rsu_count(self) -> int:
        """Get the unique RSU count."""
        return self.rsu_count

    def get_parquet_file(self) -> Path:
        """Get the parquet file."""
        return self.parquet_file

    def _write_activation_data(self, junctions: list[JunctionData]) -> None:
        """Write the activation data to a file."""
        activations = []
        for junction in junctions:
            junction_data = JunctionActivationData(
                junction.id, junction.ns3_id, self.start_time, self.end_time
            )
            activations.append(junction_data)

        activation_file = (
                self.output_path / ACTIVATIONS_FOLDER / "rsu_activations.parquet"
        )
        activation_df = pd.DataFrame(columns=ACTIVATION_COLUMNS)
        for junction in activations:
            # replace with len(junction.start_times) if multiple times are needed
            agent_id_arr = np.array([junction.id] * 1)
            ns3_id_arr = np.array([junction.ns3_id] * 1)
            start_time_arr = np.array(junction.start_times)
            end_time_arr = np.array(junction.end_times)
            temp_df = pd.DataFrame(
                {
                    ACTIVATION_COLUMNS[0]: agent_id_arr,
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

    def _write_rsu_data(self, junctions: list[JunctionData]) -> None:
        """Write the junction data to a file."""
        junction_df = pd.DataFrame(
            [
                [
                    0,
                    junction.id,
                    junction.ns3_id,
                    junction.x,
                    junction.y,
                    junction.lat,
                    junction.lon,
                ]
                for junction in junctions
            ],
            columns=RSU_COLUMNS,
        )
        junction_df.to_parquet(self.parquet_file, index=False)

    def _get_junctions(self) -> list[JunctionData]:
        """Get all junctions from the SUMO network file."""
        element_iter = Et.iterparse(self.sumo_net, events=("start", "end"))
        net_file = self.sumo_net
        offsets = get_offsets(net_file)
        offset_x, offset_y = offsets[0], offsets[1]
        junctions = []
        junction_count = 1
        for event, item in element_iter:
            if (
                event == "end"
                and item.tag == JUNCTION
                and item.attrib["type"] == "priority"
            ):
                junction_id = self.id_init + junction_count
                x = float(item.attrib[COORD_X]) - offset_x
                y = float(item.attrib[COORD_Y]) - offset_y
                lat, lon = get_lat_lon(x, y, self.sumo_net)
                junctions.append(
                    JunctionData(junction_id, self.ns3_id_init, x, y, lat, lon)
                )
                junction_count += 1
                self.ns3_id_init += 1
        return junctions
