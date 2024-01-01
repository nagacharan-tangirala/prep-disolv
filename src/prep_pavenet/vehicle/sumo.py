from __future__ import annotations

import logging
import xml.etree.ElementTree as Et
from collections import namedtuple
from pathlib import Path
from xml.etree.ElementTree import iterparse

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tqdm

from prep_pavenet.common.columns import POSITIONS_FOLDER
from prep_pavenet.common.utils import get_offsets
from prep_pavenet.common.config import NETWORK_FILE, TRACE_FILE
from prep_pavenet.vehicle.veh_activations import VehicleActivation

logger = logging.getLogger(__name__)

# Trace output keys.
TIME_STEP = "time_step"
NODE_ID = "node_id"
COORD_X = "x"
COORD_Y = "y"
VELOCITY = "velocity"
ROAD_ID = "road_id"
SUB_ROAD_ID = "sub_road_id"
LANE_ID = "lane_id"
VEH_TYPE = "veh_type"

# Composite road ID.
road_info = namedtuple("road_info", ["road_id", "sub_road_id", "lane_id"])


class FCDDataArrays:
    def __init__(self) -> None:
        self.array_size = 0
        self.time_step = np.array([], dtype=np.int64)
        self.node_id = np.array([], dtype=np.int64)
        self.x = np.array([], dtype=np.float64)
        self.y = np.array([], dtype=np.float64)
        self.velocity = np.array([], dtype=np.float64)
        self.road_id = np.array([], dtype=np.int64)
        self.sub_road_id = np.array([], dtype=np.int64)
        self.lane_id = np.array([], dtype=np.int64)
        self.veh_type = np.array([], dtype=str)

    def reset(self) -> None:
        self.array_size = 0
        self.time_step = np.array([], dtype=np.int64)
        self.node_id = np.array([], dtype=np.int64)
        self.x = np.array([], dtype=np.float64)
        self.y = np.array([], dtype=np.float64)
        self.velocity = np.array([], dtype=np.float64)
        self.road_id = np.array([], dtype=np.int64)
        self.sub_road_id = np.array([], dtype=np.int64)
        self.lane_id = np.array([], dtype=np.int64)
        self.veh_type = np.array([], dtype=str)


class SumoConverter:
    def __init__(
        self,
        trace_config: dict,
        _vehicle_config: dict,
        config_path: Path,
        output_path: Path,
    ) -> None:
        """The constructor of the SumoConverter class."""
        self.output_path = output_path
        self.config_path = config_path
        self.activation = VehicleActivation(output_path)
        self.fcd_file = self.config_path / trace_config[TRACE_FILE]
        self.net_file = self.config_path / trace_config[NETWORK_FILE]
        offsets = get_offsets(self.net_file)
        self.offset_x, self.offset_y = offsets[0], offsets[1]
        self.unique_vehicle_count = 0
        self.parquet_file: Path = output_path

    def fcd_to_parquet(self) -> None:
        """Convert the FCD output from SUMO to a parquet file."""
        logger.info("Converting %s to parquet", self.fcd_file)
        file_name = self.fcd_file.stem
        parquet_file = self.output_path / POSITIONS_FOLDER / f"{file_name}.parquet"
        self.parquet_file = parquet_file
        logger.info("Writing to %s", parquet_file)
        self._convert_fcd_to_parquet()

    def get_unique_vehicle_count(self) -> int:
        """Get the unique vehicle count."""
        return self.unique_vehicle_count

    def get_parquet_file(self) -> Path:
        """Get the parquet file."""
        return self.parquet_file

    def _convert_fcd_to_parquet(self) -> None:
        """Convert the FCD output from SUMO to a parquet file."""
        root = Et.parse(self.fcd_file).getroot()
        output_writer = _get_output_writer(self.parquet_file)
        fcd_arrays: FCDDataArrays = FCDDataArrays()

        progress_bar = tqdm.tqdm(
            total=len(root),
            unit="ts",
            desc="Processing Vehicles at ts: ",
            colour="green",
            ncols=90,
        )
        vehicle_fcd_iter = iterparse(self.fcd_file, events=("start", "end"))
        for event, veh_ele in vehicle_fcd_iter:
            if event == "end" and veh_ele.tag == "timestep":
                timestamp = int(round(float(veh_ele.attrib["time"]), 1) * 10) * 100
                logger.debug("Processing timestep %s", timestamp)

                for vehicle_ele in veh_ele:
                    fcd_arrays.time_step = np.append(fcd_arrays.time_step, timestamp)
                    vehicle_id = int(vehicle_ele.attrib["id"])
                    self.activation.update_activation(timestamp, vehicle_id)
                    fcd_arrays = self._read_vehicle_data(vehicle_ele, fcd_arrays)

                    if fcd_arrays.array_size == 1000:
                        fcd_data_df = pd.DataFrame(fcd_arrays.__dict__)
                        fcd_data_df = fcd_data_df.drop(columns=["array_size"])
                        output_writer.write_table(pa.Table.from_pandas(fcd_data_df))
                        fcd_arrays.reset()

                self.activation.time_step_complete(timestamp)
                progress_bar.update(1)

        if fcd_arrays.array_size > 0:
            fcd_data_df = pd.DataFrame(fcd_arrays.__dict__)
            fcd_data_df = fcd_data_df.drop(columns=["array_size"])
            output_writer.write_table(pa.Table.from_pandas(fcd_data_df))
            fcd_arrays.reset()

        output_writer.close()
        progress_bar.close()
        self.unique_vehicle_count = len(self.activation.activation_data)
        self.activation.write_activation_data()

    def _read_vehicle_data(
        self, vehicle_ele: Et.Element, fcd_arrays: FCDDataArrays
    ) -> FCDDataArrays:
        """Read the vehicle data from XML element and add it to FCD arrays."""
        fcd_arrays.node_id = np.append(
            fcd_arrays.node_id, int(vehicle_ele.attrib["id"])
        )
        fcd_arrays.x = np.append(
            fcd_arrays.x, float(vehicle_ele.attrib["x"]) - self.offset_x
        )
        fcd_arrays.y = np.append(
            fcd_arrays.y, float(vehicle_ele.attrib["y"]) - self.offset_y
        )
        fcd_arrays.velocity = np.append(
            fcd_arrays.velocity, float(vehicle_ele.attrib["speed"])
        )

        fcd_arrays.veh_type = np.append(fcd_arrays.veh_type, vehicle_ele.attrib["type"])
        road_data = _read_road_data(vehicle_ele.attrib["lane"])
        road_data = _convert_road_data_to_int(road_data)
        fcd_arrays.road_id = np.append(fcd_arrays.road_id, road_data.road_id)
        fcd_arrays.sub_road_id = np.append(
            fcd_arrays.sub_road_id, road_data.sub_road_id
        )
        fcd_arrays.lane_id = np.append(fcd_arrays.lane_id, road_data.lane_id)
        fcd_arrays.array_size += 1
        return fcd_arrays


def _get_output_writer(parquet_file: Path) -> pq.ParquetWriter:
    """Get the output writer for the parquet file."""
    schema = _build_fcd_schema()
    return pq.ParquetWriter(parquet_file, schema)


def _build_fcd_schema() -> pa.Schema:
    """Build the schema for the FCD data."""
    return pa.schema(
        [
            pa.field(TIME_STEP, pa.int64()),
            pa.field(NODE_ID, pa.int64()),
            pa.field(COORD_X, pa.float64()),
            pa.field(COORD_Y, pa.float64()),
            pa.field(VELOCITY, pa.float64()),
            pa.field(ROAD_ID, pa.int64()),
            pa.field(SUB_ROAD_ID, pa.int64()),
            pa.field(LANE_ID, pa.int64()),
            pa.field(VEH_TYPE, pa.string()),
        ]
    )


def _convert_road_data_to_int(road_data: road_info) -> road_info:
    """Convert the road data to integers."""
    return road_info(
        road_id=int(road_data.road_id),
        sub_road_id=int(road_data.sub_road_id),
        lane_id=int(road_data.lane_id),
    )


def _read_road_data(edge_data: str) -> road_info:
    if ":" in edge_data:
        return read_lane_data_with_colon(edge_data)
    if "#" in edge_data:
        return _read_lane_data_with_hash(edge_data)

    road_data = road_info(road_id=edge_data, sub_road_id=0, lane_id=0)
    if "_" in edge_data:
        road_data.road_id, road_data.lane_id = edge_data.split("_")
    return road_data


def _read_lane_data_with_hash(edge_data: str) -> road_info:
    road_id, lane_id = edge_data.split("_")
    sub_road_id = road_id.split("#")[1]
    road_id = road_id.split("#")[0]
    return road_info(road_id=road_id, sub_road_id=sub_road_id, lane_id=lane_id)


def read_lane_data_with_colon(edge_data: str) -> road_info:
    edge_data = edge_data.split(":")[1]
    lane_id = edge_data.split("_")[2]
    sub_road_id = edge_data.split("_")[1]
    road_id = edge_data.split("_")[0]
    return road_info(road_id=road_id, sub_road_id=sub_road_id, lane_id=lane_id)
