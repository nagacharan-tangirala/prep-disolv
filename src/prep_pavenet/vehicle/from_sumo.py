import logging
from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
import xml.etree.ElementTree as Et

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


TIME_STEP = "time_step"
NODE_ID = "node_id"
COORD_X = "x"
COORD_Y = "y"
VELOCITY = "velocity"
ROAD_ID = "road_id"
SUB_ROAD_ID = "sub_road_id"
LANE_ID = "lane_id"
VEH_TYPE = "veh_type"

road_info = namedtuple("road_info", ["road_id", "sub_road_id", "lane_id"])


class ActivationData:
    def __init__(self, node_id: int, start_time: int) -> None:
        self.id = node_id
        self.start_time = start_time
        self.end_time = -1

    def add_end_time(self, end_time: int) -> None:
        self.end_time = end_time

    def __repr__(self) -> str:
        return f"ActivationData({self.id}, {self.start_time}, {self.end_time})"


class FCDDataArrays:
    def __init__(self):
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

    def reset(self):
        self.time_step = np.array([], dtype=np.int64)
        self.node_id = np.array([], dtype=np.int64)
        self.x = np.array([], dtype=np.float64)
        self.y = np.array([], dtype=np.float64)
        self.velocity = np.array([], dtype=np.float64)
        self.road_id = np.array([], dtype=np.int64)
        self.sub_road_id = np.array([], dtype=np.int64)
        self.lane_id = np.array([], dtype=np.int64)
        self.veh_type = np.array([], dtype=str)


def fcd_to_parquet(fcd_xml: str, output_path: Path) -> None:
    """Convert the FCD output from SUMO to a parquet file.

    Parameters
    ----------
    fcd_xml : str
        The path to the FCD output file.
    output_path : str
        The path to write files.
    """
    fcd_file = output_path / fcd_xml
    logger.info("Converting %s to parquet", fcd_file)
    file_name = fcd_xml.split("/")[-1].split(".")[0]
    parquet_file = output_path / "scenario" / f"{file_name}.parquet"
    logger.info("Writing to %s", parquet_file)
    _convert_fcd_to_parquet(fcd_file, parquet_file)


def _convert_fcd_to_parquet(fcd_xml: Path, parquet_file: Path) -> None:
    """Convert the FCD output from SUMO to a parquet file."""
    root = Et.parse(fcd_xml).getroot()
    activation_data = {}
    output_writer = _get_output_writer(parquet_file)
    fcd_arrays = FCDDataArrays()

    for timestep_ele in root:
        timestamp = int(float(timestep_ele.attrib["time"]) * 1000)
        logger.debug("Processing timestep %s", timestamp)

        for vehicle_ele in timestep_ele:
            fcd_arrays.time_step = np.append(fcd_arrays.time_step, timestamp)
            vehicle_id = int(vehicle_ele.attrib["id"])
            update_activation(activation_data, timestamp, vehicle_id)
            fcd_arrays = _read_vehicle_data(vehicle_ele, fcd_arrays)

            if fcd_arrays.array_size == 1000:
                fcd_arrays.array_size = 0
                fcd_data_df = pd.DataFrame(fcd_arrays.__dict__)
                fcd_data_df.drop(columns=["array_size"], inplace=True)
                output_writer.write_table(pa.Table.from_pandas(fcd_data_df))
                fcd_arrays.reset()

    if fcd_arrays.array_size > 0:
        fcd_data_df = pd.DataFrame(fcd_arrays.__dict__)
        fcd_data_df.drop(columns=["array_size"], inplace=True)
        output_writer.write_table(pa.Table.from_pandas(fcd_data_df))
        fcd_arrays.reset()

    output_writer.close()
    _write_activation_data(activation_data, parquet_file.parent)


def _write_activation_data(activation_data: dict, output_path: Path) -> None:
    activation_df = pd.DataFrame(
        [
            [activation.id, activation.start_time, activation.end_time]
            for activation in activation_data.values()
        ],
        columns=["node_id", "start_time", "end_time"],
    )
    activation_df.to_csv(output_path / "vehicle_activations.csv", index=False,
                         header=True)


def _read_vehicle_data(vehicle_ele: Et.Element,
                       fcd_arrays: FCDDataArrays) -> FCDDataArrays:
    """Read the vehicle data from XML element and add it to FCD arrays."""
    fcd_arrays.node_id = np.append(fcd_arrays.node_id, int(vehicle_ele.attrib["id"]))
    fcd_arrays.x = np.append(fcd_arrays.x, float(vehicle_ele.attrib["x"]))
    fcd_arrays.y = np.append(fcd_arrays.y, float(vehicle_ele.attrib["y"]))
    fcd_arrays.velocity = np.append(fcd_arrays.velocity, float(vehicle_ele.attrib["speed"]))
    fcd_arrays.veh_type = np.append(fcd_arrays.veh_type, vehicle_ele.attrib["type"])
    road_data = _read_road_data(vehicle_ele.attrib["lane"])
    road_data = _convert_road_data_to_int(road_data)
    fcd_arrays.road_id = np.append(fcd_arrays.road_id, road_data.road_id)
    fcd_arrays.sub_road_id = np.append(fcd_arrays.sub_road_id, road_data.sub_road_id)
    fcd_arrays.lane_id = np.append(fcd_arrays.lane_id, road_data.lane_id)
    fcd_arrays.array_size += 1
    return fcd_arrays


def _convert_road_data_to_int(road_data: road_info) -> road_info:
    """Convert the road data to integers."""
    road_data = road_info(
        road_id=int(road_data.road_id),
        sub_road_id=int(road_data.sub_road_id),
        lane_id=int(road_data.lane_id),
    )
    return road_data


def _get_output_writer(parquet_file: Path) -> pq.ParquetWriter:
    """Get the output writer for the parquet file."""
    schema = _build_fcd_schema()
    return pq.ParquetWriter(parquet_file, schema)


def _build_fcd_schema() -> pa.Schema:
    """Build the schema for the FCD data."""
    schema = pa.schema(
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
    return schema


def update_activation(activation_data: dict, timestamp: int, vehicle_id: int) -> None:
    if vehicle_id not in activation_data:
        activation_data[vehicle_id] = ActivationData(vehicle_id, timestamp)
    else:
        activation_data[vehicle_id].add_end_time(timestamp)


def _read_road_data(edge_data: str) -> road_info:
    if ':' in edge_data:
        return read_lane_data_with_colon(edge_data)
    elif '#' in edge_data:
        return _read_lane_data_with_hash(edge_data)
    else:
        road_data = road_info(road_id=edge_data, sub_road_id=0, lane_id=0)
        if '_' in edge_data:
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
