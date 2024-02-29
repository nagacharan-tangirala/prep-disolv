from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from disolv_positions.common.columns import ACTIVATIONS_FOLDER, ACTIVATION_COLUMNS


class ActivationData:
    def __init__(self, node_id: int) -> None:
        self.id = node_id
        self.ns3_id = node_id
        self.start_times: list[int] = []
        self.end_times: list[int] = []
        self.index: int = -1
        self.trace_disrupted = True

    def start_new_trace(self, start_time: int) -> None:
        self.start_times.append(start_time)
        self.trace_disrupted = False
        self.index += 1

    def add_end_time(self, end_time: int) -> None:
        if len(self.end_times) < self.index + 1:
            self.end_times.append(end_time)
        self.end_times[self.index] = end_time

    def disrupt_trace(self) -> None:
        self.trace_disrupted = True

    def __repr__(self) -> str:
        return f"ActivationData({self.id}, {self.ns3_id}, {self.start_times}, {self.end_times})"


class VehicleActivation:
    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.activation_data: dict[int, ActivationData] = {}
        self.active_vehicles: set = set()
        self.activation_file = (
            self.output_path / ACTIVATIONS_FOLDER / "vehicle_activations.parquet"
        )

    def update_activation(self, timestamp: int, vehicle_id: int) -> None:
        self.active_vehicles.add(vehicle_id)
        if vehicle_id not in self.activation_data:
            self.activation_data[vehicle_id] = ActivationData(vehicle_id)
        if self.activation_data[vehicle_id].trace_disrupted:
            self.activation_data[vehicle_id].start_new_trace(timestamp)
        self.activation_data[vehicle_id].add_end_time(timestamp)

    def time_step_complete(self, time_stamp: int) -> None:
        for vehicle_id in self.active_vehicles:
            if self.activation_data[vehicle_id].end_times[-1] < time_stamp:
                self.activation_data[vehicle_id].disrupt_trace()

    def write_activation_data(self) -> None:
        activation_df = pd.DataFrame(columns=ACTIVATION_COLUMNS)
        for vehicle_id in self.activation_data:
            activation_data = self.activation_data[vehicle_id]
            start_time_arr = np.array(activation_data.start_times)
            end_time_arr = np.array(activation_data.end_times)
            vehicle_id_arr = np.array([vehicle_id] * len(start_time_arr))
            ns3_id_arr = np.array([activation_data.ns3_id] * len(start_time_arr))
            temp_df = pd.DataFrame(
                {
                    ACTIVATION_COLUMNS[0]: vehicle_id_arr,
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
        activation_df.to_parquet(self.activation_file, index=False)
