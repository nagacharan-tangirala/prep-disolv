from __future__ import annotations

from pathlib import Path
from prep_pavenet.common.constants import ACTIVATIONS_FOLDER

import pandas as pd

ACTIVATION_COLUMNS = ["node_id", "on_times", "off_times"]


class ActivationData:
    def __init__(self, node_id: int) -> None:
        self.id = node_id
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
        return f"ActivationData({self.id}, {self.start_times}, {self.end_times})"


class VehicleActivation:
    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.activation_data: dict[int, ActivationData] = {}
        self.active_vehicles: set = set()

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
        activation_df = pd.DataFrame(
            [
                [activation.id, activation.start_times, activation.end_times]
                for activation in self.activation_data.values()
            ],
            columns=ACTIVATION_COLUMNS,
        )
        activation_df.to_parquet(
            self.output_path / ACTIVATIONS_FOLDER / str("vehicle_activations.parquet"),
            index=False,
        )
