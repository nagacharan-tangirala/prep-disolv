import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import KDTree
from tqdm import tqdm

from prep_pavenet.common.columns import LINKS_FOLDER, LINK_COLUMNS
from prep_pavenet.links import infra_tree
from prep_pavenet.links.infra_tree import InfraTree, LinkDataArrays
from prep_pavenet.links.reader import InputReader
from prep_pavenet.links.writer import LinksWriter
from prep_pavenet.common.config import (
    DURATION,
    LINK_SETTINGS,
    OUTPUT_PATH,
    OUTPUT_SETTINGS,
    SIMULATION_SETTINGS,
    STEP_SIZE,
    Config,
)

# Link keys.
V2R_COUNT = "v2r_count"
R2R_COUNT = "r2r_count"
R2V_RADIUS = "r2v_radius"
V2V_RADIUS = "v2v_radius"
R2C_RADIUS = "r2c_radius"
C2R_RADIUS = "c2r_radius"

logger = logging.getLogger(__name__)


class DeviceLinks:
    def __init__(
        self, vehicle_file: Path, rsu_file: Path, controller_file: Path, config: Config
    ) -> None:
        self.config: Config = config
        self.vehicle_file: Path = vehicle_file
        self.rsu_file: Path = rsu_file
        self.controller_file: Path = controller_file

        self.v2r_count: int = self.config.get(LINK_SETTINGS)[V2R_COUNT]
        self.r2v_radius: float = self.config.get(LINK_SETTINGS)[R2V_RADIUS]
        self.v2v_radius: float = self.config.get(LINK_SETTINGS)[V2V_RADIUS]
        self.r2r_count: int = self.config.get(LINK_SETTINGS)[R2R_COUNT]
        self.r2c_radius: int = self.config.settings.get(LINK_SETTINGS)[R2C_RADIUS]
        self.c2r_radius: int = self.config.settings.get(LINK_SETTINGS)[C2R_RADIUS]

        self.step: int = self.config.get(SIMULATION_SETTINGS)[STEP_SIZE]
        self.duration: int = self.config.get(SIMULATION_SETTINGS)[DURATION]

    def create_all_links(self) -> None:
        """Calculate the links."""
        logger.info("Prepare the position trees")
        self._prepare_position_trees()
        logger.info("Create output writers")
        self._create_output_writers()
        logger.info("Create input trace reader")
        self._create_trace_reader()
        logger.info("Calculate infra to infra links")
        self._calculate_static_links()
        logger.info("Calculate vehicle to infra links")
        self._calculate_dynamic_links()

    def _prepare_position_trees(self) -> None:
        """Prepare the position trees."""
        self.rsu_tree = InfraTree(self.rsu_file)
        self.controller_tree = InfraTree(self.controller_file)

    def _create_trace_reader(self) -> None:
        """Create the trace reader."""
        self.trace_reader = InputReader(self.vehicle_file)
        self.trace_reader.initialize_for_reading()

    def _create_output_writers(self) -> None:
        """Prepare the output writers."""
        output_path = Path(self.config.get(OUTPUT_SETTINGS)[OUTPUT_PATH])
        v2r_file = output_path / LINKS_FOLDER / "v2r_links.parquet"
        self.v2r_writer = LinksWriter(v2r_file)
        r2v_file = output_path / LINKS_FOLDER / "r2v_links.parquet"
        self.r2v_writer = LinksWriter(r2v_file)
        v2v_file = output_path / LINKS_FOLDER / "v2v_links.parquet"
        self.v2v_writer = LinksWriter(v2v_file)
        r2r_file = output_path / LINKS_FOLDER / "r2r_links.parquet"
        self.r2r_writer = LinksWriter(r2r_file)
        r2c_file = output_path / LINKS_FOLDER / "r2c_links.parquet"
        self.r2c_writer = LinksWriter(r2c_file)
        c2r_file = output_path / LINKS_FOLDER / "c2r_links.parquet"
        self.c2r_writer = LinksWriter(c2r_file)

    def _calculate_static_links(self) -> None:
        """Calculate the static links."""
        r2r_links = self.rsu_tree.get_i2i_links_of_count(self.r2r_count)
        self.r2r_writer.write_data(r2r_links)
        self.r2r_writer.close()
        r2c_links = self.rsu_tree.get_i2_other_i_links_with_radius(
            self.controller_tree.infra_ids,
            self.controller_tree.tree,
            0,
            self.r2c_radius,
        )
        self.r2c_writer.write_data(r2c_links)
        self.r2c_writer.close()
        c2r_links = self.controller_tree.get_i2_other_i_links_with_radius(
            self.rsu_tree.infra_ids,
            self.rsu_tree.tree,
            0,
            self.c2r_radius,
        )
        self.c2r_writer.write_data(c2r_links)
        self.c2r_writer.close()

    def _calculate_dynamic_links(self) -> None:
        """Calculate the links."""
        cache_size = 100000

        # Create arrays for the links.
        v2r_links = LinkDataArrays()
        r2v_links = LinkDataArrays()
        v2v_links = LinkDataArrays()

        logger.debug("Looping from 0 to %d with step %d", self.duration, self.step)
        progress_bar = tqdm(
            total=self.duration,
            unit="ts",
            desc="Determining links for ts: ",
            colour="blue",
            ncols=100,
        )
        for ts in range(0, self.duration, self.step):
            msg = f"Calculating links for time step {ts}"
            logger.info(msg)

            vehicle_ids, veh_positions = self.trace_reader.get_positions_at(ts)
            logger.debug("Vehicle IDs: %s", vehicle_ids)
            if len(vehicle_ids) == 0:
                continue

            v2r_link_arrays = self.rsu_tree.get_n2i_links_with_count(
                vehicle_ids, veh_positions, ts, self.v2r_count
            )
            v2r_links.time_step = np.append(
                v2r_links.time_step, v2r_link_arrays.time_step
            )
            v2r_links.node_id = np.append(v2r_links.node_id, v2r_link_arrays.node_id)
            v2r_links.target_id = np.append(
                v2r_links.target_id, v2r_link_arrays.target_id
            )
            v2r_links.distance = np.append(v2r_links.distance, v2r_link_arrays.distance)
            v2r_links.array_size += v2r_link_arrays.array_size

            veh_tree = KDTree(veh_positions, leaf_size=20, metric="euclidean")

            r2v_link_arrays = self.rsu_tree.get_i2n_links_with_radius(
                vehicle_ids, veh_tree, ts, self.r2v_radius
            )

            r2v_links.time_step = np.append(
                r2v_links.time_step, r2v_link_arrays.time_step
            )
            r2v_links.node_id = np.append(r2v_links.node_id, r2v_link_arrays.node_id)
            r2v_links.target_id = np.append(
                r2v_links.target_id, r2v_link_arrays.target_id
            )
            r2v_links.distance = np.append(r2v_links.distance, r2v_link_arrays.distance)

            if len(vehicle_ids) > 1:
                v2v_link_arrays = infra_tree.get_n2n_links_with_radius(
                    vehicle_ids, veh_positions, veh_tree, ts, self.v2v_radius
                )
                v2v_links.time_step = np.append(
                    v2v_links.time_step, v2v_link_arrays.time_step
                )
                v2v_links.node_id = np.append(
                    v2v_links.node_id, v2v_link_arrays.node_id
                )
                v2v_links.target_id = np.append(
                    v2v_links.target_id, v2v_link_arrays.target_id
                )
                v2v_links.distance = np.append(
                    v2v_links.distance, v2v_link_arrays.distance
                )

            if v2r_links.array_size > cache_size:
                self.v2r_writer.write_arrays(v2r_links)
                v2r_links.reset()

            if r2v_links.array_size > cache_size:
                self.r2v_writer.write_arrays(r2v_links)
                r2v_links.reset()

            if v2v_links.array_size > cache_size:
                self.v2v_writer.write_arrays(v2v_links)
                v2v_links.reset()

            progress_bar.update(self.step)

        self.v2r_writer.write_arrays(v2r_links)
        self.v2r_writer.close()
        self.r2v_writer.write_arrays(r2v_links)
        self.r2v_writer.close()
        self.v2v_writer.write_arrays(v2v_links)
        self.v2v_writer.close()

        progress_bar.close()
        self.trace_reader.close()
