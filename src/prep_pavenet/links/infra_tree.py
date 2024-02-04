import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import KDTree

from prep_pavenet.common.columns import (
    COORD_X,
    COORD_Y,
    DISTANCE,
    LINK_COLUMNS,
    NODE_ID,
    TARGET_ID,
    TIME_STEP,
)

logger = logging.getLogger(__name__)


class LinkDataArrays:
    def __init__(self) -> None:
        self.array_size = 0
        self.time_step = np.array([], dtype=np.int64)
        self.node_id = np.array([], dtype=np.int64)
        self.target_id = np.array([], dtype=np.int64)
        self.distance = np.array([], dtype=np.float64)

    def reset(self) -> None:
        self.array_size = 0
        self.time_step = np.array([], dtype=np.int64)
        self.node_id = np.array([], dtype=np.int64)
        self.target_id = np.array([], dtype=np.int64)
        self.distance = np.array([], dtype=np.float64)


def get_n2n_links_with_count(
    node_ids: np.ndarray,
    node_positions: np.ndarray,
    node_tree: KDTree,
    time_step: int,
    link_count: int,
) -> LinkDataArrays:
    """Get the infrastructure links for the given input positions.

    Parameters
    ----------
    node_ids: np.ndarray
        The node ids.
    node_positions: np.ndarray
        The node positions.
    node_tree: KDTree
        The node tree.
    time_step: int
        The time step.
    link_count: int
        The number of links to be created.

    Returns
    -------
    LinkDataArrays
        The arrays containing the links.
    """
    node_distances, node_id_lists = node_tree.query(
        node_positions,
        k=link_count,
        return_distance=True,
    )

    return convert_query_to_arrays(
        node_ids, node_ids, node_distances, node_id_lists, time_step, same_type=True
    )


def get_n2n_links_with_radius(
    node_ids: np.ndarray,
    node_positions: np.ndarray,
    node_tree: KDTree,
    time_step: int,
    radius: float,
) -> LinkDataArrays:
    """Get the infrastructure links for the given input positions.

    Parameters
    ----------
    node_ids: np.ndarray
        The node ids.
    node_positions: np.ndarray
        The node positions.
    node_tree: KDTree
        The node tree.
    time_step: int
        The time step.
    radius: float
        The radius to be used for the search.

    Returns
    -------
    pd.DataFrame
        The dataframe containing the links.
    """
    node_id_lists, node_distances = node_tree.query_radius(
        node_positions,
        r=radius,
        return_distance=True,
    )

    # Source and target ids are the same because are finding n2n links.
    return convert_query_to_arrays(
        node_ids, node_ids, node_distances, node_id_lists, time_step, same_type=True
    )


def convert_query_to_arrays(
    source_ids,
    target_ids,
    target_distance_lists,
    target_idx_lists,
    time_step,
    same_type=False,
) -> LinkDataArrays:
    """Convert the query results to arrays."""
    link_data_arrays = LinkDataArrays()
    for target_idx_list, target_distance_list, source_id in zip(
        target_idx_lists, target_distance_lists, source_ids
    ):
        target_id_arr = np.array([target_ids[idx] for idx in target_idx_list])
        dist_arr = np.array(target_distance_list)
        time_step_arr = np.array([time_step] * len(target_id_arr))
        node_arr = np.array([source_id] * len(target_id_arr))

        # Remove links to self.
        if same_type:
            source_id_arr_copy = node_arr.copy()
            dist_arr = dist_arr[target_id_arr != node_arr]
            time_step_arr = time_step_arr[target_id_arr != node_arr]
            node_arr = node_arr[target_id_arr != source_id_arr_copy]
            target_id_arr = target_id_arr[target_id_arr != source_id_arr_copy]

        link_data_arrays.time_step = np.append(
            link_data_arrays.time_step, time_step_arr
        )
        link_data_arrays.node_id = np.append(link_data_arrays.node_id, node_arr)
        link_data_arrays.target_id = np.append(
            link_data_arrays.target_id, target_id_arr
        )
        link_data_arrays.distance = np.append(link_data_arrays.distance, dist_arr)
        link_data_arrays.array_size += len(target_id_arr)
    return link_data_arrays


def convert_query_to_df(
    source_ids, target_ids, target_distance_lists, target_idx_lists, time_step
) -> pd.DataFrame:
    """Convert the query results to a dataframe."""
    links_df = pd.DataFrame(columns=LINK_COLUMNS)
    for target_idx_list, target_distance_list, source_id in zip(
        target_idx_lists, target_distance_lists, source_ids
    ):
        target_id_arr = np.array([target_ids[idx] for idx in target_idx_list])
        dist_arr = np.array(target_distance_list)
        time_step_arr = np.array([time_step] * len(target_id_arr))
        node_arr = np.array([source_id] * len(target_id_arr))

        temp_df = pd.DataFrame(
            {
                TIME_STEP: time_step_arr,
                NODE_ID: node_arr,
                TARGET_ID: target_id_arr,
                DISTANCE: dist_arr,
            }
        )
        links_df = (
            temp_df
            if links_df.empty
            else pd.concat([links_df, temp_df], ignore_index=True)
        )
    return links_df


class InfraTree:
    def __init__(self, infra_file: Path):
        """
        The constructor of the infra tree class.

        Parameters
        ----------
        infra_file: Path
            The path to the infra file.
        """
        self.infra_file: Path = infra_file
        self._setup_tree()

    def _setup_tree(self) -> None:
        """Set up the infrastructure position tree."""
        infra_positions = pd.read_parquet(self.infra_file)
        self.positions = np.array(infra_positions[[COORD_X, COORD_Y]])
        self.tree = KDTree(self.positions, leaf_size=5, metric="euclidean")
        self.infra_ids = list(infra_positions[NODE_ID])

    def get_n2i_links_with_count(
        self,
        node_ids: np.ndarray,
        node_positions: np.ndarray,
        time_step: int,
        link_count: int,
    ) -> LinkDataArrays:
        """
        Get the infrastructure links for the given input positions.

        Parameters
        ----------
        node_ids: np.ndarray
            The node ids.
        node_positions: np.ndarray
            The node positions.
        time_step: int
            The time step.
        link_count: int
            The number of links to be created.

        Returns
        -------
        LinkDataArrays
            The arrays containing the links.
        """
        infra_distances, infra_id_lists = self.tree.query(
            node_positions,
            k=link_count,
            return_distance=True,
        )

        return convert_query_to_arrays(
            node_ids, self.infra_ids, infra_distances, infra_id_lists, time_step
        )

    def get_n2i_links_with_radius(
        self,
        node_ids: np.ndarray,
        node_positions: np.ndarray,
        time_step: int,
        radius: float,
    ) -> LinkDataArrays:
        """
        Get the infrastructure links for the given input positions.

        Parameters
        ----------
        node_ids: np.ndarray
            The node ids.
        node_positions: np.ndarray
            The node positions.
        time_step: int
            The time step.
        radius: float
            The radius to be used for the search.

        Returns
        -------
        LinkDataArrays
            The arrays containing the links.
        """
        infra_id_lists, infra_distances = self.tree.query_radius(
            node_positions,
            r=radius,
            return_distance=True,
        )

        return convert_query_to_arrays(
            node_ids, self.infra_ids, infra_distances, infra_id_lists, time_step
        )

    def get_i2n_links_with_count(
        self,
        node_ids: np.ndarray,
        node_tree: KDTree,
        time_step: int,
        link_count: int,
    ) -> LinkDataArrays:
        """Get the infrastructure links for the given input positions.

        Parameters
        ----------
        node_ids: np.ndarray
            The node ids.
        node_tree: KDTree
            The node tree.
        time_step: int
            The time step.
        link_count: int
            The number of links to be created.

        Returns
        -------
        LinkDataArrays
            The arrays containing the links.
        """
        node_distances, node_id_lists = node_tree.query(
            self.positions,
            k=link_count + 1,
            return_distance=True,
        )

        return convert_query_to_arrays(
            self.infra_ids, node_ids, node_distances, node_id_lists, time_step
        )

    def get_i2n_links_with_radius(
        self,
        node_ids: np.ndarray,
        node_tree: KDTree,
        time_step: int,
        radius: float,
    ) -> LinkDataArrays:
        """Get the infrastructure links for the given input positions.

        Parameters
        ----------
        node_ids: np.ndarray
            The node ids.
        node_tree: KDTree
            The node tree.
        time_step: int
            The time step.
        radius: float
            The radius to be used for the search.

        Returns
        -------
        LinkDataArrays
            The arrays containing the links.
        """
        node_id_lists, node_distances = node_tree.query_radius(
            self.positions,
            r=radius,
            return_distance=True,
        )

        return convert_query_to_arrays(
            self.infra_ids, node_ids, node_distances, node_id_lists, time_step
        )

    def get_i2i_links_of_count(self, link_count: int) -> pd.DataFrame:
        """Get the infrastructure links of given count.

        Parameters
        ----------
        link_count: int
            The number of links to be created.

        Returns
        -------
        pd.DataFrame
            The dataframe containing the links.
        """
        infra_distances, infra_id_lists = self.tree.query(
            self.positions,
            k=link_count + 1,  # because search includes the current device.
            return_distance=True,
        )

        return convert_query_to_df(
            self.infra_ids, self.infra_ids, infra_distances, infra_id_lists, 0
        )

    def get_i2_other_i_links_with_radius(
        self,
        other_infra_ids: np.ndarray,
        other_infra_tree: KDTree,
        time_step: int,
        radius: float,
    ) -> pd.DataFrame:
        """Get the infrastructure links for the given input positions.

        Parameters
        ----------
        other_infra_ids: np.ndarray
            The node ids.
        other_infra_tree: KDTree
            The node tree.
        time_step: int
            The time step.
        radius: float
            The radius to be used for the search.

        Returns
        -------
        pd.DataFrame
            The dataframe containing the links.
        """
        infra_id_lists, infra_distances = other_infra_tree.query_radius(
            self.positions,
            r=radius,
            return_distance=True,
        )

        i2_other_i_df = convert_query_to_df(
            self.infra_ids, other_infra_ids, infra_distances, infra_id_lists, time_step
        )
        return i2_other_i_df[i2_other_i_df[NODE_ID] != i2_other_i_df[TARGET_ID]]
