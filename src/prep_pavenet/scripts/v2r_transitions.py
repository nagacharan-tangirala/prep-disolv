# This script is used to prepare the links data for ns3 and mosaic.
# The input file is any links file. The output file contains a series of
# links for each node, where the target changes from the previous row.
# Instead of storing the entire trace file for each node, we only store the
# links when the target changes.
#
# NS3 and Mosaic use different node IDs, so we need to convert the node IDs
# to the correct format for each simulator. NS3 need parquet files, while
# Mosaic needs a CSV file.
# Paths are hard-coded, only scenario name is required as input.

from pathlib import Path
import pandas as pd
import argparse


class V2RTransitions:
    def __init__(self, rsu_file: Path):
        self.rsu_file = rsu_file
        self.mapping_with_offset = {}
        self.mapping_without_offset = {}
        self._read_node_mapping()

    def _read_node_mapping(self):
        # Read the RSU data
        rsu_df = pd.read_parquet(self.rsu_file)

        # Create a mapping between node ID and NS3 ID
        self.mapping_with_offset = dict(zip(rsu_df["node_id"], rsu_df["ns3_id"]))

        # Get offset based on first RSU NS3 ID and subtract from all IDs.
        offset = rsu_df.iloc[0]["ns3_id"]
        rsu_df["ns3_id"] = rsu_df["ns3_id"] - offset
        rsu_df["ns3_id"] = rsu_df["ns3_id"].astype(int)

        # Create a mapping between node ID and NS3 ID
        self.mapping_without_offset = dict(zip(rsu_df["node_id"], rsu_df["ns3_id"]))

    def get_mapping_with_offset(self) -> dict:
        return self.mapping_with_offset

    def get_mapping_without_offset(self) -> dict:
        return self.mapping_without_offset


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--scenario", type=str, required=True)
    scenario = args.parse_args().scenario

    input_path = Path("/mnt/hdd/workspace/pavenet/input/" + scenario)
    rsu_pos_file = input_path / "positions" / "roadside_units.parquet"
    links_file = input_path / "links" / "v2r_links.parquet"
    print("Reading RSU data from", rsu_pos_file)
    print("Reading links data from", links_file)

    links_df = pd.read_parquet(links_file)
    transitions = V2RTransitions(rsu_pos_file)

    # Group by time and node, find the row index of the minimum distance for each group
    min_distance_index = links_df.groupby(["time_step", "node_id"])["distance"].idxmin()

    # Use the row indices to filter the original DataFrame
    links_df = links_df.loc[min_distance_index]

    # Sort based on node and time
    links_df.sort_values(["node_id", "time_step"], inplace=True)
    # Identify rows where the target changes from the previous row within each node
    target_change_mask = links_df.groupby("node_id")["target_id"].diff() != 0

    # Include the first entry for each node regardless of target change
    first_entry_mask = links_df.groupby("node_id").cumcount() == 0

    # Combine the masks to filter the DataFrame
    final_mask = target_change_mask | first_entry_mask
    filtered_df = links_df[final_mask]

    # Convert node IDs to NS3 IDs
    ns3_df = filtered_df.copy()
    node_mapping = transitions.get_mapping_with_offset()
    ns3_df["target_id"].replace(node_mapping, inplace=True)

    output_file = input_path / "links" / "v2r_transitions.parquet"
    print("Writing links data for NS3 to", output_file)
    ns3_df.to_parquet(output_file, index=False)

    mosaic_df = filtered_df.copy()
    node_mapping = transitions.get_mapping_without_offset()
    mosaic_df["target_id"].replace(node_mapping, inplace=True)

    link_transition_file = input_path / "links" / "v2r_transitions.csv"
    print("Writing links data for Mosaic to", link_transition_file)
    mosaic_df.to_csv(link_transition_file, index=False, header=False)
