from pathlib import Path
import pandas as pd

links_file = Path("/mnt/hdd/workspace/pavenet/input/lehen/links/v2r_links.parquet")
links_df = pd.read_parquet(links_file)

# Group by time and node, then find the row index of the minimum distance for each group
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

filtered_df.to_parquet(
    "/mnt/hdd/workspace/pavenet/input/lehen/links/v2r_links_for_ns3" ".parquet",
    index=False,
)
