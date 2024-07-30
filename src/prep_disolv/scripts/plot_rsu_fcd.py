import pandas as pd
import matplotlib.pyplot as plt


rsu_file = "/Users/charan/workspace/disolv/input/koln/positions/picked_locations_garsud.parquet"
fcd_file = "/Users/charan/workspace/disolv/input/koln/positions/koln_fcd.parquet"

# Read the data to a dataframe
rsu_df = pd.read_parquet(rsu_file)
fcd_df = pd.read_parquet(fcd_file)

# Plot the dataframe x and y
plt.plot(rsu_df["x"], rsu_df["y"], label="rsu", linestyle=" ", marker="*", markersize=10, color="red")
plt.plot(fcd_df["x"], fcd_df["y"], label="fcd", linestyle=" ", marker="o", markersize=0.5, color="gray")
plt.legend()
plt.savefig("/Users/charan/workspace/disolv/output/koln/rsu_fcd.png")
plt.close()