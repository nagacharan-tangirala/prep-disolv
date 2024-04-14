# This script takes geojson files as input and converts them to gpkg files.

import argparse
import geopandas as gpd

if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--input", type=str, required=True)
    args.add_argument("--output", type=str, required=True)

    input_file = args.parse_args().input
    output_file = args.parse_args().output

    print("Reading geojson file from", input_file)
    gdf = gpd.read_file(input_file)
    print("Writing gpkg file to", output_file)
    gdf.to_file(output_file, driver="GPKG", layer="edges")
