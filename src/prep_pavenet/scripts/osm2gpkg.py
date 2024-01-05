# Run this with conda environment "osmnx" activated

import osmnx as ox
from pathlib import Path
import geopandas as gpd

# Read in the OSM data
osm_file = Path("/mnt/hdd/workspace/pavenet/raw/lehen.osm")
osm_graph = ox.graph_from_xml(
    osm_file, simplify=True, retain_all=True, bidirectional=False
)

# Convert to GeoDataFrame
osm_gpkg_file = Path("/mnt/hdd/workspace/pavenet/output/lehen/lehen_graph.gpkg")
ox.save_graph_geopackage(osm_graph, filepath=osm_gpkg_file)
