# Generate json file with RSU positions for MOSAIC simulator.

from pathlib import Path
import json

import pandas as pd

pavenet_scenario = "medium"
mosaic_scenario = "DataHoseMedium"

rsu_file = Path(
    "/mnt/hdd/workspace/pavenet/input/"
    + pavenet_scenario
    + "/positions/roadside_units.parquet"
)
rsu_df = pd.read_parquet(rsu_file)
json_file = Path(
    "/home/charan/intellij/eclipse-mosaic-23.1/scenarios/"
    + mosaic_scenario
    + "/mapping/mapping_config.json"
)

out_data = {
    "config": {"fixed_order": True},
    "prototypes": [
        {
            "name": "DEFAULT_VEHTYPE",
            "vehicleClass": "Car",
            "applications": ["org.pavenet.app.Vehicle"],
        }
    ],
    "servers": [
        {
            "name": "Controller",
            "group": "CentralController",
            "applications": ["org.pavenet.app.Controller"],
        }
    ],
    "rsus": [],
}

for index, row in rsu_df.iterrows():
    out_data["rsus"].append(
        {
            "name": "RSU",
            "position": {"latitude": row["lat"], "longitude": row["lon"]},
            "applications": ["org.pavenet.app.RoadSideUnit"],
        }
    )

with open(json_file, "w") as f:
    json.dump(out_data, f, indent=4)
