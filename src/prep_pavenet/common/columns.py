from __future__ import annotations

NODE_ID = "node_id"
NS3_ID = "ns3_id"
COORD_X = "x"
COORD_Y = "y"
TIME_STEP = "time_step"
ON_TIMES = "on_times"
OFF_TIMES = "off_times"
LAT = "lat"
LON = "lon"

TARGET_ID = "target_id"
DISTANCE = "distance"

ACTIVATION_COLUMNS = [NODE_ID, NS3_ID, ON_TIMES, OFF_TIMES]
RSU_COLUMNS = [TIME_STEP, NODE_ID, NS3_ID, COORD_X, COORD_Y, LAT, LON]
CONTROLLER_COLUMNS = [TIME_STEP, NODE_ID, NS3_ID, COORD_X, COORD_Y]
LINK_COLUMNS = [TIME_STEP, NODE_ID, TARGET_ID, DISTANCE]

# Folders
POSITIONS_FOLDER = "positions"
ACTIVATIONS_FOLDER = "activations"
LINKS_FOLDER = "links"
