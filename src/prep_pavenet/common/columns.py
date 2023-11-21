from __future__ import annotations

NODE_ID = "node_id"
COORD_X = "x"
COORD_Y = "y"
TIME_STEP = "time_step"

ACTIVATION_COLUMNS = [NODE_ID, "on_times", "off_times"]
RSU_COLUMNS = [TIME_STEP, NODE_ID, COORD_X, COORD_Y]
CONTROLLER_COLUMNS = [TIME_STEP, NODE_ID, COORD_X, COORD_Y]

# Folders
POSITIONS_FOLDER = "positions"
ACTIVATIONS_FOLDER = "activations"
LINKS_FOLDER = "links"
