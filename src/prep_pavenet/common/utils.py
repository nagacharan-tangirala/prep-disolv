from __future__ import annotations

import logging
import xml.etree.ElementTree as Et
from pathlib import Path

BOUNDARY = "convBoundary"
LOCATION = "location"


def get_offsets(sumo_net_file: Path) -> (float, float):
    """Get the offsets from the sumo net file."""
    sumo_iter = Et.iterparse(str(sumo_net_file), events=("start", "end"))
    for event, elem in sumo_iter:
        if event == "end" and elem.tag == LOCATION:
            boundary = elem.attrib[BOUNDARY]
            offset_x = float(boundary.split(",")[0])
            offset_y = float(boundary.split(",")[1])
            return offset_x, offset_y
    logging.error("Could not find offsets in sumo net file.")
    raise ValueError("Could not find offsets in sumo net file.")


def get_center(sumo_net_file: Path) -> (float, float):
    """Get the center of the sumo net file."""
    sumo_iter = Et.iterparse(str(sumo_net_file), events=("start", "end"))
    for event, elem in sumo_iter:
        if event == "end" and elem.tag == LOCATION:
            boundary = elem.attrib[BOUNDARY]
            low_x = float(boundary.split(",")[0])
            low_y = float(boundary.split(",")[1])
            high_x = float(boundary.split(",")[2])
            high_y = float(boundary.split(",")[3])
            return (low_x + high_x) / 2, (low_y + high_y) / 2
