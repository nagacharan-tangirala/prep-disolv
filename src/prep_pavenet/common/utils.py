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
            offsets = elem.attrib[OFFSETS]
            offset_x = float(offsets.split(",")[0])
            offset_y = float(offsets.split(",")[1])
            return offset_x, offset_y
    msg = "Could not find offsets in sumo net file."
    logging.error(msg)
    raise ValueError(msg)


def get_projection(sumo_net_file: Path) -> (float, float):
    """Get the offsets from the sumo net file."""
    sumo_iter = Et.iterparse(str(sumo_net_file), events=("start", "end"))
    for event, elem in sumo_iter:
        if event == "end" and elem.tag == LOCATION:
            return elem.attrib[PROJECTION]
    msg = "Could not find projection parameter in sumo net file."
    logging.error(msg)
    raise ValueError(msg)


def get_center(sumo_net_file: Path) -> (float, float):
    """Get the center of the sumo net file."""
    offsets = get_offsets(sumo_net_file)
    sumo_iter = Et.iterparse(str(sumo_net_file), events=("start", "end"))

    for event, elem in sumo_iter:
        if event == "end" and elem.tag == LOCATION:
            boundary = elem.attrib[BOUNDARY]

            low_x = float(boundary.split(",")[0])
            low_y = float(boundary.split(",")[1])
            high_x = float(boundary.split(",")[2])
            high_y = float(boundary.split(",")[3])

            center_x = (low_x + high_x) / 2
            center_y = (low_y + high_y) / 2

            return center_x - offsets[0], center_y - offsets[1]

    msg = "Could not find network center in sumo net file."
    logging.error(msg)
    raise ValueError(msg)
