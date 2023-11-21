from __future__ import annotations

from prep_pavenet.setup.config import *


def create_rsu(config: Config) -> None:
    """Create the RSU data."""
    output_path = config.path / config.get(OUTPUT_SETTINGS)[OUTPUT_PATH]
    if config.get(RSU_SETTINGS) is not None:
        rsu_placement_type = config.get(RSU_SETTINGS)[PLACEMENT]
        if rsu_placement_type == "junction":
            from prep_pavenet.rsu.junction import JunctionPlacement

            rsu_placement = JunctionPlacement(
                config.get(TRAFFIC_SETTINGS),
                config.get(RSU_SETTINGS),
                config.path,
                output_path,
            )
            rsu_placement.create_rsu_data()