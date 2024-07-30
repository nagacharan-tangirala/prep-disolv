from __future__ import annotations

from prep_disolv.common.config import (
    OUTPUT_PATH,
    OUTPUT_SETTINGS,
    PLACEMENT,
    RSU_SETTINGS,
    TRAFFIC_SETTINGS,
    Config,
)


class RsuConverter:
    def __init__(self, config: Config) -> None:
        """The constructor of the RsuConverter class."""
        self.config = config
        self.rsu_file = None
        self.rsu_count = 0

    def create_rsu(self, rsu_id_init: int) -> int:
        """Create the RSU data."""
        output_path = self.config.path / self.config.get(OUTPUT_SETTINGS)[OUTPUT_PATH]
        if self.config.get(RSU_SETTINGS) is not None:
            rsu_placement_type = self.config.get(RSU_SETTINGS)[PLACEMENT]
            if rsu_placement_type == "junction":
                from prep_disolv.rsu.junction import JunctionPlacement

                rsu_placement = JunctionPlacement(
                    self.config,
                    output_path,
                    rsu_id_init,
                )
                rsu_placement.create_rsu_data()
                self.rsu_count = rsu_placement.get_unique_rsu_count()
                self.rsu_file = rsu_placement.get_parquet_file()
                return self.rsu_count

            if rsu_placement_type == "given":
                rsu_placement = InputPlacement(self.config, output_path)
                rsu_placement.create_rsu_data()
                self.rsu_count = rsu_placement.get_unique_rsu_count()
                self.rsu_file = rsu_placement.get_parquet_file()
                return self.rsu_count

        return 0
