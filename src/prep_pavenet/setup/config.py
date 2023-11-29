from pathlib import Path

import toml

from prep_pavenet.common.columns import (
    ACTIVATIONS_FOLDER,
    LINKS_FOLDER,
    POSITIONS_FOLDER,
)

# Config keys.
LOG_SETTINGS = "logging"
TRAFFIC_SETTINGS = "traffic"
VEHICLE_SETTINGS = "vehicles"
RSU_SETTINGS = "rsu"
OUTPUT_SETTINGS = "output"
CONTROLLER_SETTINGS = "controller"

# Traffic keys.
NETWORK_FILE = "network"
TRACE_FILE = "trace"
OFFSET_X = "offset_x"
OFFSET_Y = "offset_y"

# Vehicle keys.
SIMULATOR = "simulator"

# Output keys.
OUTPUT_PATH = "output_path"

# RSU keys.
PLACEMENT = "placement"
START_TIME = "start_time"
END_TIME = "end_time"

# Logging keys.
LOG_LEVEL = "log_level"
LOG_OVERWRITE = "log_overwrite"

ID_INIT = "id_init"


def read_config_toml(config_toml: str) -> dict:
    """
    Read the config file.
    """
    with Path.open(Path(config_toml)) as f:
        return toml.load(f)


class Config:
    def __init__(self, config_file: str) -> None:
        """Stores the configuration data.

        Parameters
        ----------
        config_file : str
            The path to the configuration file.
        """
        self.config_file: str = config_file
        self.path: Path = Path(config_file).parent
        self.settings: dict = read_config_toml(config_file)
        self._create_output_folders()

    def _create_output_folders(self) -> None:
        """Create the output folders."""
        out_path = Path(self.settings[OUTPUT_SETTINGS][OUTPUT_PATH])
        if out_path.is_absolute():
            output_path = out_path
        else:
            output_path = (
                Path(self.config_file).parent
                / self.settings[OUTPUT_SETTINGS][OUTPUT_PATH]
            )

        output_path.mkdir(parents=True, exist_ok=True)
        Path.mkdir(output_path / ACTIVATIONS_FOLDER, exist_ok=True)
        Path.mkdir(output_path / LINKS_FOLDER, exist_ok=True)
        Path.mkdir(output_path / POSITIONS_FOLDER, exist_ok=True)

    def get(self, key: str) -> dict:
        """Get the configuration data for a specific key.

        Parameters
        ----------
        key : str
            The key for the configuration data.

        Returns
        -------
        dict
            The configuration data for the key.
        """
        return self.settings.get(key)
