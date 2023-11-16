from pathlib import Path

import toml

LOG_SETTINGS = "logging"
VEHICLE_SETTINGS = "vehicles"


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
