from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from prep_pavenet.common.columns import COORD_X, COORD_Y, NODE_ID, TIME_STEP


class InputReader:
    """The class that reads the input parquet files."""

    def __init__(self, input_file: Path):
        """
        Initialize the parquet file reader.

        Parameters
        ----------
        input_file : Path
            The input file to read.
        """
        self._input_filepath: Path = input_file
        self.input_reader: pq.ParquetFile | None = None

        # Cache to hold the currently streamed data.
        self._data_cache: pa.Table | None = None
        self._data_cache_dict: dict = {}

        self.row_group_id: int = 0
        self._max_ts: int = 0

    def initialize_for_reading(self):
        """
        Initialize the parquet files.

        This method should be called before reading any data from the files.
        The first call initializes the reader and the data cache.
        """
        self.input_reader: pq.ParquetFile = pq.ParquetFile(self._input_filepath)
        self._update_data_cache()

    def close(self):
        """
        Close the parquet file reader.

        This method should be called after reading all the data from the files.
        """
        self.input_reader.close()

    def _update_data_cache(self):
        """
        Update the data cache.

        The data is read from the row group ID and stored in the data cache.
        Row group ID is incremented if there are more row groups.
        """
        self._data_cache: pa.Table = self.input_reader.read_row_group(self.row_group_id)

        # Get the maximum of the time column.
        self._max_ts = self._data_cache[TIME_STEP][-1].as_py()

        # If the row group id is less than the number of row groups, increment it.
        if self.row_group_id < self.input_reader.num_row_groups - 1:
            self.row_group_id += 1

        # Convert the data cache to a dictionary with time as the key.
        temp_df = self._data_cache.to_pandas()
        self._data_cache_dict = (
            temp_df.groupby(TIME_STEP)
            .apply(
                lambda x: dict(
                    zip(
                        x[NODE_ID],
                        zip(x[COORD_X], x[COORD_Y], strict=True),
                        strict=True,
                    )
                )
            )
            .to_dict()
        )

    def _get_row_group_ids(self, start_times: list[int]):
        """
        Identify the row group ids for the given start times.

        This is required for multiprocessing. Each process will have a different
        start time and will need to read the data from the corresponding row group.


        Parameters
        ----------
        start_times : list[int]
            The start times for which the row group ids are required.

        Returns
        -------
        list[int]
            The row group ids corresponding to the start times.
        """
        row_group_id = 0
        final_row_group_ids = []

        data_cache = self.input_reader.read_row_group(row_group_id)
        maxtime_current_group = data_cache[TIME_STEP][-1].as_py()

        while len(start_times) > 0:
            if start_times[0] <= maxtime_current_group:
                # We can start reading from this group to get the data.
                final_row_group_ids.append(row_group_id)
                start_times.remove(start_times[0])
                continue

            # Increment the row group id and update data cache.
            row_group_id += 1
            data_cache = self.input_reader.read_row_group(
                row_group_id, columns=[TIME_STEP], use_threads=True
            )
            maxtime_current_group = data_cache[TIME_STEP][-1].as_py()

        if not final_row_group_ids:
            final_row_group_ids.append(row_group_id)
        return final_row_group_ids

    def get_positions_at(self, time_step: int) -> tuple[np.ndarray, np.ndarray]:
        """
        Get the positions of all agents at a given time.

        Parameters
        ----------
        time_step : int
            The time step at which the positions are required.

        Returns
        -------
        tuple[np.ndarray, np.ndarray]
            The vehicle ids and positions at the given time step.
        """
        if time_step < self._max_ts and time_step not in self._data_cache_dict:
            return np.array([]), np.array([])

        this_ts_data = self._data_cache_dict.get(time_step, {})
        if time_step == self._max_ts:
            self._update_data_cache()
            this_ts_data.update(self._data_cache_dict.get(time_step, {}))

        vehicle_ids = np.fromiter(this_ts_data.keys(), dtype=int)
        x = np.array([this_ts_data[vehicle_id][0] for vehicle_id in vehicle_ids])
        y = np.array([this_ts_data[vehicle_id][1] for vehicle_id in vehicle_ids])

        positions = np.vstack((x, y)).T
        return vehicle_ids, positions
