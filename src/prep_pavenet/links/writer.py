from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from prep_pavenet.common.columns import LINK_COLUMNS
from prep_pavenet.links.infra_tree import LinkDataArrays


class LinksWriter:
    """The class that writes the links to a file."""

    def __init__(self, output_file: Path):
        """
        Instantiate an object of the links writer class.

        Parameters
        ----------
        output_file : Path
            The path to the output file.
        """
        self._output_file = output_file
        self.parquet_writer = None
        self._create_links_schema()
        self._create_links_file()

    def _create_links_schema(self):
        """
        Create a schema for the links file.

        The schema contains the following columns:
        - time_step: The time step of the simulation.
        - vehicle_id: The id of the vehicle.
        - target_id: The id of the target.
        - distance: The distance between the vehicle and the target.
        """
        self._schema_links = pa.schema(
            [
                pa.field(LINK_COLUMNS[0], pa.int64()),
                pa.field(LINK_COLUMNS[1], pa.int64()),
                pa.field(LINK_COLUMNS[2], pa.int64()),
                pa.field(LINK_COLUMNS[3], pa.float64()),
            ]
        )

    def _create_links_file(self):
        """Create links file using the schema."""
        if not self._output_file.parent.exists():
            self._output_file.parent.mkdir(parents=True, exist_ok=True)
        self.parquet_writer = pq.ParquetWriter(
            self._output_file, self._schema_links, compression="snappy"
        )

    def write_data(self, links_data: pd.DataFrame):
        """
        Write the data to the output file.

        The data is initially converted to a pyarrow table.
        The table is then written to the output file.

        Parameters
        ----------
        links_data: pd.DataFrame
            The output data.
        """
        table = pa.Table.from_pandas(links_data, schema=self._schema_links)
        self.parquet_writer.write_table(table)

    def write_arrays(self, links_data: LinkDataArrays):
        """
        Write the data to the output file.

        The data is initially converted to a pyarrow table.
        The table is then written to the output file.

        Parameters
        ----------
        links_data: LinkDataArrays
            The output data.
        """
        table = pa.Table.from_pydict(links_data.__dict__, schema=self._schema_links)
        self.parquet_writer.write_table(table, row_group_size=100000)

    def close(self):
        """Close the parquet writer."""
        self.parquet_writer.close()
