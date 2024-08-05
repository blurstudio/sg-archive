import json
import logging
from pathlib import Path
from shotgun_api3.lib import mockgun

from .utils import DateTimeDecoder

logger = logging.getLogger(__name__)


class Shotgun(mockgun.Shotgun):
    """A shotgun_api3 like interface for read only selecting of archived data.

    Example:

        >>>  sg = connection.Shotgun("c:/temp/archived_root")
        >>> sg.load_tables()
        >>> sg.find(...)
    """
    def __init__(
        self, data_root, *args, base_url='https://invalid.localhost', **kwargs
    ):
        self.data_root = Path(data_root)
        schema_file = self.data_root / 'schema.pickle'
        schema_entity_file = self.data_root / 'schema_entity.pickle'
        mockgun.Shotgun.set_schema_paths(schema_file, schema_entity_file)
        super(Shotgun, self).__init__(base_url, *args, **kwargs)

    def field_names_for_table(self, table):
        """Provides a list of the field names for a given table."""
        # TODO: Use the config to ignore columns
        return self._schema[table].keys()

    def load_table(self, table):
        """Load all table data for a specific table archived in data_root."""
        logger.info(f"Loading table: {table}")

        table_root = self.data_root / "data" / table
        for fn in table_root.glob(f"{table}_*.json"):
            data = json.load(fn.open(), cls=DateTimeDecoder)
            modified = {}
            for k, v in data.items():
                # Add mockgun required field
                v.setdefault("__retired", False)

                # Process file links to reference the local files
                for field_name, field in v.items():
                    if isinstance(field, dict) and "__download_type" in field:
                        local_path = fn.parent / field["local_path"]
                        if field["__download_type"] == "image":
                            v[field_name] = local_path.as_uri()
                        elif field["__download_type"] == "url":
                            v[field_name]["url"] = local_path.as_uri()

                modified[int(k)] = v

            self._db[table].update(modified)

    def load_tables(self):
        """Load table data for all archived tables found in from data_root."""
        for directory in (self.data_root / "data").iterdir():
            if not directory.is_dir():
                continue
            self.load_table(directory.name)
