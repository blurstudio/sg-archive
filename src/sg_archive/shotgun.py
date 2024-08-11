import json
import logging
import pickle
from pathlib import Path

from shotgun_api3.lib import mockgun

from .utils import DateTimeDecoder

logger = logging.getLogger(__name__)


class Shotgun(mockgun.Shotgun):
    """A shotgun_api3 like interface for read only selecting of archived data.

    Example:

        >>>  sg = connection.Shotgun("c:/temp/archived_root")
        >>> sg.load_entity_types()
        >>> sg.find(...)
    """

    def __init__(
        self, data_root, *args, base_url="https://invalid.localhost", **kwargs
    ):
        self.data_root = Path(data_root)
        schema_file = self.data_root / "schema.pickle"
        schema_entity_file = self.data_root / "schema_entity.pickle"
        mockgun.Shotgun.set_schema_paths(schema_file, schema_entity_file)
        super(Shotgun, self).__init__(base_url, *args, **kwargs)

    def field_names_for_entity_type(self, entity_type):
        """Provides a list of the field names for a given entity_type."""
        # TODO: Use the config to ignore fields
        return self._schema[entity_type].keys()

    def load_entity_type(self, entity_type, ext="pickle"):
        """Load all entity_type data for a specific entity_type archived in data_root."""
        logger.info(f"Loading entity_type: {entity_type}")

        entity_type_root = self.data_root / "data" / entity_type
        for fn in entity_type_root.glob(f"{entity_type}_*.{ext}"):
            if ext == "pickle":
                data = pickle.load(fn.open("rb"))
            else:
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
                        elif field["__download_type"] == "attachment":
                            local_path = (
                                fn.parent.parent / "Attachment" / field["local_path"]
                            )
                            v[field_name]["url"] = local_path.as_uri()
                        elif field["__download_type"] == "url":
                            v[field_name]["url"] = local_path.as_uri()

                modified[int(k)] = v

            self._db[entity_type].update(modified)

    def load_entity_types(self, ext="pickle"):
        """Load entity_type data for all archived entity_types found in from data_root."""
        for directory in (self.data_root / "data").iterdir():
            if not directory.is_dir():
                continue
            self.load_entity_type(directory.name, ext=ext)
