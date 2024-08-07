import concurrent.futures
import io
import json
import logging
import math
import os
import pickle
import shutil
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from pprint import pformat

import yaml
from shotgun_api3.lib import mockgun
from shotgun_api3.shotgun import Shotgun

from .utils import DateTimeDecoder, DateTimeEncoder

ROOT_DIR = Path(__file__).parent
logger = logging.getLogger(__name__)


class Connection(object):
    def __init__(
        self,
        config,
        output,
        download="missing",
        filtered=True,
        strict=False,
        verbosity=0,
    ):
        self.config = yaml.load(config.open(), Loader=yaml.Loader)
        if "connection" in self.config:
            self.connection = self.config.pop("connection")
        else:
            conn_file = config.parent.joinpath(self.config["connection_file"])
            self.connection = yaml.load(conn_file.open(), Loader=yaml.Loader)

        self.download = download
        self.filtered = filtered
        self.output = output
        self.strict = strict
        self.verbosity = verbosity

        # File download processing variables
        self.total_download = 0
        self.all_download = 0
        self.pending_downloads = []
        self.executor = None
        # Wait for all pending downloads if more than X are pending before
        # processing another page of results.
        self.limit_download_count = 500

    def clean(self):
        if self.output.exists():
            logger.info('Clean: Removing "{}" and its contents'.format(self.output))
            shutil.rmtree(str(self.output))

    def download_entity_type(self, entity_type, query, limit, max_pages, formats=None):
        if formats is None:
            formats = ["pickle-high"]

        display_name = self.schema_entity[entity_type]["name"]["value"]
        logger.info("Processing: {} ({})".format(entity_type, display_name))
        data_dir = self.output / "data" / entity_type
        data_dir.mkdir(exist_ok=True, parents=True)

        # Save the schema for just this entity_type so its easier to inspect
        entity_type_schema = data_dir / "_schema.json"
        self.save_json(self.schema[entity_type], entity_type_schema)
        urls = [
            k
            for k, v in self.schema[entity_type].items()
            if v["data_type"]["value"] in ("url", "image")
        ]

        count = self.sg.summarize(
            entity_type,
            filters=query,
            summary_fields=[
                {"field": "id", "type": "count"},
            ],
        )
        count = count["summaries"]["id"]

        if max_pages is None:
            page_count = math.ceil(count / limit)
        else:
            page_count = max_pages

        logger.info(
            f"  Total records: {count}, limit: {limit}, total pages: {page_count}"
        )
        total = 0
        file_map = {}
        self.total_download = 0

        total_start = time.time()
        with concurrent.futures.ThreadPoolExecutor() as self.executor:
            for page in range(1, page_count + 1):
                # Process pending downloads before the links expire.
                pending_count = len(self.pending_downloads)
                if pending_count > self.limit_download_count:
                    logger.info(
                        f"    Waiting for {pending_count} pending downloads to "
                        "ensure the download links don't expire."
                    )
                    concurrent.futures.wait(self.pending_downloads)
                    self.pending_downloads = []

                filestem = f"{entity_type}_{page}"

                sel_start = time.time()
                out = self.find(entity_type, query, limit=limit, page=page)
                sel_end = time.time()
                total += len(out)
                if not out:
                    break
                logger.info(
                    f"  Selected {len(out)} {entity_type} in "
                    f"{sel_end - sel_start:.5} seconds. ({page}/{page_count})"
                )

                for entity in out:
                    for field in urls:
                        self.download_url(entity, field, data_dir)

                # Update the file_map data for this select
                out = self.make_index(out)
                file_map.update({sgid: filestem for sgid in out})

                # Save this page data to disk
                for fmt in formats:
                    fmt, protocol = self.parse_format(fmt)
                    filename = data_dir / f"{filestem}.{fmt}"
                    if fmt == "json":
                        self.save_json(out, filename)
                    else:
                        with filename.open("wb") as fle:
                            pickle.dump(out, fle, protocol=protocol)

                    # Check that the data we saved matches the input data
                    if self.strict:
                        if fmt == "pickle":
                            check = pickle.load(filename.open("rb"))
                        else:
                            check = json.load(filename.open(), cls=DateTimeDecoder)
                        if out != check:
                            msg = [pformat(out), "-" * 50, pformat(check)]
                            assert out == check, "\n".join(msg)

            total_end = time.time()
            logger.info(
                f"    Finished selecting pages in {total_end - total_start:.5} "
                f"seconds. Waiting for {len(self.pending_downloads)} remaining downloads."
            )
        total_end = time.time()

        logger.info(
            f"  {entity_type} Records saved: {total}, Total in SG: {count}, Files "
            f"downloaded: {self.total_download} in {total_end - total_start:.5} seconds."
        )

        # Save a map of which paged file contains the data for a given sgid.
        self.save_json(file_map, data_dir / "_page_index.json")

    def download_url(self, entity, field, dest):
        def worker(url, dest_name):
            urllib.request.urlretrieve(url, str(dest_name))
            if self.verbosity:
                logger.info(f"    Download Finished: {dest_name.name}")

        url_info = entity[field]
        if url_info is None:
            return None
        if isinstance(url_info, str):
            url = url_info
            parse = urllib.parse.urlparse(url)
            name = os.path.basename(parse.path)
            # Convert the str into a dict so we can store the downloaded path
            url_info = {"url": url, "name": name, "__download_type": "image"}
            entity[field] = url_info
        else:
            url = url_info["url"]
            name = url_info["name"]
            url_info["__download_type"] = "url"
        # Sanitize the name so we can use it in a file path
        if name is None:
            name = str(entity["id"])
        else:
            name = name.replace("\\", "_").replace("/", "_")
            name = "{}-{}".format(entity["id"], name)
        dest_fn = dest / "files" / field / name
        if self.verbosity:
            logger.info("    Downloading: {}".format(dest_fn.name))
        dest_fn.parent.mkdir(exist_ok=True, parents=True)

        # Process the download mode for the file. This is called even if downloading
        # is disabled so the database is updated with the local file paths.
        if self.download == "missing":
            download = not dest_fn.exists()
        else:
            download = self.download == "all"
        if download:
            # Safety check for duplicate local file paths
            if dest_fn.exists():
                raise RuntimeError(
                    "Destination already exists: {}".format(dest_fn),
                )
            self.pending_downloads.append(self.executor.submit(worker, url, dest_fn))
            self.total_download += 1
            self.all_download += 1

        # Store the relative file path to the file we just downloaded in the
        # data we will save into the json data
        url_info["local_path"] = str(dest_fn.relative_to(dest))

        return dest_fn

    def find(self, entity_type, query, **kwargs):
        if "fields" not in kwargs:
            kwargs["fields"] = list(self.schema[entity_type].keys())

        return self.sg.find(entity_type, query, **kwargs)

    def make_index(self, data, key="id"):
        """Convert a list of records into a dict of the key value.

        The key is converted to a str so it can be stored in json
        without modification.
        """
        if self.strict:
            ret = {}
            for row in data:
                sgid = row[key]
                if sgid in ret:
                    raise ValueError(
                        "Duplicate record found: {}:{}".format(row["entity_type"], sgid)
                    )
                ret[str(sgid)] = row
            return ret
        return {str(row[key]): row for row in data}

    @classmethod
    def parse_format(cls, fmt):
        """Parse a format string into the base format and any protocol/version
        identifier.

        Accepts `pickle-high`, `pickle-default`, `pickle-X` for each supported
        protocol version, and `json` formats.
        """
        if fmt == "pickle-high":
            return "pickle", pickle.HIGHEST_PROTOCOL
        elif fmt == "pickle-default":
            return "pickle", pickle.DEFAULT_PROTOCOL
        elif fmt.startswith("pickle-"):
            return "pickle", int(fmt.replace("pickle-", ""))
        return fmt, None

    @property
    def sg(self):
        try:
            return self._sg
        except AttributeError:
            self._sg = Shotgun(**self.connection)
            logger.info("Connected: to {}".format(self.connection["base_url"]))
        return self._sg

    @property
    def schema_full(self):
        try:
            return self._schema_full
        except AttributeError:
            self._schema_full = self.sg.schema_read()
        return self._schema_full

    @property
    def schema(self):
        try:
            return self._schema
        except AttributeError:
            self._schema = self.schema_full
            if self.filtered:
                self._schema = self.filter_schema(self._schema)
        return self._schema

    def filter_schema(self, schema):
        ignored = self.config.get("ignored", {})
        ret = {}
        for entity_type in schema:
            out_entity_type = {}
            fields = ignored.get("fields", {}).get(entity_type, [])
            for field, value in schema[entity_type].items():
                if value["data_type"]["value"] in ignored.get("data_types"):
                    continue
                if field in fields:
                    continue
                out_entity_type[field] = value
            if out_entity_type:
                ret[entity_type] = out_entity_type
        return ret

    @property
    def schema_entity(self):
        try:
            return self._schema_entity
        except AttributeError:
            self._schema_entity = self.schema_entity_full
            if self.filtered:
                self._schema_entity = self.filter_schema_entity(self._schema_entity)
        return self._schema_entity

    @property
    def schema_entity_full(self):
        try:
            return self._schema_entity_full
        except AttributeError:
            self._schema_entity_full = self.sg.schema_entity_read()
        return self._schema_entity_full

    def filter_schema_entity(self, schema_entity):
        ignored = self.config.get("ignored", {}).get("entity_types", [])
        schema_entity = {
            k: v for k, v in schema_entity.items() if v["visible"]["value"]
        }
        return {k: v for k, v in schema_entity.items() if k not in ignored}

    def save_json(self, data, output):
        if sys.version_info.major == 2:
            # Deal with unicode in python 2
            with io.open(str(output), "w", encoding="utf-8") as outfile:
                my_json_str = json.dumps(
                    data,
                    indent=4,
                    ensure_ascii=False,
                    sort_keys=True,
                    cls=DateTimeEncoder,
                )
                if isinstance(my_json_str, str):
                    my_json_str = my_json_str.decode("utf-8")

                # remove trailing white space for consistency with python 3
                lines = [line.rstrip() for line in my_json_str.splitlines()]
                my_json_str = "\n".join(lines)
                outfile.write(my_json_str)
        else:
            # Python 3 is much easier
            with output.open("w") as fle:
                json.dump(
                    data,
                    fle,
                    indent=4,
                    sort_keys=True,
                    cls=DateTimeEncoder,
                )

    def save_schema(self):
        fn_schema = self.output / "schema.json"
        fn_schema_entity = self.output / "schema_entity.json"

        logger.info("Saving schema")
        self.output.mkdir(exist_ok=True, parents=True)
        self.save_json(self.schema_full, fn_schema)
        self.save_json(self.schema_entity_full, fn_schema_entity)

        # Save the schema for mockgun to restore later
        mockgun.generate_schema(
            self.sg,
            str(self.output / "schema.pickle"),
            str(self.output / "schema_entity.pickle"),
        )
