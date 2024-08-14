import concurrent.futures
import io
import json
import logging
import math
import os
import pickle
import shutil
import sys
import threading
import urllib.parse
import urllib.request
from datetime import datetime
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
        config = Path(config)
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
        self.downloads = {"current": 0, "skipped": [], "all": 0, "failed": []}
        self.pending_downloads = []
        self.executor = None
        self._field_data_types = {}
        self.attachment_dir = self.output / "data" / "Attachment"
        self.attachment_urls = {}
        self.attachment_all_ids = set()
        self.write_lock = threading.Lock()
        # Wait for all pending downloads if more than X are pending before
        # processing another page of results.
        self.limit_download_count = 500

    def field_data_types(self, entity_type):
        """Returns a dict of useful mappings of field names matching useful
        data types.
        """
        try:
            return self._field_data_types[entity_type]
        except KeyError:
            pass

        if entity_type not in self._field_data_types:
            self._field_data_types[entity_type] = {"image": [], "attachment": []}
        field_type = self._field_data_types[entity_type]
        for name, field in self.schema[entity_type].items():
            data_type = field["data_type"]["value"]
            if data_type == "image":
                field_type["image"].append(name)
            elif data_type == "url":
                field_type["attachment"].append(name)
            elif data_type in ("entity", "multi_entity"):
                valid_types = field["properties"]["valid_types"]["value"]
                if "Attachment" in valid_types:
                    field_type["attachment"].append(name)
        return self._field_data_types[entity_type]

    def attachments_get(self, ids, fields=None):
        """Get attachments entities."""
        if not ids:
            return []

        if fields is None:
            fields = [
                "url",
                "name",
                "content_type",
                "link_type",
                "type",
                "id",
                "this_file",
                "image",
                "filmstrip_image",
            ]
        sel_start = datetime.now()
        ret = self.sg.find("Attachment", [["id", "in", list(ids)]], fields)
        sel_end = datetime.now()
        logger.info(
            f"  Selected {len(ret)} linked attachments for {len(ids)} ids took "
            f"{self.timestr(sel_end - sel_start)}."
        )
        return ret

    def attachment_ids(self, entities):
        """Returns a set of all Attachment id's linked to any column on entities."""
        attachment_ids = set()
        for entity in entities:
            for field in self.field_data_types(entity["type"])["attachment"]:
                data = entity.get(field)
                if not data:
                    continue
                if not isinstance(data, list):
                    data = [data]
                for item in data:
                    if isinstance(item, str):
                        # Project landing_page_url is a url but returns a string
                        continue
                    sgid = item.get("id")
                    if sgid:
                        attachment_ids.add(sgid)
                        self.attachment_all_ids.add(sgid)
        return attachment_ids

    def attachments_localize(self, attachments):
        """Build a drop in replacement attachment dictionary for url fields.

        This function modifies the provided attachments in place.

        Replicates the dict structure of an attachment link. Replaces the url
        with a relative local file path. Adds `__download_type` key set to
        attachment.

        https://developers.shotgridsoftware.com/python-api/cookbook/attachments.html#structure-of-local-file-values

        Returns:
            dict: A dict of localized attachments using their id as the key.
        """
        ret = {}
        for attachment in attachments:
            this_file = attachment["this_file"]
            name = this_file["name"]
            if name is None:
                name = "no_name"
            else:
                name = name.replace("\\", "_").replace("/", "_")
            name = "{}-{}".format(this_file["id"], name)
            dest = Path("files") / "this_file" / name
            this_file["name"] = name
            this_file["__download_type"] = "attachment"
            this_file["local_path"] = str(dest)
            # Store the original attachment url so we can download it later.
            self.attachment_urls[this_file["id"]] = this_file["url"]
            # Then replace it with the local path
            this_file["url"] = str(dest)

            for field in self.field_data_types("Attachment")["image"]:
                if field in attachment and attachment[field]:
                    self.download_url(attachment, field, self.attachment_dir)

            ret[attachment["id"]] = attachment

        return ret

    def download_attachment_this_file(self, attachment, field="this_file"):
        dest_fn = self.attachment_dir / "files" / field / attachment["name"]
        url = self.attachment_urls[attachment["id"]]
        self._download(url, dest_fn)

    def attachment_localize_entities(self, entities, attachment_cache):
        """Updates entities, replacing any attachment fields with the localized
        attachment data."""
        ignored = self.config.get("ignored", {}).get("file_exts", {})
        for entity in entities:
            for field in self.field_data_types(entity["type"])["attachment"]:
                data = entity.get(field)
                if not data:
                    continue
                # Replace attachment links with the localized version
                if isinstance(data, list):
                    attachments = [
                        attachment_cache[i["id"]] if i["type"] == "Attachment" else i
                        for i in data
                    ]
                    entity[field] = attachments
                else:
                    if data["type"] != "Attachment":
                        continue
                    entity[field] = attachment_cache[data["id"]]
                    attachments = [entity[field]]

                for attachment in attachments:
                    this_file = attachment["this_file"]
                    path = Path(this_file["local_path"])
                    if path.suffix in ignored.get(entity["type"], {}).get(field, []):
                        self.downloads["skipped"].append(this_file)
                        if self.verbosity:
                            logger.debug(f"    Not-downloading: {path}")
                        continue
                    self.download_attachment_this_file(this_file)

    def process_all_recorded_attachments(self):
        filename = self.attachment_dir / "_all_ids.pickle"
        with self.write_lock:
            if filename.exists():
                with filename.open("rb") as fle:
                    existing = pickle.load(fle)
            else:
                existing = set()

            existing.update(self.attachment_all_ids)

            filename.parent.mkdir(exist_ok=True, parents=True)
            with filename.open("wb") as fle:
                pickle.dump(existing, fle)
        return existing

    def process_attachments(self, entities):
        """Select and localize attachments"""
        attachment_ids = self.attachment_ids(entities)
        attachments = self.attachments_get(attachment_ids)
        cache = self.attachments_localize(attachments)
        self.attachment_localize_entities(entities, cache)
        return cache

    def clean(self):
        if self.output.exists():
            logger.info('Clean: Removing "{}" and its contents'.format(self.output))
            shutil.rmtree(str(self.output))

    def download_entity_type(self, entity_type, query, limit, max_pages, formats=None):
        if formats is None:
            formats = ["pickle-high"]

        display_name = (
            self.schema_entity.get(entity_type, {})
            .get("name", {})
            .get("value", entity_type)
        )
        logger.info("Processing: {} ({})".format(entity_type, display_name))
        data_dir = self.output / "data" / entity_type
        data_dir.mkdir(exist_ok=True, parents=True)

        # Save the schema for just this entity_type so its easier to inspect
        entity_type_schema = data_dir / "_schema.json"
        self.save_json(self.schema[entity_type], entity_type_schema)

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
        self.downloads["current"] = 0

        total_start = datetime.now()
        with concurrent.futures.ThreadPoolExecutor() as self.executor:
            for page in range(1, page_count + 1):
                # Process pending downloads before the links expire.
                pending_count = len(self.pending_downloads)
                if pending_count > self.limit_download_count:
                    logger.info(
                        f"    Waiting for {pending_count} pending downloads to "
                        "ensure the download links don't expire."
                    )
                    logger.info(
                        "    Progress: "
                        + self.estimate_time(page - 1, page_count, total_start)
                    )
                    concurrent.futures.wait(self.pending_downloads)
                    self.pending_downloads = []

                filestem = f"{entity_type}_{page}"

                sel_start = datetime.now()
                out = self.find(entity_type, query, limit=limit, page=page)
                sel_end = datetime.now()
                total += len(out)
                if not out:
                    break
                est = self.estimate_time(page - 1, page_count, total_start)
                logger.info(
                    f"  Selected {len(out)} {entity_type} in "
                    f"{self.timestr(sel_end - sel_start)}:  {est}"
                )
                self.process_attachments(out)

                for entity in out:
                    for field in self.field_data_types(entity_type)["image"]:
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

            # Save the attachment Id's that we processed in this loop
            self.process_all_recorded_attachments()

            total_end = datetime.now()
            logger.info(
                "    Finished selecting pages took "
                f"{self.timestr(total_end - total_start)}. Waiting "
                f"for {len(self.pending_downloads)} remaining downloads."
            )

        total_end = datetime.now()
        logger.info(
            f"  {entity_type} Records saved: {total}, Total in SG: {count}, Files "
            f"downloaded: {self.downloads['current']} "
            f"took {self.timestr(total_end - total_start)}."
        )

        # Save a map of which paged file contains the data for a given sgid.
        self.save_json(file_map, data_dir / "_page_index.json")

    def download_url(self, entity, field, dest):
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

        # Process the download mode for the file. This is called even if downloading
        # is disabled so the database is updated with the local file paths.
        self._download(url, dest_fn)

        # Store the relative file path to the file we just downloaded in the
        # data we will save into the json data
        url_info["local_path"] = str(dest_fn.relative_to(dest))

        return dest_fn

    def _download(self, url, dest_fn):
        if self.download == "missing":
            download = not dest_fn.exists()
        else:
            download = self.download == "all"
        if not download:
            return False

        # Create the parent directory if it doesn't exist
        dest_fn.parent.mkdir(exist_ok=True, parents=True)

        if dest_fn.exists():
            return False

        if self.verbosity:
            logger.debug("    Downloading: {}".format(dest_fn.name))

        self.pending_downloads.append(
            self.executor.submit(self._download_worker, url, dest_fn)
        )
        self.downloads["current"] += 1
        self.downloads["all"] += 1
        return True

    def _download_worker(self, url, dest):
        try:
            urllib.request.urlretrieve(url, str(dest))
        except Exception as error:
            self.downloads["failed"].append((url, dest, str(error)))
            logger.warning(f"Download Failed: {dest}, ({error})")
            raise
        if self.verbosity:
            logger.info(f"      Download Finished: {dest.name}")

    @classmethod
    def estimate_time(cls, current_page, total_pages, start_time):
        """Return a string indicating progress remaining."""
        current_time = datetime.now()
        time_elapsed = current_time - start_time
        time_per_page = time_elapsed / max(current_page, 1)
        total = time_per_page * total_pages
        remaining = (total_pages - current_page) * time_per_page

        ret = (
            f"{current_page / total_pages * 100:.2f}%({current_page}/{total_pages}) "
            f"Elapsed: {cls.timestr(time_elapsed)}, Est. Remain: {cls.timestr(remaining)}, "
            f"Est. Total: {cls.timestr(total)}"
        )
        return ret

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

    @classmethod
    def timestr(cls, delta):
        """Returns a timedelta string with ms removed."""
        return str(delta).split(".")[0]

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
