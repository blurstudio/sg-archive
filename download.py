import click
import io
import json
import math
import os
import shutil
import six
import sys
import time
import yaml

from datetime import datetime
from pathlib2 import Path
from pprint import pformat
from shotgun_api3.shotgun import Shotgun
from shotgun_api3.lib import mockgun

ROOT_DIR = Path(__file__).parent


class DateTimeDecoder(json.JSONDecoder):
    """https://gist.github.com/abhinav-upadhyay/5300137"""

    def __init__(self, *args, **kargs):
        if "object_hook" not in kargs:
            kargs["object_hook"] = self.dict_to_object
        super(DateTimeDecoder, self).__init__(*args, **kargs)

    def dict_to_object(self, d):
        if '__type__' not in d:
            return d

        _type = d.pop('__type__')
        try:
            dateobj = datetime(**d)
            return dateobj
        except Exception:
            d['__type__'] = _type
            return d


class DateTimeEncoder(json.JSONEncoder):
    """Instead of letting the default encoder convert datetime to string,
    convert datetime objects into a dict, which can be decoded by the
    DateTimeDecoder
    https://gist.github.com/abhinav-upadhyay/5300137
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                '__type__': 'datetime',
                'year': obj.year,
                'month': obj.month,
                'day': obj.day,
                'hour': obj.hour,
                'minute': obj.minute,
                'second': obj.second,
                'microsecond': obj.microsecond,
            }
        else:
            return json.JSONEncoder.default(self, obj)


class Connection(object):
    def __init__(self, config, output, download=True, filtered=True, strict=False):
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

    def clean(self):
        click.echo('Clean: Removing "{}" and its contents'.format(self.output))
        shutil.rmtree(str(self.output))

    def download_table(self, table, query, limit, max_pages):
        click.echo("Processing: {}".format(table))
        data_dir = self.output / "data" / table
        data_dir.mkdir(exist_ok=True, parents=True)

        # Save the schema for just this table so its easier to inspect
        table_schema = data_dir / "_schema.json"
        self.save_json(self.schema[table], table_schema)
        urls = [
            k
            for k, v in self.schema[table].items()
            if v["data_type"]["value"] in ("url", "image")
        ]

        count = self.sg.summarize(
            table,
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

        click.echo(
            "  Total records: {}, limit: {}, total pages: {}".format(
                count, limit, page_count
            )
        )
        total = 0
        file_map = {}
        for page in range(1, page_count + 1):
            filename = data_dir / "{}_{}.json".format(table, page)

            s = time.time()
            out = self.find(table, query, limit=limit, page=page)
            e = time.time()
            total += len(out)
            if not out:
                click.echo("  Stopping, no remaining records")
                break
            msg = "  Selected {} {} in {:.5} seconds."
            click.echo(msg.format(len(out), table, e - s))

            if self.download:
                for entity in out:
                    for column in urls:
                        self.download_url(entity, column, data_dir)

            # Update the file_map data for this select
            out = self.make_index(out)
            file_map.update({sgid: filename.name for sgid in out})

            # Save this page data to disk
            self.save_json(out, filename)

            # Check that the data we saved matches the input data
            if self.strict:
                with filename.open() as fle:
                    check = json.load(fle, cls=DateTimeDecoder)
                if out != check:
                    msg = [pformat(out), '-' * 50, pformat(check)]
                    assert out == check, '\n'.join(msg)

        click.echo("  Total: {}, count: {}".format(total, count))

        # Save a map of which paged file contains the data for a given sgid.
        self.save_json(file_map, data_dir / "_page_index.json")

    def download_url(self, entity, column, dest):
        url_info = entity[column]
        if url_info is None:
            return None, None
        if isinstance(url_info, six.string_types):
            url = url_info
            parse = six.moves.urllib.parse.urlparse(url)
            name = os.path.basename(parse.path)
            # Convert the str into a dict so we can store the downloaded path
            url_info = {"url": url, "name": name, "__download_type": "image"}
            entity[column] = url_info
        else:
            url = url_info["url"]
            name = url_info["name"]
            url_info["__download_type"] = "url"
        click.echo("    Downloading: {}".format(name))
        dest_fn = dest / "files" / column / "{}-{}".format(entity["id"], name)
        dest_fn.parent.mkdir(exist_ok=True, parents=True)
        # Safety check for duplicate local file paths
        if dest_fn.exists():
            raise RuntimeError(
                "Destination already exists: {}".format(dest_fn),
            )
        fn, headers = six.moves.urllib.request.urlretrieve(url, str(dest_fn))

        # Store the relative file path to the file we just downloaded in the
        # data we will save into the json data
        url_info["local_path"] = str(Path(fn).relative_to(dest))

        return Path(fn), headers

    def find(self, table, query, **kwargs):
        if "fields" not in kwargs:
            kwargs["fields"] = list(self.schema[table].keys())

        return self.sg.find(table, query, **kwargs)

    def make_index(self, data, key="id"):
        """Convert a list of records into a dict of the key value.

        The key is converted to a str so it can be stored in json without modification.
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

    @property
    def sg(self):
        try:
            return self._sg
        except AttributeError:
            self._sg = Shotgun(**self.connection)
            click.echo("Connected: to {}".format(self.connection["base_url"]))
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
        for table in schema:
            out_table = {}
            columns = ignored.get("columns", {}).get(table, [])
            for column, value in schema[table].items():
                if value["data_type"]["value"] in ignored.get("data_types"):
                    continue
                if column in columns:
                    continue
                out_table[column] = value
            if out_table:
                ret[table] = out_table
        return ret

    def save_json(self, data, output):
        if sys.version_info.major == 2:
            # Deal with unicode in python 2
            with io.open(str(output), 'w', encoding="utf-8") as outfile:
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
                my_json_str = '\n'.join(lines)
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

    def save_schema(self, output):
        click.echo("Saving schema to {}".format(output))
        output.parent.mkdir(exist_ok=True, parents=True)
        self.save_json(self.schema_full, output)

        # Save the schema for mockgun to restore later
        mockgun.generate_schema(
            self.sg,
            str(output.parent / 'schema.pickle'),
            str(output.parent / 'schema_entity.pickle'),
        )


@click.group()
@click.option(
    '-c',
    '--config',
    type=click.Path(exists=True, file_okay=True, resolve_path=True),
    default="config.yml",
    help='The path to a config yaml file used to connect to ShotGrid.',
)
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=True, resolve_path=True),
    help="The directory to store all output in.",
)
@click.option(
    "--strict/--no-strict",
    default=False,
    help="Double check that the json saved to disk can be restored back to "
    "the original value.",
)
@click.option(
    "--download/--no-download",
    default=True,
    help="Download attachments when downloading entities.",
)
@click.pass_context
def main(ctx, config, output, strict, download):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    config = Path(config)

    if output is None:
        output = ROOT_DIR / "output"
    output = ROOT_DIR.joinpath(output)

    conn = Connection(config, output, strict=strict, download=download)
    ctx.obj["conn"] = conn
    # Check the connection to SG
    conn.sg


@main.command()
@click.option(
    '--schema/--no-schema',
    default=True,
    help="Save the SG schema to schema.json in output.",
)
@click.option(
    "-t",
    "--table",
    "tables",
    multiple=True,
    help="Limit the output to these tables. Can be used multiple times.",
)
@click.option(
    "--limit",
    type=int,
    default=50,
    help="Limit to a maximum of this many results.",
)
@click.option(
    "--max-pages",
    type=int,
    help="Stop downloading records after this many pages even if there are "
    "still results left to download. Page size is controlled by `--limit`.",
)
@click.option(
    "--clean/--no-clean",
    default=False,
    help="Clean the output before processing any other commands.",
)
@click.pass_context
def save(ctx, schema, tables, limit, max_pages, clean):
    conn = ctx.obj["conn"]

    if clean:
        conn.clean()

    if schema:
        conn.save_schema(conn.output / "schema.json")

    for table in tables:
        query = conn.config.get("filters", {}).get(table, [])
        conn.download_table(table, query, limit, max_pages)


if __name__ == '__main__':
    main()
