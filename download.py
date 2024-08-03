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
import concurrent.futures
import urllib.request

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
    def __init__(
        self, config, output, download=True, filtered=True, strict=False, verbosity=0
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

    def clean(self):
        click.echo('Clean: Removing "{}" and its contents'.format(self.output))
        shutil.rmtree(str(self.output))

    def download_table(self, table, query, limit, max_pages):
        display_name = self.schema_entity[table]['name']['value']
        click.echo("Processing: {} ({})".format(table, display_name))
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
        total_download = 0
        file_map = {}

        total_start = time.time()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for page in range(1, page_count + 1):
                filename = data_dir / "{}_{}.json".format(table, page)

                sel_start = time.time()
                out = self.find(table, query, limit=limit, page=page)
                sel_end = time.time()
                total += len(out)
                if not out:
                    break
                msg = "  Selected {} {} in {:.5} seconds."
                click.echo(msg.format(len(out), table, sel_end - sel_start))

                if self.download:
                    for entity in out:
                        for column in urls:
                            dl = self.download_url(entity, column, data_dir, executor)
                            if dl is not None:
                                total_download += 1

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

            total_end = time.time()
            click.echo(
                "    Finished selecting pages in {:.5} seconds. Waiting for "
                "any remaining downloads.".format(total_end - total_start)
            )
        total_end = time.time()

        click.echo(
            "  {} Records saved: {}, Total in SG: {}, Files "
            "downloaded: {} in {:.5} seconds".format(
                table, total, count, total_download, total_end - total_start
            )
        )

        # Save a map of which paged file contains the data for a given sgid.
        self.save_json(file_map, data_dir / "_page_index.json")

    def download_url(self, entity, column, dest, executor):
        def worker(url, dest_name):
            six.moves.urllib.request.urlretrieve(url, str(dest_name))
            if self.verbosity:
                click.echo(f'    Download Finished: {dest_name.name}')

        url_info = entity[column]
        if url_info is None:
            return None
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
        # Sanitize the name so we can use it in a file path
        if name is None:
            name = str(entity["id"])
        else:
            name = name.replace("\\", "_").replace("/", "_")
            name = "{}-{}".format(entity["id"], name)
        dest_fn = dest / "files" / column / name
        if self.verbosity:
            click.echo("    Downloading: {}".format(dest_fn.name))
        dest_fn.parent.mkdir(exist_ok=True, parents=True)
        # Safety check for duplicate local file paths
        if dest_fn.exists():
            raise RuntimeError(
                "Destination already exists: {}".format(dest_fn),
            )
        # fn, headers = six.moves.urllib.request.urlretrieve(url, str(dest_fn))
        executor.submit(worker, url, dest_fn)

        # Store the relative file path to the file we just downloaded in the
        # data we will save into the json data
        url_info["local_path"] = str(dest_fn.relative_to(dest))

        return dest_fn

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
        ignored = self.config.get("ignored", {}).get("tables", [])
        schema_entity = {
            k: v for k, v in schema_entity.items() if v["visible"]["value"]
        }
        return {k: v for k, v in schema_entity.items() if k not in ignored}

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

    def save_schema(self):
        fn_schema = self.output / "schema.json"
        fn_schema_entity = self.output / "schema_entity.json"

        click.echo("Saving schema")
        self.output.mkdir(exist_ok=True, parents=True)
        self.save_json(self.schema_full, fn_schema)
        self.save_json(self.schema_entity_full, fn_schema_entity)

        # Save the schema for mockgun to restore later
        mockgun.generate_schema(
            self.sg,
            str(self.output / 'schema.pickle'),
            str(self.output / 'schema_entity.pickle'),
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
@click.option(
    "-v",
    "--verbose",
    "verbosity",
    count=True,
    help="Increase the verbosity of the output.",
)
@click.pass_context
def main(ctx, config, output, strict, download, verbosity):
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below)
    ctx.ensure_object(dict)

    config = Path(config)

    if output is None:
        output = ROOT_DIR / "output"
    output = ROOT_DIR.joinpath(output)

    conn = Connection(
        config, output, strict=strict, download=download, verbosity=verbosity
    )
    ctx.obj["conn"] = conn
    # Check the connection to SG
    conn.sg


@main.command(name="list")
@click.pass_context
def list_click(ctx):
    """Lists the filtred tables and their display names found in the schema."""
    conn = ctx.obj["conn"]
    rows = []
    width_1 = 4
    width_2 = 12
    for table_name, table in conn.schema_entity.items():
        display_name = table['name']['value']
        width_1 = max(len(table_name), width_1)
        if table_name == display_name:
            rows.append((table_name, ""))
        else:
            rows.append((table_name, display_name))
            width_2 = max(len(table_name), width_2)

    click.echo(f"{'Code Name':{width_1+1}} Display Name")
    click.echo(f"{'':=<{width_1}} {'':=<{width_2}}")
    for row in rows:
        click.echo(f"{row[0]:{width_1+1}}{row[1]}")


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
        conn.save_schema()

    if "all" in tables:
        tables = conn.schema_entity.keys()
    elif "missing" in tables:
        tables = [
            table
            for table in conn.schema_entity
            if not (conn.output / "data" / table).exists()
        ]

    for table in tables:
        query = conn.config.get("filters", {}).get(table, [])
        conn.download_table(table, query, limit, max_pages)


if __name__ == '__main__':
    main()
