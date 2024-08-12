import logging
import pickle
from datetime import datetime
from pathlib import Path

import click

from .connection import Connection

ROOT_DIR = Path(__file__).parent
# Enable support for `-h` to show help
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, file_okay=True, resolve_path=True),
    default="config.yml",
    help="The path to a config yaml file used to connect to ShotGrid.",
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
    "--download",
    type=click.Choice(["all", "missing", "no"]),
    default="missing",
    help="Controls how attachments are downloaded when downloading entities.",
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
    logging.basicConfig(level=logging.INFO, format="%(message)s")

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

    click.echo(f"Downloading mode: {download}")


@main.command(name="list")
@click.pass_context
def list_click(ctx):
    """Lists the filtered entity_types and their display names found in the schema."""
    conn = ctx.obj["conn"]
    rows = []
    width_1 = 4
    width_2 = 12
    for entity_type_name, entity_type in conn.schema_entity.items():
        display_name = entity_type["name"]["value"]
        width_1 = max(len(entity_type_name), width_1)
        if entity_type_name == display_name:
            rows.append((entity_type_name, ""))
        else:
            rows.append((entity_type_name, display_name))
            width_2 = max(len(entity_type_name), width_2)

    click.echo(f"{'Code Name':{width_1+1}} Display Name")
    click.echo(f"{'':=<{width_1}} {'':=<{width_2}}")
    for row in rows:
        click.echo(f"{row[0]:{width_1+1}}{row[1]}")


@main.command()
@click.option(
    "--schema/--no-schema",
    default=True,
    help="Save the SG schema to schema.json in output.",
)
@click.option(
    "-e",
    "--entity-type",
    "entity_types",
    multiple=True,
    help="Archive these entity types. Can be used multiple times. If 'all' is "
    "passed all non-ignored entity types are archived. If 'missing' is passed "
    "then it will archive all non-ignored entity types but will skip any that "
    "already have their '_page_index.json' created in output.",
)
@click.option(
    "-x",
    "--exclude-entity-type",
    "excluded_entity_types",
    multiple=True,
    help="Skip these entitity_types when passing `all` or `missing` to "
    "--entity-type. Can be used multiple times.",
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
    "-f",
    "--format",
    "formats",
    default=["pickle-high"],
    multiple=True,
    type=click.Choice(
        ["pickle-high", "pickle-default"]
        + [f"pickle-{i}" for i in range(pickle.HIGHEST_PROTOCOL, -1, -1)]
        + ["json"]
    ),
    help="Formats to save the output in. Can be used more than once. Pickle is "
    "a little faster to load, but json is easier to read in a text editor. The "
    "various pickle items control what pickle protocol is used to save.",
)
@click.option(
    "--smart-attachments/--no-smart-attachments",
    default=False,
    help="Download only attachment records referenced by other entity_types "
    "that have been backed up. An `_all_ids.pickle` file is stored in the "
    "Attachments folder when any other entities are downloaded. This option "
    "moves Attachments to the end of the entity-types and makes it only select "
    "the ids stored in that file. This can be run as its own cli call and the ids"
    "generated by other processes are included when it starts processing.",
)
@click.option(
    "--clean/--no-clean",
    default=False,
    help="Clean the output before processing any other commands.",
)
@click.pass_context
def archive(
    ctx,
    schema,
    entity_types,
    excluded_entity_types,
    limit,
    max_pages,
    formats,
    smart_attachments,
    clean,
):
    conn = ctx.obj["conn"]

    if clean:
        conn.clean()

    if schema:
        conn.save_schema()

    exclude = False
    if "all" in entity_types:
        exclude = True
        entity_types = conn.schema_entity.keys()
    elif "missing" in entity_types:
        exclude = True
        entity_types = [
            entity_type
            for entity_type in conn.schema_entity
            if not (conn.output / "data" / entity_type / "_page_index.json").exists()
        ]
    if exclude:
        entity_types = [et for et in entity_types if et not in excluded_entity_types]

    if smart_attachments:
        click.echo("Post Processing Attachments records.")
        entity_types = list(entity_types)
        # Ensure the Attachment table is last
        while "Attachment" in entity_types:
            entity_types.remove("Attachment")
        entity_types.append("Attachment")

    click.echo("")
    click.echo("Processing Entity Types:")
    start = datetime.now()
    for entity_type in entity_types:
        query = conn.config.get("filters", {}).get(entity_type, [])
        if smart_attachments and entity_type == "Attachment":
            ids = conn.process_all_recorded_attachments()
            if not ids:
                raise ValueError("No pre-recorded Attachment Id's found.")
            query = [["id", "in", list(ids)]]
        conn.download_entity_type(entity_type, query, limit, max_pages, formats=formats)
    end = datetime.now()

    click.echo("")
    click.echo(f"Finished archiving entity types in {conn.timestr(end - start)}.")
    if conn.downloads["all"]:
        click.echo(f"Downloaded {conn.downloads['all']} files.")
    if conn.downloads["skipped"]:
        click.echo(f"Skipped download of {len(conn.downloads['skipped'])} files.")
        path = conn.output / "skipped_downloads.json"
        conn.save_json(conn.downloads["skipped"], path)
    if conn.downloads["failed"]:
        click.echo(f"Failed Downloads: {len(conn.downloads['failed'])}")
        for _, dest, error in conn.downloads["failed"][:25]:
            click.echo(f"  {dest}: ({error})")
        if len(conn.downloads["failed"]) > 50:
            click.echo("  ...")


if __name__ == "__main__":
    main()
