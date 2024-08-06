import logging
import pickle
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
    help="Archive these entitity types. Can be used multiple times. If 'all' is "
    "passed all non-ignored entity types are archived. If 'missing' is passed "
    "then it will archive all non-ignored entity types but will skip any that "
    "already have their entity_type folder created in output.",
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
    "--clean/--no-clean",
    default=False,
    help="Clean the output before processing any other commands.",
)
@click.pass_context
def archive(ctx, schema, entity_types, limit, max_pages, formats, clean):
    conn = ctx.obj["conn"]

    if clean:
        conn.clean()

    if schema:
        conn.save_schema()

    if "all" in entity_types:
        entity_types = conn.schema_entity.keys()
    elif "missing" in entity_types:
        entity_types = [
            entity_type
            for entity_type in conn.schema_entity
            if not (conn.output / "data" / entity_type).exists()
        ]

    click.echo("")
    click.echo("Processing Entity Types:")
    for entity_type in entity_types:
        query = conn.config.get("filters", {}).get(entity_type, [])
        conn.download_entity_type(entity_type, query, limit, max_pages, formats=formats)

    click.echo("")
    click.echo("Finished archiving entity types.")


if __name__ == "__main__":
    main()
