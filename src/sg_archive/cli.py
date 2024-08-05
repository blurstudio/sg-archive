import click
import logging
from pathlib import Path

from .connection import Connection

ROOT_DIR = Path(__file__).parent
# Enable support for `-h` to show help
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
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
def archive(ctx, schema, tables, limit, max_pages, clean):
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

    click.echo("")
    click.echo("Processing Tables:")
    for table in tables:
        query = conn.config.get("filters", {}).get(table, [])
        conn.download_table(table, query, limit, max_pages)

    click.echo("")
    click.echo("Finished archiving tables.")


if __name__ == '__main__':
    main()
