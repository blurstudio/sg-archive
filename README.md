# Sg-Archive

A tool that lets you download SG entities and attachments to disk and provides
a shotgun_api3 interface to interact with the archived data.

It also has a rudimentary web interface for read only viewing the archived data.

# Archive Use

This package installs a `sg-archive` pip executable. Alternatively it also provides
the module interface so you can use `python -m sg_archive` instead.

## Configuration

`sg-archive` is configured using 2 yaml files. The example folder contains
a template for these files.

The `config_example_secret.yaml` file contains the connection details used to connect
to shotgrid. It is its own file to make it harder to accidentally share secret api keys.

`config_example.yml` is the main configuration file it is passed to the `sg-archive`
cli with `--config` option. This file is used to manage the data being archived
and points to the secret file. Here is a list of handled keys:

- `connection_file` points to the secret file to authenticate with ShotGrid.
- `ignored:data_types`: For all entity types, don't include any fields storing these data types in the archived data.
- `ignored:entities`: When listing available entities omit these.
- `ignored:fields`: Don't include these specific field's when archiving a entity. A dictionary where the key is the entity type. The value is a list of field names to exclude.
- `filters`: A dictionary of filters to apply when archiving a given entity type. A dictionary where the key is the entity type. The value is a filter passed to the `sg.find` call for that entity type.

To configure the cli you need to pass the `--config` path pointing to your copy of
`config_example.yml`.

```bash
sg-archive -c path/to/config.yaml
```

The `--output` option specifies the top level folder where the archive is save/loaded.

## Archiving

```bash
sg-archive -c path/to/config.yaml -o path/to/output/folder archive -e Version --limit 100 --max-pages 2
```

In this example only the first 200 `Version` entity records will be downloaded
including their attachments.

`-e Version` specifies the entity type Version. You can reuse `-e` multiple times
to download multiple entity types at once.

`--limit 100` limits the size of each sg request to 100 entities. It will do as
many pages are required to select all records matching the entity filter.
`--max-pages 2` stops after 2 pages are processed.

Given the paged nature of the archive process you want to generate an archive when
no other users are modifying the database or  you may miss some results.

# Accessing the archive

Once you have archived the records you can access the data using `sg_archive.shotgun.Shotgun`
python class. This is an instance of the `shotgun_api3.lib.mockgun.Shotgun` class
configured to load the archived data and downloaded attachments are converted to
working `file://` links.

```py
from sg_archive.shotgun import Shotgun

# Create a sg object pointing to the archive root directory
sg = Shotgun('E:/project_archive')
# Load the archive into memory so we can select the records.
sg.load_entity_types()
```

When creating the Shotgun object you don't provide connection details, you just
need to pass the path to the archive root folder.

You need to call `sg.load_entity_types()` to load the entire archive into memory.
If you only want to load a specific entity type you can use
`sg.load_entity_type("Version")` instead. After that you can use the sg object
like normal.

```py
import webbrowser
from pprint import pprint

# Select a record using the shotgun_api3 interface.
ver = sg.find_one("Version", [], ["image", "sg_uploaded_movie"])
pprint(ver)
# Simple "image" field type stored as a file path
webbrowser.open(ver["image"])
# "url" field type stored as a dict of attachment information.
webbrowser.open(ver["sg_uploaded_movie"]["url"])
```

# Web server

A quick and dirty web server has been setup to allow viewing of the archived sg data. It is not fast or memory efficient, but provides basic list and details pages for the archived data including showing the thumbnail for entity_types that support it and Version shows the `uploaded_movie_mp4` file.

The site is called "We have SG at home" because it is not very powerful, memory efficient, or user friendly. The mockgun api doesn't support limit or page offsets so all entities are shown in the list views. See the home page for the simple filter api features. It's there so non-scripters can access the data somewhat easily.

## Installation

Use pip to install the optional `server` requirements.

```bash
pip install path/to/sg-archive[server]
cd path/to/sg-archive/html
```

Set the configuration environment variables.
- `SG_ARCHIVE_CFG`: Points to your `config_example.yml` file.
- `SG_ARCHIVE_DATA`: Points to the output folder you archived your data to. This is the folder that has the `schema.pickle` and `schema_entity.pickle` files.

Use `uvicorn.exe main:app` or `fastapi dev main.py` to start the server.

The `html` section of the `config_example.yml` file shows how to remove fields from the details and list views. For large entities like Version, Note, Task, etc you likely will want to reduce the fields shown in the list view significantly to prevent the tab from running out of memory while loading the data.
