# main.py with the following content:

import logging
import os
from pathlib import Path

import markdown
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sg_archive.connection import Connection
from sg_archive.shotgun import Shotgun

logger = logging.getLogger(__name__)

# These env vars configure the server to load this config from this data directory
ARCHIVE_CFG = os.environ["SG_ARCHIVE_CFG"]
ARCHIVE_DATA = os.environ["SG_ARCHIVE_DATA"]

sg = Shotgun(ARCHIVE_DATA)
con = Connection(ARCHIVE_CFG, output=sg.data_root)
html_cfg = con.config.get("html", {})
# Get the filtered sg schemas used in this site
filtered_schema = con.filter_schema(sg.schema_read())
filtered_schema_entity = con.filter_schema_entity(sg.schema_entity_read())
loaded_entity_types = set()

# Initialize FastAPI app
app = FastAPI()

# Initialize Jinja2Templates with your html template
templates = Jinja2Templates(directory="templates")


def load_entity_type(entity_type, force=False):
    """Lazily load a given entity_type on first access"""
    if not force and entity_type in loaded_entity_types:
        return False
    logger.warning(f"Loading Entity_type: {entity_type}")
    sg.load_entity_type(entity_type)
    loaded_entity_types.add(entity_type)


def sg_find(entity_type, keys=None, query=None, fields=None):
    # Call out to sg api against your local files
    if fields is None:
        fields = sg.field_names_for_entity_type(entity_type)
    if keys:
        query = [["id", "in", keys]]
    if query is None:
        query = []

    load_entity_type(entity_type)
    return (
        sg.find(
            entity_type,
            query,
            sg.field_names_for_entity_type(entity_type),
        ),
        fields,
    )


def sg_find_one(entity_type, key, fields=None):
    # Call out to sg api against your local files
    if fields is None:
        fields = sg.field_names_for_entity_type(entity_type)

    load_entity_type(entity_type)
    return sg.find_one(entity_type, [["id", "in", int(key)]], fields), fields


class Helper:
    def __init__(self, request: Request):
        self.request = request

    def entity_href(self, entity):
        name = (
            entity["name"] if "name" in entity else f"{entity['type']}({entity['id']})"
        )
        return "<a href=/details/{}/{}>{}</a>".format(
            entity["type"], entity["id"], name
        )

    def field_data_type(self, entity_type, field):
        return (
            filtered_schema.get(entity_type, {})
            .get(field, {})
            .get("data_type", {})
            .get("value", field)
        )

    def field_name(self, entity_type, field):
        return (
            filtered_schema.get(entity_type, {})
            .get(field, {})
            .get("name", {})
            .get("value", field)
        )

    def fmt_sg_value(self, entity, field):
        if isinstance(entity, int):
            return "No Value"
        if field not in entity:
            return "No Value"
        value = entity[field]
        data_type = self.field_data_type(entity["type"], field)
        if value is None:
            return "No Value"
        if data_type == "text":
            # Format text using markdown and convert newlines to br
            return markdown.markdown(value, extensions=["nl2br"])
        if data_type == "multi_entity":
            ret = [self.entity_href(v) for v in value]
            return ", ".join(ret)
        if isinstance(value, dict) and "type" in value and "id" in value:
            name = (
                value["name"] if "name" in value else f"{value['type']}({value['id']})"
            )
            return "<a href=/details/{}/{}>{}</a>".format(
                value["type"], value["id"], name
            )
        if isinstance(value, dict) and "name" in value:
            return value["name"]
        if isinstance(value, list):
            return ", ".join([self.fmt_sg_value(v, "name") for v in value])
        return value

    def link_params(self):
        if self.request.query_params:
            return f"?{self.request.query_params}"
        return ""

    def details_fields(self, entity_type, remove=None):
        """Returns a list of fields to show for an entity_type in a details view."""
        fields = sg.field_names_for_entity_type(entity_type)

        if remove is None:
            remove = []

        # Exclude fields defined on in yaml for details view
        exclude = html_cfg.get("exclude_details", {})
        remove.extend(exclude.get("global", []))
        remove.extend(exclude.get(entity_type, []))

        fields = self.remove_fields(fields, remove)
        return fields

    def list_fields(self, entity_type):
        fields = html_cfg.get("list_fields", {}).get(entity_type)
        if fields is None:
            return sg.field_names_for_entity_type(entity_type)
        return fields

    def remove_fields(self, fields, remove):
        for field in remove:
            if field in fields:
                fields.remove(field)
        return fields

    def sg_request_query(self, entity_type: str):
        """Build a SG query for the given request and entity_type.
        Any params specified will be added to the query as `[key, "is", value]`
        if key is a field for the given entity_type.
        """
        query = []
        params = dict(self.request.query_params.items())
        fields = sg.field_names_for_entity_type(entity_type)
        for key, value in params.items():
            if key not in fields:
                # If the entity doesn't have this field, ignore it in selection
                continue
            if key == "project":
                value = int(value)
                query.append(["project", "is", {"type": "Project", "id": value}])
                load_entity_type("Project")
            else:
                query.append([key, "is", value])
        return query


app.mount(
    "/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static"
)
app.mount("/data", StaticFiles(directory=sg.data_root / "data"), name="data")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "schema_entity": filtered_schema_entity,
            "helper": Helper(request),
        },
    )


# Define the FastAPI route with a URL parameter
@app.get("/details/{entity_type}/{key}", response_class=HTMLResponse)
async def details_entity(request: Request, entity_type: str, key: int):
    entity, _ = sg_find_one(entity_type, key)
    if entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    helper = Helper(request)

    remove = []
    name = entity.get("code")
    template = "entity.html"
    if entity_type == "Note":
        remove.extend(
            [
                "attachments",
                "content",
                "filmstrip_image",
                "image",
                "replies",
                "reply_content",
                "subject",
            ]
        )
        name = entity["subject"] or ""
    elif entity_type == "Project":
        name = entity["name"]
    elif entity_type == "HumanUser":
        name = " ".join((entity["firstname"], entity["lastname"]))

    fields = helper.details_fields(entity_type, remove)

    fields = sorted(fields, key=lambda i: helper.field_name(entity_type, i))
    return templates.TemplateResponse(
        template,
        {
            "request": request,
            "entity": entity,
            "fields": fields,
            "helper": helper,
            "name": name,
        },
    )


@app.get("/list_entities/{entity_type}", response_class=HTMLResponse)
async def list_entities(request: Request, entity_type: str):
    helper = Helper(request)
    query = helper.sg_request_query(entity_type)
    entities, fields = sg_find(entity_type, query=query)
    show_fields = helper.list_fields(entity_type)
    return templates.TemplateResponse(
        "list_entities.html",
        {
            "request": request,
            "entities": entities,
            "fields": show_fields,
            "entity_type": entity_type,
            "helper": helper,
        },
    )


@app.get("/entities", response_class=HTMLResponse)
async def entities(request: Request):
    return templates.TemplateResponse(
        "entities.html",
        {
            "request": request,
            "schema_entity": filtered_schema_entity,
            "helper": Helper(request),
        },
    )


@app.get("/raw_entity/{entity_type}/{key}")
async def details_raw(request: Request, entity_type: str, key: int):
    entity, fields = sg_find_one(entity_type, key)
    return {"entity": entity, "fields": fields}


@app.get("/schema_entity/{entity_type}")
async def schema_entity(request: Request, entity_type: str):
    return filtered_schema.get(entity_type, {})
