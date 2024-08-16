# main.py with the following content:

import logging
import os

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
for entity_type in html_cfg.get("load_entity_type", []):
    logger.info(f"Loading Entity_type: {entity_type}")
    sg.load_entity_type(entity_type)

# Initialize FastAPI app
app = FastAPI()

# Initialize Jinja2Templates with your html template
templates = Jinja2Templates(directory="templates")
# templates.env.add_extension(MarkdownExtension)


def field_name(entity_type, field):
    return (
        sg.schema_read()
        .get(entity_type, {})
        .get(field, {})
        .get("name", {})
        .get("value", field)
    )


def field_data_type(entity_type, field):
    return (
        sg.schema_read()
        .get(entity_type, {})
        .get(field, {})
        .get("data_type", {})
        .get("value", field)
    )


def list_fields(entity_type):
    fields = html_cfg.get("list_fields", {}).get(entity_type)
    if fields is None:
        return sg.field_names_for_entity_type(entity_type)
    return fields


def sg_find(entity_type, keys, fields=None):
    # Call out to sg api against your local files
    if fields is None:
        fields = sg.field_names_for_entity_type(entity_type)
    return (
        sg.find(
            entity_type,
            [["id", "in", keys]],
            sg.field_names_for_entity_type(entity_type),
        ),
        fields,
    )


def sg_find_one(entity_type, key, fields=None):
    # Call out to sg api against your local files
    if fields is None:
        fields = sg.field_names_for_entity_type(entity_type)
    return sg.find_one(entity_type, [["id", "in", int(key)]], fields), fields


def fmt_dict(value):
    if isinstance(value, dict) and "name" in value:
        return value["name"]
    return value


def entity_href(entity):
    name = entity["name"] if "name" in entity else f"{entity['type']}({entity['id']})"
    return "<a href=/details/{}/{}>{}</a>".format(entity["type"], entity["id"], name)


def fmt_sg_value(entity, field):
    if field not in entity:
        return "No Value"
    value = entity[field]
    data_type = field_data_type(entity["type"], field)
    if value is None:
        return "No Value"
    if data_type == "text":
        # Format text using markdown and convert newlines to br
        return markdown.markdown(value, extensions=["nl2br"])
    if data_type == "multi_entity":
        ret = [entity_href(v) for v in value]
        return ", ".join(ret)
    if isinstance(value, dict) and "type" in value and "id" in value:
        name = value["name"] if "name" in value else f"{value['type']}({value['id']})"
        return "<a href=/details/{}/{}>{}</a>".format(value["type"], value["id"], name)
    if isinstance(value, dict) and "name" in value:
        return value["name"]
    if isinstance(value, list):
        return ", ".join([fmt_sg_value(v, "name") for v in value])
    return value


app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/data", StaticFiles(directory=sg.data_root / "data"), name="data")


# Define the FastAPI route with a URL parameter
@app.get("/process/{url_param}", response_class=HTMLResponse)
async def process(request: Request, url_param: str):
    html_blob, fields = sg_find("Version", url_param)
    return templates.TemplateResponse(
        "response.html", {"request": request, "response": html_blob}
    )


# Define the FastAPI route with a URL parameter
@app.get("/details/{entity_type}/{key}", response_class=HTMLResponse)
async def details_entity(request: Request, entity_type: str, key: int):
    entity, fields = sg_find_one(entity_type, key)
    if entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    remove = []
    remove.extend(html_cfg.get("fields", {}).get(entity_type, []))
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
    for field in remove:
        if field in fields:
            fields.remove(field)
    fields = sorted(fields, key=lambda i: field_name(entity_type, i))
    return templates.TemplateResponse(
        template,
        {
            "request": request,
            "entity": entity,
            "fields": fields,
            "field_name": field_name,
            "fmt_sg_value": fmt_sg_value,
            "name": name,
        },
    )


@app.get("/list_view/{entity_type}", response_class=HTMLResponse)
async def schema(request: Request, entity_type: str):
    fields = sg.field_names_for_entity_type(entity_type)
    entities = sg.find(entity_type, [], fields)
    show_fields = list_fields(entity_type)
    return templates.TemplateResponse(
        "list_view.html",
        {
            "request": request,
            "entities": entities,
            "fields": show_fields,
            # "link_field": link_field,
            "entity_type": entity_type,
            "field_name": field_name,
            "fmt_sg_value": fmt_sg_value,
            "entity_href": entity_href,
        },
    )


@app.get("/entities", response_class=HTMLResponse)
async def entities(request: Request):
    return templates.TemplateResponse(
        "entities.html",
        {
            "request": request,
            "schema_entity": con.schema_entity,
            "field_name": field_name,
            "fmt_sg_value": fmt_sg_value,
            "entity_href": entity_href,
        },
    )


@app.get("/raw_entity/{entity_type}/{key}")
async def details_raw(request: Request, entity_type: str, key: int):
    entity, fields = sg_find_one(entity_type, key)
    return {"entity": entity, "fields": fields}


@app.get("/schema_entity/{entity_type}")
async def schema_entity(request: Request, entity_type: str):
    return sg.schema_read().get(entity_type, {})
