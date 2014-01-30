from amara.thirdparty import json
from akara.services import simple_service
from akara import response
from akara import logger
from dplaingestion.selector import getprop, delprop
from dplaingestion.schema import schema

@simple_service("POST", "http://purl.org/la/dp/compare_with_schema",
                "compare_with_schema", "application/json")
def comparewithschema(body, ctype):
    """
    Service that accepts a JSON document and removes any fields not listed
    as part of the schema.
    """

    # TODO: Send GET request to API once schema endpoint is created

    try :
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header("content-type", "text/plain")
        return "Unable to parse body as JSON"

    if "_id" not in data or ("sourceResource" not in data and
                             data.get("ingestType") == "item"):
        return body

    type = data.get("ingestType")
    if type:
        props = ["collection/properties"] if type == "collection" else \
                ["item/properties",
                 "item/properties/sourceResource/properties"]
        for prop in props:
            schema_keys = getprop(schema, prop).keys()

            if "sourceResource" in prop:
                data_keys = data["sourceResource"].keys()
                field_prefix = "sourceResource/"
            else:
                data_keys = data.keys()
                data_keys.remove("_id")
                field_prefix = ""

            # Remove any keys in the document that are not found in the schema
            for field in [k for k in data_keys if k not in schema_keys]:
                field = field_prefix + field
                logger.error("Field %s for %s not found in schema; deleting" %
                             (field, data.get("_id")))
                delprop(data, field)
    else:
        logger.error("Unknown type %s for %s" % (type, data.get("_id")))

    return json.dumps(data)
