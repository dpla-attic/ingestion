from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from dplaingestion.utilities import iterify
import re

@simple_service("POST", "http://purl.org/la/dp/texas_enrich_location",
                "texas_enrich_location", "application/json")
def texas_enrich_location(body, ctype, action="texas_enrich_location",
                          prop="sourceResource/spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of
    that document.

    For use with the texas profile
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header("content-type", "text/plain")
        return "Unable to parse body as JSON"


    def _get_coordinates(value):
        lat, lon = None, None
        for v in value.split(";"):
            if "north=" in v:
                lat = v.split("=")[-1]
            elif "east=" in v:
                lon = v.split("=")[-1]

        if lat and lon:
            return (lat, lon)
        else:
            return ()

    if exists(data, prop):
        spatial = []
        values = getprop(data,prop)

        for v in values:
            sp = {"name": v}
            shredded = [s.strip() for s in v.split(" - ")]

            coordinates = _get_coordinates(sp["name"]) 
            if coordinates:
                sp["name"] = "%s, %s" % coordinates

            if len(shredded) < 5:
                if not re.search("\d", sp["name"]):
                    sp["country"] = shredded[0]
                if "country" in sp:
                    if sp["country"] in ["United States", "Canada"]:
                        try:
                            sp["state"] = shredded[1]
                            sp["county"] = shredded[2]
                            sp["city"] = shredded[3]
                        except Exception, e:
                            logger.debug("Error enriching location %s: %s" %
                                         (data["_id"], e))
            spatial.append(sp)
        logger.debug("SPATIAL: %s" % spatial)
        setprop(data, prop, spatial)

    return json.dumps(data)
