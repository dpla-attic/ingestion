from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from dplaingestion.utilities import iterify

@simple_service('POST', 'http://purl.org/la/dp/usc_enrich_location',
                'usc_enrich_location', 'application/json')
def uscenrichlocation(body, ctype, action="usc_enrich_location",
                      prop="sourceResource/spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of
    that document by:

    1. If one of the spatial values is a lat/lon coordinate, removing all other
       values
    2. Removing 1-3 digit numbers and values that contain "s.d"

    For primary use with USC documents.
    """
    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        spatial = getprop(data, prop)

        coordinates = find_coordinates(spatial)
        if coordinates:
            spatial = [{"name": "%s, %s" % coordinates}]
        else:
            spatial = clean(spatial)
            spatial = join_values(spatial)

        setprop(data, prop, spatial)

    return json.dumps(data)

def find_coordinates(spatial): 
    """Find the first spatial value that is a lat/lon coordinate""" 
    for sp in iterify(spatial):
        coordinates = sp["name"].split(",")
        if len(coordinates) == 2:
            try:
                coordinates = map(float, coordinates)
            except:
                continue
            # USC provides coordinates as longitude, latitude
            return (coordinates[1], coordinates[0])

    return None

def clean(spatial):
    """Remove 1-3 digit numbers"""
    cleaned_spatial = []
    for sp in iterify(spatial):
        name = sp["name"].strip()
        if not (name.isdigit() and len(name) < 4) and not \
               "s.d" in name:
            cleaned_spatial.append(sp)

    return cleaned_spatial

def join_values(spatial):
    """Join all spatial values into a single value"""
    name = " ".join([sp["name"] for sp in spatial])
    spatial = [{"name": name}]

    return spatial
