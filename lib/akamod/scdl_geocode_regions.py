from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists
from dplaingestion.utilities import iterify

@simple_service('POST', 'http://purl.org/la/dp/scdl_geocode_regions', 'scdl_geocode_regions', 'application/json')
def scdl_geocode_regions(body, ctype, action="scdl_geocode_regions", prop="sourceResource/spatial"):
    """
    Service that accepts a JSON document and forcibly sets the coordinates for South Carolina regions.

    For use with the scdl profiles
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        value = getprop(data, prop)
        for v in iterify(value): 
            if (is_region(v)):
                geocode_region(v)

    return json.dumps(data)

REGIONS = { 
    "Upstate": (34.848270416259766, -82.40010833740234), 
    "Midlands": (33.99882125854492, -81.04537200927734),
    "Lowcountry": (32.781150817871094, -79.93160247802734),
    "Low Country": (32.781150817871094, -79.93160247802734),
    "Pee Dee": (34.19363021850586, -79.76905822753906)
}

def is_region(spatial):
    return (getprop(spatial, "name") in REGIONS)

def geocode_region(spatial):
    setprop(spatial, "coordinates", "%s, %s" % REGIONS[getprop(spatial, "name")])
    delprop(spatial, "county")
    setprop(spatial, "state", "South Carolina")
    setprop(spatial, "country", "United States")
    return spatial
