from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

@simple_service('POST', 'http://purl.org/la/dp/scdl_enrich_location', 'scdl_enrich_location', 'application/json')
def scdl_enrich_location(body, ctype, action="scdl_enrich_location", prop="aggregatedCHO/spatial"):
    """
    Service that accepts a JSON document and enriches the "spatial" field of that document.

    For use with the scdl profiles
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        value = getprop(data,prop)
        for v in iterify(value): 
            name = v["name"].rstrip()

            # Try to extract a County
            if " county " in name.lower(): 
                # "XXX County (S.C.)" => county: XXX
                v["county"] = name[0:name.lower().index(" county")]
            elif "(S.C.)" in name:
                # "XXX (S.C)" => city: XXX
                v["city"] = name[0:name.index(" (S.C.)")]

    return json.dumps(data)


def iterify(iterable): 
    ''' 
    Treat iterating over a single item or an interator seamlessly.
    '''
    if isinstance(iterable, basestring):
        iterable = [iterable]
    try:
        iter(iterable)
    except TypeError:
        iterable = [iterable]
    return iterable
