from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
import re

@simple_service('POST', 'http://purl.org/la/dp/enrich_language', 'enrich_language', 'application/json')
def enrich_language(body, ctype, action="enrich_language", prop="aggregatedCHO/language"):
    '''
    Service that accepts a JSON document and enriches the "language" field of that document
    by:

    a) converting a list of language values into list of dictionaries: {"name": language}

    By default it works on the 'language' field, but can be overridden by passing the name of the field to use
    as a parameter
    '''

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    if exists(data, prop):
        langs = getprop(data, prop)

        if isinstance(langs, basestring):
            setprop(data, prop, {"name": langs})
        elif isinstance(langs, list):
            languages = []
            for l in langs:
                languages.append({"name": l})
            setprop(data, prop, languages)

    return json.dumps(data)
