import re

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists


@simple_service('POST', 'http://purl.org/la/dp/shred', 'shred',
                'application/json')
def shred(body, ctype, action="shred", prop=None, delim=';', keepdup=None):
    """
    Service that accepts a JSON document and "shreds" or "unshreds" the value
    of the field(s) named by the "prop" parameter

    "prop" can include multiple property names, delimited by a comma (the delim
    property is used only for the fields to be shredded/unshredded). This
    requires that the fields share a common delimiter however.
    """

    try:
        data = json.loads(body)
    except Exception as e:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON\n" + str(e)

    if action == "shred":
        par_re = re.compile("([^"+delim+"]*\(.*?\)[^"+delim+"]*)")
    else:
        par_re = None
    for p in prop.split(','):
        if exists(data, p):
            v = getprop(data, p)
            if action == "shred":
                if isinstance(v, list):
                    try:
                        v = delim.join(v)
                    except Exception as e:
                        logger.error("Can't join on delim. ID: %s\n%s" % (data["_id"], str(e)))
                if delim in v:
                    setprop(data, p, v)
                else:
                    continue

                shredded = []
                par_expressions = re.split(par_re, v)
                for exp in par_expressions:
                    if "(" not in exp and ")" not in exp:
                        shredded += exp.split(delim)
                    else:
                        shredded.append(exp)
                shredded = [i.strip() for i in shredded if i.strip()]
                if not keepdup:
                    result = []
                    for s in shredded:
                        if s not in result:
                            result.append(s)
                    shredded = result
                setprop(data, p, shredded)
            elif action == "unshred":
                if isinstance(v, list):
                    setprop(data, p, delim.join(v))

    return json.dumps(data)
