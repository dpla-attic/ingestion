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

    def single_parens(s):
        return (s.count("(") == 0 or s.count(")") == 0)

    def mismatch_parens(s):
        return s.count("(") != s.count(")")

    for p in prop.split(','):
        if exists(data, p):
            v = getprop(data, p)
            if action == "shred":
                if isinstance(v, list):
                    try:
                        v = delim.join(v)
                        v = v.replace("%s%s" % (delim, delim), delim)
                    except Exception as e:
                        logger.warn("Can't join list %s on delim for %s, %s" %
                                    (v, data["_id"], e))
                if delim in v:
                    setprop(data, p, v)
                else:
                    continue

                shredded = [""]
                for s in re.split(re.escape(delim), v):
                    if not single_parens(v) and mismatch_parens(shredded[-1]):
                        shredded[-1] += "%s%s" % (delim, s)
                    else:
                        shredded.append(s)
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
