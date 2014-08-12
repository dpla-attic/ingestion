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

    def index_for_first_open_paren(_list):
        for v in _list:
            if v.count("(") > v.count(")"):
                return _list.index(v)
        return None

    def index_for_matching_close_paren(_list):
        index = None
        for v in _list:
            if index is not None and v.count("(") > v.count(")"):
                return index
            elif v.count(")") > v.count("("):
                index = _list.index(v)
        return index

    def rejoin_partials(_list, delim):
        index1 = index_for_first_open_paren(_list)
        index2 = index_for_matching_close_paren(_list)
        if index1 is not None and index2 is not None:
            if index1 == 0 and index2 == len(_list) - 1:
                return [delim.join(_list)]
            elif index1 == 0:
                _list = [delim.join(_list[:index2+1])] + _list[index2+1:]
            elif index2 == len(_list) - 1:
                _list = _list[:index1] + [delim.join(_list[index1:])]
            else:
                _list = _list[:index1] + [delim.join(_list[index1:index2+1])] + _list[index2+1:]
            return rejoin_partials(_list, delim)
        else:
            return _list

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
                    shredded.append(s)
                shredded = rejoin_partials(shredded, delim)
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
