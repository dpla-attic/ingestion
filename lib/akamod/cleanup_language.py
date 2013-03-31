from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists
from dplaingestion.iso639_3 import ISO639_3_SUBST
from dplaingestion.iso639_1 import ISO639_1
import re

@simple_service("POST", "http://purl.org/la/dp/cleanup_language",
                "cleanup_language", "application/json")
def cleanup_language(body, ctype, action="cleanup_language",
                     prop="sourceResource/language"):
    """
    Service that accepts a JSON document and cleans each value of the language
    field of that document by:

    a) stripping periods, brackets and parentheses
    b) looking for matches in the value using ISO639_3_SUBST values
    c) if not matches are found, attempts to convert from ISO 639-1 to
       ISO 639-3
    """

    def iso1_to_iso3(s):
        s = re.sub("[-_/].*$", "", s).strip()
        return ISO639_1.get(s, s)

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header("content-type", "text/plain")
        return "Unable to parse body as JSON"

    if exists(data, prop):
        v = getprop(data, prop)
        v = [v] if not isinstance(v, list) else v

        languages = []
        r = r"^{0}$|^{0}\s|\s{0}$|\s{0}\s"
        #TODO: regex compilations occur with each record, not good
        name_regexes = [re.compile(r.format(val.lower())) for
                        val in ISO639_3_SUBST.values()]

        for s in v:
            s = re.sub("[\.\[\]\(\)]", "", s).lower().strip()
            # First convert iso1 to iso3
            s = iso1_to_iso3(s)
            if s in ISO639_3_SUBST and s not in languages:
                languages.append(s)
            else:
                # Find name_regexes matches
                match = [r.search(s).group() for r in name_regexes if
                         r.search(s)]
                if match:
                    languages += list(set([m.strip().title() for m in match]) -
                                      set(languages))

        if languages:
            # Remove duplicates
            lang = []
            [lang.append(l) for l in languages if ISO639_3_SUBST.get(l, None) not in languages]
            setprop(data, prop, filter(None, lang))

    return json.dumps(data)
