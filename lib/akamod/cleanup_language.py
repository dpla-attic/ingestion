from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists
from dplaingestion.iso639_3 import ISO639_3_SUBST
from dplaingestion.iso639_3 import ISO639_3_1
from dplaingestion.iso639_3 import LANGUAGE_NAME_REGEXES
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
    b) convert from ISO 639-1 to ISO 639-3
    c) looking for matches in the value using LANGUAGE_NAME_REGEXES
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
        for s in v:
            if s not in languages and s in ISO639_3_SUBST:
                languages.append(s)
            else:
                s = re.sub("[\.\[\]]", "", s).lower().strip()
                iso = re.sub("[\(\)]", "", s)
                # First convert iso1 to iso3
                iso = iso1_to_iso3(iso)
                if iso in ISO639_3_SUBST and iso not in languages:
                    languages.append(iso)
                else:
                    for n in iso.split(" "):
                        # Since we split on whitespace, we only want to check
                        # against single word reference names so we use
                        # ISO639_3_1
                        n = n.title()
                        if n in ISO639_3_1.values() and n not in languages:
                            languages.append(n)

                    # Use s (with parentheses intact)
                    match = [r.search(s).group() for r in
                             LANGUAGE_NAME_REGEXES if r.search(s)]
                    if match:
                        languages += list(set([m.strip().title() for m in
                                          match]) - set(languages))

        if languages:
            # Remove duplicates
            lang = []
            [lang.append(l) for l in languages
             if ISO639_3_SUBST.get(l, None) not in languages]
            setprop(data, prop, filter(None, lang))
        else:
            delprop(data, prop)

    return json.dumps(data)
