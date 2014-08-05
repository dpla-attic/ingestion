from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists
from dplaingestion.iso639_3 import ISO639_3_SUBST
from dplaingestion.iso639_3 import EXACT_LANGUAGE_NAME_REGEXES
from dplaingestion.iso639_3 import WB_LANGUAGE_NAME_REGEXES
from dplaingestion.iso639_3 import ISO639_3_1
from dplaingestion.iso639_1 import ISO639_1
import re

@simple_service("POST", "http://purl.org/la/dp/enrich_language",
                "enrich_language", "application/json")
def enrich_language(body, ctype, action="enrich_language",
                      prop="sourceResource/language"):
    """
    Service that accepts a JSON document and sets the language ISO 639-3
    code(s) and language name from the current language value(s) by:

    a) Checking if the value is a language code, else
    a) Attempting to convert value the value from ISO 639-1 to ISO639-3, else
    c) Attempting to find an exact language name match, else
    d) Attempting to find language name matches withing the value
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
        language_strings = [v] if not isinstance(v, list) else v

        iso_codes = []
        for lang_string in language_strings:
            # Check if raw value is a code
            if lang_string not in iso_codes and lang_string in ISO639_3_SUBST:
                iso_codes.append(lang_string)
            else:
                # If lang_string is an ISO 639-1 code, convert to ISO 639-3
                iso3 = iso1_to_iso3(
                        re.sub("[\.\[\]\(\)]", "", lang_string).lower().strip()
                        )
                if iso3 not in iso_codes and iso3 in ISO639_3_SUBST:
                    iso_codes.append(iso3)
                else:
                    # First check for exact language name matches
                    for iso_code, regex in EXACT_LANGUAGE_NAME_REGEXES.items():
                        match = regex.match(lang_string.strip())
                        if match:
                            iso_codes.append(iso_code)
                            break

                    if match is None:
                        # Check for language names with word boundary regex
                        for iso_code, regex in WB_LANGUAGE_NAME_REGEXES.items():
                            if regex.search(lang_string):
                                iso_codes.append(iso_code)

        if iso_codes:
            seen = set()
            language = [{"iso639_3": code, "name": ISO639_3_SUBST[code]} for
                        code in iso_codes if not
                        (code in seen or seen.add(code))]
            setprop(data, prop, language)
        else:
            logger.warning("Did not find language code in [%s] for record %s" %
                           (language_strings, data["_id"]))
            delprop(data, prop)

    return json.dumps(data)
