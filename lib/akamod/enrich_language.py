from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, delprop, exists
from dplaingestion.iso639_3 import ISO639_3_SUBST
from dplaingestion.iso639_3 import ISO639_3_SUBST_REGEXES
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
        language_strings = [v] if not isinstance(v, list) else v

        iso_codes = []
        for lang_string in language_strings:
            # Check if raw value is a code
            if lang_string not in iso_codes and lang_string in ISO639_3_SUBST:
                iso_codes.append(lang_string)
            else:
                # Remove periods and square brackets from lang_string for all
                # further operations
                lang_string = re.sub("[\.\[\]]", "", lang_string).lower() \
                                                                 .strip()

                # If lang_string is an ISO 639-1 code, convert to ISO 639-3
                iso3 = iso1_to_iso3(re.sub("[\(\)]", "", lang_string))
                if iso3 not in iso_codes and iso3 in ISO639_3_SUBST:
                    iso_codes.append(iso3)
                else:
                    # Split the lang_string on whitespace then check against
                    # single word reference names from ISO639_3_1 values
                    for sub_string in lang_string.split(" "):
                        for iso_code, language_name in ISO639_3_1.items():
                            if (iso_code not in iso_codes and
                                sub_string.decode("utf-8").lower() ==
                                language_name.decode("utf-8").lower()):
                                iso_codes.append(iso_code)
                                break

                    # Finally, use lang_string with regexes
                    for iso_code, regex in ISO639_3_SUBST_REGEXES.items():
                        match = regex.search(lang_string)
                        if match:
                            iso_codes.append(iso_code)
                            break

        if iso_codes:
            iso_codes = list(set(iso_codes))
            language = [{"iso639_3": iso_code,
                         "name": ISO639_3_SUBST[iso_code]} for k in iso_codes]
            setprop(data, prop, language)
        else:
            delprop(data, prop)

    return json.dumps(data)
