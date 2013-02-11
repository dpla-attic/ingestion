from akara import logger
from akara import module_config
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists


def find_conversion_dictionary(mapping_key):
    """
    Finds the dictionary with values to use for conversion.
    """
    # Mapping should be in akara.conf
    mapping = module_config().get('lookup_mapping')
    logger.debug("Looking for mapping using key [%s]" % mapping_key)
    dict_name = mapping[mapping_key].upper()
    logger.debug("Found substitution dict [%s] for key mapping [%s]" % (dict_name, mapping_key,) )
    return globals()[dict_name]


def convert_data(value, dict_name):
    """
    Converts given data using provided dict name.
    """
    d = globals()[dict_name]
    result = None

    if isinstance(value, basestring):
        logger.debug("Changing value of ['{0}':'{1}'] to {2}".
                format(prop, value, d[value]))
        result = d[value]

    if isinstance(value, list):
        msg = "Changing each value of array ['{0}':'{1}'] to array of values."
        logger.debug(msg.format(prop, value))
        result = []
        for v in value:
            if v in d:
                logger.debug("Changing value of '{0}' to {1}".format(v, d[v]))
                result.append(d[v])
            else:
                logger.debug("Not changing value of '{0}', didn't find in {1}".
                        format(v, substitution))
                result.append(v)

    return result


@simple_service('POST', 'http://purl.org/la/dp/lookup', 'lookup', 'application/json')
def lookup(body, ctype, prop, target, substitution):
    """
    Performs simple lookup.
    """

    logger.debug("BODY  : [%s]" % body)
    logger.debug("INPUT : [%s]" % prop)
    logger.debug("OUTPUT: [%s]" % target)

    LOG_JSON_ON_ERROR = True

    def log_json():
        if LOG_JSON_ON_ERROR:
            logger.debug(body)

    # Parse incoming JSON
    data = {}
    try:
        data = json.loads(body)
    except Exception as e:
        msg = "Bad JSON: " + e.args[0]
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    # Check target variable.
    if not target:
        msg = "There is not provided output field name."
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    prop_value = None
    # Get prop value.
    try:
        prop_value = getprop(data, prop)
    except KeyError as e:
        logger.error("Didn't find the key [%s] in JSON" % prop)
        return body

    # Dictionary to use for conversion.
    convdict = None
    try:
        convdict = find_conversion_dictionary(substitution)
    except KeyError as e:
        msg = "Missing substitution dictionary [%s]" % substitution
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg


    def substitute(data, value, target):
        asd


    if isinstance(prop_value, basestring):
        #subst_string(prop, value, convdict)
        msg = "Changing value of ['{0}':'{1}'] to {2}"
        logger.debug(msg.format(prop, prop_value, convdict[prop_value]))
        setprop(data, target, convdict[prop_value])

    if isinstance(prop_value, list):
        msg = "Changing each value of array ['{0}':'{1}'] to array of values."
        logger.debug(msg.format(prop, prop_value))
        outlist = []
        for v in prop_value:
            if v in convdict:
                logger.debug("Changing value of '{0}' to {1}".format(v, convdict[v]))
                outlist.append(convdict[v])
            else:
                logger.debug("Not changing value of '{0}', didn't find in {1}".
                        format(v, substitution))
                outlist.append(v)

        setprop(data, target, outlist)

    return json.dumps(data)


TEST_2_SUBST = {
    "aaa": "AAA222",
    "bbb": "BBB222",
    "ccc": "CCC222"
}


TEST_SUBST = {
    "aaa": "AAA",
    "bbb": "BBB",
    "ccc": "CCC",
    "ddd": "DDD",
}
