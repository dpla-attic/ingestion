from akara import logger
from akara import module_config
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists

#TODO add documentation for this module


def convert(data, path, name, conv, PATH_DELIM="/"):
    """
    TODO: write here
        data:
        path:
        name: name of the last field
        conv: dictionary used for conversion
    """

    logger.debug("DATA")
    logger.debug(data)

    def convert_last(data, path, name, conv):
        logger.debug("PATH: " + path)
        if not path in data:
            return

        value = data[path]
        if isinstance(value, list):
            out = []
            logger.debug("Value is a list")
            logger.debug(conv)
            for el in value:
                if el in conv:
                    logger.debug("Converting " + el)
                    logger.debug("TO: " + conv[el])
                    out.append(conv[el])
                else:
                    out.append(el)
            data[name] = out
        else:
            data[name] = conv[value]
            logger.debug("Converting data[name] = " + data[name])

    # So we should convert now
    if not PATH_DELIM in path:
        # There is a list to convert.
        # If there is a list of dictionaries, each dictionary has to be
        # converted.
        if isinstance(data, list):
            for el in data:
                logger.debug("CONVERTING ")
                logger.debug(el)
                convert_last(el, path, name, conv)
        else:
            convert_last(data, path, name, conv)

        return

    # If there is deeper path, let's check it
    pp, pn = tuple(path.lstrip(PATH_DELIM).split(PATH_DELIM,1))

    # For list: iterate over elements
    if isinstance(data, list):
        for el in data:
            convert(el, path, name, conv)
    elif isinstance(data, dict):
        if pp not in data:
            # Then just do nothing
            logger.error("Couldn't find {0} in data.".format(pp))
        else:
            convert(data[pp], pn, name, conv)
    else:
        logger.error("Got data of unknown type")

    return



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


def the_same_beginning(prop, target):
    return ("/" not in prop and "/" not in target ) \
            or (prop.split("/")[:-1] == target.split("/")[:-1])


@simple_service('POST', 'http://purl.org/la/dp/lookup', 'lookup', 'application/json')
def lookup(body, ctype, prop, target, substitution):
    """
    Performs simple lookup.
    """

    logger.debug("BODY  : [%s]" % body)
    logger.debug("INPUT : [%s]" % prop)
    logger.debug("OUTPUT: [%s]" % target)

    LOG_JSON_ON_ERROR = True

    # Check if the prop and target fields has got the same beginning.
    if not the_same_beginning(prop, target):
        msg = "The `prop`=[{0}] and `get`=[{1}] fields do not point to the same dictionary."
        msg = msg.format(prop, target)
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

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


    convert(data, prop, target.split("/")[-1:][0], convdict)
    return json.dumps(data)

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
