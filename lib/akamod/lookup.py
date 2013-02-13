from akara import logger
from akara import module_config
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists


def convert_last(data, path, name, conv):
    """ Converts the elements in data object.

    Args:
        data (obj):

        path (Str): Name of the key for the data object to read value from.

        name (Str): Name of the key to save the value to.

        conv (Dict): Dictionary used for conversion.

    Returns:
        Nothing, conversion is done in place.

    Raises:
        Nothing.

    If the data is a list, then it has to be a list of dictionaries.
    Then all the dictionaries in the list are converted.

    If the value is not a list, it has to be a dictionary.

    Then for each dictionary, there is made a conversion:
        valuedict[name] = conv[ valuedict[path] ]

    If the dictionary doesn't contain the element named path, then
    nothing is done.

    This function doesn't delete anything. It can replace the value
    or add another key with converted value.

    """
    if not path in data:
        return

    value = data[path]
    if isinstance(value, list):
        out = []
        for el in value:
            if el in conv:
                out.append(conv[el])
            else:
                out.append(el)
        data[name] = out
    else:
        data[name] = conv[value]


def convert(data, path, name, conv, PATH_DELIM="/"):
    """ Converts data using converters.

    Args:
        data (obj) : Structure changed in place.

        path (Str) : Path to the key to read from and convert value.

        name (Str) : Name of the key for writing the converted value.
                     This key is stored in the same dictionary as value read
                     from using the path argument.

        conv (dict): Dictionary used for conversion

    Returns:
        Nothing, the data argument is changed in place.

    Raises:
        Nothing

    This function is called recursively. Inspired by the setprop from selector
    module.

    Each call the path is stripped by one element.

    If the data argumetn is a dictionary, then data[path_part] is passed to
    the next recursive call.

    If the data is a list, then the function is called for each element.

    """
    # So we should convert now
    if not PATH_DELIM in path:
        # There is a list to convert.
        # If there is a list of dictionaries, each dictionary has to be
        # converted.
        if isinstance(data, list):
            for el in data:
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


def find_conversion_dictionary(mapping_key):
    """Finds the dictionary with values to use for conversion.

    Args:
        mapping_key (Str): Name of conversion key read from Akara.conf

    Returns:
        Dictionary used for converting values.
    """
    # Mapping should be in akara.conf
    mapping = module_config().get('lookup_mapping')
    logger.debug("Looking for mapping using key [%s]" % mapping_key)
    dict_name = mapping[mapping_key].upper()
    logger.debug("Found substitution dict [%s] for key mapping [%s]"
            % (dict_name, mapping_key,) )
    return globals()[dict_name]


def the_same_beginning(prop, target):
    """Checks if the fields are paths with the same beginning.

    Args:
        prop   (Str): Path for comparison.
        target (Str): Path for comparison.

    Returns:
        True if the paths have the same beginning.
        False otherwise.

    The paths consist of fields divided with "/". The files are considered
    to be similar if all, except for the last, fields are the same.

    Fields which are the same are considered as similar.

    Example of similar fields:
        "a/b/c" - "a/b/x"
        "a/b"   - "a/b"
        "a"     - "a"

    Example of not similar fields:
        "a/b/c" - "a/b/c/d"
        "a/b/c" - "a/x/x"
        "a"     - "a/b"

    """
    return ("/" not in prop and "/" not in target ) \
            or (prop.split("/")[:-1] == target.split("/")[:-1])


@simple_service('POST', 'http://purl.org/la/dp/lookup', 'lookup', 'application/json')
def lookup(body, ctype, prop, target, substitution):
    """ Performs simple lookup.

    This module makes a simple conversion of the values from the `prop` path,
    using dictionary described with `substitution` argument, and stores it in
    the `target` path. The description of using prop and target path is below.

    Args:
        body (Str): JSON with structure to change.
        prop (Str): Path for reading the value.
        target (Str): Path for storing the value.
        substitution (Str): Name of the dicionary read from akara.conf.

    The paths must be divided using "/".

    The prop must point to an existing dictionary, however there can be lists
    in the hierarchy, see the examples below.

    The target must point the same dictionary as prop path, however the last
    part of the target path can be missing. In this case it will be created.
    If it exists, then the value can be replaced.

    The `prop` and `target` paths must be similar and can differ only with the
    last parts.

    Example of similar fields:
        "a/b/c" - "a/b/x"
        "a/b"   - "a/b"
        "a"     - "a"

    Example of not similar fields:
        "a/b/c" - "a/b/c/d"
        "a/b/c" - "a/x/x"
        "a"     - "a/b"

    The substitution argument is used for getting the name of the variable
    from lookup_mapping dictionary akara.conf. Then this dictionary is used
    for conversion.

    If the value for conversion is not a key in the conversion dictionary, then
    no conversion is made.

    Searching dictionary.

    The `prop` path is used for getting the dictionary to read value for conversion.
    The lookup modules supports lists as well, if there is a list in the dictionaries
    hierarchy, then all the dictionaries/values are converted.

    Examples:
        Assume that the dictionary used for conversion is:
            {"a": "A",
             "b": "B",
             "c": "C"}

      #1
            json   = {"a": "b"}
            prop   = "a"
            target = "a"
            value for conversion = "b"
            converted json: {"a": "b"}

      #2
            json   = {"a": "b"}
            prop   = "a"
            target = "x"
            value for conversion = "b"
            converted json: {"a": "b", "x": "B"}

      #3
            JSON   = {"a": "x"}
            prop   = "a"
            target = "a"
            value for conversion = None
            converted JSON: {"a": "x"}

      #4
            JSON   = {"a": ["b", "c", "d"]}
            prop   = "a"
            target = "a"
            values for conversion = "b", "c", "d"
            converted JSON = {"a": ["B", "C", "d"]}

      #5
            JSON   = {"a": ["b", "c", "d"]}
            prop   = "a"
            target = "x"
            values for conversion = "b", "c", "d"
            converted JSON = {"a": ["b", "c", "d"],
                              "x": ["B", "C", "d"]}

      #6
            JSON   = {"a": {"b": "c"}}
            prop   = "a/b"
            target = "a/b"
            values for conversion = "c"
            converted JSON = {"a": {"b": "C"}}

      #7
            JSON   = {"a": {"b": "c"}}
            prop   = "a/b"
            target = "a/c"
            values for conversion = "c"
            converted JSON = {"a": {"b": "c", "c": "C"}}

      #8
            JSON   = {"a": [
                              {"b": ["a", "b", "x"]},
                              {"x": "y"},
                              {"b": "a"},
                              {"b": ["x", "a"]}
                           ]}
            prop   = "a/b"
            target = "a/b"
            values for conversion = ...
            converted JSON = {"a": [
                                      {"b": ["A", "B", "x"]},
                                      {"x": "y"},
                                      {"b": "A"},
                                      {"b": ["x", "A"]}
                                   ]}

      #9
            JSON   = {"a": [
                              {"b": ["a", "b", "x"]},
                              {"x": "y"},
                              {"b": "a"},
                              {"b": ["x", "a"]}
                           ]}
            prop   = "a/b"
            target = "a/x"
            values for conversion = ...
            converted JSON = {"a": [
                                     {"b": ["a", "b", "x"],
                                      "x": ["A", "B", "x"]},
                                     {"x": "y"},
                                     {"b": "a",
                                      "x": "A"},
                                     {"b": ["x", "a"],
                                      "x": ["x", "A"]}
                                   ]}
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
