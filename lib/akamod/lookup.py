from akara import logger
from akara import module_config
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.iso639_3 import ISO639_3_SUBST


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
        if value in conv:
            data[name] = conv[value]


def convert(data, path, name, conv, path_delim="/"):
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
    if not path_delim in path:
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
    pp, pn = tuple(path.lstrip(path_delim).split(path_delim, 1))

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
    logger.debug("Found substitution dict [%s] for key mapping [%s]" % (dict_name, mapping_key,))
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
    return ("/" not in prop and "/" not in target) \
        or (prop.split("/")[:-1] == target.split("/")[:-1])


@simple_service('POST', 'http://purl.org/la/dp/lookup', 'lookup', 'application/json')
def lookup(body, ctype, prop, target, substitution, inverse=None):
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
        """Logs whole body JSON."""
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
        if inverse:
            convdict = dict((v, k) for k, v in convdict.items())
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

DC_DATA_PROVIDER = {
"Beverly High School Digital Collection": "Beverly High School",

"Brookline Photograph Collection": "Brookline Public Library",
"Amherst Collection": "C/W MARS",
"Athol Collection": "C/W MARS",
"Baystate Medical Center Collection": "C/W MARS",
"Blackstone Collection": "C/W MARS",
"Buckland Collection": "C/W MARS",
"C/W MARS State Library of Massachusetts Collection": "C/W MARS",
"Chicopee Collection": "C/W MARS",
"Clark University Collection": "C/W MARS",
"East Longmeadow Collection": "C/W MARS",
"Elms College Collection": "C/W MARS",
"Erving Collection": "C/W MARS",
"Greenfield Collection": "C/W MARS",
"Holyoke Collection": "C/W MARS",
"Hopedale Collection": "C/W MARS",
"Hopkinton Collection": "C/W MARS",
"Hudson Collection": "C/W MARS",
"Lancaster Collection": "C/W MARS",
"Ludlow Collection": "C/W MARS",
"Mt. Wachusett Community College Collection": "C/W MARS",
"National Guard Museum Collection": "C/W MARS",
"Nichols College Collection": "C/W MARS",
"North Brookfield Collection": "C/W MARS",
"Northampton Collection": "C/W MARS",
"Orange Collection": "C/W MARS",
"Oxford Collection": "C/W MARS",
"Palmer Collection": "C/W MARS",
"Princeton Collection": "C/W MARS",
"Shrewsbury Collection": "C/W MARS",
"South Hadley Collection": "C/W MARS",
"Southbridge Collection": "C/W MARS",
"Spencer Collection": "C/W MARS",
"Springfield College Collection": "C/W MARS",
"Sturbridge Collection": "C/W MARS",
"Uxbridge Collection": "C/W MARS",
"Webster Collection": "C/W MARS",
"West Boylston Collection": "C/W MARS",
"Westborough Collection": "C/W MARS",
"Westfield Collection": "C/W MARS",
"Wilbraham Collection": "C/W MARS",
"Williamsburg Collection": "C/W MARS",
"Worcester Collection": "C/W MARS",
"WPI Collection": "C/W MARS",

"Essex Aggie Digital Collection": "Essex Agricultural and Technical High School",

"Henry Whittemore Library": "Framingham State University",


"John W. Ragle Archives Room": "Governor’s Academy",

"Oral History Projects": "Jewish Women's Archive",
"Lawrence Public Libary": "Jewish Women's Archive",
"Lawrence Public Library Digital Collection": "Jewish Women's Archive",
"Lincoln Public Library": "Jewish Women's Archive",
"Farrar Book": "Jewish Women's Archive",

"City of Lowell Images Collection": "Lowell National Historical Park",


"Mass. Historical Finding Aids": "Massachusetts Historical Society",
"Mass. Historical Online Collections": "Massachusetts Historical Society",

"1878 Map of Newton Center": "Newton Free Public Library",
"Allen House Oversized Photos": "Newton Free Public Library",
"Assessed Polls": "Newton Free Public Library",
"Auburndale Tradecards": "Newton Free Public Library",
"Biographical Pamphlet (Fields)": "Newton Free Public Library",
"Biographical Pamphlet (Livermore)": "Newton Free Public Library",
"Blue Books of Newton": "Newton Free Public Library",
"Class Statistics NHS Class of 1890": "Newton Free Public Library",
"Diary and Account Book, 1859-1862": "Newton Free Public Library",
"Diary of Edgar W. Warren": "Newton Free Public Library",
"Eliot Church Annual 1845-1887": "Newton Free Public Library",
"For the Dinner of the First City Government": "Newton Free Public Library",
"Glass Plate Negatives": "Newton Free Public Library",
"History of the Newton Fire Department": "Newton Free Public Library",
"Hunnewell Club": "Newton Free Public Library",
"Jersey Stock Club": "Newton Free Public Library",
"Journal of Edward Jackson": "Newton Free Public Library",
"Lantern Slides": "Newton Free Public Library",
"Metal Plate Negative": "Newton Free Public Library",
"Newell Family Album": "Newton Free Public Library",
"Newell Family Misc": "Newton Free Public Library",
"Newton Engineering Dept. Photos": "Newton Free Public Library",
"Newton Forestry Dept. Photos": "Newton Free Public Library",
"Newton High School Class of 1885 Photographs": "Newton Free Public Library",
"Newton High School, class of 1890 photographs": "Newton Free Public Library",
"Newton High School, class of 1895 photographs": "Newton Free Public Library",
"Newton High School, class of 1900 photographs": "Newton Free Public Library",
"Newton Illustrated": "Newton Free Public Library",
"Newton Oversized: Misc": "Newton Free Public Library",
"Newton Street Directory": "Newton Free Public Library",
"Newton Tradecards": "Newton Free Public Library",
"Newton Village Shops": "Newton Free Public Library",
"Pictures of Newton Corner/Nonantum Square": "Newton Free Public Library",
"Plan of Proctor Map": "Newton Free Public Library",
"Samuel Smith Manuscript": "Newton Free Public Library",
"Seth Davis Notebook": "Newton Free Public Library",
"Some Newtonville Homes": "Newton Free Public Library",
"Sparrows, Finches, Etc. of New England": "Newton Free Public Library",
"Waban (Newton, Mass.)": "Newton Free Public Library",
"Waban Historical Col. Glass Plates": "Newton Free Public Library",
"Waban Historical Col. Lantern Slides": "Newton Free Public Library",

"Alumni Project: Andover History in Photographs and Stories": "Noble",
"Beverly Picture Collection": "Noble",
"Beverly Postcard Collection": "Noble",
"Boston Mat Leather Company, Peabody, Mass.": "Noble",
"Current Images of Wakefield": "Noble",
"Danvers Mass. Memories Road Show": "Noble",
"Gloucester Oral History Collection": "Noble",
"Gloucester Postcard Collection": "Noble",
"Helen Cutter Slides": "Noble",
"History of the Academy": "Noble",
"Humphrey Street, Swampscott, Mass.": "Noble",
"List of Vessels Belonging to the District of Gloucester": "Noble",
"Lucius Beebe Memorial Library Artwork Collection": "Noble",
"Lynn Historic Images : Getting around in Lynn": "Noble",
"Lynn Historic Images : Buildings": "Noble",
"Lynn Historic Images : Churches": "Noble",
"Lynn Historic Images : Lynn's Great Outdoors": "Noble",
"Lynn Historic Images : Memorials and Cemeteries": "Noble",
"Lynn Historic Images : Parades and Events": "Noble",
"Lynn Historic Images : People": "Noble",
"Lynn Historic Images : Streets": "Noble",
"Lynn Historic Images : World War I": "Noble",
"Murphy Postcard Collection": "Noble",
"Old Photographs of Wakefield": "Noble",
"Peabody Glass Plate Photographs": "Noble",
"Peabody Oak Hill Estate": "Noble",
"Phillips Academy Athletics Images": "Noble",
"Reading Historical Images": "Noble",
"Saugus Glass Slides": "Noble",
"Stoneham Historic Images": "Noble",
"Swampscott Buildings": "Noble",
"Swampscott Schools": "Noble",
"Swampscott Through the Years: A pictorial history": "Noble",
"Swampscott Town Departments": "Noble",
"Swampscott's Neighboring Towns": "Noble",
"Wakefield Municipal Gas": "Noble",
"Wakefield Postcards": "Noble",
"Wakefield Then": "Noble",
"Walkable Reading": "Noble",

"Ambrose F. Keeley Library (Durfee High School, Fall River) Local History Collection": "SAILS Digital History Collection",
"Ambrose F. Keeley Library (Durfee High School, Fall River) Yearbooks": "SAILS Digital History Collection",
"Carver Public Library, Carver Cemetery Records": "SAILS Digital History Collection",
"East Bridgewater High School Yearbooks": "SAILS Digital History Collection",
"East Bridgewater Public Library Historical Photographs": "SAILS Digital History Collection",
"Fiske Public Library (Wrentham) Helen Keller Collection": "SAILS Digital History Collection",
"Hanson Public Library , Hanson History": "SAILS Digital History Collection",
"Holmes Public Library (Halifax) Halifax Historical Society": "SAILS Digital History Collection",
"Holmes Public Library (Halifax) Halifax History": "SAILS Digital History Collection",
"Holmes Public Library (Halifax) Silver Lake Yearbooks": "SAILS Digital History Collection",
"Joseph Plumb Memorial Library (Rochester) Architectural Survey": "SAILS Digital History Collection",
"Middleborough Public Library Cranberry Collection": "SAILS Digital History Collection",
"Plainville Public Library": "SAILS Digital History Collection",
"West Bridgewater Public Library Historical Postcards": "SAILS Digital History Collection",


"College Archives Digital Collections": "Springfield College",
"State Library of Massachusetts Collection": "Springfield College",
"Attorney General Annual Reports": "Springfield College",
"Bird’s Eye Maps": "Springfield College",
"Broadsides": "Springfield College",
"Canal Maps": "Springfield College",
"Executive Orders": "Springfield College",
"Highway Department Annual Reports": "Springfield College",
"Highway Department Maps": "Springfield College",
"Highway Department Photographs": "Springfield College",
"Hoosac Tunnel Photographs": "Springfield College",
"Land and Harbor Commissioner Atlases": "Springfield College",
"Land and Harbor Commissioner Maps": "Springfield College",
"Legislative Maps": "Springfield College",
"Other Historical Maps": "Springfield College",
"Railroad Commission Annual Reports": "Springfield College",
"Railroad Commission Manuscripts": "Springfield College",
"Railroad Commission Maps": "Springfield College",

"State of the Commonwealth Addresses": "State Library Publications",

"Postcard Collection": "Stephen Phillips Trust House",

"Digitized Materials from the Library's Rare Book Collection":"Sterling and Francine Clark Art Institute Library",
"Mary Ann Beinecke Decorative Art Collection":"Sterling and Francine Clark Art Institute Library",
"Sterling and Francine Clark Art Institute Library Exhibition Files":"Sterling and Francine Clark Art Institute Library",

"Lowell Cultural Resources Inventory":"University of Massachusetts Lowell Libraries",
"Lowell Massachusetts: Building Surveys Overview Reports":"University of Massachusetts Lowell Libraries",
"Lowell Neighborhoods: Historical and Architectural Survey":"University of Massachusetts Lowell Libraries",
"Stereoview Collection":"University of Massachusetts Lowell Libraries",

"Convers Francis, 1795–1863":"Watertown Free Public Library",
"Watertown Arsenal":"Watertown Free Public Library",
"Watertown Businesses":"Watertown Free Public Library",
"Watertown Cemeteries":"Watertown Free Public Library",
"Watertown Charles River":"Watertown Free Public Library",
"Watertown Churches":"Watertown Free Public Library",
"Watertown Graves":"Watertown Free Public Library",
"Watertown Historic Buildings Gallery":"Watertown Free Public Library",
"Watertown Houses":"Watertown Free Public Library",
"Watertown Library Branch":"Watertown Free Public Library",
"Watertown Main Library":"Watertown Free Public Library",
"Watertown Miscellaneous":"Watertown Free Public Library",
"Watertown Monuments and Historical Markers":"Watertown Free Public Library",
"Watertown People":"Watertown Free Public Library",
"Watertown Public Celebrations":"Watertown Free Public Library",
"Watertown Railroads":"Watertown Free Public Library",
"Watertown Schools":"Watertown Free Public Library",
"Watertown Square":"Watertown Free Public Library",
"Watertown Street Views":"Watertown Free Public Library",
"Watertown Town Hall":"Watertown Free Public Library",

"WGBH OpenVault":"WGBH",
"Home Contact FAQs Items Member Resources Collection Tree":"WGBH",
"Proudly powered by Omeka. © Digital Commonwealth 2013":"WGBH"
}
