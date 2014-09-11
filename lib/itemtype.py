
"""Functions for determining item types"""


class NoTypeError(Exception):
    pass


def _type_for_keyword(s, mappings):
    """Given a string, return our mapped type, or None if we can't look it up.
    
    mapping: a tuple of (string, type)
    """
    for pair in mappings:
        # example:  ('book', 'text')
        if pair[0] in s:
            return pair[1]
    return None

def type_for_strings_and_mappings(string_map_combos):
    """_type_for_strings_and_mappings([(list, list_of_tuples), ...])
    
    Given pairs of (list of strings, list of mapping tuples), try to find a
    suitable item type.
    """
    for strings, mappings in string_map_combos:
        # Try each of our strings to see if a keyword falls within it
        for s in strings:
            t = _type_for_keyword(s, mappings)
            if t:
                return t
    raise NoTypeError

def rejects(string_map_combos):
    """rejects([(list, list_of_tuples), ...])

    Given pairs of strings and mapping tuples, as with
    type_for_strings_and_mappings, return a list of strings that do _not_
    map to valid types.
    """
    return [s for strings, mappings in string_map_combos
            for s in strings
            if not s == _type_for_keyword(s, mappings)]
