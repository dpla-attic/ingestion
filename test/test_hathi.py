import sys
from dplaingestion.akamod.marc_to_dpla import _get_values, _get_subject_values

_DICT = {
    "subfield": [
        {"code": "a", "#text": "A"},
        {"code": "b", "#text": "B"},
        {"code": "c", "#text": "C"},
        {"code": "d", "#text": "D"}
    ]
}
_SUBJECT = {
    "subfield": [
        {"code": "a", "#text": "A"},
        {"code": "b", "#text": "B"},
        {"code": "c", "#text": " C"},
        {"code": "d", "#text": "D"},
        {"code": "e", "#text": "E"},
        {"code": "v", "#text": "V"},
        {"code": "x", "#text": "X"},
        {"code": "y", "#text": "Y"},
        {"code": "z", "#text": "Z"}
    ]
}

def test_get_values1():
    """Should use all codes"""
    codes = None

    values = _get_values(_DICT, codes)
    assert values == ["A", "B", "C", "D"]

def test_get_values2():
    """Should use only codes b and d"""
    codes = "bd"

    values = _get_values(_DICT, codes)
    assert values == ["B", "D"]

def test_get_values3():
    """Should exclude codes a and c"""
    codes = "!ca"

    values = _get_values(_DICT, codes)
    assert values == ["B", "D"]

def test_get_subject_values1():
    """Should not prefix any value with double hyphen and should prefix
       each value with whitespace if value is not the first value or value
       already starts with whitespace
    """
    tag = "611"

    values = _get_subject_values(_SUBJECT, tag)
    assert values == ["A", " B", " C", " D", " E", " V", " X", " Y", " Z"]

def test_get_subject_values2():
    """Should prefix V, X, Y, and Z with double hyphen and should prefix each
       value with whitespace if value is not first value, value doest not
       already start with whitespace, or value was prefixed with double hyphen
    """
    tag = "600"

    values = _get_subject_values(_SUBJECT, tag)
    assert values == ["A", " B", " C", " D", " E", "--V", "--X", "--Y", "--Z"]

def test_get_subject_values3():
    """Should prefix E, V, X, Y, and Z with double hyphen and should prefix
       each value with whitespace if value is not first value, value doest not
       already start with whitespace, or value was prefixed with double hyphen
    """
    tag = "610"

    values = _get_subject_values(_SUBJECT, tag)
    assert values == ["A", " B", " C", " D", "--E", "--V", "--X", "--Y", "--Z"]

def test_get_subject_values4():
    """Should skip numeric codes"""
    _SUBJECT["subfield"].append({"code": "2", "#text": "Ignore me"})

    test_get_subject_values1() 

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
