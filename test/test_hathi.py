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
        {"code": "c", "#text": "C"},
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

def test_get_subject_values_tag_653():
    tag = "653"

    values = _get_subject_values(_SUBJECT, tag)
    assert values == ["A--B--C--D--E--V--X--Y--Z"]

def test_get_subject_values_tags_654_and_655():
    for tag in ("654", "655"):
        values = _get_subject_values(_SUBJECT, tag)
        assert values == ["A--B. C, D. E--V--X--Y--Z"]

def test_get_subject_values_tag_658():
    tag = "658"

    values = _get_subject_values(_SUBJECT, tag)
    assert values == ["A:B [C]--D. E. V. X. Y. Z"]

def test_get_subject_values_tag_69x():
    for tag in range(690, 700):
        values = _get_subject_values(_SUBJECT, str(tag))
        assert values == ["A--B--C--D--E--V--X--Y--Z"]

def test_get_subject_values_tag_610():
    tag = "610"

    values = _get_subject_values(_SUBJECT, tag)
    assert values == ["A. B. C, D. E--V--X--Y--Z"]

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
