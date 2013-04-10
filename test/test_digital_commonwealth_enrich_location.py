import sys
from server_support import server, H, print_error_log
from amara.thirdparty import json
from dplaingestion.akamod.digital_commonwealth_enrich_location import format_spatial

    
def test_convert_spatial_string_to_dictionary():
    """
    Convert a spatial string into a dictionary with a key of 'name'
    """
    INPUT = {
        "id": "12345",
        "sourceResource": {
            "spatial": [u'42.24  N 71.49 W', 
                        u"Bear Park (Reading Mass.)"]
        },
        "creator": "David"
    }
    EXPECTED = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": u"42.24N 71.49W"
                },
                {
                    "name": u"Bear Park (Reading MA)"
                }
            ]
        },
        "creator": "David"
    }

    url = server() + "digital_commonwealth_enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED


def test_strip_non_spatial_entries():
    """
    Strip out strings that are not locations.
    """
    INPUT = {
        "id": "12345",
        "sourceResource": {
            "spatial": ["Pictorial works", "Somerville, MA"]
        },
        "creator": "David"
    }
    EXPECTED = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "Somerville, MA"
                }
            ]
        },
        "creator": "David"
    }

    url = server() + "digital_commonwealth_enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED


def test_format_spatial_latlng():
    """
    Normalize lat/lng values from various formats
    """
    INPUT = "42212N72345W\n"
    EXPECTED = "42.212N 72.345W"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "43212N73345W"
    EXPECTED = "43.212N 73.345W"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "421300N0723600W"
    EXPECTED = "42.1300N 72.3600W"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "42.24  N 71.49 W"
    EXPECTED = "42.24N 71.49W"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "42.24N; 71.49W"
    EXPECTED = "42.24N 71.49W"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "42-18N 73.36W"
    EXPECTED = "42.18N 73.36W"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "42 degrees 04' N 72 degrees 02' W"
    EXPECTED = "42.04N 72.02W"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "42 degrees 04' N 72 degrees 02'"
    EXPECTED = "42.04N 72.02W"
    assert EXPECTED == format_spatial(INPUT)


def test_format_spatial_state():
    """
    Normalize state abbreviations
    """
    INPUT = "Wakefield (Mass.)"
    EXPECTED = "Wakefield, MA"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "(Wakefield, Mass.)"
    EXPECTED = "(Wakefield, MA)"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "Bear Hill (Reading Mass.)"
    EXPECTED = "Bear Hill (Reading MA)"
    assert EXPECTED == format_spatial(INPUT)

    INPUT = "Bear Hill (Reading, Mass.)"
    EXPECTED = "Bear Hill (Reading, MA)"
    assert EXPECTED == format_spatial(INPUT)


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
