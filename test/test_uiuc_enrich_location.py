import sys
from server_support import server, H, print_error_log
from amara.thirdparty import json
from dplaingestion.akamod.uiuc_enrich_location import is_spatial, format_spatial

    
def test_convert_spatial_string_to_dictionary():
    """
    Format UIUC spatial dictionaries 
    """
    INPUT = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                { 
                    "name": "Honolulu, HI"
                },
                { 
                    "name": "1972 to Present"
                }
            ]
        },
        "creator": "David"
    }
    EXPECTED = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "Honolulu, HI"
                }
            ]
        },
        "creator": "David"
    }
        
    url = server() + "uiuc_enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED


def test_is_spatial(): 
    """
    Identify non-spatial strings 
    """
    INPUT = {"name": "1392 to 1400 A.D."}
    assert False == is_spatial(INPUT)

    INPUT = {"name": "Early 20th century"}
    assert False == is_spatial(INPUT)

    INPUT = {"name": "Cambridge (Mass.)"}
    assert True == is_spatial(INPUT)


def test_format_spatial():
    """
    Format strings for proper geocoding
    """
    INPUT = {"name": "Africa, North"}
    EXPECTED = {"name": "North, Africa"}
    assert EXPECTED == format_spatial(INPUT)


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
