import sys
from amara.thirdparty import json
from server_support import server, H, print_error_log
from dict_differ import assert_same_jsons

url = server() + "usc_enrich_location"

def test_usc_enrich_location():
    """Should join values on whitespace"""
    INPUT = {
        "sourceResource": {
            "spatial": [
                {"name": "-130.4560,,32.9870"},
                {"name": "1234"},
                {"name": "Asheville"}
            ]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "spatial": [
                {"name": "-130.4560,,32.9870 1234 Asheville"}
            ]
        }
    }

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp["status"] == "200"
    assert_same_jsons(EXPECTED, json.loads(content))

def test_usc_enrich_location_find_coordinates(): 
    """Should remove all spatial values except for the lat/long coordinate"""
    INPUT = {
        "sourceResource": {
            "spatial": [
                {"name": " 123 "},
                {"name": "-130.4560,,32.9870"},
                {"name": "1234"},
                {"name": "Asheville"},
                {"name": "82.5542, 35.6008"}
            ]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "spatial": [
                {"name": "35.6008, 82.5542"}
            ]
        }
    }

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp["status"] == "200"
    assert_same_jsons(EXPECTED, json.loads(content))

def test_usc_enrich_location_clean():
    """Should remove all 1-3 digit numbers and values containing 's.d', then
       join the remaining values on whitespace
    """
    INPUT = {
        "sourceResource": {
            "spatial": [
                {"name": " 123 "},
                {"name": "-130.4560,,32.9870"},
                {"name": "s.d]"},
                {"name": "s.d"},
                {"name": "1234"},
                {"name": "456"},
                {"name": "s.d."},
                {"name": "Asheville"},
                {"name": "789"}
            ]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "spatial": [
                {"name": "-130.4560,,32.9870 1234 Asheville"}
            ]
        }
    }

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp["status"] == "200"
    assert_same_jsons(EXPECTED, json.loads(content))

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
