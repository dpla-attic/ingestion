from server_support import server, H
from amara.thirdparty import json

def test_texas_enrich_location1():    
    """Should map city, county, state, and country"""
    INPUT = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                "United States - California - San Diego County - La Jolla"
            ]
        }
    }
    EXPECTED = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "United States - California - San Diego County - La Jolla",
                    "country": "United States",
                    "state": "California",
                    "county": "San Diego County",
                    "city": "La Jolla"
                }
            ]
        }
    }
        
    url = server() + "texas_enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_texas_enrich_location2():
    """Should map city, county, state, and country"""
    INPUT = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                "Canada - British Columbia Province - Vancouver Island - Victoria"
            ]
        }
    }
    EXPECTED = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "Canada - British Columbia Province - Vancouver Island - Victoria",
                    "country": "Canada",
                    "state": "British Columbia Province",
                    "county": "Vancouver Island",
                    "city": "Victoria"
                }
            ]
        }
    }
        
    url = server() + "texas_enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_texas_enrich_location3():
    """Should extract coordinates"""
    INPUT = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                "Canada - British Columbia Province - Vancouver Island - Victoria",
                "north=34.19; east=-99.94;"
            ]
        }
    }
    EXPECTED = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "Canada - British Columbia Province - Vancouver Island - Victoria",
                    "country": "Canada",
                    "state": "British Columbia Province",
                    "county": "Vancouver Island",
                    "city": "Victoria"
                },
                {
                    "name": "34.19, -99.94"
                }
            ]
        }
    }
        
    url = server() + "texas_enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_texas_enrich_location4():
    """Should do nothing with limits"""
    INPUT = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                "Canada - British Columbia Province - Vancouver Island - Victoria",
                "north=34.19; east=-99.94;",
                "northlimit=34.25; eastlimit=-99.88; southlimit=34.13; westlimit=-100;"
            ]
        }
    }
    EXPECTED = {
        "id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "Canada - British Columbia Province - Vancouver Island - Victoria",
                    "country": "Canada",
                    "state": "British Columbia Province",
                    "county": "Vancouver Island",
                    "city": "Victoria"
                },
                {
                    "name": "34.19, -99.94"
                },
                {
                    "name": "northlimit=34.25; eastlimit=-99.88; southlimit=34.13; westlimit=-100;"
                }
            ]
        }
    }
        
    url = server() + "texas_enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
