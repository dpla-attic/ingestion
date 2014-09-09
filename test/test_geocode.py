# -*- coding: utf-8 -*-
import itertools
import sys
from server_support import server, H, print_error_log
from amara.thirdparty import json
from nose.plugins.attrib import attr
from dict_differ import assert_same_jsons
from dplaingestion.selector import setprop, getprop, exists

@attr(travis_exclude='yes')    
def test_geocode():
    """
    Simple geocode
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                { 
                    "name": "Bakersfield, CA"
                }
            ]
        },
        "creator": "David"
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "Bakersfield, CA",
                    "state": "California",
                    "county": "Kern County",
                    "country": "United States",
                    "coordinates": "35.3669586182, -119.018859863"
                }
            ]
        },
        "creator": "David"
    }

    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    print json.loads(content)
    assert_same_jsons(EXPECTED, json.loads(content))


@attr(travis_exclude='yes')    
def test_close_multiple_results():
    """
    Geocode that returns multiple results from Bing, that are close enough to
    each other.
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                { 
                    "name": "Philadelphia, PA"
                },
                { 
                    "name": "San Francisco, CA"
                },
                { 
                    "name": "New York, NY"
                },
                { 
                    "name": "Georgia"
                }
            ]
        },
        "creator": "David"
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "Philadelphia, PA",
                    "state": "Pennsylvania",
                    "country": "United States",
                    "coordinates": "39.9522781372, -75.1624526978"
                }, 
                { 
                    "county": "San Francisco County", 
                    "country": "United States", 
                    "state": "California", 
                    "name": "San Francisco, CA", 
                    "coordinates": "37.7771186829, -122.419639587"
                }, 
                {
                    "name": "New York, NY" 
                },
                { 
                    "name": "Georgia",
                    "state": "Georgia",
                    "country": "United States",
                    "coordinates": "32.6483230591, -83.4445343018"
                }
            ]
        },
        "creator": "David"
    }
        
    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))


@attr(travis_exclude='yes')    
def test_geocode_coordinate_provided1():
    """Should use coordinates provided in the name property"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                { 
                    "name": "42.358631134, -71.0567016602"
                }
            ]
        },
        "creator": "David"
    }

    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "state": "Massachusetts",
                    "country": "United States",
                    "name": "42.358631134, -71.0567016602",
                    "coordinates": "42.358631134, -71.0567016602"
                }
            ]
        },
        "creator": "David"
    }
        
    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')    
def test_geocode_coordinate_provided2():
    """Should use coordinates provided in the coordinates property"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                { 
                    "name": "United States--Massachussetts",
                    "coordinates": "42.358631134, -71.0567016602"
                }
            ]
        },
        "creator": "David"
    }

    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "state": "Massachusetts",
                    "country": "United States",
                    "name": "United States--Massachussetts",
                    "coordinates": "42.358631134, -71.0567016602"
                }
            ]
        },
        "creator": "David"
    }
        
    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')    
def test_geocode_with_existing_props():
    """Should not update existing fields since "state" value exists"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "name": "United States--Massachussetts",
                    "coordinates": "42.358631134, -71.0567016602",
                    "country": "Bananas",
                    "state": "Apples"
                }
            ]
        },
        "creator": "David"
    }

    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "state": "Apples",
                    "country": "Bananas",
                    "name": "United States--Massachussetts",
                    "coordinates": "42.358631134, -71.0567016602"
                }
            ]
        },
        "creator": "David"
    }
        
    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_set_name_coordinates():
    """Should set the name property to the coordinates value"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "coordinates": "37.7771186829, -122.419639587",
                "city": "Bananas"
            }
        }
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                    "coordinates": "37.7771186829, -122.419639587",
                    "city": "Bananas",
                    "state": "California",
                    "name": "37.7771186829, -122.419639587",
                    "county": "San Francisco County", 
                    "country": "United States" 
            }
        }
    }

    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_set_name_city():
    """Should set the name property to the city value"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "city": "Los Angeles",
                "state": "California"
            }
        }
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "city": "Los Angeles",
                "state": "California",
                "name": "Los Angeles"
            }
        }
    }

    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_set_name_county():
    """Should set the name property to the county value"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "county": "Los Angeles County",
                "country": "Bananas"
            }
        }
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "county": "Los Angeles County",
                "country": "Bananas",
                "name": "Los Angeles County",
                "state": "California",
                "coordinates": "33.9934997559, -118.29750824"
            }
        }
    }

    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_set_name_region():
    """Should set the name property to the region value"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "region": "Ecuador"
            }
        }
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "region": "Ecuador",
                "name": "Ecuador",
                "coordinates": "-1.42152893543, -78.8710403442",
                "country": "Republic of Ecuador",
                "state": u"Provincia de Bolívar"
            }
        }
    }

    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_set_name_state():
    """Should set the name property to the state value"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "state": "California"
            }
        }
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "state": "California",
                "name": "California"
            }
        }
    }

    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_set_name_country():
    """Should set the name property to the country value"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "country": "Canada",
                "region": "Bananas"
            }
        }
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "country": "Canada",
                "region": "Bananas",
                "name": "Canada",
                "state": "Nunavut",
                "coordinates": "62.8329086304, -95.9133224487"
            }
        }
    }

    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_skip_united_states():
    """Should not geocode when name or country value is 'United States' or
       'États-Unis' or 'USA'
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": ""
        }
    }

    url = server() + "geocode"
    for v in ["United States", "United States.", u"États-Unis", u"États-Unis.", "USA"]:
        for field in ["name", "country"]:
            setprop(INPUT, "sourceResource/spatial", {field: v})
            resp, content = H.request(url, "POST", body=json.dumps(INPUT))
            assert resp.status == 200
            if not exists(INPUT, "sourceResource/spatial/name"):
                setprop(INPUT, "sourceResource/spatial/name", v)
            assert_same_jsons(INPUT, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_do_not_skip_united_states():
    """Should geocode when name value is 'United States' is followed by a '-'
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {"name": "United States--California"}
        }
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "coordinates": "37.2551002502, -119.617523193",
                "country": "United States",
                "name": "United States--California",
                "state": "California"
            }
        }
    }

    url = server() + "geocode"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))
