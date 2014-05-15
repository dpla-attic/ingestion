import itertools
import sys
from server_support import server, H
from amara.thirdparty import json
from nose.plugins.attrib import attr
from dict_differ import assert_same_jsons

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
