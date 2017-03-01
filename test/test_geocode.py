# -*- coding: utf-8 -*-
from server_support import server, H
from amara.thirdparty import json
from nose.plugins.attrib import attr
from dict_differ import assert_same_jsons
from dplaingestion.selector import setprop
from dplaingestion.akamod import geocode

@attr(travis_exclude='yes')
def test_basic_forward_lookup():
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
                    "city": "Bakersfield",
                    "state": "California",
                    "county": "Kern County",
                    "country": "United States",
                    "coordinates": "35.37329, -119.01871"
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
                    "city": "Philadelphia",
                    'county': 'Philadelphia County',
                    "state": "Pennsylvania",
                    "country": "United States",
                    "coordinates": "39.95233, -75.16379"
                },
                {
                    "county": "San Francisco County",
                    "country": "United States",
                    "state": "California",
                    "name": "San Francisco, CA",
                    "city": "San Francisco",
                    "coordinates": "37.77493, -122.41942"
                },
                {
                    'coordinates': '40.742185, -73.992602',
                    'country': 'United States',
                    'state': 'New York',
                    "name": "New York, NY",
                    "city": "New York"
                },
                {
                    "name": "Georgia",
                    "state": "Georgia",
                    "country": "United States",
                    "coordinates": "32.75042, -83.50018"
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
                    "county": "Suffolk County",
                    "state": "Massachusetts",
                    "country": "United States",
                    "name": "Downtown, MA",
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
                    "county": "Suffolk County",
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
                    "county": "Suffolk County",
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
    """Should set the name property to the lowest hierarchy value"""
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
            "spatial": [
                {
                "coordinates": "37.7771186829, -122.419639587",
                    "city": "Bananas",
                    "state": "California",
                    "name": "Downtown/Civic Center, CA",
                    "county": "San Francisco County",
                    "country": "United States"
                }
            ]
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
            "spatial": [
                {
                    "coordinates": '34.05223, -118.24368',
                    "city": "Los Angeles",
                    'county': 'Los Angeles County',
                    "state": "California",
                    "country": "United States",
                    "name": "Los Angeles"
                }
            ]
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
                "country": "United States"
            }
        }
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [
                {
                    "county": "Los Angeles County",
                    "country": "United States",
                    "name": "Los Angeles County",
                    "state": "California",
                    "coordinates": "34.19801, -118.26102"
                }
            ]
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
            "spatial": [
                {
                    "region": "Ecuador",
                    "name": "Ecuador",
                    "country": "Ecuador"
                }
            ]
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
            "spatial": [{
                'coordinates': '37.25022, -119.75126',
                "country": "United States",
                "state": "California",
                "name": "California"
            }]
        }
    }

    url = server() + "geocode"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_skip_united_states():
    """Should not add coordinates when name or country value is 
    'United States' or 'États-Unis' or 'USA'
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": ""
        }
    }

    url = server() + "geocode"
    for v in ["United States", "United States.", u"États-Unis", 
              u"États-Unis.", "USA"]:
        for field in ["name", "country"]:
            setprop(INPUT, "sourceResource/spatial", {field: v})
            resp, content = H.request(url, "POST", body=json.dumps(INPUT))
            assert resp.status == 200
            for place in json.loads(content)['sourceResource']['spatial']:
                assert 'coordinates' not in place.keys()

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
            "spatial": [{
                "coordinates": "37.25022, -119.75126",
                "country": "United States",
                "name": "United States--California",
                "state": "California"
            }]
         }
    }
    url = server() + "geocode"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_exclude_coordinates_from_countries():
    """Should not include coordinates or smaller administrative units in 
    country enhancements
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {"name": "Greece"}
        }
    }
    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [{
                "country": "Greece",
                "name": "Greece"
            }]
        }
    }

    url = server() + "geocode"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_name_search_context():
    """Contextualize a place name using any additional feature names

    If feature names for city, county, or state are given, use them to
    disambiguate place names that have multiple interpretations.
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "name": "Portland",
                "state": "Maine"
            }
        }
    }

    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [{
                "city": "Portland",
                "county": "Cumberland County",
                "country": "United States",
                "state": "Maine",
                "name": "Portland",
                "coordinates": "43.66147, -70.25533"
            }
        ]}
    }

    url = server() + "geocode"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_works_with_dotted_abbreviations():
    """Resolves something like "Greenville (S.C.)" as well as "SC" """
    # Note when retrofitting Twofishes later: Twofishes handles "(S.C.)" just
    # fine, so most of this test's assertion should be kept, but the code that
    # works around this syntax should be altered.  When we use Twofishes,
    # we're going to be able to preserve the "S.C." spelling in the "name"
    # property, and when we do this for Ingestion 3 with MAPv4 we'll be able
    # to preserve that spelling in the providedLabel property.
    INPUT =  {
        "_id": "foo",
        "sourceResource": {
            "spatial": {
                "name": "Greenville (S.C.)"
            }
        }
    }
    EXPECTED = {
        "_id": "foo",
        "sourceResource": {
            "spatial": [
                {
                    "city": "Greenville",
                    "county": "Greenville County",
                    "country": "United States",
                    "state": "South Carolina",
                    "name": "Greenville (S.C.)",
                    "coordinates": "34.85262, -82.39401"
                }
            ]
        }
    }
    url = server() + "geocode"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_name_search_failure():
    """Shouldn't fall down when nothing is returned.
    """
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "name": "Some Nonexistent Place"
            }
        }
    }

    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [{
                "name": "Some Nonexistent Place"
            }]
        }
    }

    url = server() + "geocode"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

@attr(travis_exclude='yes')
def test_geocode_unicode():
    """Handles unicode values that can be cast as UTF-8"""
    INPUT = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": {
                "name": u"États-Unis"
            }
        }
    }

    EXPECTED = {
        "id": "12345",
        "_id": "12345",
        "sourceResource": {
            "spatial": [{
                "country": "United States",
                "name": u"États-Unis"
            }]
        }
    }

    url = server() + "geocode"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert_same_jsons(EXPECTED, json.loads(content))

def test_geocode_place_to_map_json():
    """Should convert Place to a dict and drop empty fields
    """
    INPUT = {'name': u'blah', 'coordinates': 'my coordinates'}
    assert geocode.Place(INPUT).to_map_json() == INPUT

def test_geocode_place_set_name():
    """Should get Place name from field other than coordinates if name is empty
    """
    pl = geocode.Place({'city': 'Portland',
                        'country': 'Oregon',
                        'coordinates': 'my coordinates'})
    pl.set_name()
    assert pl.name == 'Portland'
    pl.name = ''
    pl.city = ''
    pl.country = ''
    pl.set_name()
    assert pl.name == ''
