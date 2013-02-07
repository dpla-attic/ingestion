import sys
from server_support import server
from amara.thirdparty import json, httplib2
from dplaingestion.akamod.enrich_location import \
    from_abbrev, get_isostate, create_dictionaries, remove_space_around_semicolons, STATES

CT_JSON = {"content-type": "application/json"}

H = httplib2.Http()
    
# BEGIN remove_space_around_semilocons tests
def test_remove_space_around_semicolons():
    INPUT = "California; Texas;New York ;  Minnesota  ; Arkansas ;Hawaii;"
    EXPECTED = "California;Texas;New York;Minnesota;Arkansas;Hawaii;"

    OUTPUT = remove_space_around_semicolons(INPUT)
    assert OUTPUT == EXPECTED

# BEGIN from_abbrev tests
def test_from_abbrev1():
    """
    "Ga." should produce Georgia but "in" should not produce Indiana
    """
    INPUT = "Athens in Ga."
    EXPECTED = ["Georgia"]

    OUTPUT = from_abbrev(INPUT)
    assert OUTPUT == EXPECTED

def test_from_abbrev2():
    """
    Should return array with complete State name as the only element.
    """
    INPUT = "San Diego, C.A."
    EXPECTED = ["California"]

    OUTPUT = from_abbrev(INPUT)
    assert OUTPUT == EXPECTED

def test_from_abbrev3():
    """
    Should return array with complete State name as the only element.
    """
    INPUT = "San Diego, (CA)"
    EXPECTED = ["California"]

    OUTPUT = from_abbrev(INPUT)
    assert OUTPUT == EXPECTED

def test_from_abbrev4():
    """
    Should return array with complete State name as the only element.
    """
    INPUT = "Greenville, (S.C.)."
    EXPECTED = ["South Carolina"]

    OUTPUT = from_abbrev(INPUT)
    assert OUTPUT == EXPECTED

def test_from_abbrev5():
    """
    Should return array with complete State name as the only element.
    """
    INPUT = "Asheville NC "
    EXPECTED = ["North Carolina"]

    OUTPUT = from_abbrev(INPUT)
    assert OUTPUT == EXPECTED

def test_from_abbrev6():
    """
    Should return array with complete State names as the elements.
    """
    INPUT = "San Diego (C.A.), Asheville (NC), Brookings S.d., Brooklyn  NY."
    EXPECTED = ["California", "North Carolina", "South Dakota", "New York"] 

    OUTPUT = from_abbrev(INPUT)
    assert OUTPUT.sort() == EXPECTED.sort()

def test_from_abbrev7():
    INPUT = "Cambridge, Mass."
    EXPECTED = ["Massachusetts"]

    OUTPUT = from_abbrev(INPUT)
    assert OUTPUT == EXPECTED

# BEGIN get_isostate tests
def test_get_isostate_non_string_param_fail():
    """
    Should return error if non-string passed as parameter"
    """
    pass

def test_get_isostate_mass():
    INPUT = "Cambridge, Mass."
    EXPECTED = ("US-MA", "Massachusetts") 

    OUTPUT = get_isostate(INPUT,frm_abbrev="Yes")
    assert OUTPUT == EXPECTED

def test_get_isostate_string_without_state_names_returns_none():
    """
    Should return (None,None) if string parameter does not contain state names/abbreviations.
    """
    INPUT = "Canada, Mexico, U.S.; Antarctica."
    OUTPUT = get_isostate(INPUT)

    assert OUTPUT == (None, None)

def test_get_isostate_string_with_state_names_returns_iso_and_state():
    """
    Should return (iso_string, state_string) where iso_string and state_string are semicolon-
    separated iso3166-2 values and state names, respectively. Should not include South Carolina
    because the optional frb_abbrev parameter was not passed.
    """
    INPUT = "California;New Mexico;Arizona;New York;(S.C.);"
    EXPECTED = ("US-CA;US-NM;US-AZ;US-NY", "California;New Mexico;Arizona;New York")

    OUTPUT = get_isostate(INPUT)
    assert OUTPUT == EXPECTED

# BEGIN create_dictionaries tests
def test_create_dictionaries_one():
    """
    Should return array with one dictionary as the only element if splitting each spatial field on
    semicolons produces only one string for each spatial field.
    """
    INPUT = {
        "city": "Asheville",
        "county": "Buncombe",
        "state": "North Carolina;",
        "country": "United States",
        "iso3166-2": "US-NC"
    }
    EXPECTED = [
        {
            "city": "Asheville",
            "county": "Buncombe",
            "state": "North Carolina",
            "country": "United States",
            "iso3166-2": "US-NC"
        }
    ]

    OUTPUT = create_dictionaries(INPUT) 
    assert OUTPUT == EXPECTED

def test_create_dictionaries_many():
    """
    Should return array with dictionaries as elements if splitting each spatial field on
    semicolons produces multiple strings in any spatial field.
    """
    INPUT = {
        "city": "La Jolla;Pasadena",
        "county": "San Diego;Los Angeles;Buncombe",
        "state": "California;North Carolina",
        "country": "United States",
        "iso3166-2": "US-CA;US-NC"
    }
    EXPECTED = [
        {
            "city": "La Jolla",
            "county": "San Diego",
            "state": "California",
            "country": "United States",
            "iso3166-2": "US-CA"
        },
        {
            "city": "Pasadena",
            "county": "Los Angeles",
            "state": "North Carolina",
            "iso3166-2": "US-NC"
        },
        {
            "county": "Buncombe"
        }
    ]

    OUTPUT = create_dictionaries(INPUT)
    assert OUTPUT == EXPECTED

def test_enrich_location_after_provider_specific_enrich_location1():
    """
    Previous specific-provider location enrichment set city, county, state,
    and country. No semicolons. EXPECTED should be the same as INPUT with the
    additional iso3166-2 field in spatial.
    """
    INPUT = {
        "id": "12345",
        "spatial": [
            {
                "city": "Asheville",
                "county": "Buncombe",
                "state": "North Carolina",
                "country": "United States"
            }
        ],
        "creator": "Miguel"
    }
    EXPECTED = {
        "id": "12345",
        "spatial": [
            {
                "city": "Asheville",
                "county": "Buncombe",
                "state": "North Carolina",
                "country": "United States",
                "iso3166-2": "US-NC"
            }
        ],
        "creator": "Miguel"
    }

    url = server() + "enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_enrich_location_after_provider_specific_enrich_location2():
    """
    Previous specific-provider location enrichment set city, county, state,
    and country. With semicolons but no multiple values. EXPECTED should be
    the same as INPUT with semicolons in the state field removed and the
    additional iso3166-2 field in spatial.
    """
    INPUT = {
        "id": "12345",
        "spatial": [
            {
                "city": "Asheville;",
                "county": "Buncombe;",
                "state": "North Carolina;",
                "country": "United States;"
            }
        ],
        "creator": "Miguel"
    }
    EXPECTED = {
        "id": "12345",
        "spatial": [
            {
                "city": "Asheville",
                "county": "Buncombe",
                "state": "North Carolina",
                "country": "United States",
                "iso3166-2": "US-NC"
            }
        ],
        "creator": "Miguel"
    }

    url = server() + "enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_enrich_location_after_provider_specific_enrich_location3():
    """
    Previous specific-provider location enrichment set city, county, state,
    and country. Multiple values separated by semicolons.
    """
    INPUT = {
        "id": "12345",
        "spatial": [
            {
                "city": "Asheville; La Jolla",
                "county": "Buncombe;San Diego",
                "state": "North Carolina; California ; Texas;",
                "country": "United States"
            }
        ],
        "creator": "Miguel"
    }
    EXPECTED = {
        "id": "12345",
        "spatial": [
            {
                "city": "Asheville",
                "county": "Buncombe",
                "state": "North Carolina",
                "country": "United States",
                "iso3166-2": "US-NC"
            },
            {
                "city": "La Jolla",
                "county": "San Diego",
                "state": "California",
                "iso3166-2": "US-CA"
            },
            {
                "state": "Texas",
                "iso3166-2": "US-TX"
            }
        ],
        "creator": "Miguel"
    }

    url = server() + "enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_enrich_location_after_provider_specific_enrich_location4():
    """
    Previous specific-provider location did not set state.
    """
    INPUT = {
        "id": "12345",
        "spatial": [
            {
                "city": "Asheville; La Jolla",
                "county": "Buncombe;San Diego",
                "country": "United States"
            }
        ],
        "creator": "Miguel"
    }
    EXPECTED = {
        "id": "12345",
        "spatial": [
            {
                "city": "Asheville",
                "county": "Buncombe",
                "country": "United States",
            },
            {
                "city": "La Jolla",
                "county": "San Diego",
            }
        ],
        "creator": "Miguel"
    }

    url = server() + "enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_enrich_location_after_provider_specific_enrich_location5():
    """
    Should remoe state if previous specific-provider location set incorrect state.
    """
    INPUT = {
        "id": "12345",
        "spatial": [
            {
                "city": "Anoka; Champlin",
                "county": "Minnesota",
                "state": "United States",
                "country": "Mississippi River"
            }
        ],
        "creator": "Miguel"
    }
    EXPECTED = {
        "id": "12345",
        "spatial": [
            {
                "city": "Anoka",
                "county": "Minnesota",
                "country": "Mississippi River"
            },
            {
                "city": "Champlin"
            }
        ],
        "creator": "Miguel"
    }

    url = server() + "enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED



def test_enrich_location_no_provider_specific_enrich_location1():
    """
    No previous provider-specific location enrichment and does not contain states
    or state abbreviations.
    """
    INPUT = {
        "id": "12345",
        "spatial": [
            { "name": "Asheville" },
            { "name": "Buncombe" },
            { "name": "United States" }
        ],
        "creator": "Miguel"
    }
    OUTPUT = {
        "id": "12345",
        "spatial": [
            { "name": "Asheville" },
            { "name": "Buncombe" },
            { "name": "United States" }
        ],
        "creator": "Miguel"
    }


    url = server() + "enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert resp.status == 200
    assert json.loads(content) == OUTPUT

def test_enrich_location_no_provider_specific_enrich_location2():
    """
    No previous provider-specific location enrichment and contains states
    and state abbreviations.
    """
    INPUT = {
        "id": "12345",
        "spatial": [
            { "name": "Asheville, North Carolina" },
            { "name": "Greenville, SC" },
            { "name": "San Diego, (C.A.)" },
            { "name": "Athens, Ga." }
        ],
        "creator": "Miguel"
    }
    EXPECTED = {
        "id": "12345",
        "spatial": [
            {
                "state": "North Carolina",
                "iso3166-2": "US-NC",
                "name" : "Asheville, North Carolina"
            },
            {
                "state": "South Carolina",
                "iso3166-2": "US-SC",
                "name": "Greenville, SC"
            },
            {
                "state": "California",
                "iso3166-2": "US-CA",
                "name": "San Diego, (C.A.)"
            },
            {
                "state": "Georgia",
                "iso3166-2": "US-GA",
                "name": "Athens, Ga."
            }
        ],
        "creator": "Miguel"
    }


    url = server() + "enrich_location"
    resp,content = H.request(url,"POST",body=json.dumps(INPUT),headers=CT_JSON)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
