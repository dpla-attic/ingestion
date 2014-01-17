import re
import sys
from server_support import server, H, print_error_log
from amara.thirdparty import json
from dplaingestion.akamod.scdl_geocode_regions import is_region, geocode_region

def test_is_region():
    """
    Identify non-SC region strings
    """
    TESTS = [({"name": "Upstate"}, True),
             ({"name": "Midlands"}, True),
             ({"name": "Lowcountry"}, True),
             ({"name": "Low Country"}, True),
             ({"name": "Pee Dee"}, True),
             ({"name": "34.19363021850586 -79.76905822753906"}, False),
             ({"name": "Not a region"}, False)]

    for test, expected in TESTS:
        original = dict(test)
        result = is_region(test)
        print "%s => %s" % (original, result,)
        assert expected == result

def test_geocode_region():
    """
    Format strings for proper geocoding
    """
    TESTS = [({"name": "Upstate"}, {"name": "Upstate",
                                    "coordinates": "34.8482704163, -82.4001083374",
                                    "state": "South Carolina",
                                    "country": "United States"}),
             ({"name": "Lowcountry"}, {"name": "Lowcountry",
                                    "coordinates": "32.7811508179, -79.931602478",
                                    "state": "South Carolina",
                                    "country": "United States"}),
             ({"name": "Low Country"}, {"name": "Low Country",
                                    "coordinates": "32.7811508179, -79.931602478",
                                    "state": "South Carolina",
                                    "country": "United States"}),
             ({"name": "Midlands"}, {"name": "Midlands",
                                    "coordinates": "33.9988212585, -81.0453720093",
                                    "state": "South Carolina",
                                    "country": "United States"}),
             ({"name": "Pee Dee"}, {"name": "Pee Dee",
                                    "coordinates": "34.1936302185, -79.7690582275",
                                    "state": "South Carolina",
                                    "country": "United States"})]

    for test, expected in TESTS:
        original = dict(test)
        result = geocode_region(test)
        print "%s => %s" % (expected, result,)
        assert expected == result

if __name__ == "__main__":
    raise SystemExit("Use nosetest")
