import re 
import sys
from server_support import server, H, print_error_log
from amara.thirdparty import json
from dplaingestion.akamod.mwdl_enrich_location import is_spatial, format_spatial

    
def test_is_spatial(): 
    """
    Identify non-spatial strings 
    """
    TESTS = [({"name": "Honolulu, HI"}, True),
             ({"name": "Beaver County (Utah)"}, True),
             ({"name": "44.234153 -72.23414"}, False),
             ({"name": "northlimit=44.234153"}, False),
             ({"name": "44.234153"}, False),
             ({"name": "44.234153 -72.23414"}, False)]
    for test, expected in TESTS: 
        original = dict(test)
        result = is_spatial(test)
        print "%s => %s" % (original, result,)
        assert expected == result 


def test_format_spatial():
    """
    Format strings for proper geocoding
    """
    TESTS = [({"name": "Arizona(state)-Something(county)-Phoenix(inhabited place)"}, {"name": "Phoenix, Something, Arizona"}),
             ({"name": "Utah (state)"}, {"name": "Utah"}),
             ({"name": "Cambridge inhabited place"}, {"name": "Cambridge"})]
    for test, expected in TESTS: 
        original = dict(test)
        result = format_spatial(test)
        print "%s => %s" % (original, result,)
        assert expected == result 


if __name__ == "__main__":
    raise SystemExit("Use nosetest")
