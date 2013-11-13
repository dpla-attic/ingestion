import os
import sys
from amara.thirdparty import json

def test_parse_profiles():
    """Should parse all profiles"""
    for file in os.listdir("profiles"):
        with open("profiles/" + file, "r") as f:
            try:
                profile = json.loads(f.read())
                assert True
            except Exception, e:
                print >> sys.stderr, "Error parsing profile %s: %s" % (file, e)
                assert False

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
