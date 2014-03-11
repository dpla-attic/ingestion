from amara.thirdparty import json
from server_support import server, H, print_error_log
import sys


def test_mdl_state_located_in1():
    """Should set stateLocatedIn from "Minnesota" in dataProvider"""
    INPUT = {
        "dataProvider": [
            "Iron Range Research Center, 1005 Discovery Drive, Chisholm, Minnesota 55719",
            "http://mndiscoverycenter.com/research-center"
        ],
        "sourceResource": {}
    }

    url = server() + "mdl_state_located_in"

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content)["sourceResource"]["stateLocatedIn"] == "Minnesota"

def test_mdl_state_located_in2():
    """Should set stateLocatedIn from "MN" in dataProvider"""
    INPUT = {
        "dataProvider": [
            "Iron Range Research Center, 1005 Discovery Drive, Chisholm, MN 55719",
            "http://mndiscoverycenter.com/research-center"
        ],
        "sourceResource": {}
    }

    url = server() + "mdl_state_located_in"

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content)["sourceResource"]["stateLocatedIn"] == "Minnesota"

def test_mdl_state_located_in3():
    """Should not set stateLocatedIn from dataProvider"""
    INPUT = {
        "dataProvider": [
            "Iron Range Research Center, 1005 Discovery Drive, Chisholm, 55719",
            "http://mndiscoverycenter.com/research-center"
        ],
        "sourceResource": {}
    }

    url = server() + "mdl_state_located_in"

    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == INPUT

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
