from amara.thirdparty import json
from server_support import server, H, print_error_log
import sys

def test_scdl_format_to_type1():
    """Should set sourceResource/type"""
    INPUT = {
        "sourceResource": {
            "format": ["Objects", "MP3", "Text", " banana Scrapbooks ",
                       " apples Miscellaneous"]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "format": ["Objects", "MP3", "Text", " banana Scrapbooks ",
                       " apples Miscellaneous"],
            "type": list(set(["image", "sound", "text"]))
        }
    }

    url = server() + "scdl_format_to_type"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_scdl_format_to_type2():
    """Should append to sourceResource/type"""
    INPUT = {
        "sourceResource": {
            "format": ["MP3", "Text"],
            "type": "image"
        }
    }
    EXPECTED = {
        "sourceResource": {
            "format": ["MP3", "Text"],
            "type": list(set(["image", "sound", "text"]))
        }
    }

    url = server() + "scdl_format_to_type"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

def test_scdl_format_to_type3():
    """Should not set sourceResource/type"""
    INPUT = {
        "sourceResource": {
            "format": ["bananas"]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "format": ["bananas"]
        }
    }

    url = server() + "scdl_format_to_type"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert resp.status == 200
    assert json.loads(content) == EXPECTED

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
