from server_support import server
from amara.thirdparty import httplib2
from amara.thirdparty import json
from nose.plugins.attrib import attr
from urllib import urlencode

CT_JSON = {"Content-Type": "application/json"}

H = httplib2.Http()


def _get_server_response(body, action="set", headers=CT_JSON, **kwargs):
    url = server() + "select-id?%s" % urlencode(kwargs)
    print (url)
    return H.request(url, "POST", body=body, headers=headers)


@attr(travis_exclude='yes')
def test_select_id_without_prefix():
    """Penn's DPLA IDs should not use a source prefix value
    Testing: https://dp.la/item/8f5cf4bfc46fe9960a66808786a15637
    """
    prop = "id"

    INPUT = {
        "id": "oai:libcollab.temple.edu:dplapa:TEMPLE_p15037coll3_13204"
    }

    EXPECTED = {
        "_id": "oai:libcollab.temple.edu:dplapa:TEMPLE_p15037coll3_13204",
        "id": "8f5cf4bfc46fe9960a66808786a15637"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
                                        use_source="no")
    assert resp.status == 200
    assert json.loads(content) == EXPECTED


def test_select_id_with_prefix():
    """Tenn's DPLA IDs should use a source prefix value
    Testing: https://dp.la/item/8af4638ed07394075e1248b0b99e2085
    """
    prop = "id"

    use_src_head = { "Content-Type": "application/json",
                     "Source": "tennessee"
                   }

    INPUT = {
        "id": "urn:dpla.lib.utk.edu.mtsu_p15838coll4:oai:cdm15838.contentdm.oclc.org:p15838coll4/1225"
    }

    EXPECTED = {
        "_id": "tennessee--urn:dpla.lib.utk.edu.mtsu_p15838coll4:oai:cdm15838.contentdm.oclc.org:p15838coll4/1225",
        "id": "8af4638ed07394075e1248b0b99e2085"
    }

    resp,content = _get_server_response(json.dumps(INPUT), prop=prop,
                                        use_source="yes", headers=use_src_head)
    assert resp.status == 200
    assert json.loads(content) == EXPECTED