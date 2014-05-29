from server_support import server, H, print_error_log
from amara.thirdparty import json
from dict_differ import assert_same_jsons
from copy import deepcopy
import sys

def test_validate_mapv3_valid_item():
    ITEM = {
        "id": "4717ec6bee8c23840e8fcbea9cf7d5e8",
        "@id": "http://dp.la/api/items/4717ec6bee8c23840e8fcbea9cf7d5e8",
        "ingestType": "item",
        "@context": {
            "LCSH": "http://id.loc.gov/authorities/subjects",
            "provider": "edm:provider",
            "originalRecord": "dpla:originalRecord",
            "edm": "http://www.europeana.eu/schemas/edm/",
            "collection": "dpla:aggregation",
            "dpla": "http://dp.la/terms/",
            "object": "edm:object",
            "name": "xsd:string",
            "hasView": "edm:hasView",
            "@vocab": "http://purl.org/dc/terms/",
            "begin": {
                "@type": "xsd:date",
                "@id": "dpla:dateRangeStart"
            },
            "end": {
                "@type": "xsd:date",
                "@id": "dpla:end"
            },
            "state": "dpla:state",
            "aggregatedDigitalResource": "dpla:aggregatedDigitalResource",
            "coordinates": "dpla:coordinates",
            "isShownAt": "edm:isShownAt",
            "stateLocatedIn": "dpla:stateLocatedIn",
            "sourceResource": "edm:sourceResource",
            "dataProvider": "edm:dataProvider"
        },
        "provider": {
            "name": "The Portal to Texas History",
            "@id": "http://dp.la/api/contributor/the_portal_to_texas_history"
        },
        "_id": "texas--info:ark/67531/metapth245858",
        "dataProvider": [
            "Gillespie County Historical Society"
        ],
        "sourceResource": {
            "subject": [
                {"name": "Social Life and Customs"},
                {"name": "Pets"},
                {"name": "Dogs"},
                {"name": "Cats"},
                {"name": "People"},
                {"name": "Children"}
            ],
            "rights": "The contents of The Portal to Texas History (digital content including images, text, and sound and video recordings) are made publicly available by the collection-holding partners for use in research, teaching, and private study. For the full terms of use, see http://texashistory.unt.edu/terms-of-use/",
            "description": [
                "Photograph of two boys holding a kitten and puppy. The boy holding the kitten in his lap is wearing suspenders, shorts, button-up shirt, and a dark-colored hat. The boy with the puppy is wearing shorts, a button-up shirt, and a light-colored hat. Behind them is a wall covered in leaves.",
                "1 photograph : b&w, faded ; 10 x 7 cm., on mat 18 x 13 cm."
            ],
            "title": [
                "[Photograph of Two Boys Holding a Kitten and Puppy]"
            ],
            "format": "Image",
            "collection": {
                "description": "",
                "id": "6814902bd6e5f107185a764123c18dda",
                "@id": "http://dp.la/api/collections/6814902bd6e5f107185a764123c18dda",
                "title": "Gillespie County Historical Society"
            },
            "spatial": [
                {
                    "coordinates": "30.3180904388, -98.9464492798",
                    "name": "United States - Texas - Gillespie County",
                    "state": "Texas",
                    "country": "United States",
                    "county": "Gillespie County"
                }
            ],
            "identifier": [
                "itemURL: http://texashistory.unt.edu/ark:/67531/metapth245858/",
                "LOCAL-CONT-NO: GCHS_1978-566-001",
                "thumbnailURL: http://texashistory.unt.edu/ark:/67531/metapth245858/thumbnail/",
                "ARK: ark:/67531/metapth245858"
            ],
            "type": "image"
        },
        "object": "http://texashistory.unt.edu/ark:/67531/metapth245858/thumbnail/",
        "ingestDate": "2014-02-26T07:47:43.391637",
        "originalRecord": {
            "provider": {
                "name": "The Portal to Texas History",
                "@id": "http://dp.la/api/contributor/the_portal_to_texas_history"
            },
            "collection": {
                "description": "",
                "id": "6814902bd6e5f107185a764123c18dda",
                "@id": "http://dp.la/api/collections/6814902bd6e5f107185a764123c18dda",
                "title": "Gillespie County Historical Society"
            },
            "id": "info:ark/67531/metapth245858",
            "metadata": {
                "untl:metadata": {
                    "untl:format": "image",
                    "untl:language": "nol"
                }
            }
        },
        "ingestionSequence": 5,
        "isShownAt": "http://texashistory.unt.edu/ark:/67531/metapth245858/"
    }
    VALID_ITEM = deepcopy(ITEM)
    VALID_ITEM["admin"] = {"valid_after_enrich": True}

    url = server() + "validate_mapv3"
    resp,content = H.request(url,"POST",body=json.dumps(ITEM))
    assert resp.status == 200
    assert_same_jsons(json.loads(content), VALID_ITEM)


def test_validate_mapv3_valid_coll():
    COLL = {
        "description": "",
        "id": "6814902bd6e5f107185a764123c18dda",
        "@id": "http://dp.la/api/collections/6814902bd6e5f107185a764123c18dda",
        "title": "Gillespie County Historical Society"
    }
    VALID_COLL = deepcopy(COLL)
    VALID_COLL["admin"] = {"valid_after_enrich": True}

    url = server() + "validate_mapv3"
    resp,content = H.request(url,"POST",body=json.dumps(COLL))
    assert resp.status == 200
    assert_same_jsons(json.loads(content), VALID_COLL)

def test_validate_mapv3_invalid_item():
    ITEM = {
        "id": "4717ec6bee8c23840e8fcbea9cf7d5e8",
        "@id": "http://dp.la/api/items/4717ec6bee8c23840e8fcbea9cf7d5e8",
        "ingestType": "item",
        "admin": {"object_status": 1},
        "@context": {
            "LCSH": "http://id.loc.gov/authorities/subjects",
            "provider": "edm:provider",
            "originalRecord": "dpla:originalRecord",
            "edm": "http://www.europeana.eu/schemas/edm/",
            "collection": "dpla:aggregation",
            "dpla": "http://dp.la/terms/",
            "object": "edm:object",
            "name": "xsd:string",
            "hasView": "edm:hasView",
            "@vocab": "http://purl.org/dc/terms/",
            "begin": {
                "@type": "xsd:date",
                "@id": "dpla:dateRangeStart"
            },
            "end": {
                "@type": "xsd:date",
                "@id": "dpla:end"
            },
            "state": "dpla:state",
            "aggregatedDigitalResource": "dpla:aggregatedDigitalResource",
            "coordinates": "dpla:coordinates",
            "isShownAt": "edm:isShownAt",
            "stateLocatedIn": "dpla:stateLocatedIn",
            "sourceResource": "edm:sourceResource",
            "dataProvider": "edm:dataProvider"
        },
        "provider": {
            "name": "The Portal to Texas History",
            "@id": "http://dp.la/api/contributor/the_portal_to_texas_history"
        },
        "_id": "texas--info:ark/67531/metapth245858",
        "dataProvider": [
            "Gillespie County Historical Society"
        ],
        "sourceResource": {
            "format": "Image",
            "collection": {
                "description": "",
                "id": "6814902bd6e5f107185a764123c18dda",
                "@id": "http://dp.la/api/collections/6814902bd6e5f107185a764123c18dda",
                "title": "Gillespie County Historical Society"
            },
        },
        "object": "http://texashistory.unt.edu/ark:/67531/metapth245858/thumbnail/",
        "ingestDate": "2014-02-26T07:47:43.391637",
        "ingestionSequence": 5,
        "isShownAt": "http://texashistory.unt.edu/ark:/67531/metapth245858/"
    }
    INVALID_ITEM = deepcopy(ITEM)
    INVALID_ITEM["admin"]["valid_after_enrich"] = False

    url = server() + "validate_mapv3"
    resp,content = H.request(url,"POST",body=json.dumps(ITEM))
    assert resp.status == 200
    assert_same_jsons(json.loads(content), INVALID_ITEM)

def test_validate_mapv3_invalid_coll():
    COLL = {"title": "Gillespie County Historical Society"}
    INVALID_COLL = deepcopy(COLL)
    INVALID_COLL["admin"] = {"valid_after_enrich": False}

    url = server() + "validate_mapv3"
    resp,content = H.request(url,"POST",body=json.dumps(COLL))
    assert resp.status == 200
    assert_same_jsons(json.loads(content), INVALID_COLL)
