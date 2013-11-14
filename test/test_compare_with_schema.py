import sys
from amara.thirdparty import json
from server_support import server, H

URL = server() + "compare_with_schema"

def test_compare_collection_with_schema():
    """Should remove extraneous fields from collection document"""

    resp,content = H.request(URL, "POST", body=json.dumps(COLLECTION))
    assert str(resp.status).startswith("2")

    del COLLECTION["apples"]
    del COLLECTION["bananas"]
    assert json.loads(content) == COLLECTION

def test_compare_item_with_schema():
    """Should remove extraneous fields from item document"""

    resp,content = H.request(URL, "POST", body=json.dumps(ITEM))
    assert str(resp.status).startswith("2")

    del ITEM["apples"]
    del ITEM["bananas"]
    del ITEM["sourceResource"]["apples"]
    del ITEM["sourceResource"]["bananas"]
    assert json.loads(content) == ITEM

if __name__ == "__main__":
    raise SystemExit("Use nosetests")

COLLECTION = {
    "_id": "clemson--gmb",
    "_rev": "1-d4f6511a6a17e442bb7b5fcea7de6f47",
    "title": "Alexander McBeth Store Ledger, 1794",
    "ingestDate": "2013-11-12T11:24:07.141372",
    "ingestionSequence": 1,
    "ingestType": "collection",
    "@id": "http://dp.la/api/collections/878aab36fc67d5f50cb263ad09ab1b99",
    "id": "878aab36fc67d5f50cb263ad09ab1b99",
    "apples": "yummy",
    "bananas": "yummy"
}

ITEM = {
    "_id": "clemson--http://repository.clemson.edu/u?/gmb,1127",
    "_rev": "1-88c4cc08b936030f8b45e7925b51a93c",
    "@context": {
       "begin": {
           "@id": "dpla:dateRangeStart",
           "@type": "xsd:date"
       },
       "@vocab": "http://purl.org/dc/terms/",
       "hasView": "edm:hasView",
       "name": "xsd:string",
       "sourceResource": "edm:sourceResource",
       "object": "edm:object",
       "dpla": "http://dp.la/terms/",
       "collection": "dpla:aggregation",
       "edm": "http://www.europeana.eu/schemas/edm/",
       "state": "dpla:state",
       "aggregatedDigitalResource": "dpla:aggregatedDigitalResource",
       "coordinates": "dpla:coordinates",
       "isShownAt": "edm:isShownAt",
       "stateLocatedIn": "dpla:stateLocatedIn",
       "end": {
           "@id": "dpla:dateRangeEnd",
           "@type": "xsd:date"
       },
       "dataProvider": "edm:dataProvider",
       "originalRecord": "dpla:originalRecord",
       "provider": "edm:provider",
       "LCSH": "http://id.loc.gov/authorities/subjects"
    },
    "dataProvider": [
       "Greenville County Library, Carolina First South Carolina Room",
       "Clemson University Libraries"
    ],
    "admin": {
       "object_status": 1
    },
    "originalRecord": {
       "publisher": [
           "Greenville County Library, Carolina First South Carolina Room.",
           "Clemson University Libraries"
       ],
       "handle": [
           "gcls_mbth_ldgr_cov",
           "http://repository.clemson.edu/u?/gmb,1127"
       ],
       "description": "Heavy fabric, reddish-brown, deteriorating in spots.  Layers of paper that make up the bulk of the cover visible.  Dark ink, hand-written: 'Day Book/A McB & C/1794'.",
       "language": "English",
       "rights": "This image is free from copyright and may be used without prior consent.",
       "date": "1904-11-28",
       "format": [
           "image/jpeg",
           "Manuscripts"
       ],
       "contributor": "McBeth, Alexander",
       "collection": {
           "@id": "http://dp.la/api/collections/878aab36fc67d5f50cb263ad09ab1b99",
           "id": "878aab36fc67d5f50cb263ad09ab1b99",
           "title": "Alexander McBeth Store Ledger, 1794"
       },
       "label": "Alexander McBeth Store Ledger",
       "source": "Hand-made ledger book.  Greenville County Library, South Carolina Room Archives.",
       "relation": "Alexander McBeth Store Ledger, 1794",
       "coverage": [
           "Greenville County (S.C.)",
           "Upstate"
       ],
       "provider": {
           "@id": "http://dp.la/api/contributor/scdl-clemson",
           "name": "South Carolina Digital Library"
       },
       "datestamp": "2009-07-28",
       "title": "Alexander McBeth Store Ledger",
       "type": "Text",
       "id": "oai:repository.clemson.edu:gmb/1127",
       "setSpec": "gmb",
       "subject": "McBeth, Alexander; Greenville (S.C.); South Carolina; Account books; General stores"
    },
    "object": "http://repository.clemson.edu/cgi-bin/thumbnail.exe?CISOROOT=/gmb&CISOPTR=1127",
    "ingestDate": "2013-11-12T11:24:07.141465",
    "ingestionSequence": 1,
    "isShownAt": "http://repository.clemson.edu/u?/gmb,1127",
    "provider": {
       "@id": "http://dp.la/api/contributor/scdl-clemson",
       "name": "South Carolina Digital Library"
    },
    "sourceResource": {
       "apples": "yummy",
       "bananas": "yummy",
       "rights": "This image is free from copyright and may be used without prior consent.",
       "date": {
           "begin": "1904-11-28",
           "end": "1904-11-28",
           "displayDate": "1904-11-28"
       },
       "description": "Heavy fabric, reddish-brown, deteriorating in spots. Layers of paper that make up the bulk of the cover visible. Dark ink, hand-written: 'Day Book/A McB & C/1794'.",
       "language": [
           {
               "name": "English",
               "iso639_3": "eng"
           }
       ],
       "title": "Alexander McBeth Store Ledger",
       "format": "Manuscripts",
       "collection": {
           "@id": "http://dp.la/api/collections/878aab36fc67d5f50cb263ad09ab1b99",
           "id": "878aab36fc67d5f50cb263ad09ab1b99",
           "title": "Alexander McBeth Store Ledger, 1794"
       },
       "stateLocatedIn": [
           {
               "name": "South Carolina"
           }
       ],
       "relation": "Alexander McBeth Store Ledger, 1794",
       "spatial": [
           {
               "county": "Greenville County",
               "country": "United States",
               "state": "South Carolina",
               "name": "Greenville County (S.C.)",
               "coordinates": "34.8938789368, -82.3703079224"
           },
           {
               "country": "United States",
               "state": "South Carolina",
               "name": "Upstate",
               "coordinates": "34.8482704163 -82.4001083374"
           }
       ],
       "contributor": "McBeth, Alexander",
       "type": "text",
       "subject": [
           {
               "name": "McBeth, Alexander"
           },
           {
               "name": "Greenville (S.C.)"
           },
           {
               "name": "South Carolina"
           },
           {
               "name": "Account books"
           },
           {
               "name": "General stores"
           }
       ]
    },
    "ingestType": "item",
    "@id": "http://dp.la/api/items/d30812c04676019933931bef2546a591",
    "id": "d30812c04676019933931bef2546a591",
    "apples": "yummy",
    "bananas": "yummy"
}
