from server_support import server, H, print_error_log

from amara.thirdparty import json
from nose.tools import nottest
from dict_differ import DictDiffer

def test_artstor_identify_object():
    """Fetching Artstor document thumbnail (schema v3)"""

    INPUT_JSON = """
    {
   "_id": "artstor--oai:oaicat.oclc.org:AKRESS_10310356237",
   "_rev": "1-7ac8a9618041eff32ad52ad44d95e69c",
   "@context": {
       "begin": {
           "@id": "dpla:dateRangeStart",
           "@type": "xsd:date"
       },
       "@vocab": "http://purl.org/dc/terms/",
       "hasView": "edm:hasView",
       "name": "xsd:string",
       "object": "edm:object",
       "sourceResource": "edm:sourceResource",
       "dpla": "http://dp.la/terms/",
       "collection": "dpla:aggregation",
       "edm": "http://www.europeana.eu/schemas/edm/",
       "end": {
           "@id": "dpla:dateRangeEnd",
           "@type": "xsd:date"
       },
       "state": "dpla:state",
       "aggregatedDigitalResource": "dpla:aggregatedDigitalResource",
       "coordinates": "dpla:coordinates",
       "provider": "edm:provider",
       "stateLocatedIn": "dpla:stateLocatedIn",
       "isShownAt": "edm:isShownAt",
       "dataProvider": "edm:dataProvider",
       "originalRecord": "dpla:originalRecord",
       "LCSH": "http://id.loc.gov/authorities/subjects"
   },
   "originalRecord": {
       "rights": [
           "",
           "Please note that if this image is under copyright, you may need to contact one or more copyright owners for any use that is not permitted under the ARTstor Terms and Conditions of Use or not otherwise permitted by law. While ARTstor tries to update contact information, it cannot guarantee that such information is always accurate. Determining whether those permissions are necessary, and obtaining such permissions, is your sole responsibility."
       ],
       "handle": [
           "Thumbnail: http://media.artstor.net/imgstor/size2/kress/d0001/kress_1103_post.jpg",
           "Image View: http://www.artstor.org/artstor/ViewImages?id=8DtZYyMmJloyLyw7eDt5QHgt&userId=gDBAdA%3D%3D",
           "Ranking: 13000"
       ],
       "format": [
           "55.6 x 71.1 cm",
           "Oil on wood panel"
       ],
       "creator": "Girolamo da Santa Croce, Italian, active 1503-1556",
       "label": "The Annunciation",
       "date": "c. 1540",
       "relation": "",
       "coverage": [
           "",
           "Repository: Columbia Museum of Art, Columbia, SC"
       ],
       "datestamp": "2011-11-07",
       "title": "The Annunciation",
       "type": [
           "Paintings",
           "Painting"
       ],
       "id": "oai:oaicat.oclc.org:AKRESS_10310356237",
       "subject": "Annunciation: Mary, Usually Reading, Is Visited by the Angel"
   },
   "sourceResource": {
       "rights": [
           "",
           "Please note that if this image is under copyright, you may need to contact one or more copyright owners for any use that is not permitted under the ARTstor Terms and Conditions of Use or not otherwise permitted by law. While ARTstor tries to update contact information, it cannot guarantee that such information is always accurate. Determining whether those permissions are necessary, and obtaining such permissions, is your sole responsibility."
       ],
       "title": "The Annunciation",
       "creator": "Girolamo da Santa Croce, Italian, active 1503-1556",
       "physicalmedium": [
           "Paintings",
           "Painting",
           "55.6 x 71.1 cm",
           "Oil on wood panel"
       ],
       "relation": "",
       "spatial": [
           {
               "name": ""
           },
           {
               "state": "South Carolina",
               "name": "Repository: Columbia Museum of Art, Columbia, SC",
               "iso3166-2": "US-SC"
           }
       ],
       "date": "c. 1540",
       "type": null,
       "subject": [
           {
               "name": "Annunciation: Mary, Usually Reading, Is Visited by the Angel"
           }
       ]
   },
   "collection": {
       "@id": "http://dp.la/api/collections/artstor--SetDPLA",
       "name": "SetDPLA",
       "description": "SetDPLA"
   },
   "ingestDate": "2013-02-14T07:33:20.207815",
   "provider": {
       "@id": "http://dp.la/api/contributor/artstor",
       "name": "ARTstor OAICatMuseum"
   },
   "isShownAt": "Thumbnail: http://media.artstor.net/imgstor/size2/kress/d0001/kress_1103_post.jpg",
   "ingestType": "item",
   "@id": "http://dp.la/api/items/6ae54cee603f75c275fd913e04c49a3f",
   "id": "6ae54cee603f75c275fd913e04c49a3f"
}
    """

    EXPECTED_PREVIEW = "http://media.artstor.net/imgstor/size2/kress/d0001/kress_1103_post.jpg"

    url = server() + "artstor_identify_object"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2")

    doc = json.loads(content)
    assert u"object" in doc, "objectpath not found in document"
    FETCHED_PREVIEW = doc[u"object"]
    assert FETCHED_PREVIEW == EXPECTED_PREVIEW, "%s != %s" % (FETCHED_PREVIEW, EXPECTED_PREVIEW)

def test_artstor_source_fetching():
    """
    Fetching Artstor document source
    """

    INPUT_JSON = """
    {
   "_id": "artstor--oai:oaicat.oclc.org:AKRESS_10310356237",
   "_rev": "1-7ac8a9618041eff32ad52ad44d95e69c",
   "@context": {
       "begin": {
           "@id": "dpla:dateRangeStart",
           "@type": "xsd:date"
       },
       "@vocab": "http://purl.org/dc/terms/",
       "hasView": "edm:hasView",
       "name": "xsd:string",
       "object": "edm:object",
       "sourceResource": "edm:sourceResource",
       "dpla": "http://dp.la/terms/",
       "collection": "dpla:aggregation",
       "edm": "http://www.europeana.eu/schemas/edm/",
       "end": {
           "@id": "dpla:dateRangeEnd",
           "@type": "xsd:date"
       },
       "state": "dpla:state",
       "aggregatedDigitalResource": "dpla:aggregatedDigitalResource",
       "coordinates": "dpla:coordinates",
       "provider": "edm:provider",
       "stateLocatedIn": "dpla:stateLocatedIn",
       "isShownAt": "edm:isShownAt",
       "dataProvider": "edm:dataProvider",
       "originalRecord": "dpla:originalRecord",
       "LCSH": "http://id.loc.gov/authorities/subjects"
   },
   "originalRecord": {
       "rights": [
           "",
           "Please note that if this image is under copyright, you may need to contact one or more copyright owners for any use that is not permitted under the ARTstor Terms and Conditions of Use or not otherwise permitted by law. While ARTstor tries to update contact information, it cannot guarantee that such information is always accurate. Determining whether those permissions are necessary, and obtaining such permissions, is your sole responsibility."
       ],
       "handle": [
           "Thumbnail: http://media.artstor.net/imgstor/size2/kress/d0001/kress_1103_post.jpg",
           "Image View: http://www.artstor.org/artstor/ViewImages?id=8DtZYyMmJloyLyw7eDt5QHgt&userId=gDBAdA%3D%3D",
           "Ranking: 13000"
       ],
       "format": [
           "55.6 x 71.1 cm",
           "Oil on wood panel"
       ],
       "creator": "Girolamo da Santa Croce, Italian, active 1503-1556",
       "label": "The Annunciation",
       "date": "c. 1540",
       "relation": "",
       "coverage": [
           "",
           "Repository: Columbia Museum of Art, Columbia, SC"
       ],
       "datestamp": "2011-11-07",
       "title": "The Annunciation",
       "type": [
           "Paintings",
           "Painting"
       ],
       "id": "oai:oaicat.oclc.org:AKRESS_10310356237",
       "subject": "Annunciation: Mary, Usually Reading, Is Visited by the Angel"
   },
   "sourceResource": {
       "rights": [
           "",
           "Please note that if this image is under copyright, you may need to contact one or more copyright owners for any use that is not permitted under the ARTstor Terms and Conditions of Use or not otherwise permitted by law. While ARTstor tries to update contact information, it cannot guarantee that such information is always accurate. Determining whether those permissions are necessary, and obtaining such permissions, is your sole responsibility."
       ],
       "title": "The Annunciation",
       "creator": "Girolamo da Santa Croce, Italian, active 1503-1556",
       "physicalmedium": [
           "Paintings",
           "Painting",
           "55.6 x 71.1 cm",
           "Oil on wood panel"
       ],
       "relation": "",
       "spatial": [
           {
               "name": ""
           },
           {
               "state": "South Carolina",
               "name": "Repository: Columbia Museum of Art, Columbia, SC",
               "iso3166-2": "US-SC"
           }
       ],
       "date": "c. 1540",
       "type": null,
       "subject": [
           {
               "name": "Annunciation: Mary, Usually Reading, Is Visited by the Angel"
           }
       ]
   },
   "collection": {
       "@id": "http://dp.la/api/collections/artstor--SetDPLA",
       "name": "SetDPLA",
       "description": "SetDPLA"
   },
   "ingestDate": "2013-02-14T07:33:20.207815",
   "provider": {
       "@id": "http://dp.la/api/contributor/artstor",
       "name": "ARTstor OAICatMuseum"
   },
   "isShownAt": "Thumbnail: http://media.artstor.net/imgstor/size2/kress/d0001/kress_1103_post.jpg",
   "ingestType": "item",
   "@id": "http://dp.la/api/items/6ae54cee603f75c275fd913e04c49a3f",
   "id": "6ae54cee603f75c275fd913e04c49a3f"
}
    """

    EXPECTED_SOURCE = "http://www.artstor.org/artstor/ViewImages?id=8DtZYyMmJloyLyw7eDt5QHgt&userId=gDBAdA%3D%3D"

    url = server() + "artstor_select_isshownat"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2")
    FETCHED_SOURCE = json.loads(content)[u"isShownAt"]
    assert FETCHED_SOURCE == EXPECTED_SOURCE

def test_artstor_cleanup_data_provider():
    """
    Remove "Repository:" from Artstor data provider
    """

    INPUT = {
        "dataProvider":
                "Repository: Columbia Museum of Art, Columbia, SC"
    }
    EXPECTED = {
        "dataProvider": "Columbia Museum of Art, Columbia, SC"
    }
    url = server() + "artstor_cleanup"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    data = json.loads(content)
    assert data["dataProvider"] == EXPECTED["dataProvider"], DictDiffer(data, EXPECTED).diff()

def test_artstor_cleanup_creator1():
    """
    Cleanup the creator field
    """

    INPUT = {
        "sourceResource": {
            "creator": "And bananas"
        }
    }
    EXPECTED = {
        "sourceResource": {
            "creator": "bananas"
        }
    }

    url = server() + "artstor_cleanup_creator"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    data = json.loads(content)
    assert data == EXPECTED, DictDiffer(data, EXPECTED).diff() 

def test_artstor_cleanup_creator2():
    """
    Cleanup the creator field
    """

    INPUT = {
        "sourceResource": {
            "creator": [
                " and bananas",
                "   Artist: bananas",
                "Author: bananas",
                "Binder: bananas",
                "Drawn by bananas",
                "drawn by bananas",
                "  illuminator: bananas",
                "Or    bananas  ",
                "Scribe: bananas",
                "Resolve bananas",
                " Apples"
            ]
        }
    }
    EXPECTED = {
        "sourceResource": {
            "creator": [
                "bananas",
                "bananas",
                "bananas",
                "bananas",
                "bananas",
                "bananas",
                "bananas",
                "bananas",
                "bananas",
                "bananas",
                "Apples"
            ]
        }
    }

    url = server() + "artstor_cleanup_creator"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    data = json.loads(content)
    assert data == EXPECTED, DictDiffer(data, EXPECTED).diff() 

def test_artstor_cleanup_creator3():
    """
    Should do nothing since creator field does not exist
    """

    INPUT = {
        "sourceResource": {
            "subject": "bananas"
        }
    }

    url = server() + "artstor_cleanup_creator"
    resp, content = H.request(url, "POST", body=json.dumps(INPUT))
    assert str(resp.status).startswith("2")
    data = json.loads(content)
    assert data == INPUT, DictDiffer(data, INPUT).diff() 

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
