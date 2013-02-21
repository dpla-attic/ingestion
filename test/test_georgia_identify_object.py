from server_support import server

from amara.thirdparty import httplib2
from amara.thirdparty import json
from nose.tools import nottest


CT_JSON = {"Content-Type": "application/json"}
HEADERS = {
    "Content-Type": "application/json",
    "Context": "{}",
    }

H = httplib2.Http()


def test_georgia_identify_object():
    """Fetching Georgia document thumbnail"""

    INPUT_JSON = """
    {
   "_id": "georgia--http://archives.columbusstate.edu/LGunbyJonesDubose/frenchletter.php",
   "_rev": "1-211e68b21366f06d0401a12b0efafbe1",
   "originalRecord": {
       "publisher": "Columbus, Ga. : Columbus State University, Schwob Library, Archives",
       "handle": "http://archives.columbusstate.edu/LGunbyJonesDubose/frenchletter.php",
       "description": "Georgia author Augusta Jane Evans writes Mrs. Lucy Virginia French, Tennessee poet who wrote under the name of L'Inconue and editor of Atlanta's Crusader, in a letter dated January 13, 1861 to refuse the addition of her name to a pro-Union memorial to be presented at the Georgia State Convention. Evans indicates that she advocates secession. ",
       "rights": "Permission to publish original material from the Louise Jones Dubose Papers must be obtained from the Columbus State University Archives. Use of the following credit line for publication or exhibit is required: [Title of Collection], Columbus State University Archives, Columbus, Georgia.",
       "type": "Letters (correspondence)",
       "title": "Letter from Augusta Jane Evans to Mrs. L.V. French, January 13, 1861 ",
       "contributor": "Columbus State University. Archives",
       "label": "Letter from Augusta Jane Evans to Mrs. L.V. French, January 13, 1861 ",
       "source": [
           "4 p.",
           "Letter from Augusta Jane Evans to Mrs. G. W. French, January 13, 1861, Box 1, Folder 9, Louise Jones Dubose Papers, Columbus State University Archives"
       ],
       "coverage": [
           "Columbus (Ga.)",
           "Muscogee County (Ga.)",
           "1861-01-13"
       ],
       "date": "2005",
       "datestamp": "2011-12-21",
       "creator": "Evans, Augusta J. (Augusta Jane), 1835-1909",
       "id": "oai:dlg.galileo.usg.edu:columbus_mc2_evans2",
       "subject": [
           "Georgia. Convention of the People (1861 : Milledgeville and Savannah, Ga.)",
           "Secession--Georgia",
           "Georgia--Politics and government--1861-1865",
           "French, L. Virginia (Lucy Virginia), 1825-1881"
       ]
   },
   "aggregatedCHO": {
       "publisher": "Columbus, Ga. : Columbus State University, Schwob Library, Archives",
       "rights": "Permission to publish original material from the Louise Jones Dubose Papers must be obtained from the Columbus State University Archives. Use of the following credit line for publication or exhibit is required: [Title of Collection], Columbus State University Archives, Columbus, Georgia.",
       "description": "Georgia author Augusta Jane Evans writes Mrs. Lucy Virginia French, Tennessee poet who wrote under the name of L'Inconue and editor of Atlanta's Crusader, in a letter dated January 13, 1861 to refuse the addition of her name to a pro-Union memorial to be presented at the Georgia State Convention. Evans indicates that she advocates secession. ",
       "title": "Letter from Augusta Jane Evans to Mrs. L.V. French, January 13, 1861 ",
       "creator": "Evans, Augusta J. (Augusta Jane), 1835-1909",
       "physicalMedium": "Letters (correspondence)",
       "date": {
           "begin": "2005",
           "end": "2005",
           "displayDate": "2005"
       },
       "spatial": [
           {
               "state": "Georgia",
               "name": "Columbus (Ga.)",
               "iso3166-2": "US-GA"
           },
           {
               "state": "Georgia",
               "name": "Muscogee County (Ga.)",
               "iso3166-2": "US-GA"
           },
           {
               "name": "1861-01-13"
           }
       ],
       "contributor": "Columbus State University. Archives",
       "type": null,
       "subject": [
           {
               "name": "Georgia. Convention of the People (1861 : Milledgeville and Savannah, Ga.)"
           },
           {
               "name": "Secession--Georgia"
           },
           {
               "name": "Georgia--Politics and government--1861-1865"
           },
           {
               "name": "French, L. Virginia (Lucy Virginia), 1825-1881"
           }
       ]
   },
   "collection": {
       "@id": "http://dp.la/api/collections/georgia--dpla",
       "name": ""
   },
   "ingestDate": "2013-02-20T05:46:34.144484",
   "isShownAt": {
       "rights": "Permission to publish original material from the Louise Jones Dubose Papers must be obtained from the Columbus State University Archives. Use of the following credit line for publication or exhibit is required: [Title of Collection], Columbus State University Archives, Columbus, Georgia.",
       "@id": "http://archives.columbusstate.edu/LGunbyJonesDubose/frenchletter.php",
       "format": null
   },
   "provider": {
       "@id": "http://dp.la/api/contributor/georgia",
       "name": "Digital Library of Georgia"
   },
   "@context": {
       "begin": {
           "@id": "dpla:dateRangeStart",
           "@type": "xsd:date"
       },
       "@vocab": "http://purl.org/dc/terms/",
       "hasView": "edm:hasView",
       "name": "xsd:string",
       "object": "edm:object",
       "aggregatedCHO": "edm:aggregatedCHO",
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
   "ingestType": "item",
   "dataProvider": [
       "4 p.",
       "Letter from Augusta Jane Evans to Mrs. G. W. French, January 13, 1861, Box 1, Folder 9, Louise Jones Dubose Papers, Columbus State University Archives"
   ],
   "@id": "http://dp.la/api/items/2e33c1022969de45e50b9fb9d7e62f40",
   "id": "2e33c1022969de45e50b9fb9d7e62f40"
}
    """

    EXPECTED_PREVIEW = "http://dlg.galileo.usg.edu/columbus/mc2/do-th:evans2"

    url = server() + "georgia_identify_object"
    resp, content = H.request(url, "POST", body=INPUT_JSON, headers=CT_JSON)
    assert str(resp.status).startswith("2"), str(resp)

    doc = json.loads(content)
    assert u"object" in doc and u"@id" in doc[u"object"], "object/@id path not found in document"
    FETCHED_PREVIEW = doc[u"object"][u'@id']
    assert FETCHED_PREVIEW == EXPECTED_PREVIEW, "%s != %s" % (FETCHED_PREVIEW, EXPECTED_PREVIEW)

if __name__ == "__main__":
    raise SystemExit("Use nosetests")