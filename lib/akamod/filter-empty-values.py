"""
 Akara module that cleans empty lives of given json;
 to test locally, save TEST_EXAMPLE to local file (i.e. artstor_doc.js) and then run:

 url -X POST -d @test/artstor_doc.js -H "Content-Type: application/json" http://localhost:8879/filter_empty_values
"""

__author__ = 'Alexey R.'

import sys
import copy

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_TYPE_JSON = 'application/json'
HTTP_TYPE_TEXT = 'text/plain'
HTTP_HEADER_TYPE = 'Content-Type'


TEST_EXAMPLE = """{
   "_id": "artstor--oai:oaicat.oclc.org:AKRESS_10310356231",
   "_rev": "1-56c6a0d094f7642d53dc0f5fb7dc0c6b",
   "description": "Full view; pre-restoration; Formerly in the Contini Bonacossi Collection, Florence.",
   "rights": [
       "",
       "Please note that if this image is under copyright, you may need to contact one or more copyright owners for any use that is not permitted under the ARTstor Terms and Conditions of Use or not otherwise permitted by law. While ARTstor tries to update contact information, it cannot guarantee that such information is always accurate. Determining whether those permissions are necessary, and obtaining such permissions, is your sole responsibility."
   ],
   "dplaSourceRecord": {
       "handle": [
           "Thumbnail: http://media.artstor.net/imgstor/size2/kress/d0001/kress_1091_post_as.jpg",
           "Image View: http://www.artstor.org/artstor/ViewImages?id=8DtZYyMmJloyLyw7eDt5QHgr&userId=gDBAdA%3D%3D",
           "Ranking: 13000"
       ],
       "description": "Full view; pre-restoration; Formerly in the Contini Bonacossi Collection, Florence.",
       "format": [
           "41.5 x 23.3 cm",
           "Tempera and gold leaf on poplar panel",
           "panel "
       ],
       "rights": [
           "",
           "Please note that if this image is under copyright, you may need to contact one or more copyright owners for any use that is not permitted under the ARTstor Terms and Conditions of Use or not otherwise permitted by law. While ARTstor tries to update contact information, it cannot guarantee that such information is always accurate. Determining whether those permissions are necessary, and obtaining such permissions, is your sole responsibility."
       ],
       "label": "Madonna and Child with Saints",
       "datestamp": "2011-11-07",
       "creator": "Guariento d'Arpo, Italian, c. 1310-1368/70",
       "coverage": [
           "",
           "Repository: North Carolina Museum of Art."
       ],
       "date": "c. 1360",
       "title": "Madonna and Child with Saints",
       "type": [
           "Paintings",
           "Painting"
       ],
       "id": "oai:oaicat.oclc.org:AKRESS_10310356231",
       "subject": "Madonna and Child with Saints; Virgin and Child.; Francis, of Assisi, Saint, 1182-1226.; Giles, Saint, 8th century.; Virgin and Child--with Saint(s).; Jesus Christ.; Mary, Blessed Virgin, Saint.; Anthony, Abbot, Saint, ca. 251-356.; John, the Baptist, Saint."
   },
   "created": {
       "start": "1360-01-01",
       "end": "1360-01-01",
       "displayDate": "c. 1360"
   },
   "ingestDate": "2013-02-02T01:12:27.659532",
   "dplaContributor": {
       "@id": "http://dp.la/api/contributor/artstor",
       "name": "ARTstor OAICatMuseum"
   },
   "TBD_physicalformat": [
       "41.5 x 23.3 cm",
       "Tempera and gold leaf on poplar panel",
       "panel"
   ],
   "creator": "Guariento d'Arpo, Italian, c. 1310-1368/70",
   "title": "Madonna and Child with Saints",
   "source": "Thumbnail: http://media.artstor.net/imgstor/size2/kress/d0001/kress_1091_post_as.jpg",
   "spatial": [
       {
           "name": ""
       },
       {
           "name": "Repository: North Carolina Museum of Art."
       }
   ],
   "isPartOf": {
       "@id": "http://dp.la/api/collections/artstor--SetDPLA",
       "name": "SetDPLA",
       "description": "SetDPLA"
   },
   "@context": {
       "iso639": "dpla:iso639",
       "@vocab": "http://purl.org/dc/terms/",
       "end": {
           "@id": "dpla:end",
           "@type": "xsd:date"
       },
       "name": "xsd:string",
       "dplaSourceRecord": "dpla:sourceRecord",
       "dpla": "http://dp.la/terms/",
       "coordinates": "dpla:coordinates",
       "dplaContributor": "dpla:contributor",
       "start": {
           "@id": "dpla:start",
           "@type": "xsd:date"
       },
       "state": "dpla:state",
       "iso3166-2": "dpla:iso3166-2",
       "LCSH": "http://id.loc.gov/authorities/subjects"
   },
   "ingestType": "item",
   "@id": "http://dp.la/api/items/36a3ac936167a48e2f4ddf5c99f5cf1d",
   "id": "36a3ac936167a48e2f4ddf5c99f5cf1d",
   "subject": [
       {
           "name": "Madonna and Child with Saints"
       },
       {
           "name": "Virgin and Child"
       },
       {
           "name": "Francis, of Assisi, Saint, 1182-1226"
       },
       {
           "name": "Giles, Saint, 8th century"
       },
       {
           "name": "Virgin and Child--with Saint(s)"
       },
       {
           "name": "Jesus Christ"
       },
       {
           "name": "Mary, Blessed Virgin, Saint"
       },
       {
           "name": "Anthony, Abbot, Saint, ca. 251-356"
       },
       {
           "name": "John, the Baptist, Saint"
       }
   ]
}
"""


def filter_empty_lives(d, ignore_keys=tuple()):
    """
    Removes keys with empty value from dictionary;
    Returns: cleaned dictionary;
    """
    def sub_cleaner(el):
        if isinstance(el, dict):
            return filter_empty_lives(copy.deepcopy(el), ignore_keys)
        else:
            return el

    for k, v in d.items():
        if k in ignore_keys:
            continue
        elif isinstance(v, dict) and v:
            d[k] = filter_empty_lives(v, ignore_keys)
        elif isinstance(v, list) and v:
            d[k] = [sub_cleaner(e) for e in v if e]
        else:
            if not v and k not in ignore_keys:
                del d[k]
    return d

def filter_empty_values(d, ignore_keys=tuple()):
    hash_before = hash(str(d))
    hash_current = None
    while hash_before != hash_current:
        hash_before = hash_current
        hash_current = hash(str(filter_empty_lives(d, ignore_keys)))
    return d

def test_filtering():
    d = {"v1": "", "v2": "value2", "v3": {"vv1": "", "vv2": "v_value2"}, "v4": {}, "v5": {"0": {"name": ""}, "1": {"name": "name_value_1"}}, "v6": ["", "vvalue6", {}, {"v_sub": ""}], "v7": [""]}
    print d, hash(str(d))
    print filter_empty_values(d), hash(str(d))
    d2 = json.loads(TEST_EXAMPLE)
    print d2, hash(str(d2))
    print filter_empty_values(d2, ("dplaSourceRecord",)), hash(str(d2))

@simple_service('POST', 'http://purl.org/la/dp/filter_empty_values', 'filter_empty_values', HTTP_TYPE_JSON)
def filter_empty_values_endpoint(body, ctype, ignore_key="dplaSourceRecord"):
    try:
        assert ctype == HTTP_TYPE_JSON, "%s is not %s" % (HTTP_HEADER_TYPE, HTTP_TYPE_JSON)
        data = json.loads(body)
    except Exception as e:
        error_text = "%s: %s" % (e.__class__.__name__, str(e))
        logger.exception(error_text)
        response.code = HTTP_INTERNAL_SERVER_ERROR
        response.add_header(HTTP_HEADER_TYPE, HTTP_TYPE_TEXT)
        return error_text

    ignore_keys = [k.strip() for k in ignore_key.split(",") if k]
    data = filter_empty_values(data, ignore_keys)
    return json.dumps(data)


def main(args):
    test_filtering()

if __name__ == "__main__":
    main(sys.argv)

