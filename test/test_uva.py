# coding: utf-8

from server_support import server, H
from amara.thirdparty import json
from nose.tools import nottest


def test_uva_creator():
    """UVA Creator Mapping"""

    INPUT_JSON = """
    {
   "physicalDescription": {
       "internetMediaType": "image/jpeg",
       "note": [
           {
               "displayLabel": "size inches",
               "#text": "5x7"
           },
           {
               "displayLabel": "condition"
           }
       ],
       "digitalOrigin": "reformatted digital",
       "form": {
           "#text": "Glass negatives",
           "authority": "tgm"
       }
   },
   "xmlns": "http://www.loc.gov/mods/v3",
   "xmlns:mods": "http://www.loc.gov/mods/v3",
   "_id": "uva-lib:1038850",
   "titleInfo": {
       "title": "Farmington "
   },
   "subject": [
       {
           "topic": "Photography",
           "authority": "lcsh"
       },
       {
           "topic": "Country clubs",
           "authority": "lcsh"
       },
       {
           "topic": "Dining rooms",
           "authority": "lcsh"
       },
       {
           "topic": "Furniture",
           "authority": "lcsh"
       },
       {
           "geographic": "Albemarle County (Va.)",
           "authority": "lcsh"
       },
       {
           "topic": "Interior architecture",
           "authority": "lcsh"
       },
       {
           "name": {
               "namePart": "Holsinger Studio (Charlottesville, Va.)",
               "type": "corporate",
               "authority": "lcnaf"
           },
           "authority": "lcsh"
       },
       {
           "geographic": " Charlottesville (Va.)",
           "authority": "lcsh"
       }
   ],
   "recordInfo": {
       "recordOrigin": "University of Virginia Library",
       "recordCreationDate": {
           "#text": "2007-08-23T12:15:11",
           "encoding": "iso8601"
       },
       "recordChangeDate": {
           "#text": "2009-07-23T10:35:57.04-04:00",
           "encoding": "iso8601"
       },
       "languageOfCataloging": {
           "languageTerm": {
               "#text": "eng",
               "type": "code",
               "authority": "iso639-2b"
           }
       },
       "recordContentSource": "University of Virginia Library"
   },
   "relatedItem": {
       "displayLabel": "Part of",
       "originInfo": {
           "place": {
               "placeTerm": {
                   "#text": "Charlottesville (Va.)",
                   "type": "text"
               }
           },
           "dateCreated": [
               {
                   "encoding": "w3cdtf",
                   "#text": "1889",
                   "point": "start"
               },
               {
                   "encoding": "w3cdtf",
                   "#text": "1939",
                   "point": "end"
               }
           ]
       },
       "titleInfo": {
           "nonSort": "The",
           "title": "Holsinger Studio Collection"
       },
       "type": "series",
       "name": {
           "namePart": [
               {
                   "#text": "Rufus W.",
                   "type": "given"
               },
               {
                   "#text": "Holsinger",
                   "type": "family"
               },
               {
                   "#text": "1866-1930",
                   "type": "date"
               }
           ],
           "type": "personal",
           "authority": "lcnaf"
       }
   },
   "note": {
       "displayLabel": "staff",
       "#text": "Interior"
   },
   "accessCondition": [
       {
           "displayLabel": "Access to the Collection",
           "type": "restrictionOnAccess"
       },
       {
           "displayLabel": "Use of the Collection",
           "#text": "This image may be reproduced without additional permission but must be credited © Rector and Visitors of the University of Virginia.",
           "type": "useAndReproduction"
       }
   ],
   "version": "3.3",
   "originInfo": {
       "dateCreated": {
           "#text": "1915-08-20",
           "keyDate": "yes",
           "encoding": "w3cdtf"
       }
   },
   "location": [
       {
           "url": [
               {
                   "access": "object in context",
                   "#text": "http://search.lib.virginia.edu/catalog/uva-lib:1038850"
               },
               {
                   "access": "preview",
                   "#text": "http://fedoraproxy.lib.virginia.edu/fedora/objects/uva-lib:1038850/methods/djatoka:StaticSDef/getThumbnail"
               },
               {
                   "access": "raw object",
                   "#text": "http://fedoraproxy.lib.virginia.edu/fedora/objects/uva-lib:1038850/methods/djatoka:StaticSDef/getStaticImage"
               },
               null
           ]
       },
       {
           "physicalLocation": "Special Collections, University of Virginia Library, Charlottesville, Va."
       },
       {
           "physicalLocation": {
               "#text": "VA@",
               "authority": "oclcorg"
           }
       }
   ],
   "genre": "Photographs",
   "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
   "identifier": [
       {
           "displayLabel": "Negative Number",
           "#text": "H03784B",
           "type": "legacy"
       },
       {
           "displayLabel": "UVA Library Fedora Repository PID",
           "#text": "uva-lib:1038850",
           "type": "pid"
       },
       {
           "displayLabel": "Special Collections legacy database record number",
           "#text": "40348",
           "type": "legacy",
           "invalid": "yes"
       },
       {
           "displayLabel": "Accessible index record displayed in VIRGO",
           "#text": "uva-lib:1038850",
           "type": "uri"
       },
       {
           "displayLabel": "Digitization Services Tracksys Unit ID",
           "#text": "7688",
           "type": "local"
       },
       {
           "displayLabel": "Digitization Services Tracksys MasterFile ID",
           "#text": "365108",
           "type": "local"
       },
       {
           "displayLabel": "Digitization Services Archive Filename",
           "#text": "000007688_0006.tif",
           "type": "local"
       },
       {
           "displayLabel": "Collection Accession Number",
           "#text": "MSS 9862",
           "type": "accessionNumber"
       }
   ],
   "xsi:schemaLocation": "http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-3.xsd",
   "typeOfResource": {
       "#text": "still image",
       "collection": "yes"
   },
   "name": {
       "role": {
           "roleTerm": {
               "#text": "pht",
               "type": "code",
               "authority": "marcrelator"
           }
       },
       "namePart": [
           {
               "#text": "Rufus W.",
               "type": "given"
           },
           {
               "#text": "Holsinger",
               "type": "family"
           },
           {
               "#text": "1866-1930",
               "type": "date"
           }
       ],
       "type": "personal",
       "authority": "lcnaf"
   }
}
    """

    EXPECTED_CREATOR = ["Holsinger, Rufus W., 1866-1930"]

    url = server() + "mods-to-dpla?provider=UVA"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2"), str(resp) + "\n" + content

    doc = json.loads(content)
    assert u"sourceResource" in doc, "sourceResource path not found in document"
    assert u"creator" in doc[u"sourceResource"], "creator path not found in sourceResource"
    FETCHED_CREATOR = doc[u"sourceResource"][u"creator"]
    assert FETCHED_CREATOR == EXPECTED_CREATOR, "%s != %s" % (FETCHED_CREATOR, EXPECTED_CREATOR)


if __name__ == "__main__":
    raise SystemExit("Use nosetests")
