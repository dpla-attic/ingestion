# coding: utf-8

from server_support import server, H
from amara.thirdparty import json
from nose.tools import nottest


def test_ia_identify_object():
    """Fetching Internet Archive document thumbnail"""

    INPUT_JSON = """
    {
   "originalRecord": {
       "_id": "test_id",
       "files": {"gif": "test_id.gif"}
       }
    }
    """

    EXPECTED_PREVIEW = "http://www.archive.org/download/test_id/test_id.gif"

    url = server() + "ia_identify_object"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2"), str(resp) + "\n" + content

    doc = json.loads(content)
    assert u"object" in doc, "object path not found in document"
    FETCHED_PREVIEW = doc[u"object"]
    assert FETCHED_PREVIEW == EXPECTED_PREVIEW, "%s != %s" % (FETCHED_PREVIEW, EXPECTED_PREVIEW)

def test_marc_processor():
    """IA MARC Processor"""
    INPUT_JSON = u"""
    {
   "files": {
       "shown_at": "http://archive.org/details/cosmossketchofph185601humb",
       "marc": "cosmossketchofph185601humb_marc.xml",
       "dc": "cosmossketchofph185601humb_dc.xml",
       "gif": "cosmossketchofph185601humb.gif",
       "meta": "cosmossketchofph185601humb_meta.xml",
       "pdf": "cosmossketchofph185601humb.pdf"
   },
   "record": {
       "controlfield": [
           {
               "#text": "965977",
               "tag": "001"
           },
           {
               "#text": "19910311075938.0",
               "tag": "005"
           },
           {
               "#text": "740729m18501860nyuc          00010 eng  ",
               "tag": "008"
           }
       ],
       "xmlns": "http://www.loc.gov/MARC21/slim",
       "leader": "01083cam a2200277I  4500",
       "datafield": [
           {
               "subfield": {
                   "code": "a",
                   "#text": "18002543 //r37"
               },
               "tag": "010",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "DLC"
                   },
                   {
                       "code": "c",
                       "#text": "BCT"
                   },
                   {
                       "code": "d",
                       "#text": "m.c."
                   },
                   {
                       "code": "d",
                       "#text": "SER"
                   },
                   {
                       "code": "d",
                       "#text": "OCL"
                   },
                   {
                       "code": "d",
                       "#text": "WCM"
                   }
               ],
               "tag": "040",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "eng"
                   },
                   {
                       "code": "h",
                       "#text": "ger"
                   }
               ],
               "tag": "041",
               "ind1": "1",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "WCMM"
               },
               "tag": "049",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Q158"
                   },
                   {
                       "code": "b",
                       "#text": ".H9"
                   }
               ],
               "tag": "050",
               "ind1": "0",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Humboldt, Alexander von,"
                   },
                   {
                       "code": "d",
                       "#text": "1769-1859."
                   }
               ],
               "tag": "100",
               "ind1": "1",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Cosmos :"
                   },
                   {
                       "code": "b",
                       "#text": "a sketch of a physical description of the universe /"
                   },
                   {
                       "code": "c",
                       "#text": "by Alexander von Humboldt ; translated from the German, by E.C. Otte."
                   }
               ],
               "tag": "245",
               "ind1": "1",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "New York :"
                   },
                   {
                       "code": "b",
                       "#text": "Harper & Brothers,"
                   },
                   {
                       "code": "c",
                       "#text": "1850-1860"
                   },
                   {
                       "code": "g",
                       "#text": "(1856-1860 printing)"
                   }
               ],
               "tag": "260",
               "ind1": "0",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "5 v. :"
                   },
                   {
                       "code": "b",
                       "#text": "port. ;"
                   },
                   {
                       "code": "c",
                       "#text": "21 cm."
                   }
               ],
               "tag": "300",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "Reprinted from Bohn's edition, in his Scientific library, London, 1849-58."
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "Vol. 4, translated by E.C. Otte and B.H. Paul; v. 5, by Otte and W.S. Dallas."
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "Cosmography."
               },
               "tag": "650",
               "ind1": " ",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Science"
                   },
                   {
                       "code": "x",
                       "#text": "History."
                   }
               ],
               "tag": "650",
               "ind1": " ",
               "ind2": "0"
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "Astronomy."
               },
               "tag": "650",
               "ind1": " ",
               "ind2": "0"
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "Physical geography."
               },
               "tag": "650",
               "ind1": " ",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Otte, E. C."
                   },
                   {
                       "code": "q",
                       "#text": "(Elise C.),"
                   },
                   {
                       "code": "e",
                       "#text": "tr."
                   }
               ],
               "tag": "700",
               "ind1": "1",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Paul, B. H."
                   },
                   {
                       "code": "q",
                       "#text": "(Benjamin Horatio),"
                   },
                   {
                       "code": "e",
                       "#text": "tr."
                   }
               ],
               "tag": "700",
               "ind1": "1",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Dallas, W. S."
                   },
                   {
                       "code": "q",
                       "#text": "(William Sweetland),"
                   },
                   {
                       "code": "d",
                       "#text": "1824-1890,"
                   },
                   {
                       "code": "e",
                       "#text": "tr."
                   }
               ],
               "tag": "700",
               "ind1": "1",
               "ind2": "0"
           },
           {
                   "subfield": [
                       {
                           "code": "a",
                           "#text": "Massachusetts Institute of Technology. Alfred P. Sloan School of Management. Working papers,"
                       },
                       {
                           "code": "v",
                           "#text": "441-70"
                       }
                   ],
                   "tag": "490",
                   "ind1": "1",
                   "ind2": " "
            }
       ]
   },
   "_id": "cosmossketchofph185601humb"

}
    """
    url = server() + "ia-to-dpla"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2"), str(resp) + "\n" + content
    doc = json.loads(content)
    assert "sourceResource" in doc, "sourceResource field is absent"
    sr = doc["sourceResource"]
    assert "contributor" in sr, "contributor not found"
    assert "extent" in sr, "extent not found"
    assert "format" in sr, "format not found"
    assert "isPartOf" in sr, "isPartOf not found"

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
