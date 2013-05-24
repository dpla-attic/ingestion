# coding: utf-8

from server_support import server, H
from amara.thirdparty import json


def test_ia_spatial_cleanup():
    """Cleanup trailing period at spatial field"""

    INPUT_JSON = """
    {
   "sourceResource": {
       "spatial": "New Hampshire, Durham."
       }
    }
    """

    EXPECTED = "New Hampshire, Durham"

    url = server() + "cleanup_value?prop=sourceResource%2Fspatial"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2"), str(resp) + "\n" + content

    doc = json.loads(content)
    FETCHED = doc[u"sourceResource"][u"spatial"]
    assert FETCHED == EXPECTED, "%s != %s" % (FETCHED, EXPECTED)


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
                           "#text": "Insurance, Health"
                       },
                       {
                           "code": "z",
                           "#text": "Massachusetts."
                       }
                   ],
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
    #assert "contributor" in sr, "contributor not found"
    assert "extent" in sr, "extent not found"
    #assert "format" in sr, "format not found"
    assert "isPartOf" in sr, "isPartOf not found"
    assert "spatial" in sr, "spatial not found"


def test_spatial_mapping():
    """IA Spatial Mapping"""
    INPUT_JSON = u"""
    {
   "files": {
       "shown_at": "http://archive.org/details/yournewhealthins00mass",
       "marc": "yournewhealthins00mass_marc.xml",
       "dc": "yournewhealthins00mass_dc.xml",
       "gif": "yournewhealthins00mass.gif",
       "meta": "yournewhealthins00mass_meta.xml",
       "pdf": "yournewhealthins00mass.pdf"
   },
   "record": {
       "controlfield": [
           {
               "#text": "003738002",
               "tag": "001"
           },
           {
               "#text": "19970709093430.0",
               "tag": "005"
           },
           {
               "#text": "970306s1997    mau          s000 0 eng d",
               "tag": "008"
           }
       ],
       "xmlns": "http://www.loc.gov/MARC21/slim",
       "leader": "01130nam a2200313Ia 4500",
       "datafield": [
           {
               "subfield": {
                   "code": "a",
                   "#text": "(OCoLC)ocm36495517"
               },
               "tag": "035",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "UMA-27286149"
               },
               "tag": "035",
               "ind1": "9",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "MAS"
                   },
                   {
                       "code": "c",
                       "#text": "MAS"
                   }
               ],
               "tag": "040",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "n-us-ma"
               },
               "tag": "043",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "MASS. AG 1.2:Y 88"
               },
               "tag": "099",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Your new health insurance rights :"
                   },
                   {
                       "code": "b",
                       "#text": "a guide to new laws that improve your access to health insurance coverage."
                   }
               ],
               "tag": "245",
               "ind1": "0",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Boston, Mass. :"
                   },
                   {
                       "code": "b",
                       "#text": "Office of the Attorney General,"
                   },
                   {
                       "code": "c",
                       "#text": "1997."
                   }
               ],
               "tag": "260",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "1 folded sheet (8) p. ;"
                   },
                   {
                       "code": "c",
                       "#text": "34 x 22 cm. folded to 22 x 9 cm."
                   }
               ],
               "tag": "300",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "January 1997."
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "Title from cover."
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Insurance, Health"
                   },
                   {
                       "code": "z",
                       "#text": "Massachusetts."
                   }
               ],
               "tag": "650",
               "ind1": " ",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Medically uninsured persons"
                   },
                   {
                       "code": "z",
                       "#text": "Massachusetts."
                   }
               ],
               "tag": "650",
               "ind1": " ",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Massachusetts."
                   },
                   {
                       "code": "b",
                       "#text": "Attorney General's Office"
                   }
               ],
               "tag": "710",
               "ind1": "1",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "b",
                       "#text": "UMDUB"
                   },
                   {
                       "code": "c",
                       "#text": "UDMAS"
                   },
                   {
                       "code": "h",
                       "#text": "MASS. AG 1.2:Y 88"
                   }
               ],
               "tag": "852",
               "ind1": "8",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "uad"
                   },
                   {
                       "code": "b",
                       "#text": "07-09-97"
                   },
                   {
                       "code": "c",
                       "#text": "m"
                   },
                   {
                       "code": "d",
                       "#text": "a"
                   },
                   {
                       "code": "e",
                       "#text": "r"
                   },
                   {
                       "code": "f",
                       "#text": "eng"
                   },
                   {
                       "code": "g",
                       "#text": "mau"
                   },
                   {
                       "code": "h",
                       "#text": "0"
                   }
               ],
               "tag": "998",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "CONV"
                   },
                   {
                       "code": "b",
                       "#text": "20"
                   },
                   {
                       "code": "c",
                       "#text": "20060724"
                   },
                   {
                       "code": "l",
                       "#text": "FCL01"
                   },
                   {
                       "code": "h",
                       "#text": "1016"
                   }
               ],
               "tag": "956",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "BATCH-UPD"
                   },
                   {
                       "code": "b",
                       "#text": "20"
                   },
                   {
                       "code": "c",
                       "#text": "20090922"
                   },
                   {
                       "code": "l",
                       "#text": "FCL01"
                   },
                   {
                       "code": "h",
                       "#text": "1911"
                   }
               ],
               "tag": "956",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "BATCH-UPD"
                   },
                   {
                       "code": "b",
                       "#text": "20"
                   },
                   {
                       "code": "c",
                       "#text": "20110525"
                   },
                   {
                       "code": "l",
                       "#text": "FCL01"
                   },
                   {
                       "code": "h",
                       "#text": "0751"
                   }
               ],
               "tag": "956",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": ".b27286149"
                   },
                   {
                       "code": "b",
                       "#text": "06-21-00"
                   },
                   {
                       "code": "c",
                       "#text": "07-09-97"
                   }
               ],
               "tag": "907",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "a",
                   "#text": "AUN4"
               },
               "tag": "049",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "code": "0",
                   "#text": "3738002"
               },
               "tag": "903",
               "ind1": " ",
               "ind2": " "
           }
       ]
   },
   "_id": "yournewhealthins00mass",
   "metadata": {
       "scanner": "scribe5.boston.archive.org",
       "creator": "Massachusetts. Attorney General's Office",
       "ppi": "400",
       "operator": "associate-gordon-weissmann@archive.org",
       "sponsor": "Boston Library Consortium Member Libraries",
       "contributor": "UMass Amherst Libraries",
       "scanningcenter": "boston",
       "scanfee": "250",
       "subject": [
           "Insurance, Health",
           "Medically uninsured persons"
       ],
       "call_number": "003738002",
       "title": "Your new health insurance rights : a guide to new laws that improve your access to health insurance coverage",
       "repub_state": "4",
       "camera": "Canon EOS 5D Mark II",
       "ocr": "ABBYY FineReader 8.0",
       "foldoutcount": "0",
       "description": [
           "January 1997.",
           "Title from cover"
       ],
       "identifier-ark": "ark:/13960/t7fr15z0b",
       "mediatype": "texts",
       "collection": [
           "umass_amherst_libraries",
           "blc",
           "americana"
       ],
       "repub_seconds": "38",
       "updater": "Associate-Tim-Bigelow",
       "updatedate": "2012-12-11 16:05:21",
       "uploader": "Associate-Tim-Bigelow@archive.org",
       "date": "1997",
       "republisher": "associate-jordan-underhill@archive.org",
       "addeddate": "2012-12-11 16:05:23",
       "publisher": "Boston, Mass. : Office of the Attorney General",
       "publicdate": "2012-12-11 16:05:26",
       "language": "eng",
       "page-progression": "lr",
       "notes": "No title page found. No copyright page found. No table-of-contents pages found.",
       "identifier-access": "http://archive.org/details/yournewhealthins00mass",
       "identifier": "yournewhealthins00mass",
       "sponsordate": "20121231",
       "imagecount": "6",
       "scandate": "20121212162534"
   }
}
    """
    url = server() + "ia-to-dpla"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2"), str(resp) + "\n" + content
    doc = json.loads(content)
    assert "sourceResource" in doc, "sourceResource field is absent"
    sr = doc["sourceResource"]
    assert "spatial" in sr, "spatial not found"
    spatial = sr["spatial"]
    expected_spatial = "Massachusetts."
    assert expected_spatial == spatial, "%s != %s" % (expected_spatial, spatial)


def test_multi_spatial_mapping():
    """IA Multi Spatial Mapping"""
    INPUT_JSON = u"""
    {
   "files": {
       "shown_at": "http://archive.org/details/yournewhealthins00mass",
       "marc": "yournewhealthins00mass_marc.xml",
       "dc": "yournewhealthins00mass_dc.xml",
       "gif": "yournewhealthins00mass.gif",
       "meta": "yournewhealthins00mass_meta.xml",
       "pdf": "yournewhealthins00mass.pdf"
   },
   "record": {
       "datafield": [
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Insurance, Health"
                   },
                   {
                       "code": "z",
                       "#text": "value1"
                   }
               ],
               "tag": "650",
               "ind1": " ",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "code": "a",
                       "#text": "Medically uninsured persons"
                   },
                   {
                       "code": "z",
                       "#text": "value2"
                   }
               ],
               "tag": "650",
               "ind1": " ",
               "ind2": "0"
           }
       ]
   },
   "_id": "yournewhealthins00mass"
}
    """
    url = server() + "ia-to-dpla"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2"), str(resp) + "\n" + content
    doc = json.loads(content)
    assert "sourceResource" in doc, "sourceResource field is absent"
    sr = doc["sourceResource"]
    assert "spatial" in sr, "spatial not found"
    spatial = sr["spatial"]
    expected_spatial = ["value1", "value2"]
    assert expected_spatial == spatial, "%s != %s" % (expected_spatial, spatial)


if __name__ == "__main__":
    raise SystemExit("Use nosetests")
