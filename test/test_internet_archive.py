# coding: utf-8

from server_support import server, H
from amara.thirdparty import json
from dplaingestion.mappers.ia_mapper import IAMapper

def test_ia_add_to_list():
    """Test IAMapper._add_to_list() method"""
    mapper = IAMapper(None)
    lst = ["a", "b", "c"]
    EXPECTED_FIRST = ["a", "b", "c", "123"]
    EXPECTED_SECOND = ["a", "b", "c", "123", "x", "y", "z"]
    mapper._add_to_list(lst, "123")
    assert lst == EXPECTED_FIRST, "%s != %s" (lst, EXPECTED_FIRST)
    mapper._add_to_list(lst, ["x", "y", "z"])
    assert lst == EXPECTED_SECOND, "%s != %s" (lst, EXPECTED_SECOND)


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


def test_ia_title_and_volume_mapping():
    "Ensure `title` and `volume` are mapped appropriately from IA metadata"
    INPUT_JSON = """
    {
  "files": {
    "shown_at": "http://archive.org/details/annualreportd1985mass",
    "marc": "annualreportd1985mass_marc.xml",
    "dc": "annualreportd1985mass_dc.xml",
    "gif": "annualreportd1985mass.gif",
    "meta": "annualreportd1985mass_meta.xml",
    "pdf": "annualreportd1985mass.pdf"
  },
  "_id": "annualreportd1985mass",
  "metadata": {
    "title": [
      "Annual report",
      "Annual report - Department of Public Welfare"
    ],
    "volume": "1985",
    "creator": "Massachusetts. Dept. of Public Welfare",
    "subject": [
      "Massachusetts. Dept. of Public Welfare",
      "Public welfare administration"
    ],
    "description": "Title varies slightly",
    "publisher": "Boston, Mass. : The Department",
    "language": "eng",
    "page-progression": "lr",
    "sponsor": "Boston Library Consortium Member Libraries",
    "contributor": "UMass Amherst Libraries",
    "scanningcenter": "boston",
    "mediatype": "texts",
    "collection": [
      "umass_amherst_libraries",
      "blc",
      "americana"
    ],
    "call_number": "MASS. HS 20.1:",
    "repub_state": "4",
    "updatedate": "2013-02-20 20:34:48",
    "updater": "associate-nicholas-delancey",
    "identifier": "annualreportd1985mass",
    "uploader": "associate-nicholas-delancey@archive.org",
    "addeddate": "2013-02-20 20:34:50",
    "publicdate": "2013-02-20 20:34:55",
    "scanner": "scribe2.boston.archive.org",
    "notes": "No title page found. No copyright page found. No table-of-contents pages found.",
    "repub_seconds": "126",
    "ppi": "300",
    "camera": "Canon EOS 5D Mark II",
    "operator": "associate-lauren-dunn@archive.org",
    "scandate": "20130305135754",
    "republisher": "associate-emilie-pagano@archive.org",
    "imagecount": "26",
    "foldoutcount": "0",
    "identifier-access": "http://archive.org/details/annualreportd1985mass",
    "identifier-ark": "ark:/13960/t9b57xd66",
    "ocr": "ABBYY FineReader 8.0",
    "scanfee": "200",
    "sponsordate": "20130331"
  }
}
    """
    EXPECTED = [u'Annual report, 1985',
                u'Annual report - Department of Public Welfare, 1985']
    url = server() + "dpla_mapper?mapper_type=ia"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2"), str(resp) + "\n" + content
    doc = json.loads(content)
    assert "sourceResource" in doc, "sourceResource field is absent"
    assert "title" in doc["sourceResource"], "title field is absent"
    title = doc["sourceResource"]["title"]
    assert title == EXPECTED, "%s != %s" % (title, EXPECTED)

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
    url = server() + "dpla_mapper?mapper_type=ia"
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
    url = server() + "dpla_mapper?mapper_type=ia"
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
    url = server() + "dpla_mapper?mapper_type=ia"
    resp, content = H.request(url, "POST", body=INPUT_JSON)
    assert str(resp.status).startswith("2"), str(resp) + "\n" + content
    doc = json.loads(content)
    assert "sourceResource" in doc, "sourceResource field is absent"
    sr = doc["sourceResource"]
    assert "spatial" in sr, "spatial not found"
    spatial = sr["spatial"]
    expected_spatial = ["value1", "value2"]
    assert expected_spatial == spatial, "%s != %s" % (expected_spatial, spatial)

def test_ia_set_rights():
    """Test application of blanket rights statement"""
    INPUT = [{"expected_rights": u"Access to the Internet Archive's Collections is granted for scholarship and research purposes only. Some of the content available through the Archive may be governed by local, national, and/or international laws and regulations, and your use of such content is solely at your own risk.",
              "json": u"""
{
   "files": {
       "shown_at": "http://archive.org/details/wellesleynews4018well",
       "marc": "wellesleynews4018well_marc.xml",
       "dc": "wellesleynews4018well_dc.xml",
       "gif": "wellesleynews4018well.gif",
       "meta": "wellesleynews4018well_meta.xml",
       "pdf": "wellesleynews4018well.pdf"
   },
   "record": {
       "controlfield": [
           {
               "#text": "631245733",
               "tag": "001"
           },
           {
               "#text": "201005c19019999mau           0     eng d",
               "tag": "008"
           }
       ],
       "xmlns": "http://www.loc.gov/MARC21/slim",
       "leader": "01489nam a2200265 a 450 ",
       "datafield": [
           {
               "subfield": [
                   {
                       "#text": "WELL",
                       "code": "a"
                   },
                   {
                       "#text": "WELW",
                       "code": "a"
                   }
               ],
               "tag": "049",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Newspaper",
                   "code": "a"
               },
               "tag": "099",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Wellesley news.",
                   "code": "a"
               },
               "tag": "245",
               "ind1": "0",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "#text": "Wellesley, Mass :",
                       "code": "a"
                   },
                   {
                       "#text": "Wellesley College.",
                       "code": "b"
                   }
               ],
               "tag": "260",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "v. ; 26-41 cm.",
                   "code": "a"
               },
               "tag": "300",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Weekly from September to May.",
                   "code": "a"
               },
               "tag": "310",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "v.1-   Oct. 10,1901-",
                   "code": "a"
               },
               "tag": "362",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Title varies: Oct.10, 1901-June 28, 1911, College news; Oct.1911-Feb.1967, Wellesley College news.",
                   "code": "a"
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "College news merged with Wellesley magazine to form Wellesley College news,, Oct.1911-July 1916, continuing volume-numbering of the Magazine for a while, its format.",
                   "code": "a"
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "In Oct.1913, News resumed folio size but the magazine section remained the same, thus the magazine supplement to the News for 1913/14-1914/15 (v.22-23) are separately bound and supplement for 1915/1916 (v.24 is bound with the Wellesley College magazine for 1916/17 (v.25). In Oct.1916 the Magazine resumed its separate existence.",
                   "code": "a"
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Wellesley College news literary supplement, issued 1922-26, is bound with v.31-34 of the News.  In Nov.1926, this supplement became a separate publication.",
                   "code": "a"
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Clapp Library Holdings: v.1 (1901)-     (Shelved in Brackett Room)",
                   "code": "a"
               },
               "tag": "599",
               "ind1": "9",
               "ind2": "9"
           },
           {
               "subfield": {
                   "#text": "Archives Holdings: v.1 (1901)-",
                   "code": "a"
               },
               "tag": "599",
               "ind1": "9",
               "ind2": "9"
           },
           {
               "subfield": [
                   {
                       "#text": "Wellesley College",
                       "code": "a"
                   },
                   {
                       "#text": "Periodicals.",
                       "code": "v"
                   }
               ],
               "tag": "610",
               "ind1": "2",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "#text": "Wellesley College",
                       "code": "a"
                   },
                   {
                       "#text": "Students.",
                       "code": "x"
                   }
               ],
               "tag": "610",
               "ind1": "2",
               "ind2": "0"
           },
           {
               "subfield": {
                   "#text": "College news.",
                   "code": "a"
               },
               "tag": "730",
               "ind1": "0",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Wellesley College news.",
                   "code": "a"
               },
               "tag": "730",
               "ind1": "0",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": ".b16299425",
                   "code": "a"
               },
               "tag": "907",
               "ind1": " ",
               "ind2": " "
           }
       ]
    },
   "_id": "wellesleynews4018well",
   "metadata": {
       "scanner": "scribe6.boston.archive.org",
       "ppi": "500",
       "operator": "associate-nicholas-delancey@archive.org",
       "sponsor": "Wellesley College Library",
       "contributor": "Wellesley College Library",
       "foldoutcount": "0",
       "scanfee": "250",
       "subject": [
           "Wellesley College",
           "Wellesley College"
       ],
       "call_number": "b16299425",
       "title": "Wellesley news",
       "repub_state": "4",
       "foldout_seconds": "209",
       "source": "folio",
       "camera": "Canon EOS 5D Mark II",
       "ocr": "ABBYY FineReader 8.0",
       "scanningcenter": "boston",
       "description": [
           "Title varies: Oct.10, 1901-June 28, 1911, College news; Oct.1911-Feb.1967, Wellesley College news",
           "College news merged with Wellesley magazine to form Wellesley College news,, Oct.1911-July 1916, continuing volume-numbering of the Magazine for a while, its format",
           "In Oct.1913, News resumed folio size but the magazine section remained the same, thus the magazine supplement to the News for 1913/14-1914/15 (v.22-23) are separately bound and supplement for 1915/1916 (v.24 is bound with the Wellesley College magazine for 1916/17 (v.25). In Oct.1916 the Magazine resumed its separate existence",
           "Wellesley College news literary supplement, issued 1922-26, is bound with v.31-34 of the News. In Nov.1926, this supplement became a separate publication",
           "Clapp Library Holdings: v.1 (1901)- (Shelved in Brackett Room)",
           "Archives Holdings: v.1 (1901)-"
       ],
       "identifier-ark": "ark:/13960/t41r80q1q",
       "mediatype": "texts",
       "collection": [
           "Wellesley_College_Library",
           "blc",
           "americana"
       ],
       "volume": "vol. 40 no. 18",
       "updater": "Associate-Tim-Bigelow",
       "updatedate": "2012-07-19 13:45:44",
       "uploader": "Associate-Tim-Bigelow@archive.org",
       "date": "1901",
       "republisher": "associate-nicholas-delancey@archive.org",
       "addeddate": "2012-07-19 13:45:46",
       "foldout-operator": "associate-kayleigh-hinckley@archive.org",
       "publisher": "Wellesley, Mass : Wellesley College",
       "publicdate": "2012-07-19 13:45:50",
       "language": "eng",
       "page-progression": "lr",
       "notes": "No title or copyright pages.",
       "identifier-access": "http://archive.org/details/wellesleynews4018well",
       "identifier": "wellesleynews4018well",
       "sponsordate": "20120831",
       "imagecount": "8",
       "scandate": "20120723175550"
   }
}
    """},
    {"expected_rights": u"NOT_IN_COPYRIGHT", "json": u"""
{
   "files": {
       "shown_at": "http://archive.org/details/reportofsuperint1872fitz",
       "marc": "reportofsuperint1872fitz_marc.xml",
       "dc": "reportofsuperint1872fitz_dc.xml",
       "gif": "reportofsuperint1872fitz.gif",
       "meta": "reportofsuperint1872fitz_meta.xml",
       "pdf": "reportofsuperint1872fitz_bw.pdf"
   },
   "record": {
       "controlfield": [
           {
               "#text": "34783177",
               "tag": "001"
           },
           {
               "#text": "OCoLC",
               "tag": "003"
           },
           {
               "#text": "19990422114840.0",
               "tag": "005"
           },
           {
               "#text": "960524c18uu9999nhuar         0   a0eng d",
               "tag": "008"
           }
       ],
       "xmlns": "http://www.loc.gov/MARC21/slim",
       "leader": "02658nas a2200445Ia 4500",
       "datafield": [
           {
               "subfield": {
                   "#text": "21028254",
                   "code": "b"
               },
               "tag": "035",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "#text": "NHS",
                       "code": "a"
                   },
                   {
                       "#text": "NHS",
                       "code": "c"
                   },
                   {
                       "#text": "NHM",
                       "code": "d"
                   }
               ],
               "tag": "040",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "n-us-nh",
                   "code": "a"
               },
               "tag": "043",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "NHMN",
                   "code": "a"
               },
               "tag": "049",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Fitzwilliam (N.H. : Town)",
                   "code": "a"
               },
               "tag": "110",
               "ind1": "1",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Report of the superintending school committee of Fitzwilliam, for the year ending ..",
                   "code": "a"
               },
               "tag": "245",
               "ind1": "1",
               "ind2": "0"
           },
           {
               "subfield": [
                   {
                       "#text": "Report of Fitzwilliam schools made .... with a catalogue of the scholars",
                       "code": "a"
                   },
                   {
                       "#text": "1858.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Reports of the town officers of Fitzwilliam, N.H. made ...",
                       "code": "a"
                   },
                   {
                       "#text": "1860,1864.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Reports of the school committee, selectemen and treasurer of Fitzwilliam",
                       "code": "a"
                   },
                   {
                       "#text": "1862-63.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual report of the school committee of the Town of Fitzwilliam, for the year ending ...",
                       "code": "a"
                   },
                   {
                       "#text": "1865.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Reports of the superintending school committee, selectmen and treasurer of the Town of Fitzwilliam, for the year ending ...",
                       "code": "a"
                   },
                   {
                       "#text": "1866-1868.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual reports of the superintending school committee, selectmen and treasurer, of the Town of Fitzwilliam, for the year ending ...",
                       "code": "a"
                   },
                   {
                       "#text": "1869-1873.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual reports of the superintending school committee, with selectmen and treasurer of the Town of Fitzwilliam also report of the town library for the year ending ...",
                       "code": "a"
                   },
                   {
                       "#text": "1874.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual report of the superintending school committee of the Town of Fitzwilliam for the year ending ...",
                       "code": "a"
                   },
                   {
                       "#text": "1875.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual report of the town officers of Fitzwilliam, N.H. for the year ending ...",
                       "code": "a"
                   },
                   {
                       "#text": "1876-1897,1899-1929,1965-1966,1968-1969,1971,1984-1988,1990-",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual reports of the town officers and inventory of polls and ratable property of Fitzwilliam, N.H. for the year ending ...",
                       "code": "a"
                   },
                   {
                       "#text": "1930-1964.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual reports of the Town of Fitzwilliam, New Hampshire for the year ending ...",
                       "code": "a"
                   },
                   {
                       "#text": "1972-1975,1977-1983.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "2"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual reports of the town officers of Fitzwilliam, New Hampshire for the year ending ...",
                       "code": "a"
                   },
                   {
                       "#text": "1949-1955.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "4"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual reports Fitzwilliam, N.H. ",
                       "code": "a"
                   },
                   {
                       "#text": "1961-1971,1977-1978.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "4"
           },
           {
               "subfield": [
                   {
                       "#text": "Fitzwilliam, New Hampshire annual reports",
                       "code": "a"
                   },
                   {
                       "#text": "1972-1976,1979-1984.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "4"
           },
           {
               "subfield": [
                   {
                       "#text": "Town reports Fitzwilliam, N.H.",
                       "code": "a"
                   },
                   {
                       "#text": "1845-1936.",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "8"
           },
           {
               "subfield": [
                   {
                       "#text": "Annual reports of Fitzwilliam, N.H. ",
                       "code": "a"
                   },
                   {
                       "#text": " 1985-",
                       "code": "f"
                   }
               ],
               "tag": "246",
               "ind1": "1",
               "ind2": "4"
           },
           {
               "subfield": [
                   {
                       "#text": "[Boston, Mass. :",
                       "code": "a"
                   },
                   {
                       "#text": "S.N. Dickinson & Co. Printers,]",
                       "code": "b"
                   }
               ],
               "tag": "260",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "#text": "v. :",
                       "code": "a"
                   },
                   {
                       "#text": "ill. ;",
                       "code": "b"
                   },
                   {
                       "#text": "24 cm.",
                       "code": "c"
                   }
               ],
               "tag": "300",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Annual.",
                   "code": "a"
               },
               "tag": "310",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Description based on 1845.",
                   "code": "a"
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Place of publiction and publisher varies.",
                   "code": "a"
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "Titles vary slightly.",
                   "code": "a"
               },
               "tag": "500",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": {
                   "#text": "End of report year varies.",
                   "code": "a"
               },
               "tag": "515",
               "ind1": " ",
               "ind2": " "
           },
           {
               "subfield": [
                   {
                       "#text": "Fitzwilliam (N.H. : Town)",
                       "code": "a"
                   },
                   {
                       "#text": "Appropriations and expenditures",
                       "code": "x"
                   },
                   {
                       "#text": "Periodicals.",
                       "code": "v"
                   }
               ],
               "tag": "651",
               "ind1": " ",
               "ind2": "0"
           },
           {
               "subfield": {
                   "#text": "mjc retro",
                   "code": "a"
               },
               "tag": "910",
               "ind1": " ",
               "ind2": " "
           }
       ]
   },
   "_id": "reportofsuperint1872fitz",
   "metadata": {
       "scanner": "scribe1.boston.archive.org",
       "creator": "Fitzwilliam (N.H. : Town)",
       "ppi": "400",
       "rcamid": null,
       "operator": "scanner-mary-holt@archive.org",
       "missingpages": null,
       "sponsor": "University of New Hampshire Library",
       "contributor": "University of New Hampshire Library",
       "foldoutcount": "0",
       "scanfactors": "1",
       "call_number": "2102825",
       "title": "Report of the superintending school committee of Fitzwilliam, for the year ending .",
       "repub_state": "4",
       "lcamid": null,
       "possible-copyright-status": "NOT_IN_COPYRIGHT",
       "camera": "Canon 5D",
       "ocr": "ABBYY FineReader 8.0",
       "scanningcenter": "boston",
       "description": [
           "Description based on 1845",
           "Place of publiction and publisher varies",
           "Titles vary slightly",
           "End of report year varies"
       ],
       "identifier-ark": "ark:/13960/t2z324395",
       "mediatype": "texts",
       "collection": [
           "University_of_New_Hampshire_Library",
           "blc",
           "americana"
       ],
       "volume": "1872",
       "updater": "tricia-gray@archive.org",
       "updatedate": "2009-01-22 20:38:33",
       "uploader": "tricia-gray@archive.org",
       "date": "1872",
       "addeddate": "2009-01-22 20:38:35",
       "publisher": "[Boston, Mass. : S.N. Dickinson & Co. Printers,]",
       "publicdate": "2009-01-22 20:38:40",
       "language": "eng",
       "curation": "[curator]julie@archive.org[/curator][date]20090220210806[/date][state]approved[/state]",
       "identifier-access": "http://www.archive.org/details/reportofsuperint1872fitz",
       "identifier": "reportofsuperint1872fitz",
       "sponsordate": "20090131",
       "imagecount": "40",
       "scandate": "20090127004724"
   }
}
    """}]

    def ia_pipeline(endpoint, body):
        url = server() + endpoint
        resp, content = H.request(url, "POST", body)
        assert str(resp.status).startswith("2"), str(resp) + "\n" + content
        return content

    for i in INPUT:
        content = ia_pipeline("dpla_mapper?mapper_type=ia", i["json"])
        content = ia_pipeline("ia-set-rights", content)
        doc = json.loads(content)
        assert "sourceResource" in doc, "sourceResource field is absent"
        sr = doc["sourceResource"]
        assert "rights" in sr, "rights in sourceResource not found"
        sr_rights = sr["rights"]
        expected_rights = i["expected_rights"]
        assert expected_rights == sr_rights, "Expected rights not equal to sourceResource.rights: %s != %s" % (expected_rights, sr_rights)
        if "hasView" in doc:
            hv_rights = doc["hasView"]["rights"]
            assert expected_rights == hv_rights, "Expected rights not equal to hasView.rights: %s != %s" % (expected_rights, hv_rights)

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
