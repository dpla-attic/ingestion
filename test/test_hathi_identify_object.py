import sys
from nose.tools import nottest
from amara.thirdparty import json
from dplaingestion.selector import setprop
from server_support import H, server, print_error_log

with open("test/test_data/hathi_records", "r") as f:
    hathi_records = json.loads(f.read())

def _get_server_response(body):
    url = server() + "hathi_identify_object"
    return H.request(url, "POST", body=body)

def test_thumbnail_url_prefix_CHI():
    hathi_record = hathi_records["chi"]
    thumbnail_url = "http://bks0.books.google.com/books?id=LXA4AQAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_COO():
    hathi_record = hathi_records["coo"]
    thumbnail_url = "http://bks7.books.google.com/books?id=EDhEAAAAYAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_HVD():
    hathi_record = hathi_records["hvd"]
    thumbnail_url = "http://bks6.books.google.com/books?id=yTsuAAAAYAAJ&printsec=frontcover&img=1&zoom=5&edge=curl"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_IEN():
    hathi_record = hathi_records["ien"]
    thumbnail_url = "http://bks8.books.google.com/books?id=ExczAQAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_INU():
    hathi_record = hathi_records["inu"]
    thumbnail_url = "http://bks4.books.google.com/books?id=uEzQAAAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_MDP():
    hathi_record = hathi_records["mdp"]
    thumbnail_url = "http://bks6.books.google.com/books?id=VpbhAAAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

@nottest
def test_thumbnail_url_prefix_NJP1():
    hathi_record = hathi_records["njp1"]
    thumbnail_url = "http://bks4.books.google.com/books?id=GswtcRL0tZ0C&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    print_error_log()
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

@nottest
def test_thumbnail_url_prefix_NJP2():
    hathi_record = hathi_records["njp2"]
    thumbnail_url = "http://bks4.books.google.com/books?id=tygtAAAAYAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_NNC1():
    hathi_record = hathi_records["nnc1"]
    thumbnail_url = "http://bks2.books.google.com/books?id=cWlIAAAAYAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_NYP():
    hathi_record = hathi_records["nyp"]
    thumbnail_url = "http://bks7.books.google.com/books?id=5SSi7ajyBfkC&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_PST():
    hathi_record = hathi_records["pst"]
    thumbnail_url = "http://bks7.books.google.com/books?id=Fr2S5LbGGHYC&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_PUR1():
    hathi_record = hathi_records["pur1"]
    thumbnail_url = "http://bks3.books.google.com/books?id=7-x7vL5gDj4C&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UMN():
    hathi_record = hathi_records["umn"]
    thumbnail_url = "http://bks2.books.google.com/books?id=8-wxAQAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UVA():
    hathi_record = hathi_records["uva"]
    thumbnail_url = "http://bks0.books.google.com/books?id=P70_AAAAYAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_WU():
    hathi_record = hathi_records["wu"]
    thumbnail_url = "http://bks8.books.google.com/books?id=I0g2AQAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCSD():
    hathi_record = hathi_records["uc1_ucsd"]
    thumbnail_url = "http://bks1.books.google.com/books?id=vLQPAQAAIAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCI():
    hathi_record = hathi_records["uc1_uci"]
    thumbnail_url = "http://bks7.books.google.com/books?id=-JY1AQAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCSF():
    hathi_record = hathi_records["uc1_ucsf"]
    thumbnail_url = "http://bks7.books.google.com/books?id=yw03AQAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCSC1():
    hathi_record = hathi_records["uc1_ucsc1"]
    thumbnail_url = "http://bks8.books.google.com/books?id=1jSaAAAAIAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    print_error_log()
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCSC2():
    hathi_record = hathi_records["uc1_ucsc2"]
    thumbnail_url = "http://bks1.books.google.com/books?id=SgINAAAAIAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    print_error_log()
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCSC3():
    hathi_record = hathi_records["uc1_ucsc3"]
    thumbnail_url = "http://bks7.books.google.com/books?id=66C4AAAAIAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    print_error_log()
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCD():
    hathi_record = hathi_records["uc1_ucd"]
    thumbnail_url = "http://bks9.books.google.com/books?id=I5U5AQAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCLA1():
    hathi_record = hathi_records["uc1_ucla1"]
    thumbnail_url = "http://bks1.books.google.com/books?id=48YRAQAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCLA2():
    hathi_record = hathi_records["uc1_ucla2"]
    thumbnail_url = "http://bks0.books.google.com/books?id=CaQsAQAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_prefix_UC1_UCAL():
    hathi_record = hathi_records["uc1_ucal"]
    thumbnail_url = "http://bks1.books.google.com/books?id=FUxJAAAAIAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

def test_thumbnail_url_with_ISBN():
    hathi_record = hathi_records["isbn"]
    thumbnail_url = "http://bks6.books.google.com/books?id=rVhdAAAAMAAJ&printsec=frontcover&img=1&zoom=5"
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

@nottest
def test_thumbnail_url_prefix_UC1_UCSB():
    # TODO: Find UCSB record (barcode part of full hathitrust ID should have
    # length 14 and 2nd-5th characters should be 1205)
    hathi_record = hathi_records["uc1_ucsb"]
    thumbnail_url = ""
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

@nottest
def test_thumbnail_url_prefix_UC1_UCR():
    # TODO: Find UCR record (barcode part of full hathitrust ID should have
    # length 14 and 2nd-5th characters should be 1210)
    hathi_record = hathi_records["uc1_ucr"]
    thumbnail_url = ""
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

@nottest
def test_thumbnail_url_prefix_UC1_UCB():
    # TODO: Find UCB record (barcode part of full hathitrust ID should have
    # length 10)
    hathi_record = hathi_records["uc1_ucr"]
    thumbnail_url = ""
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls

@nottest
def test_thumbnail_url_prefix_UCM():
    # TODO: Find UCM record with OCLC
    hathi_record = hathi_records["ucm"]
    thumbnail_url = ""
    thumbnail_urls = [thumbnail_url, thumbnail_url + "&edge=curl"]

    resp, content = _get_server_response(json.dumps(hathi_record))
    assert resp.status == 200
    assert json.loads(content).get("object") in thumbnail_urls
