import os
import sys
import ConfigParser
from nose import with_setup
from nose.tools import nottest
from nose.plugins.attrib import attr
from server_support import server
from server_support import print_error_log
from amara.thirdparty import json
from amara.thirdparty import httplib2
from dplaingestion.create_fetcher import create_fetcher
from dplaingestion.selector import getprop as _getprop
from urllib2 import urlopen
import xmltodict

def getprop(obj, path):
    return _getprop(obj, path, True)

# TODO:
# We should create our own data feed so as not to rely on a provider feed. 

# Remove trailing forward slash
uri_base = server()[:-1]

scdl_blacklist = ["ctm", "spg", "jfb", "jbt", "pre", "dnc", "scp", "swl",
                  "weg", "ghs", "wsb", "mbe", "gcj", "cwp", "nev", "hfp",
                  "big"]

list_sets = xmltodict.parse(urlopen("http://repository.clemson.edu/cgi-bin/oai.exe?verb=ListSets").read())
scdl_all_sets = [s['setSpec'] for s in list_sets['OAI-PMH']['ListSets']['set']]

# Test config file
config_file = "test/test_data/test.conf"

def test_oai_fetcher_valid_set():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.sets = ["gmb"]
    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]

    assert fetcher.collections.keys() == ["gmb"]

def test_oai_fetcher_invalid_set():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.sets = ["banana"]
    for response in fetcher.fetch_all_data():
        assert response["errors"]
        assert not response["records"]

    assert fetcher.collections.keys() == []

def test_oai_fetcher_all_sets():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]

    diff = [s for s in scdl_all_sets if
            s not in fetcher.collections]
    assert diff == []

def test_oai_fetcher_with_blacklist():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.blacklist = scdl_blacklist
    for response in fetcher.fetch_all_data():
        pass

    sets = list(set(scdl_all_sets) - set(scdl_blacklist))
    diff = [s for s in sets if
            s not in fetcher.collections]
    assert diff == []

def test_absolute_url_fetcher_nypl():
    profile_path = "profiles/nypl.pjs"
    fetcher =  create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "NYPLFetcher"

    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]
        break

def test_absolute_url_fetcher_uva1():
    profile_path = "profiles/virginia.pjs"
    fetcher =  create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "UVAFetcher"

    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]
        break

def test_absolute_url_fetcher_uva2():
    profile_path = "profiles/virginia_books.pjs"
    fetcher =  create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "UVAFetcher"

    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]
        break

@nottest
def test_absolute_url_fetcher_ia():
    profile_path = "profiles/ia.pjs"
    fetcher =  create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "IAFetcher"

    fetcher.endpoint_url_params["rows"] = 10
    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]
        break

# Exclude the MWDL test in Travis as access to the feed is restricted
@attr(travis_exclude='yes')
def test_absolute_url_fetcher_mwdl():
    profile_path = "profiles/mwdl.pjs"
    fetcher =  create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "MWDLFetcher"

    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]
        break

# Exclude since certain feeds are restricted
@attr(travis_exclude='yes')
def test_all_oai_verb_fetchers():
    for profile in os.listdir("profiles"):
        if profile.endswith(".pjs"):
            # David Rumsey ListSets is returning 500 on hz4 and Travis
            if profile == "david_rumsey.pjs":
                continue

            profile_path = "profiles/" + profile
            with open(profile_path, "r") as f:
                prof = json.loads(f.read())
            if prof.get("type") == "oai_verbs":
                fetcher =  create_fetcher(profile_path, uri_base, config_file)
                assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

                # Digital Commonwealth sets 217, 218 are giving errors
                if prof.get("name") == "digital-commonwealth":
                    fetcher.blacklist.extend(["217", "218"])

                for response in fetcher.fetch_all_data():
                    if response['errors']:
                        print >> sys.stderr, response['errors']
                    assert not response["errors"]
                    assert response["records"]
                    break

def test_file_fetcher_nara():
    profile_path = "profiles/nara.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "NARAFetcher"

    fetcher.endpoint_url = "file:/%s/test/test_data/nara/" % os.getcwd()
    for response in fetcher.fetch_all_data():
        assert response["errors"] == []
        assert response["records"]
        break

def test_file_fetcher_smithsonian():
    profile_path = "profiles/smithsonian.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "EDANFetcher"

    fetcher.endpoint_url = "file:/%s/test/test_data/smithsonian/" % os.getcwd()
    for response in fetcher.fetch_all_data():
        assert response["errors"] == []
        assert response["records"]
        break

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
