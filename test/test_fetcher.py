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
from dplaingestion.fetcher import create_fetcher
from dplaingestion.selector import getprop as _getprop

def getprop(obj, path):
    return _getprop(obj, path, True)

# TODO:
# We should create our own data feed so as not to rely on a provider feed. 

# Remove trailing forward slash
uri_base = server()[:-1]

scdl_blacklist = ["ctm", "cfb", "spg", "jfb", "jbt", "pre", "dnc", "scp",
                  "swl", "weg", "ghs", "wsb", "mbe", "gcj", "cwp", "nev",
                  "hfp", "big"]
scdl_all_subresources = ["gmb", "ctm", "cfb", "spg", "jfb", "jbt", "pre",
                         "dnc", "scp", "swl", "weg", "ghs", "wsb", "mbe",
                         "gcj", "cwp", "nev", "hfp", "big", "dae"]

def test_oai_fetcher_valid_subresource():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base)
    fetcher.uri_base = uri_base
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.subresources = ["gmb"]
    for response in fetcher.fetch_all_data():
        assert response.get("error") is None
        assert getprop(response, "data/records") is not None

    assert fetcher.subresources.keys() == ["gmb"]

def test_oai_fetcher_invalid_subresource():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.subresources = ["banana"]
    for response in fetcher.fetch_all_data():
        assert response.get("error") is not None
        assert getprop(response, "data/records") is None

    assert fetcher.subresources.keys() == []

def test_oai_fetcher_all_subresources():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    for response in fetcher.fetch_all_data():
        assert response.get("error") is None
        assert getprop(response, "data/records") is not None

    diff = [subresource for subresource in scdl_all_subresources if
            subresource not in fetcher.subresources]
    assert diff == []

def test_oai_fetcher_with_blacklist():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.blacklist = scdl_blacklist
    for response in fetcher.fetch_all_data():
        pass

    subresources = list(set(scdl_all_subresources) - set(scdl_blacklist))
    diff = [subresource for subresource in subresources if
            subresource not in fetcher.subresources]
    assert diff == []

def test_absolute_url_fetcher_nypl():
    profile_path = "profiles/nypl.pjs"
    fetcher =  create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "NYPLFetcher"

    for response in fetcher.fetch_all_data():
        assert response.get("error") is None
        assert getprop(response, "data/records") is not None
        break

# Exclude since request to endpoint URL from Travis are returning 404
@attr(travis_exclude='yes')
def test_absolute_url_fetcher_uva1():
    profile_path = "profiles/virginia.pjs"
    fetcher =  create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "UVAFetcher"

    for response in fetcher.fetch_all_data():
        assert response.get("error") is None
        assert getprop(response, "data/records") is not None
        break

def test_absolute_url_fetcher_uva2():
    profile_path = "profiles/virginia_books.pjs"
    fetcher =  create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "UVAFetcher"

    for response in fetcher.fetch_all_data():
        assert response.get("error") is None
        assert getprop(response, "data/records") is not None
        break

@nottest
def test_absolute_url_fetcher_ia():
    profile_path = "profiles/ia.pjs"
    fetcher =  create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "IAFetcher"

    fetcher.endpoint_url_params["rows"] = 10
    for response in fetcher.fetch_all_data():
        assert response.get("error") == []
        assert getprop(response, "data/records") is not None
        break

# Exclude the MWDL test in Travis as access to the feed is restricted
@attr(travis_exclude='yes')
def test_absolute_url_fetcher_mwdl():
    profile_path = "profiles/mwdl.pjs"
    fetcher =  create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "MWDLFetcher"

    for response in fetcher.fetch_all_data():
        assert response.get("error") is None
        assert getprop(response, "data/records") is not None
        break

def test_all_oai_verb_fetchers():
    for profile in os.listdir("profiles"):
        if profile.endswith(".pjs"):
            # TODO: Remove this once KDL feed is working
            if profile == "kentucky.pjs":
                continue

            profile_path = "profiles/" + profile
            with open(profile_path, "r") as f:
                prof = json.loads(f.read())
            if prof.get("type") == "oai_verbs":
                fetcher =  create_fetcher(profile_path, uri_base)
                assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

                # Digital Commonwealth sets 217, 218 are giving errors
                if prof.get("name") == "digital-commonwealth":
                    fetcher.blacklist.extend(["217", "218"])

                for response in fetcher.fetch_all_data():
                    assert response.get("error") is None
                    assert getprop(response, "data/records") is not None
                    break

def test_file_fetcher_nara():
    profile_path = "profiles/nara.pjs"
    fetcher = create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "NARAFetcher"

    fetcher.endpoint_url = "file:/%s/test/test_data/nara/" % os.getcwd()
    for response in fetcher.fetch_all_data():
        assert response.get("error") == []
        assert getprop(response, "data/records") is not None
        break

def test_file_fetcher_smithsonian():
    profile_path = "profiles/smithsonian.pjs"
    fetcher = create_fetcher(profile_path, uri_base)
    assert fetcher.__class__.__name__ == "EDANFetcher"

    fetcher.endpoint_url = "file:/%s/test/test_data/smithsonian/" % os.getcwd()
    for response in fetcher.fetch_all_data():
        assert response.get("error") == []
        assert getprop(response, "data/records") is not None
        break

if __name__ == "__main__":
    raise SystemExit("Use nosetests")
