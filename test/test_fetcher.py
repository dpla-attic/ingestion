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
from akara import logger
from dplaingestion.create_fetcher import create_fetcher
from dplaingestion.selector import getprop as _getprop
from dplaingestion.oai import oaiservice
from urllib2 import urlopen
import xmltodict

def getprop(obj, path):
    return _getprop(obj, path, True)

# TODO:
# We should create our own data feed so as not to rely on a provider feed. 

# Remove trailing forward slash
uri_base = server()[:-1]

# Test config file
config_file = "test/test_data/test.conf"

@attr(uses_network="yes")
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

@attr(uses_network="yes")
def test_oai_fetcher_invalid_set():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"

    fetcher.sets = ["banana"]
    for response in fetcher.fetch_all_data():
        assert response["errors"]
        assert not response["records"]

    assert fetcher.collections.keys() == []

@attr(uses_network="yes")
def test_oai_fetcher_all_sets():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"
    url = "http://repository.clemson.edu/cgi-bin/oai.exe?verb=ListSets"
    list_sets = xmltodict.parse(urlopen(url).read())
    scdl_all_sets = [s['setSpec']
                     for s in list_sets['OAI-PMH']['ListSets']['set']]
    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]

    diff = [s for s in scdl_all_sets if
            s not in fetcher.collections]
    assert diff == []

@attr(uses_network="yes")
def test_oai_fetcher_with_blacklist():
    profile_path = "profiles/clemson.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "OAIVerbsFetcher"
    fetcher.blacklist = ["ctm", "spg", "jfb", "jbt", "pre", "dnc", "scp",
                         "swl", "weg", "ghs", "wsb", "mbe", "gcj", "cwp",
                         "nev", "hfp", "big"]
    for response in fetcher.fetch_all_data():
        pass
    url = "http://repository.clemson.edu/cgi-bin/oai.exe?verb=ListSets"
    list_sets = xmltodict.parse(urlopen(url).read())
    scdl_all_sets = [s['setSpec']
                     for s in list_sets['OAI-PMH']['ListSets']['set']]
    sets = list(set(scdl_all_sets) - set(fetcher.blacklist))
    diff = [s for s in sets if
            s not in fetcher.collections]
    assert diff == []

@attr(travis_exclude='yes', uses_network='yes')
def test_absolute_url_fetcher_nypl():
    profile_path = "profiles/nypl.pjs"
    fetcher =  create_fetcher(profile_path, uri_base, "akara.ini")
    assert fetcher.__class__.__name__ == "NYPLFetcher"

    for response in fetcher.fetch_all_data(
            "cd4c3430-c6cb-012f-ccf3-58d385a7bc34"
            ):
        assert not response["errors"]
        assert response["records"]
        break

@attr(uses_network="yes")
def test_absolute_url_fetcher_uva1():
    profile_path = "profiles/virginia.pjs"
    fetcher =  create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "UVAFetcher"

    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]
        break

@attr(uses_network="yes")
def test_absolute_url_fetcher_uva2():
    profile_path = "profiles/virginia_books.pjs"
    fetcher =  create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "UVAFetcher"

    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]
        break

@nottest
@attr(uses_network="yes")
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
# @attr(travis_exclude='yes', uses_network='yes')
# TEMPORARY:  disable this test because the recent MWDL upgrade introduced
#             data errors that have yet to be resolved.
@nottest
def test_absolute_url_fetcher_mwdl():
    profile_path = "profiles/mwdl.pjs"
    fetcher =  create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "MWDLFetcher"

    for response in fetcher.fetch_all_data():
        assert not response["errors"]
        assert response["records"]
        break

@nottest
@attr(uses_network="yes")
def test_all_oai_verb_fetchers():
    # Profiles that are representative of each type and are not restricted:
    profiles = [
        "harvard.pjs",   # mods
        "clemson.pjs",   # qdc
        "texas.pjs",     # untl
        "uiuc.pjs",      # oai_qdc
        "uiuc_book.pjs", # marc
        "artstor.pjs"    # oai_dc
    ]
    for profile in profiles:
        try:
            profile_path = "profiles/" + profile
            with open(profile_path, "r") as f:
                prof = json.loads(f.read())
            if prof.get("type") == "oai_verbs":
                fetcher =  create_fetcher(profile_path,
                                          uri_base,
                                          config_file)
                assert fetcher.__class__.__name__ == "OAIVerbsFetcher"
                for response in fetcher.fetch_all_data():
                    if response['errors']:
                        print >> sys.stderr, response['errors']
                    assert not response["errors"]
                    assert response["records"]
                    break
        except Exception as e:
            print >> sys.stderr, "\nError with %s: %s" % (profile,
                                                          e.message)
            assert False


def first_non_collection_record(records_list):
    """
    Return the first non-collection record in a list of records
    """
    records = iter(records_list)
    while True:
        record = records.next()
        # Collections have "ingestType", items don't
        if "ingestType" not in record:
            return record

@attr(uses_network="yes")
def test_oai_qdc_field_conversion():
    """
    oai.oaiservice returns dict with correct fields for QDC-format XML
    """
    svc = oaiservice("http://repository.clemson.edu/cgi-bin/oai.exe", logger)
    lr_result = svc.list_records(set_id="mbe", metadataPrefix="qdc")
    record = first_non_collection_record(lr_result["records"])
    actual_fields = record[1].keys()  # (id, record)
    actual_fields.sort()
    expected_fields = ['contributor', 'date', 'datestamp', 'description',
                       'format', 'handle', 'language', 'medium', 'publisher',
                       'relation', 'rights', 'setSpec', 'source', 'spatial',
                       'status', 'subject', 'title', 'type']
    assert actual_fields == expected_fields, \
            "\n%s\ndoes not match expected:\n%s\n" % (actual_fields,
                                                      expected_fields)

@attr(uses_network="yes")
def test_oai_dc_field_conversion():
    """
    oai.oaiservice returns dict with correct fields for DC-format XML
    """
    svc = oaiservice("http://digitallibrary.usc.edu/oai/oai.php", logger)
    lr_result = svc.list_records(set_id="p15799coll46",
                                 metadataPrefix="oai_dc")
    record = first_non_collection_record(lr_result["records"])
    actual_fields = record[1].keys()  # (id, record)
    actual_fields.sort()
    expected_fields = ['contributor', 'coverage', 'creator', 'date',
                       'datestamp', 'handle', 'publisher', 'relation',
                       'rights', 'setSpec', 'source', 'status', 'title',
                       'type']
    assert actual_fields == expected_fields

@attr(uses_network="yes")
def test_mods_field_conversion():
    """
    oai.oaiservice returns dict with correct fields for MODS-format XML

    It's difficult at the moment to test all of the metadata fields because of
    variations between providers, and we don't have a configuration that
    specifies valid fields per provider.
    """
    svc = oaiservice("http://vcoai.lib.harvard.edu/vcoai/vc", logger)
    lr_result = svc.list_records(set_id="manuscripts", metadataPrefix="mods")
    record = first_non_collection_record(lr_result["records"])
    actual_record_fields = record[1].keys()  # (id, record)
    actual_record_fields.sort()
    actual_mods_fields = record[1]['metadata']['mods'].keys()
    actual_mods_fields.sort()
    expected_record_fields = ['header', 'metadata']  # minimum fields
    for f in expected_record_fields:
        assert f in actual_record_fields

@nottest
@attr(uses_network="yes")
def test_marc_field_conversion():
    """
    oai.oaiservice returns dict with correct fields for MARC-format XML
    """
    svc = oaiservice(
        # uiuc_book profile
        "http://quest.library.illinois.edu/OCA-OAIProvider/oai.asp",
        logger)
    lr_result = svc.list_records(set_id="UC", metadataPrefix="marc")
    record = first_non_collection_record(lr_result["records"])
    actual_record_fields = record[1].keys()  # (id, record)
    actual_record_fields.sort()
    metadata = record[1]['metadata']['record']
    actual_marc_fields = metadata.keys()
    actual_marc_fields.sort()
    expected_record_fields = ['header', 'metadata']
    expected_marc_fields = ['controlfield', 'datafield', 'leader', 'xmlns',
                            'xmlns:xsi', 'xsi:schemaLocation']
    assert actual_record_fields == expected_record_fields
    assert actual_marc_fields == expected_marc_fields

@nottest
@attr(uses_network="yes")
def test_untl_field_conversion():
    """
    oai.oaiservice returns dict with correct fields for UNTL-format XML
    """
    svc = oaiservice("http://texashistory.unt.edu/oai/", logger)
    lr_result = svc.list_records(set_id="partner:RGPL", metadataPrefix="untl")
    record = first_non_collection_record(lr_result["records"])
    actual_record_fields = record[1].keys()  # (id, record)
    actual_record_fields.sort()
    metadata = record[1]['metadata']['untl:metadata']
    actual_untl_fields = metadata.keys()
    actual_untl_fields.sort()
    expected_record_fields = ['header', 'metadata']
    expected_untl_fields = ['untl:collection', 'untl:coverage', 'untl:creator',
                            'untl:date', 'untl:description', 'untl:format',
                            'untl:identifier', 'untl:institution',
                            'untl:language', 'untl:meta', 'untl:note',
                            'untl:primarySource', 'untl:publisher',
                            'untl:resourceType', 'untl:rights', 'untl:subject',
                            'untl:title', 'xmlns:untl']
    assert actual_record_fields == expected_record_fields
    assert actual_untl_fields == expected_untl_fields

@attr(uses_network="yes")
def test_file_fetcher_nara():
    profile_path = "profiles/nara.pjs"
    fetcher = create_fetcher(profile_path, uri_base, config_file)
    assert fetcher.__class__.__name__ == "NARAFetcher"

    fetcher.endpoint_url = "file:/%s/test/test_data/nara/" % os.getcwd()
    for response in fetcher.fetch_all_data():
        assert response["errors"] == []
        assert response["records"]
        break

@attr(uses_network="yes")
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
