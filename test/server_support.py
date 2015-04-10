"""Support module to start a local Akara test server"""

import atexit
import os
from os.path import abspath, dirname
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import urllib2
import urlparse
import httplib
import ConfigParser

# XXX I only use one function from here. Bring it into this file?
import python_support

##############################################################################

class MyHttp:
    "Class providing request function for calling HTTP service."

    HEADERS = {
        "Content-Type": "application/json",
        "Context": "{}",
        "Connection": "close"
    }

    def request(self, url, method, body, headers=HEADERS):
        from amara.thirdparty import httplib2
        h = httplib2.Http()
        return h.request(url, method, body, headers)

# Use this object in tests.

H = MyHttp()

##############################################################################

# Set 'False' to keep the temporary directory used for the server tests
DELETE_TEMPORARY_SERVER_DIRECTORY = False
# DELETE_TEMPORARY_SERVER_DIRECTORY = True
if "AKARA_SAVE" in os.environ:
    DELETE_TEMPORARY_SERVER_DIRECTORY = False


######

# All of the tests use a single server instance.
# This is started the first time it's needed.
# It is stopped during test shutdown.

def _override_server_uri():
    server = os.environ.get("AKARA_TEST_SERVER", None)
    if not server:
        return None
    assert "/" not in server
    return "http://" + server + "/"
    
SERVER_URI = _override_server_uri()
config_root = None
config_filename = None
server_pid = None
server_did_not_start = False

# Create a temporary directory structure for Akara.
# Needs a configuration .ini file and the logs subdirectory.
def create_server_dir(port):
    global config_root, config_filename
    
    config_root = tempfile.mkdtemp(prefix="akara_test_")
    config_filename = os.path.join(config_root, "akara_test.config")

    ini = ConfigParser.ConfigParser()
    ini.optionxform=str  # Maintain case for configuration keys 
    ini.read(os.path.dirname(os.path.realpath(__file__)) + "/../akara.ini")
    bing_apikey = "notset"
    if (ini.has_section("Bing") \
        and ini.has_option("Bing", "ApiKey")): 
        bing_apikey = ini.get("Bing", "ApiKey")
    geonames_username = "notset"
    geonames_token = "notset"
    if (ini.has_section("Geonames")):
        if ini.has_option("Geonames", "Username"): 
            geonames_username = ini.get("Geonames", "Username")
        if ini.has_option("Geonames", "Token"): 
            geonames_token = ini.get("Geonames", "Token")

    f = open(config_filename, "w")
    f.write("""
class Akara:
  ConfigRoot = %(config_root)r
  InternalServerRoot = 'http://localhost:%(port)s/'
  Listen = 'localhost:%(port)s'
  LogLevel = 'DEBUG'
  MinSpareServers = 3
  # These affect test_server.py:test_restart
  MaxServers = 5
  MaxRequestsPerServer = 500

MODULES = [
    "freemix_akara.contentdm",
    "freemix_akara.oai",
    "dplaingestion.oai",
    "dplaingestion.couch",
    "dplaingestion.create_fetcher",
    "dplaingestion.fetchers.fetcher",
    "dplaingestion.fetchers.oai_verbs_fetcher",
    "dplaingestion.fetchers.absolute_url_fetcher",
    "dplaingestion.fetchers.file_fetcher",
    "dplaingestion.fetchers.ia_fetcher",
    "dplaingestion.fetchers.mwdl_fetcher",
    "dplaingestion.fetchers.nypl_fetcher",
    "dplaingestion.fetchers.uva_fetcher",
    "dplaingestion.fetchers.nara_fetcher",
    "dplaingestion.fetchers.edan_fetcher",
    "dplaingestion.fetchers.hathi_fetcher",
    "dplaingestion.create_mapper",
    "dplaingestion.mappers.mapper",
    "dplaingestion.mappers.dublin_core_mapper",
    "dplaingestion.mappers.mods_mapper",
    "dplaingestion.mappers.harvard_mapper",
    "dplaingestion.mappers.bpl_mapper",
    "dplaingestion.mappers.mwdl_mapper",
    "dplaingestion.mappers.getty_mapper",
    "dplaingestion.akamod.dpla_mapper",
    "dplaingestion.akamod.set_context",
    "dplaingestion.mappers.primo_mapper",
    "dplaingestion.mappers.missouri_mapper",
    "dplaingestion.akamod.enrich",
    "dplaingestion.akamod.enrich-subject",
    "dplaingestion.akamod.enrich-type",
    "dplaingestion.akamod.enrich-format",
    "dplaingestion.akamod.enrich_date",
    "dplaingestion.akamod.select-id",
    "dplaingestion.akamod.shred",
    "dplaingestion.akamod.geocode",
    "dplaingestion.akamod.oai-set-name",
    "dplaingestion.akamod.dpla-list-records",
    "dplaingestion.akamod.dpla-list-sets",
    "dplaingestion.akamod.harvard_enrich_location",
    "dplaingestion.akamod.mdl-enrich-location",
    "dplaingestion.akamod.mwdl_enrich_location",
    "dplaingestion.akamod.nara_enrich_location",
    "dplaingestion.akamod.scdl_enrich_location",
    "dplaingestion.akamod.uiuc_enrich_location",
    "dplaingestion.akamod.scdl_geocode_regions",
    "dplaingestion.akamod.filter_empty_values",
    "dplaingestion.akamod.artstor_select_isshownat",
    "dplaingestion.akamod.artstor_identify_object",
    "dplaingestion.akamod.contentdm_identify_object",
    "dplaingestion.akamod.cdl_identify_object",
    "dplaingestion.akamod.move_date_values",
    "dplaingestion.akamod.enrich_location",
    "dplaingestion.akamod.lookup",
    "dplaingestion.akamod.indiana_identify_object",
    "dplaingestion.akamod.kentucky_identify_object",
    "dplaingestion.akamod.georgia_identify_object",
    "dplaingestion.akamod.bhl_contributor_to_collection",
    "dplaingestion.akamod.copy_prop",
    "dplaingestion.akamod.cleanup_value",
    "dplaingestion.akamod.set_prop",
    "dplaingestion.akamod.enrich_language",
    "dplaingestion.akamod.mwdl_enrich_state_located_in",
    "dplaingestion.akamod.artstor_cleanup",
    "dplaingestion.akamod.nypl_identify_object",
    "dplaingestion.akamod.nypl_coll_title",
    "dplaingestion.akamod.nypl_select_hasview",
    "dplaingestion.akamod.mwdl_cleanup_field",
    "dplaingestion.akamod.ia_identify_object",
    "dplaingestion.akamod.ia_set_rights",
    "dplaingestion.akamod.dc_clean_invalid_dates",
    "dplaingestion.akamod.edan_select_id",
    "dplaingestion.akamod.dc_clean_invalid_dates",
    "dplaingestion.akamod.decode_html",
    "dplaingestion.akamod.artstor_spatial_to_dataprovider",
    "dplaingestion.akamod.david_rumsey_identify_object",
    "dplaingestion.akamod.dedup_value",
    "dplaingestion.akamod.set_type_from_physical_format",
    "dplaingestion.akamod.capitalize_value",
    "dplaingestion.akamod.artstor_cleanup_creator",
    "dplaingestion.akamod.replace_substring",
    "dplaingestion.akamod.uiuc_cleanup_spatial_name",
    "dplaingestion.akamod.remove_list_values",
    "dplaingestion.akamod.usc_enrich_location",
    "dplaingestion.akamod.hathi_identify_object",
    "dplaingestion.akamod.texas_enrich_location",
    "dplaingestion.akamod.set_spec_type",
    "dplaingestion.akamod.usc_set_dataprovider",
    "dplaingestion.akamod.compare_with_schema",
    "dplaingestion.akamod.mdl_state_located_in",
    "dplaingestion.akamod.scdl_format_to_type",
    "dplaingestion.marc_code_to_relator",
    "dplaingestion.akamod.validate_mapv3",
    "dplaingestion.akamod.strip_html"
    ]

class geocode: 
    bing_api_key = '%(bing_apikey)s'
    geonames_username = '%(geonames_username)s'
    geonames_token = '%(geonames_token)s'

class lookup:
    lookup_mapping = {
        'test': 'test_subst',
        'test2': 'test_2_subst',
        'iso639_3': 'iso639_3_subst',
        'scdl_fix_format': 'SCDL_FIX_FORMAT'
    }

class identify_object:
    IGNORE = 0
    PENDING = 1

class contentdm_identify_object(identify_object):
    pass

class indiana_identify_object(identify_object):
    pass

class kentucky_identify_object(identify_object):
    pass

class artstor_identify_object(identify_object):
    pass

class georgia_identify_object(identify_object):
    pass

class nypl_identify_object(identify_object):
    pass

class ia_identify_object(identify_object):
    pass

class hathi_identify_object(identify_object):
    pass

class type_conversion:
    # Map of "format" or "physical description" substring to
    # sourceResource.type.  This format field is considered first, and these
    # values should be as specific as possible, to avoid false assignments,
    # because this field is usually pretty free-form, unlike the type fields.
    type_for_phys_keyword = [
        ('holiday card', 'image'),
        ('christmas card', 'image'),
        ('mail art', 'image'),
        ('postcard', 'image'),
    ]
    # Map of type-related substring to desired sourceResource.type.
    # For simple "if substr in str" matching.  Place more specific
    # patterns higher up, before more general ones.
    type_for_ot_keyword = [
        ('photograph', 'image'),
        ('sample book', 'image'),
        ('book', 'text'),
        ('specimen', 'image'),
        ('electronic resource', 'interactive resource'),
        # Keep "textile" above "text"
        ('textile', 'image'),
        ('text', 'text'),
        ('frame', 'image'),
        ('costume', 'image'),
        ('object', 'physical object'),
        ('statue', 'image'),
        ('sculpture', 'image'),
        ('container', 'image'),
        ('jewelry', 'image'),
        ('furnishing', 'image'),
        ('furniture', 'image'),
        # Keep "moving image" above "image"
        ('moving image', 'moving image'),
        # And, yes, "MovingImage" is a valid DC type.
        ('movingimage', 'moving image'),
        ('image', 'image'),
        ('drawing', 'image'),
        ('print', 'image'),
        ('painting', 'image'),
        ('illumination', 'image'),
        ('poster', 'image'),
        ('appliance', 'image'),
        ('tool', 'image'),
        ('electronic component', 'image'),
        ('publication', 'text'),
        ('magazine', 'text'),
        ('journal', 'text'),
        ('postcard', 'image'),
        ('correspondence', 'text'),
        ('writing', 'text'),
        ('manuscript', 'text'),
        # keep "equipment" above "audio" ("Audiovisual equipment")
        ('equipment', 'image'),
        ('cartographic', 'image'),
        ('notated music', 'image'),
        ('mixed material', 'image'),
        ('audio', 'sound'),
        ('sound', 'sound'),
        ('oral history recording', 'sound'),
        ('finding aid', 'collection'),
        ('online collection', 'collection'),
        ('online exhibit', 'interactive resource'),
        ('motion picture', 'moving image'),
        ('film', 'moving image'),
        ('video game', 'interactive resource'),
        ('video', 'moving image')
    ]

class enrich_type(type_conversion):
    pass

""" % dict(config_root = config_root,
           port = port,
           bing_apikey = bing_apikey,
           geonames_username = geonames_username,
           geonames_token = geonames_token
           ))
    f.close()

    os.mkdir(os.path.join(config_root, "logs"))

#FIXME: add back config for:
#[collection]
#folder=/tmp/collection

# Remove the temporary server configuration directory,
# if I created it
def remove_server_dir():
    global server_pid, config_root
    if server_pid is not None:
        # I created the server, I kill it
        os.kill(server_pid, signal.SIGTERM)
        server_pid = None

    if config_root is not None:
        # Very useful when doing development and testing.
        # Would like this as a command-line option somehow.
        if DELETE_TEMPORARY_SERVER_DIRECTORY:
            shutil.rmtree(config_root)
        else:
            print "Test server configuration and log files are in", config_root
        config_root = None

atexit.register(remove_server_dir)

# Start a new Akara server in server mode.
def start_server():
    global server_pid, server_did_not_start
    # There's a PID if the spawning worked
    assert server_pid is None
    # Can only tell if the server starts by doing a request
    # If the request failed, don't try to restart.
    if server_did_not_start:
        raise AssertionError("Already tried once to start the server")

    port = python_support.find_unused_port()
    create_server_dir(port)
    args = ['akara', "--config-file", config_filename, "start"]
    try:
        result = subprocess.call(args)
    except:
        print "Failed to start", args
        raise

    # Akara started, but it might have failed during startup.
    # Report errors by reading the error log
    if result != 0:
        f = open(os.path.join(config_root, "logs", "error.log"))
        err_text = f.read()
        raise AssertionError("Could not start %r:\n%s" % (args, err_text))

    # Akara server started in the background. The main
    # process will only exit with a success (0) if the
    # pid file has been created.
    f = open(os.path.join(config_root, "logs", "akara.pid"))
    line = f.readline()
    f.close()

    # Save the pid information now so the server will be shut down
    # if there are any problems.
    temp_server_pid = int(line)

    # Check to see that the server process really exists.
    # (Is this overkill? Is this portable for Windows?)
    os.kill(temp_server_pid, 0)  # Did Akara really start?

    server_did_not_start = True
    check_that_server_is_available(port)
    server_did_not_start = False

    # Looks like it's legit!
    server_pid = temp_server_pid
    return port

# It takes the server a little while to get started.
# In the worst case (trac #6), top-level import failures
# will loop forever, and the server won't hear requests.
def check_that_server_is_available(port):
    old_timeout = socket.getdefaulttimeout()
    try:
        # How long do you want to wait?
        socket.setdefaulttimeout(20.0)
        try:
            urllib2.urlopen("http://localhost:%d/" % port).read()
        except urllib2.URLError, err:
            print "Current error log is:"
            f = open(os.path.join(config_root, "logs", "error.log"))
            err_text = f.read()
            print err_text
            raise
    finally:
        socket.setdefaulttimeout(old_timeout)

# Get the server URI prefix, like "http://localhost:8880/"
def server():
    global SERVER_URI
    if SERVER_URI is None:
        # No server specified and need to start my own
        port = start_server()
        SERVER_URI = "http://localhost:%d/" % port

    return SERVER_URI

def httplib_server():
    url = server()
    # <scheme>://<netloc>/<path>?<query>#<fragment>
    scheme, netloc, path, query, fragment = urlparse.urlsplit(url)
    assert path in ("", "/"), "Cannot handle path in %r" % url
    assert query == "", "Cannot handle query in %r" % url
    assert fragment == "", "Cannot handle fragment in %r" % url
    assert "@" not in netloc, "Cannot handle '@' in %r" % url
    if ":" in netloc:
        host, port = netloc.split(":")
        port = int(port)
    else:
        host = netloc
        port = 80
    conn = httplib.HTTPConnection(host, port, strict=True)
    return conn

def print_error_log():
    """
    Prints the Akara's error log. Is useful when running `nodetests --verbose`.
    """
    print "ERROR LOG\n"
    print os.path.join(config_root, 'logs', 'error.log')
    with open(os.path.join(config_root, 'logs', 'error.log'), "r") as errfile:
        for line in errfile:
            sys.stdout.write(line)
    print "END OF ERROR LOG\n"
