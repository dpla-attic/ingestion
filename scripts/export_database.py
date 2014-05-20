#!/usr/bin/env python

"""
This is a script for downloading couchdb documents into a compressed file
and uploading the file into Rackspace CDN.

For usage look at print_usage() function below or just run the script
without arguments.

To use listing all source names there have to be loaded script
couchdb_views/export_database.js.

For accessing the Couch database the following variables are read from the
CouchDb section of the akara.ini file:
    Url
    Username
    Password

For accessing the Rackspace the following variables are read from the
Rackspace section of the akara.ini file:
    Username
    ApiKey
    ContainerName
"""
import ConfigParser
try:
    import cloudfiles
except:
    msg = """
Cannot import cloudfiles.
The cloudfiles library has been added to requirements.txt file,
so it should be enough to run:

pip install -r requrements.txt

"""
    print msg
    exit(1)
import sys
from dplaingestion.couch import Couch


def set_global_variables(container):
    # Set the Rackspace and Database variables as global
    global RS_USERNAME
    global RS_APIKEY
    global RS_CONTAINER_NAME
    global DB_URL
    global DB_USERNAME
    global DB_PASSWORD

    config_file = "akara.ini"
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file))
    RS_USERNAME = config.get("Rackspace", "Username")
    RS_APIKEY = config.get("Rackspace", "ApiKey")
    RS_CONTAINER_NAME = config.get("Rackspace", container)
    DB_URL = config.get("CouchDb", "Url") + "dpla"
    DB_USERNAME = config.get("CouchDb", "Username")
    DB_PASSWORD = config.get("CouchDb", "Password")

    if not RS_USERNAME:
        print "There is no Rackspace Username in the configuration file"
        exit(1)

    if not RS_APIKEY:
        print "There is no Rackspace ApiKey in the configuration file"
        exit(1)

    if not RS_CONTAINER_NAME:
        print "There is no Rackspace Container in the configuration file"
        exit(1)

    if not DB_URL:
        print "There is no CouchDb Url in the configuration file"
        exit(1)

    if not DB_USERNAME:
        print "There is no CouchDb Username in the configuration file"
        exit(1)

    if not DB_PASSWORD:
        print "There is no CouchDb Password in the configuration file"
        exit(1)

def send_file_to_rackspace(arguments):
    """Sends the created file to the Rackspace CDN.

    Arguments:
        arguments - dictionary returned by the validate_arguments function

    The file saved to arguments["file"] is uploaded to Rackspace CDN
    and stored in container RS_CONTAINER_NAME using the current file name.
    The file name is taken with the extension, but without the whole path.

    After loding the file, th function checks if the file is listed in the
    container objects list.
    """

    if not arguments["upload"]:
        return

    fname = arguments['file']
    rsfname = fname.split('/')[-1:][0]

    container = get_rackspace_container()

    f = container.create_object(rsfname)

    print "Loading file [%s] to Rackspace CDN." % fname
    f.load_from_filename(fname)

    if file_is_in_container(rsfname, container):
        rs_file_uri = url_join(container.public_uri(), rsfname)
        print "File loaded, it is available at: %s" % rs_file_uri
        print "You can now check the information about rackspace files " \
              + "using `export_database rsinfo`"
    else:
        print "Couldn't upload file to Rackspace CDN."

    return rs_file_uri


def get_rackspace_connection():
    """Returns a new Rackspace connection.

    Returns:
        new Rackspace connection
    """
    return cloudfiles.get_connection(RS_USERNAME, RS_APIKEY)


def get_rackspace_container(connection=None):
    """Returns a new Rackspace CDN container.

    Arguments:
        connection - opened connection to the Rackspace CDN

    Returns:
        Rackspace container - created with provided connection,
                              if the connection is None, then a new one
                              is created.

    This container is made public if it is not public.

    According to Rackspace CDN API documentation, if such a container exists,
    then it is returned. Otherwise a new one is created.
    """
    if connection is None:
        connection = get_rackspace_connection()

    container = connection.create_container(RS_CONTAINER_NAME)
    if not container.is_public():
        container.make_public()

    return container


def print_rackspace_info(arguments):
    """Prints out information about all Rackspace files from the container,
    including their public links.

    Arguments:
        arguments - dictionary returned by the validate_arguments function

    """
    container = get_rackspace_container()

    print "Rackspace CDN info"
    print "Container name      : %s" % container.name
    print "Container public URI: %s" % container.public_uri()
    print "Container size      : %s" % container.size_used

    objects = get_sorted_objects_from_container(container)
    print "There are %d files." % len(objects)

    l = 0
    for ob in objects:
        l = max(l, len(ob["name"]) + len("Public URI:    ") + 2)

    for ob in objects:
        print "x" * (l + len(container.public_uri()))
        print "File name:     %s" % ob["name"]
        print "Last modified: %s" % ob["last_modified"]
        print "Size:          %s" % convert_bytes(ob["bytes"])
        print "Public URI:    %s/%s" % (container.public_uri(), ob["name"])

    print "x" * (l + len(container.public_uri()))


def download_source_data(arguments):
    """Downloads all documents for given source.

    Arguments:
        arguments - dictionary returned by the validate_arguments function

    The function uses couchdb bulk API for downloading the documents.
    The source name is taken from arguments["source"].

    The file name is set to <source>.gz where <source> is the lower case
    arguments["source"] string with whitespace replaced by underscores.

    Then it uploads the file to Rackspace CDN if required.
    """
    import urllib
    s = arguments["source"]

    db_url = url_join(
        DB_URL,
        ('_design/export_database/_view/all_source_docs?include_docs=true&' +
          urllib.urlencode({"startkey": '"%s"' % s, "endkey": '"%s"' % s}))
    )

    resp = open_db_connection(db_url)

    # Set the file name
    arguments["file"] = s.lower().replace(" ", "_") + ".gz"

    status, file_size = store_result_into_file(resp, arguments)
    if status == 0 and arguments.get("upload"):
        rs_file_uri = send_file_to_rackspace(arguments)
        update_bulk_download_document(s, rs_file_uri, file_size)
    else:
        print >> sys.stderr, "File %s not uploaded" % arguments["file"]

    return status

def download_each_source_data(arguments):
    """Gets a list of all sources in the Couch database and downloads each
       source's data.

       Arguments:
           arguments - dictionary returned by the validate_arguments function

       The function uses the get_all_sources function and extracts the list of
       sources in the database then passes each source to the
       download_source_data function.
    """
    data = get_all_sources()

    for row in data["rows"]:
        arguments["source"] = row["key"]
        download_source_data(arguments)


def get_all_sources():
    """Gets all source names.

    This function uses javascript couchdb function which you can find
    in file: couchdb_views/export_database.js.

    Returns:
        A dictionary containing the rows from the Couch view
    """

    import json as j
    db_url = url_join(
        DB_URL,
        "_design/export_database/_view/all_source_names?group=true"
    )
    msg_404 = """
Couldn't call the url: %s.

This can be caused by not working couchdb, bad couchdb url, or missing couchdb
function which you should load before running this script option.
The file to be loaded can be found at couchdb_views/export_database.js
""" % db_url

    resp = open_db_connection(db_url, msg_404)
    data = resp.read()
    d = j.loads(data)

    return d


def print_all_sources(arguments):
    """Prints all source names.

    Arguments:
        arguments - dictionary returned by the validate_arguments function
    """

    data = get_all_sources()

    print "%(key)s  --  %(value)s" % \
        {
         "key": "Collection name",
         "value": "Count"
        }

    for row in data["rows"]:
        print "%(key)s  --  %(value)s" % row


def url_join(*args):
    """Joins and returns given urls.

    Arguments:
        list of elements to join

    Returns:
        string with all elements joined with '/' inserted between

    """
    return "/".join(map(lambda x: str(x).rstrip("/"), args))


def open_db_connection(db_url, msg_on_error=None):
    """Opens an authenticated connection to the provided db_url.

    Arguments:
        arguments - the database url

    Returns:
        opened connection ready for reading data.
    """
    import base64
    import urllib2 as u
    print "Calling URL " + db_url

    req = u.Request(db_url)
    base64string = base64.encodestring('%s:%s' %
                                       (DB_USERNAME, DB_PASSWORD))[:-1]
    authheader =  "Basic %s" % base64string
    req.add_header("Authorization", authheader)
    try:
        return u.urlopen(req)
    except Exception as e:
        print e
        if msg_on_error:
            print msg_on_error
        exit(1)


def convert_bytes(byteno):
    """Converts number of bytes into some bigger unit like MB/GB.


    Arguments:
        byteno (Int) - number of bytes for conversion

    Returns:
        String with converted number and proper unit.
    """
    size = 1.0 * byteno
    unit = "B"
    units = ["kB", "MB", "GB"]
    for u in units:
        if size >= 1000:
            size /= 1024.0
            unit = u

    return "%0.1f %s" % (size, unit)


def get_sorted_objects_from_container(container):
    """Returns sorted objects from container.

    Arguments:
        container - opened container handle

    Returns:
        List of objects sorted by object name.
    """
    objects = container.list_objects_info()
    objects = sorted(objects, key=lambda k: k['name'])
    return objects


def file_is_in_container(fname, container):
    """
    Arguments:
        fname     - file name
        container - opened container handle

    Returns:
        True if file is in container.
        False otherwise.
    """
    for ob in get_sorted_objects_from_container(container):
        if fname == ob["name"]:
            return True

    return False


def remove_rackspace_file(arguments):
    """Removes a file from the Rackspace CDN container.

    Arguments:
        arguments - dictionary returned by the validate_arguments function

    Returns:
        nothing
    """
    fname = arguments["file"]

    print "Removing file [%s] from the [%s] container." % \
            (fname, RS_CONTAINER_NAME)

    container = get_rackspace_container()

    if not file_is_in_container(fname, container):
        print "There is no file [%s] in the [%s] container." % \
            (fname, RS_CONTAINER_NAME)

        print "In this container there are files:"
        for ob in get_sorted_objects_from_container(container):
            print ob["name"]
        print "You can use `export_database rsinfo` for more information."
        return

    container.delete_object(fname)

    if file_is_in_container(fname, container):
        print "Couldn't delete the file [%s]." % fname
    else:
        print "Successfully deleted the file [%s] from container [%s]." % \
            (fname, RS_CONTAINER_NAME)


def download_all_database(arguments):
    """Downloads data from couchdb database and stores it in the file dpla.gz

    Arguments:
        arguments - dictionary returned by the validate_arguments function

    Returns:
        Nothing
    """
    from gzip import GzipFile as zipf

    db_url = url_join(DB_URL, "_all_docs?include_docs=true")
    response = open_db_connection(db_url)

    # Set file name
    arguments["file"] = "dpla.gz"

    status, file_size = store_result_into_file(response, arguments)
    if status == 0 and arguments.get("upload"):
        rs_file_uri = send_file_to_rackspace(arguments)
        update_bulk_download_document("Complete Repository", rs_file_uri,
                                      file_size)
    else:
        print >> sys.stderr, "Did not send file %s " % arguments["file"] + \
                             "to the Rackspace container"

    return status

def store_result_into_file(result, arguments):
    """Stores given result into a compressed file.

    Arguments:
        result - opened connection, file like object with read() function
        arguments - dictionary returned by the validate_arguments function

    Returns:
        0 for success, -1 for error
    """
    from gzip import GzipFile as zipf
    from httplib import IncompleteRead
    from tempfile import mkstemp
    import os
    import sys

    downloaded_size = 0
    block_size = 5 * 1024 * 1024
    with zipf(arguments["file"], "w") as zf:
        while True:
            try:
                buffer = result.read(block_size)
                if not buffer:
                    break
            except IncompleteRead, e:
                fd, partial_file_name = mkstemp()
                os.write(fd, e.partial)
                os.close(fd)
                print >> sys.stderr, "Incomplete read of stream.  ", \
                    "Partial block saved to ", partial_file_name
                return -1
            except Exception, e:
                print "Error reading from result: %s" % e
                return -1
            downloaded_size += len(buffer)
            zf.write(buffer)
            status = "Downloaded " + convert_bytes(downloaded_size)
            print status

    return (0, convert_bytes(downloaded_size))


def print_usage():
    """Prints information about script usage."""

    print """

Script for downloading the couchdb database into compressed files.

Usage:

  Exporting whole database into a compressed file dpla.gz:

    export_database all <upload>

  Exporting all data for given source into a compressed file <source_name>.gz:

    export_database source <source_name> <upload>

  Exporting all sources data into individual compressed files <source_name>.gz:

    export_database each_source <upload>

  Listing all sources:

    export_database list

  Listing Rackspace CDN Container information:

    export_database rsinfo

  Removing file from the Rackspace CDN:

    export_database remove <filename>

  Arguments:

    source_name  - the contributor.name value taken from the provider's profile
    upload       - string "upload" if the file should be uploaded to Rackspace
                 - nothing if the files shouldn't be uploaded to Rackspace
    filename     - file name to remove from Rackspace CDN

    """
    exit(1)


def validate_arguments(argv):
    """Validates arguments passed to the script."""
    res = {}

    if len(argv) < 2:
        print_usage()

    operation = argv[1]
    res["operation"] = operation

    if operation == "all" or operation == "each_source":

        if not len(argv) in [2, 3]:
            print_usage()

        res["upload"]   = False

        if len(argv) == 3:
            if argv[2] == "upload":
                res["upload"] = True
            else:
                print_usage()

        return res

    elif operation == "list":

        if len(argv) != 2:
            print_usage()

        return res

    elif operation == "source":

        if not len(argv) in [3, 4]:
            print_usage()

        res["source"]   = argv[2]
        res["upload"]   = False

        if len(argv) == 4:
            if argv[3] == "upload":
                res["upload"] = True
            else:
                print_usage()

        return res

    elif operation == "rsinfo":

        if not len(argv) == 2:
            print_usage()

        return res

    elif operation == "remove":

        if not len(argv) == 3:
            print_usage()

        res["file"] = argv[2]

        return res

    else:
        print_usage()

def update_bulk_download_document(provider, file_path, file_size):
    c = Couch()
    bulk_download_doc_id = c.update_bulk_download_document(
                            provider, file_path, file_size
                            )
    print "Updated bulk_download database document with ID %s" % \
          bulk_download_doc_id

def get_action_dispatcher():
    """Creates a structure for dispatching actions.

    Returns:
        dictionary (action_name: action_function_to_call)

    All of those functions get only one argument: the dictionary returned
    by the validate_arguments function.
    """

    res = {
        "all":    download_all_database,
        "list":   print_all_sources,
        "source": download_source_data,
        "rsinfo": print_rackspace_info,
        "remove": remove_rackspace_file,
        "each_source":   download_each_source_data,
    }
    return res

def main(argv):
    set_global_variables("DPLAContainer")

    arguments = validate_arguments(argv)
    operation = arguments["operation"]

    dispatcher = get_action_dispatcher()
    dispatcher[operation](arguments)

if __name__ == "__main__":
    main(sys.argv)
