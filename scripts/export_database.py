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
import os
import sys
import json
import gzip
from dplaingestion.couch import Couch


def set_global_variables(container):
    # Set the Rackspace and Database variables as global
    global RS_USERNAME
    global RS_APIKEY
    global RS_CONTAINER_NAME

    config_file = "akara.ini"
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file))
    RS_USERNAME = config.get("Rackspace", "Username")
    RS_APIKEY = config.get("Rackspace", "ApiKey")
    RS_CONTAINER_NAME = config.get("Rackspace", container)

    if not RS_USERNAME:
        print "There is no Rackspace Username in the configuration file"
        exit(1)

    if not RS_APIKEY:
        print "There is no Rackspace ApiKey in the configuration file"
        exit(1)

    if not RS_CONTAINER_NAME:
        print "There is no Rackspace Container in the configuration file"
        exit(1)

def send_file_to_rackspace(fname):
    """Sends the created file to the Rackspace CDN.

    Arguments:
        arguments - dictionary returned by the validate_arguments function

    The file saved to arguments["file"] is uploaded to Rackspace CDN
    and stored in container RS_CONTAINER_NAME using the current file name.
    The file name is taken with the extension, but without the whole path.

    After loding the file, th function checks if the file is listed in the
    container objects list.
    """
    rsfname = fname.split('/')[-1:][0]
    container = get_rackspace_container()
    f = container.create_object(rsfname)

    print "Uploading %s" % fname
    f.load_from_filename(fname)

    if file_is_in_container(rsfname, container):
        rs_file_uri = url_join(container.public_uri(), rsfname)
        print "... %s" % rs_file_uri
    else:
        print "Couldn't upload file."

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

    print
    print "Container name      : %s" % container.name
    print "Container public URI: %s" % container.public_uri()
    print "Container size      : %s" % container.size_used

    objects = get_sorted_objects_from_container(container)
    print "There are %d files." % len(objects)

    for ob in objects:
        print
        print "File name:     %s" % ob["name"]
        print "Last modified: %s" % ob["last_modified"]
        print "Size:          %s" % convert_bytes(ob["bytes"])
        print "Public URI:    %s/%s" % (container.public_uri(), ob["name"])

def item_docs(provider_name=None):
    """Yield all item documents for the given provider, else all providers"""
    couch = Couch()
    if provider_name:
        docs = couch._query_all_dpla_provider_docs(provider_name)
    else:
        docs = couch.all_dpla_docs()
    for doc in docs:
        if doc.get("ingestType") == "item":
            yield doc

def profile_names_for_contributor(contributor):
    """Return a list of profile names that use the given contributor name"""
    profiles = {}
    couch = Couch()
    view = "export_database/profile_and_source_names"
    for row in couch.dpla_view(view, group=True):
        k = row["key"]
        profiles.setdefault(k[0], []).append(k[1])
    return profiles.get(contributor, [])

def download_data(arguments):
    """
    Download all documents for the given source.

    Arguments:
        arguments - dictionary returned by the validate_arguments function

    The source name is taken from arguments["source"].

    The file name is set to the lower case arguments["source"] string with
    whitespace replaced by underscores.

    Upload the file to Rackspace CDN, if required.
    """
    source = arguments["source"]
    try:
        if source == "all":
            filename = "dpla.gz"
            bulk_doc_provider_name = "Complete Repository"
        else:
            filename = source.lower().replace(" ", "_") + ".gz"
            bulk_doc_provider_name = source
        with gzip.GzipFile(filename, "w") as outfile:
            outfile.write("[\n")
            if source == "all":
                total_rows = download_all_data(outfile)
            else:
                total_rows = download_source_data(outfile, source)
            outfile.write("\n]\n")
            outfile.close()
            print "source: %s, total records: %s" % (source, total_rows)
        if arguments["upload"]:
            uri = send_file_to_rackspace(filename)
            statinfo = os.stat(filename)
            update_bulk_download_document(bulk_doc_provider_name,
                                          uri,
                                          convert_bytes(statinfo.st_size))
    except Exception as e:
        raise
        print >> sys.stderr, "Caught %s: %s" % (e.__class__, e.message)
        print >> sys.stderr, "File %s not uploaded" % filename

def download_all_data(outfile):
    """
    Download all item records from the given database and write them to the
    given file.
    """
    total_rows = 0
    comma = ""  # Comma between item_docs() results, empty before first one
    for item in item_docs():
        total_rows += 1
        outfile.write(comma)
        if not comma:
            comma = ",\n"
        outfile.write(json.dumps(item))
    return total_rows

def download_source_data(outfile, source):
    """
    Download all item records from the given database, for the given source,
    and write them tot he given file.
    """
    profile_names = profile_names_for_contributor(source)
    total_rows = 0
    comma = ""  # Comma between item_docs() results, empty before first one
    for profile_name in profile_names:
        for item in item_docs(profile_name):
            total_rows += 1
            outfile.write(comma)
            if not comma:
                comma = ",\n"
            outfile.write(json.dumps(item))
    return total_rows

def download_each_source_data(arguments):
    """Gets a list of all sources in the Couch database and downloads each
       source's data.

       Arguments:
           arguments - dictionary returned by the validate_arguments function

    """
    couch = Couch()
    rows = couch.dpla_view("export_database/all_source_names", group=True)
    for row in rows:
        arguments["source"] = row["key"]
        print arguments["source"]
        download_data(arguments)

def print_all_sources(arguments):
    """Print all source names"""
    couch = Couch()
    rows = couch.dpla_view("export_database/all_source_names", group=True)
    for row in rows:
        print "%(key)s (count: %(value)d)" % dict(row)

def url_join(*args):
    """Joins and returns given urls.

    Arguments:
        list of elements to join

    Returns:
        string with all elements joined with '/' inserted between

    """
    return "/".join(map(lambda x: str(x).rstrip("/"), args))

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

        res["source"] = "all"
        res["upload"] = False

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
        "all":    download_data,
        "list":   print_all_sources,
        "source": download_data,
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
