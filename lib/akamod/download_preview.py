from amara.thirdparty import json, httplib2
from amara.lib.iri import join
from StringIO import StringIO
from akara import module_config
import pprint
import sys
import re
import os
import os.path
import urllib
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json
from dplaingestion.selector import getprop, setprop, exists


# The main directory where the images will be saved.
THUMBS_ROOT_PATH = module_config().get('thumbs_root_path')


# The dictionary containing mapping of MIME type to file extension.
# What's more, only those MIME types will be saved.
MIME_TYPES = module_config().get('mime_to_type')


def update_document(document, filepath, mime, status):
    """
    Updates the document with a filepath of downloaded thumbnail..

    Arguments:
        document object - document for updating (decoded by json module)
        filepath string - filepath to insert

    Returns:
        The document from parameter with additional field containing the
        filepath.
    """
    if filepath:
        base_url = module_config().get('thumbs_root_url')
        obj = document["object"]
        obj["@id"] = base_url + filepath
        obj["format"] = mime
        document["object"] = obj
    if mime:
        obj = document["object"]
        obj["format"] = mime
    if status:
        setprop(document, "admin/object_status", status)

    return document


def generate_file_path(id, file_extension):
    """
    Generates and returns the file path based in provided params.

    Algorithm for generating the file path:

      The file path is generated using the following algorithm:

        -   convert all not allowed characters from the document id to "_"
        -   to the above string add number and extension getting FILE_NAME
        -   fetch id (it will already be the md5 of the _id field)
        -   convert to uppercase
        -   insert "/" between each to characters of this hash getting CALCULATED_PATH
        -   join the MAIN_PATH, CALCULATED_PATH and FILE_NAME

    Arguments:
        id             - document id from couchdb
        file_extension - extension of the file

    Returns:
        filepath       - path, without file name
        full_filepath  - path, with file name
        relative_fname - path, relative, without ROOT_PATH

    Example:
        Function call:
            generate_file_path('clemsontest--hcc001-hcc016', ".jpg")

        Generated values for the algorithm steps:

        TODO: Update doc here for the new algorithm.

        CLEARED_ID: clemsontest__hcc001_hcc016
        FILE_NAME:  clemsontest__hcc001_hcc016.jpg
        HASHED_ID:  8E393B3B5DA0E0B3A7AEBFB91FE1278A
        PATH:       8E/39/3B/3B/5D/A0/E0/B3/A7/AE/BF/B9/1F/E1/27/8A/
        FULL_NAME:  /main_pic_dir/8E/39/3B/3B/5D/A0/E0/B3/A7/AE/BF/B9/1F/E1/27/8A/clemsontest__hcc001_hcc016.jpg
    """

    cleared_id = id.upper()
    logger.debug("Generating filename for document with id: [%s].", id)

    fname = "%s%s" % (cleared_id, file_extension)
    logger.debug("File name:  " + fname)

    path = re.sub("(.{2})", "\\1" + os.sep, cleared_id, re.DOTALL)
    logger.debug("PATH:       " + path)

    relative_fname = os.path.join(path, fname)

    path = os.path.join(THUMBS_ROOT_PATH, path)
    full_fname = os.path.join(path, fname)
    logger.debug("FULL PATH:  " + full_fname)

    return (path, full_fname, relative_fname)


class FileExtensionException(Exception):
    pass


def find_file_extension(mime):
    """
    Finds out the file extension based on the MIME type from the opened
    connection.

    Implementation:
        Function is using the configuration field 'mime_to_type' stored
        at akara.conf.

    Arguments:
        mime (String)   -   MIME type read from the HTTP headers

    Returns:
        file extension (String) - extension for the file -
        WITH DOT AT THE BEGINNING!!!

    Throws:
        throws exception if it cannot find the extension
    """

    if mime in MIME_TYPES:
        ext = MIME_TYPES[mime]
        logger.debug("MIME type is [%s], returning extension [%s]" % \
                (mime, ext))
        return ext
    else:
        msg = "Cannot find extension for mime type: [%s]." % mime
        logger.error(msg)
        raise FileExtensionException(msg)


def download_image(url, id, download):
    """
    Downloads the thumbnail from the given url and stores it on disk.

    Current implementation stores the file on disk

    Arguments:
        url      String - the url of the file for downloading
        id       String - document id, used for the file name generation
        download Bool   - True if download image
                          False if only check the mime type

    Returns:
        (Name, mime, status) - if everything was OK:

                - Name of the file where the image was stored
                - MIME type for the image
                - Status ("download"|"error")

    """
    name = None
    mime = None
    status = "error"

    def res(name, mime, status):
        return (name, mime, status)

    # Open connection to the image using provided URL.
    try:
        conn = urllib.urlopen(url)
    except IOError as e:
        logger.error("Cannot open url [%s] for downloading thumbnail." % url)
        return res(name, mime, status)

    if not conn.getcode() / 100 == 2:
        logger.error("Got %s from url: [%s] for document: [%s]" %
                     (conn.getcode(), url, id))
        return res(name, mime, status)

    # Get the thumbnail extension from the URL, needed for storing the
    # file on disk with proper extension.
    file_extension = ""
    mime = None
    try:
        # The content type from HTTP headers.
        mime = conn.headers['content-type']
        file_extension = find_file_extension(mime)
    except FileExtensionException as e:
        logger.error("Couldn't find file extension.")
        return res(name, mime, status)

    # so we should just check mime type
    if not download:
        return res(None, mime, None)

    # Get the directory path and file path for storing the image.
    (path, fname, relative_fname) = generate_file_path(id, file_extension)

    # Let's create the directory for storing the file name.
    if not os.path.exists(path):
        logger.info("Creating directory: " + path)
        os.makedirs(path)
    else:
        logger.debug("Path [%s] exists." % path)

    # Download the image.
    try:
        logger.info("Downloading file to: " + fname)
        local_file = open(fname, 'wb')
        local_file.write(conn.read())
    except Exception as e:
        logger.error(e.message)
        return res(name, mime, status)
    else:
        conn.close()
        local_file.close()

    logger.debug("Downloaded file from [%s] to [%s]." % (url, fname, ))
    status = "downloaded"
    name = relative_fname
    return res(name, mime, status)


class DownloadPreviewException(Exception):
    pass


def set_error(data):
    """
    Sets the "error" at "admin/object_status".
    """
    if "admin" in data:
        data["admin"]["object_status"] = "error"
    else:
        data["admin"] = {"object_status": "error"}

    return data


@simple_service('POST', 'http://purl.org/la/dp/download_preview',
    'download_preview', 'application/json')
def download_preview(body, ctype):
    """
    Reponsible for:  downloading a preview for a document
    Usage: as a module in separate pipeline, to be run on existing
    documents in the repository to download the thumbnails.
    """

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    # Check the "admin/object_status" field
    status = None
    try:
        status = getprop(data, "admin/object_status")
        if status in ["error", "downloaded"]:
            logger.debug("Status is %s, doing nothing" % status)
            return body
    except KeyError as e:
        logger.error(e.args[0])
        data = set_error(data)
        return json.dumps(data)

    # Thumbnail URL
    url = None
    try:
        url = getprop(data, "object/@id")
    except KeyError as e:
        logger.error(e.args[0])
        data = set_error(data)
        return json.dumps(data)

    # Document ID
    id = None
    try:
        id = getprop(data, "id")
    except KeyError as e:
        logger.error(e.args[0])
        data = set_error(data)
        return json.dumps(data)

    download = False
    if status == "pending":
        download = True

    (relative_fname, mime, status) = download_image(url, id, download)

    if not relative_fname:
        logger.error("Cannot save thumbnail from: %s." % (url))

    # so everything is OK and the file is on disk
    doc = update_document(data, relative_fname, mime, status)
    return json.dumps(doc)
