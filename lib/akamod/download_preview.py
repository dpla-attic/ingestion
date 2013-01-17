#
# Reponsible for:  downloading a preview for a document
# Usage: as a module in separate pipeline, to be run on existing documents in the repository to download the thumbnails
#

from amara.thirdparty import json, httplib2
from amara.lib.iri import join
from StringIO import StringIO
from akara import module_config
import pprint
import sys
import re
import hashlib
import os
import os.path
import urllib
from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

# Used for searching for the thumbnail URL.
URL_FIELD_NAME = u"preview_source_url"

# Used for storing the path to the local filename.
URL_FILE_PATH = u"preview_file_path"

THUMBS_ROOT_PATH = module_config().get('thumbs_root_path')


def update_document(document, filepath):
    """
    Updates the document setting a filepath to a proper variable.

    Arguments:
        document object - document for updating (decoded by json module)
        filepath string - filepath to insert

    Returns:
        the document from parameter with additional field containing the filepath.
    """
    document[URL_FILE_PATH] = filepath
    return document


def generate_file_path(id, file_extension):
    """
    Generates and returns the file path based in provided params.

    Algorithm:

      The file path is generated using the following algorithm:

        -   convert all not allowed characters from the document id to "_"
        -   to the above string add number and extension getting FILE_NAME
        -   calculate md5 from original id
        -   convert to uppercase
        -   insert "/" between each to characters of this hash getting CALCULATED_PATH
        -   join the MAIN_PATH, CALCULATED_PATH and FILE_NAME

    Arguments:
        id             - document id from couchdb  
        file_extension - extension of the file

    Returns:
        filepath       - path, without file name
        full_filepath  - path, with file name

    Example:
        Function call:
            generate_file_path('clemsontest--hcc001-hcc016', ".jpg")

        Generated values for the algorithm steps:

        CLEARED_ID: clemsontest__hcc001_hcc016
        FILE_NAME:  clemsontest__hcc001_hcc016.jpg
        HASHED_ID:  8E393B3B5DA0E0B3A7AEBFB91FE1278A
        PATH:       8E/39/3B/3B/5D/A0/E0/B3/A7/AE/BF/B9/1F/E1/27/8A/
        FULL_NAME:  /tmp/szymon/main_pic_dir/8E/39/3B/3B/5D/A0/E0/B3/A7/AE/BF/B9/1F/E1/27/8A/clemsontest__hcc001_hcc016.jpg
    """

    logger.debug("Generating filename for document with id: [%s].", id)

    cleared_id = re.sub(r'[-]', '_', id)
    logger.debug("Cleared id: " + cleared_id)
    
    fname = "%s%s" % (cleared_id, file_extension)
    logger.debug("File name:  " + fname)
    
    md5sum = hashlib.md5(id).hexdigest().upper()
    logger.debug("Hashed id:  " + md5sum)
    
    path = re.sub("(.{2})", "\\1" + os.sep, md5sum, re.DOTALL)
    logger.debug("PATH:       " + path)
    
    path = os.path.join(THUMBS_ROOT_PATH, path)
    full_fname = os.path.join(path, fname)
    logger.debug("FULL PATH:  " + full_fname)

    return (path, full_fname)


class FileExtensionException(Exception):
    pass


def find_file_extension(conn):
    """
    Finds out the file extension based on the MIME type from the opened connection.

    Arguments:
        conn - opened connection to a resource

    Returns:
        file extension (String) - extension for the file - WITH DOT AT THE BEGINNING!!

    Throws:
        throws exception if it cannot find the extension
    """
    import mimetypes as m
    header = conn.headers['content-type']
    possible_extensions = m.guess_all_extensions(header)
    if possible_extensions:
        ext = ""
        if ".jpg" in possible_extensions: # as the default extension for 'image/jpeg' mimetype
            ext = ".jpg"
        else:
            ext = possible_extensions[0]
        logger.debug("Trying to find out extension for header [%s], found %s." % (header, possible_extensions))
        logger.debug("Chose %s." % ext)
        return ext
    else:
        msg = "Cannot find extension for mime type: [%s]." % header
        logger.error(msg)
        raise FileExtensionException(msg)


def download_image(url, id):
    """
    Downloads the thumbnail from the given url and stores it on disk.

    Current implementation stores the file on disk

    Arguments:
        url         - the url of the file for downloading
        id          - document id, used for the file name generation

    Returns:
        Name of the file where the image was stored - if everything is OK
        False       - otherwise
    """
    
    # Open connection to the image using provided URL.
    conn = urllib.urlopen(url)
    if not conn.getcode() / 100 == 2:
        msg = "Got %s from url: [%s] for document: [%s]" % (conn.getcode(), url, id)
        logger.error(msg)
        return False

    # Get the thumbnail extension from the URL, needed for storing the 
    # file on disk with proper extension.
    file_extension = ""
    try:
        file_extension = find_file_extension(conn)
    except FileExtensionException as e:
        logger.error("Couldn't find file extension.")
        return False
    
    # Get the directory path and file path for storing the image.
    (path, fname) = generate_file_path(id, file_extension)
    
    # Let's create the directory for storing the file name.
    if not os.path.exists(path):
        logger.info("Creating directory: " + path)
        os.makedirs(path)
    else:
        logger.debug("Path [%s] exists." % path)

    # Download the image.
    try:
        logger.info("Downloading file to: " + fname)
        local_file = open(fname, 'w')
        local_file.write(conn.read())
    except Exception as e:
        msg = traceback.format_exception(*sys.exc_info())
        logger.error(msg)
        return False
    else:
        conn.close()
        local_file.close()
    logger.debug("Downloaded file from [%s] to [%s].")
    return fname


class DownloadPreviewException(Exception):
    pass


@simple_service('POST', 'http://purl.org/la/dp/download_preview', 'download_preview', 'application/json')
def download_preview(body, ctype):
    """
    Responsible for downloading thumbnail.
    """

    data = {}
    try:
        data = json.loads(body)
    except Exception as e:
        msg = "Bad JSON: " + e.args[0]
        logger.error(msg)
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return msg

    if not data.has_key(URL_FIELD_NAME):
        logger.error("There is no '%s' key in JSON." % URL_FIELD_NAME)
        return body

    url = data[URL_FIELD_NAME]
    
    if not data.has_key(u'id'):
        logger.error("There is no '%s' key in JSON." % 'id')
        return body

    id = data[u'id']
    filepath = download_image(url, id)

    if filepath: 
        # so everything is OK and the file is on disk
        doc = update_document(data, filepath)
        return json.dumps(doc)
    else:
        logger.error("Cannot save thumbnail from: %s." % (url))
        return body

    
