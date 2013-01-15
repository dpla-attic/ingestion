#
# Reponsible for:  downloading a preview for a document
# Usage: as a module in separate pipeline, to be run on existing documents in the repository to download the thumbnails
#

from amara.thirdparty import json, httplib2
from amara.lib.iri import join
import logger
import logger.handlers
import logger.config
from StringIO import StringIO
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

def update_document(document, filepath):
    """
    updates the document setting a filepath to a proper variable.

    arguments:
        document object - document for updating (decoded by json module)
        filepath string - filepath to insert

    returns:
        the document from parameter with additional field containing the filepath.
    """
    document[URL_FILE_PATH] = filepath
    return document


def generate_file_path(id, file_number, file_extension):
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
        file_number    - the number of the file added just before the extension
        file_extension - extension of the file

    Returns:
        filepath       - path, without file name
        full_filepath  - path, with file name

    Example:
        Function call:
            generate_file_path('clemsontest--hcc001-hcc016', 1, "jpg")

        Generated values for the algorithm steps:

        CLEARED_ID: clemsontest__hcc001_hcc016
        FILE_NAME:  clemsontest__hcc001_hcc016_1.jpg
        HASHED_ID:  8E393B3B5DA0E0B3A7AEBFB91FE1278A
        PATH:       8E/39/3B/3B/5D/A0/E0/B3/A7/AE/BF/B9/1F/E1/27/8A/
        FULL_NAME:  /tmp/szymon/main_pic_dir/8E/39/3B/3B/5D/A0/E0/B3/A7/AE/BF/B9/1F/E1/27/8A/clemsontest__hcc001_hcc016_1.jpg
    """

    logger.debug("Generating filename for document")

    cleared_id = re.sub(r'[-]', '_', id)
    logger.debug("Cleared id: " + cleared_id)
    
    fname = "%s_%s.%s" % (cleared_id, file_number, file_extension)
    logger.debug("File name:  " + fname)
    
    md5sum = hashlib.md5(id).hexdigest().upper()
    logger.debug("Hashed id:  " + md5sum)
    
    path = re.sub("(.{2})", "\\1" + os.sep, md5sum, re.DOTALL)
    logger.debug("PATH:       " + path)
    
    path = os.path.join(THUMBS_ROOT_PATH, path)
    full_fname = os.path.join(path, fname)
    logger.debug("FULL PATH:  " + full_fname)

    return (path, full_fname)


def download_image(url, id, file_number=1):
    """
    Downloads the thumbnail from the given url and stores it on disk.

    Current implementation stores the file on disk

    Params:
        url         - the url of the file for downloading
        id          - document id, used for the file name generation
        file_number - number of the file for this document

    Returns:
        Name of the file where the image was stored - if everything is OK
        False       - otherwise
    """

    # Get the thumbnail extension from the URL, needed for storing the 
    # file on disk with proper extension.
    fileName, fileExtension = os.path.splitext(url)
    file_extension = fileExtension[1:]

    # Get the directory path and file path for storing the image.
    (path, fname) = generate_file_path(id, file_number, file_extension)
    
    # Let's create the directory for storing the file name.
    if not os.path.exists(path):
        logger.info("Creating directory: " + path)
        os.makedirs(path)
    else:
        logger.debug("Path exists")

    # Open connection to the image using provided URL.
    conn = urllib.urlopen(url)
    if not conn.getcode() / 100 == 2:
        msg = "Got %s from url: [%s] for document: [%s]" % (conn.getcode(), url, id)
        logger.error(msg)
        return False

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
    logger.debug("File downloaded")
    return fname

@simple_service('POST', 'http://purl.org/la/dp/indentify_preview_location', 'download_preview', 'application/json')
def download_preview(body, ctype):
    """
    Responsible for downloading thumbnail.
    """

    try:
        data = json.loads(body)
        url = data[URL_FIELD_NAME]
        id = data[u'id']
        filepath = download_image(url, id)
        if filepath: 
            # so everything is OK and the file is on disk
            doc = update_document(document, filepath)
            return json.dumps(doc)
        else:
            raise Exception("Cannot save thumbnail.")
    except Exception as e:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return e.message

    
