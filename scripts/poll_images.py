#!/usr/bin/env python
#
# Usage: python poll_images.py <profiles-glob> <enrichment-service-URI.

import sys, os, glob

# Field name to search for the thumbnail URL.
URL_FIELD_NAME = "preview_source_url"

# Field name used for storing the path to the local filename.
URL_FILE_PATH = "preview_file_path"

# Root directory used for storing images
MAIN_PICTURE_DIRECTORY = "/tmp/main_pic_dir"

def generate_file_path(id, file_number, file_extension):
    """
    Function generates and returns the file path based in provided params.

    The file path is generated using the following algorithm:

        -   convert all not allowed characters from the document id to "_"
        -   to the above string add number and extension getting FILE_NAME
        -   calculate md5 from original id
        -   convert to uppercase
        -   insert "/" between each to characters of this hash getting CALCULATED_PATH
        -   join the MAIN_PATH, CALCULATED_PATH and FILE_NAME
    
    Params:
        id             - document id from couchdb  
        file_number    - the number of the file added just before the extension
        file_extension - extension of the file

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
    import re
    import hashlib
    import os

    cleared_id = re.sub(r'[-]', '_', id)
    print "Cleared id: " + cleared_id
    
    fname = "%s_%s.%s" % (cleared_id, file_number, file_extension)
    print "File name:  " + fname
    
    md5sum = hashlib.md5(id).hexdigest().upper()
    print "Hashed id:  " + md5sum
    
    path = re.sub("(.{2})", "\\1" + os.sep, md5sum, re.DOTALL)
    print "PATH:       " + path
    
    full_fname = os.path.join(MAIN_PICTURE_DIRECTORY, path, fname)

    return full_fname

print generate_file_path('clemsontest--hcc001-hcc016', 1, "jpg")


exit;

def process_profile():
    None

if __name__ == '__main__':

    process_profile()

    sys.exit()

    # Verify that both given directories exist
    for d in sys.argv[1:]:
        if os.path.isabs(d): continue # skip URIs

        dirExists = False

        try:
            if os.stat(d): dirExists = True
        except:
            pass

        if not dirExists:
            print >> sys.stderr, 'Directory '+d+' does not exist. Aborting.'
            sys.exit(1)

    for profile in glob.glob(sys.argv[1]):
        print >> sys.stderr, 'Processing profile: '+profile
        process_profile(sys.argv[2], profile)
