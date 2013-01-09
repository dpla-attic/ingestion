# -*- encoding: utf-8 -*-
'''
This file is part of the open source Akara project,
provided under the Apache 2.0 license.
See the files LICENSE and NOTICE for details.
Project home, documentation, distributions: http://wiki.xml3k.org/Akara

 Module name:: dpla_thumbs

= Defined REST entry points =

dpla-thumbs-update-doc Handles POST

    Updates document adding the thumbnail file path.

dpla-thumbs-list-for-downloading Handles GET

    Returns documents wich need to have thumbnail downloaded.

= Configuration =

This module inherits database configuration from enrich.py,
everything is defined in akara.conf.

= Notes on security =


= Notes =


'''

import sys, time

from amara.thirdparty import json

from akara.services import simple_service
from akara import logger
from akara import module_config

from dplaingestion.oai import oaiservice
from couchdb.client import Server

from akara.services import simple_service
from akara import request, response
from akara import module_config, logger
from akara.util import copy_headers_to_dict
from amara.thirdparty import json, httplib2
from amara.lib.iri import join
from urllib import quote
import datetime
import uuid
import base64

# Configuration for accessing the database.
COUCH_DATABASE = module_config().get('couch_database')
COUCH_DATABASE_USERNAME = module_config().get('couch_database_username')
COUCH_DATABASE_PASSWORD = module_config().get('couch_database_password')

COUCH_AUTH_HEADER = { 'Authorization' : 'Basic ' + base64.encodestring(COUCH_DATABASE_USERNAME+":"+COUCH_DATABASE_PASSWORD) }
CT_JSON = {'Content-Type': 'application/json'}

# The app name used for accessing the views.
VIEW_APP = "thumbnails"

# The view name for accessing the documents which need getting the thumbnail.
VIEW_NAME = "all_for_downloading"

UPDATE_SERVICE_ID = 'http://purl.org/la.dp/dpla-thumbs-update-doc'
LISTRECORDS_SERVICE_ID = 'http://purl.org/la.dp/dpla-thumbs-list-for-downloading'

#
# Service for updating the document. It just sends the document to the database.
# To avoid parsing the json multiple times, here will be sent the document and 
# the document id as separate parameter.
#
# PARAMS:
#   document_id - document id
#   document    - json containing the document
#
@simple_service('POST', UPDATE_SERVICE_ID, 'dpla-thumbs-update-doc', 'application/json')
def update_document(body, ctype):
    logger.debug(body)
    from StringIO import StringIO
    io = StringIO(body) 
    parsed_doc = json.load(io) 
    document_id = parsed_doc[u"id"]
    document  = body

    logger.debug("Storing the document: " + document_id)
    import httplib
    h = httplib2.Http()
    h.force_exception_as_status_code = True
    url = join(COUCH_DATABASE, document_id)
    resp, content = h.request(url, 'PUT', body=document, headers=COUCH_AUTH_HEADER)
    if str(resp.status).startswith('2'):
        return content
    else:
        logger.error("Couldn't store the document %(document) with the id: %(id)s. " % {'document':document, 'id':document_id} )

# 
# Service for getting all the documents which need downloading thumbnails.
# The logic is simple, it just queries a view defined in couchdb database.
# 
# PARAMS:
#   limit - the maximum number of records to return
#           default value is 100
#
@simple_service('GET', LISTRECORDS_SERVICE_ID, 'dpla-thumbs-list-for-downloading', 'application/json')
def listrecords(limit=100):
    import httplib
    h = httplib2.Http()
    h.force_exception_as_status_code = True
    url = join(COUCH_DATABASE, '_design', VIEW_APP, '_view', VIEW_NAME)
    url += '?limit=' + str(limit)
    logger.debug(url)
    resp, content = h.request(url, "GET", headers=COUCH_AUTH_HEADER)
    logger.debug("Content: " + content)
    if str(resp.status).startswith('2'):
        return content
    else:
        logger.error("Couldn't get documents via: " + repr(resp))
    

