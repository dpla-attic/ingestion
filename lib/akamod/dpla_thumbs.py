# -*- encoding: utf-8 -*-
'''
This file is part of the open source Akara project,
provided under the Apache 2.0 license.
See the files LICENSE and NOTICE for details.
Project home, documentation, distributions: http://wiki.xml3k.org/Akara

 Module name:: freemix_akara.oai

Scrapes collections from a OAI site into JSON form for Freemix

= Defined REST entry points =

Updated document.

dpla-thumbs-update-doc Handles POST

Returns documents wich need to have thumbnail downloaded.

dpla-thumbs-list-for-downloading GET

= Configuration =

None

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

COUCH_DATABASE = module_config().get('couch_database')
COUCH_DATABASE_USERNAME = module_config().get('couch_database_username')
COUCH_DATABASE_PASSWORD = module_config().get('couch_database_password')

COUCH_AUTH_HEADER = { 'Authorization' : 'Basic ' + base64.encodestring(COUCH_DATABASE_USERNAME+":"+COUCH_DATABASE_PASSWORD) }
CT_JSON = {'Content-Type': 'application/json'}

VIEW_APP = "thumbnails"
VIEW_NAME = "all_for_downloading"

UPDATE_SERVICE_ID = 'http://purl.org/la.dp/dpla-thumbs-update-doc'
LISTRECORDS_SERVICE_ID = 'http://purl.org/la.dp/dpla-thumbs-list-for-downloading'


@simple_service('POST', UPDATE_SERVICE_ID, 'dpla-thumbs-update-doc', 'application/json')
def update_document(document, doctype):
    couch = Server(COUCH_DATABASE_URL)
    db = couch[COUCH_DATABASE_NAME]
    # TODO update the document
    return document

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
    

