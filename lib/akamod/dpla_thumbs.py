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

RAW_COUCH_DATABASE = module_config().get('couch_database')
RAW_COUCH_DATABASE_USERNAME = module_config().get('couch_database_username')
RAW_COUCH_DATABASE_PASSWORD = module_config().get('couch_database_password')

db = RAW_COUCH_DATABASE.split("/")

COUCH_DATABASE_URL = "%s//%s:%s@%s" % (db[0], RAW_COUCH_DATABASE_USERNAME, RAW_COUCH_DATABASE_PASSWORD, db[2])
COUCH_DATABASE_NAME=db[3]

VIEW_NAME = "thumbnails/all_for_downloading"

UPDATE_SERVICE_ID = 'http://purl.org/la.dp/dpla-thumbs-update-doc'
LISTRECORDS_SERVICE_ID = 'http://purl.org/la.dp/dpla-thumbs-list-for-downloading'

@simple_service('POST', UPDATE_SERVICE_ID, 'dpla-thumbs-update-doc', 'application/json')
def update_document(document, doctype):
    couch = Server(COUCH_DATABASE_URL)
    db = couch[COUCH_DATABASE_NAME]
    # TODO update the document
    return document

@simple_service('GET', LISTRECORDS_SERVICE_ID, 'dpla-thumbs-list-for-downloading', 'application/json')
def listrecords(limit=1000):

    couch = Server(COUCH_DATABASE_URL)
    db = couch[COUCH_DATABASE_NAME]
    return db.view(VIEW_NAME)[1]
    

