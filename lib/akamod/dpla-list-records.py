# -*- encoding: utf-8 -*-
'''
@ 2011 by Uche ogbuji <uche@ogbuji.net>

This file is part of the open source Akara project,
provided under the Apache 2.0 license.
See the files LICENSE and NOTICE for details.
Project home, documentation, distributions: http://wiki.xml3k.org/Akara

 Module name:: freemix_akara.oai

Scrapes collections from a OAI site into JSON form for Freemix

= Defined REST entry points =

http://purl.org/com/zepheira/freemix/services/oai.json (freemix_akara.oai) Handles GET

= Configuration =

None

= Notes on security =

This makes heavy access to remote OAI sites

= Notes =

Adapted 2012 by Jeffrey Licht to support resumption tokens

'''

import sys, time

from amara.thirdparty import json

from akara.services import simple_service
from akara import logger
from akara import module_config

from dplaingestion.oai import oaiservice


LISTRECORDS_SERVICE_ID = 'http://purl.org/la.dp/dpla-list-records'

@simple_service('GET', LISTRECORDS_SERVICE_ID, 'dpla-list-records', 'application/json')
def listrecords(endpoint, oaiset=None, resumption_token=None, limit=1000):
    """
    e.g.:

    curl "http://localhost:8880/oai.listrecords.json?oaiset=hdl_1721.1_18193&limit=10"
    """
    limit = int(limit)
    if not oaiset:
        raise ValueError('OAI set required')

    remote = oaiservice(endpoint, logger)
    list_records_result = remote.list_records(set=oaiset,resumption_token=resumption_token)

    records = list_records_result['records'][:limit]
    resumption_token = list_records_result['resumption_token'] if 'resumption_token' in list_records_result else ''

    exhibit_records = []
    properties_used = set() # track the properties in use
    for rid, rinfo in records:
        erecord = {u'id': rid}
        for k, v in rinfo.iteritems():
            if len(v) == 1:
                erecord[k] = v[0]
            else:
                erecord[k] = v
            if u'title' in erecord:
                erecord[u'label'] = erecord[u'title']

        properties_used.update(erecord.keys())
        exhibit_records.append(erecord)

    PROFILE["properties"][:] = strip_unused_profile_properties(PROFILE["properties"],properties_used)

    #FIXME: This profile is NOT correct.  Dumb copy from CDM endpoint.  Please fix up below
    return json.dumps({'items': exhibit_records, 'data_profile': PROFILE, 'resumption_token': resumption_token}, indent=4)

# Rebuild the data profile by removing optional, unused properties
strip_unused_profile_properties = lambda prof_props, used: [ p for p in prof_props if p["property"] in used ]

#FIXME: This profile is NOT correct.  Dumb copy from CDM endpoint.
PROFILE = {
    #"original_MIME_type": "application/vnd.ms-excel", 
    #"Akara_MIME_type_magic_guess": "application/vnd.ms-excel", 
    #"url": "/data/uche/amculturetest/data.json", 
    #"label": "amculturetest", 
    "properties": [
        {
            "property": "handle", 
            "enabled": True, 
            "label": "Handle", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "language", 
            "enabled": True, 
            "label": "Language", 
            "types": [
                "text"
            ], 
            "tags": [
            ]
        }, 
        {
            "property": "creator", 
            "enabled": True, 
            "label": "Creators", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "format", 
            "enabled": True, 
            "label": "Formats", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "relation", 
            "Enabled": True, 
            "label": "Relations", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "id", 
            "enabled": False, 
            "label": "id", 
            "types": [
                "text"
            ], 
            "tags": [
                "property:type=url"
            ]
        }, 
        {
            "property": "date", 
            "enabled": True, 
            "label": "Date", 
            "tags": [
                "property:type=date", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "datestamp", 
            "enabled": True, 
            "label": "Date stamp", 
            "tags": ["property:type=date"]
        }, 
        {
            "property": "title", 
            "enabled": True, 
            "label": "Title", 
            "types": [
                "text"
            ], 
            "tags": []
        }, 
        {
            "property": "description", 
            "enabled": True, 
            "label": "Description", 
            "types": [
                "text"
            ], 
            "tags": []
        }, 
        {
            "property": "subject", 
            "enabled": True, 
            "label": "Subject", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "contributor", 
            "enabled": True, 
            "label": "Contributor", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "publisher", 
            "enabled": True, 
            "label": "Publisher", 
            "types": [
                "text"
            ], 
            "tags": []
        }, 
        {
            "property": "instructionalmethod", 
            "enabled": True, 
            "label": "Instructional Method", 
            "types": [
                "text"
            ], 
            "tags": []
        }, 
        {
            "property": "accrualmethod", 
            "enabled": True, 
            "label": "Accrual Method", 
            "types": [
                "text"
            ], 
            "tags": []
        }, 
        {
            "property": "source", 
            "enabled": True, 
            "label": "Source", 
            "types": [
                "text"
            ], 
            "tags": []
        }, 
        {
            "property": "provenance", 
            "enabled": True, 
            "label": "Provenance", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "rights", 
            "enabled": True, 
            "label": "Rights", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "rightsholder", 
            "enabled": True, 
            "label": "Rights Holder", 
            "types": [
                "text"
            ], 
            "tags": []
        }, 
        {
            "property": "coverage", 
            "enabled": True, 
            "label": "Coverage", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "audience", 
            "enabled": True, 
            "label": "Audience", 
            "tags": [
                "property:type=text", "property:type=shredded_list"
            ]
        }, 
        {
            "property": "label", 
            "enabled": True, 
            "label": "Label", 
            "types": [
                "text"
            ], 
            "tags": []
        }, 
        {
            "property": "type", 
            "enabled": True, 
            "label": "Document Type", 
            "types": [
                "text"
            ], 
            "tags": []
        },
    ], 
    #"Akara_MIME_type_guess": "application/vnd.ms-excel"
}
