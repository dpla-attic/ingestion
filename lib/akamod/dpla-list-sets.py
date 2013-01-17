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

Forked for DPLA, 2013

'''

import sys, time

from amara.thirdparty import json

from akara.services import simple_service
from akara import logger
from akara import module_config

from dplaingestion.oai import oaiservice

LISTSETS_SERVICE_ID = 'http://purl.org/la.dp/dpla-list-sets'

@simple_service('GET', LISTSETS_SERVICE_ID, 'oai.listsets.json', 'application/json')
def listsets(endpoint, limit=100):
    """
    e.g.:

    curl "http://localhost:8880/oai.listsets.json?limit=10"
    """
    limit = int(limit)
    remote = oaiservice(endpoint, logger)
    sets = remote.list_sets()[:limit]
    return json.dumps(sets, indent=4)
