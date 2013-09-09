"""
Akara module for extracting document source from dplaSourceRecord;
Assumes that original version of document is stored at dplaSourceRecord key in a json tree.
"""

__author__ = 'alexey'

import re

from akara import logger
from akara import response
from akara.services import simple_service
from amara.thirdparty import json

from dplaingestion import selector

@simple_service('POST', 'http://purl.org/la/dp/artstor_select_isshownat',
                'artstor_select_isshownat', 'application/json')
def artstor_select_source(body, ctype):

    try:
        data = json.loads(body)
    except:
        response.code = 500
        response.add_header('content-type', 'text/plain')
        return "Unable to parse body as JSON"

    original_document_key = u"originalRecord"
    original_sources_key = u"handle"
    artstor_source_probe = ("/object/", "Image View:")
    source_key = u"isShownAt"

    if original_document_key not in data:
        logger.error("There is no '%s' key in JSON for doc [%s].", original_document_key, data[u'id'])
        return body

    if original_sources_key not in data[original_document_key]:
        logger.error("There is no '%s/%s' key in JSON for doc [%s].", original_document_key, original_sources_key, data[u'id'])
        return body

    source = None
    http_re = re.compile("https?://.*$", re.I)
    for s in data[original_document_key][original_sources_key]:
        for probe in artstor_source_probe:
            if probe in s:
                match = re.search(http_re, s)
                if match:
                    source = match.group(0)
                    break

    if not source:
        logger.error("Can't find url with any of '%s' probe in [%s] for fetching document source for Artstor.", artstor_source_probe, data[original_document_key][original_sources_key])
        return body

    try:
        selector.setprop(data, source_key, source)
    except KeyError:
        logger.error("Can't set value, \"%s\" path does not exist in doc [%s]", source_key, data[u'id'])
        return body
    else:
        return json.dumps(data)


