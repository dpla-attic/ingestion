
"""
oaiservice class, for listing OAI sets and records
"""


import sys
import time, logging
import urllib
import urllib2
from amara.pushtree import pushtree
from akara import logger
from dplaingestion.utilities import iterify
import xmltodict
from dplaingestion.selector import getprop

XML_PARSE = lambda doc: xmltodict.parse(doc, xml_attribs=True, attr_prefix='', force_cdata=False, ignore_whitespace_cdata=True)

PREFIXES = {
    u'o': u'http://www.openarchives.org/OAI/2.0/',
    u'dc': u'http://purl.org/dc/elements/1.1/',
    u'oai_dc': u'http://www.openarchives.org/OAI/2.0/oai_dc/',
    u'qdc': u'http://epubs.cclrc.ac.uk/xmlns/qdc/',
    u'marc': u'http://www.loc.gov/MARC21/slim'
}

metadata_field_map = {
    "oai_dc": {
        "dc:accrualmethod": lambda (v): {"accrualmethod": v},
        "dc:audience": lambda (v): {"audience": v},
        "dc:contributor": lambda (v): {"contributor": v},
        "dc:coverage": lambda (v): {"coverage": v},
        "dc:creator": lambda (v): {"creator": v},
        "dc:date": lambda (v): {"date": v},
        "dc:description": lambda (v): {"description": v},
        "dc:format": lambda (v): {"format": v},
        "dc:identifier": lambda (v): {"handle": v},
        "dc:instructionalmethod": lambda (v): {"instructionalmethod": v},
        "dc:language": lambda (v): {"language": v},
        "dc:provenance": lambda (v): {"provenance": v},
        "dc:publisher": lambda (v): {"publisher": v},
        "dc:relation": lambda (v): {"relation": v},
        "dc:rights": lambda (v): {"rights": v},
        "dc:rightsholder": lambda (v): {"rightsholder": v},
        "dc:source": lambda (v): {"source": v},
        "dc:subject": lambda (v): {"subject": v},
        "dc:title": lambda (v): {"title": v},
        "dc:type": lambda (v): {"type": v}
    },
    "qdc": {
        "dc:contributor": lambda (v): {"contributor": v},
        "dc:creator": lambda (v): {"creator": v},
        "dc:date": lambda (v): {"date": v},
        "dc:description": lambda (v): {"description": v},
        "dc:format": lambda (v): {"format": v},
        "dc:identifier": lambda (v): {"handle": v},
        "dc:language": lambda (v): {"language": v},
        "dc:publisher": lambda (v): {"publisher": v},
        "dc:relation": lambda (v): {"relation": v},
        "dc:rights": lambda (v): {"rights": v},
        "dc:source": lambda (v): {"source": v},
        "dc:subject": lambda (v): {"subject": v},
        "dc:title": lambda (v): {"title": v},
        "dc:type": lambda (v): {"type": v},
        "dcterms:alternative": lambda (v): {"alternative": v},
        "dcterms:contributor": lambda (v): {"contributor": v},
        "dcterms:created": lambda (v): {"created": v},
        "dcterms:creator": lambda (v): {"creator": v},
        "dcterms:extent": lambda (v): {"extent": v},
        "dcterms:hasFormat": lambda (v): {"hasFormat": v},
        "dcterms:isPartOf": lambda (v): {"isPartOf": v},
        "dcterms:mediator": lambda (v): {"mediator": v},
        "dcterms:medium": lambda (v): {"medium": v},
        "dcterms:provenance": lambda (v): {"provenance": v},
        "dcterms:spatial": lambda (v): {"spatial": v},
        "dcterms:temporal": lambda (v): {"temporal": v},
        "edm:dataProvider": lambda (v): {"dataProvider": v},
        "edm:isShownAt": lambda (v): {"isShownAt": v},
        "edm:preview": lambda (v): {"preview": v},
        "dct:accessRights": lambda (v): {"accessRights": v},
        "dct:alternative": lambda (v): {"dctAlternative": v},
        "dct:isPartOf": lambda (v): {"isPartOf": v},
        "dct:rightsHolder": lambda (v): {"rightsHolder": v},
        "dct:spatial": lambda (v): {"spatial": v},
        "dct:temporal": lambda (v): {"temporal": v}
    }
}

header_field_map = {
    "oai_dc": {
        "datestamp": lambda (v): {"datestamp": v},
        "setSpec": lambda (v): {"setSpec": v}
    },
    "qdc": {
        "datestamp": lambda (v): {"datestamp": v},
        "setSpec": lambda (v): {"setSpec": v}
    }
}


class OAIError(Exception):
    pass

class OAIHTTPError(Exception):
    pass

class OAIParseError(Exception):
    pass


class oaiservice(object):
    """
    Class for listing OAI sets and records

    list_sets()
      - returns list of dictionaries
    list_records()
      - returns dictionary with keys "records", "resumption_token", and
        "error", where records is a list of dictionaries
    """
    def __init__(self, root, logger=logging):
        '''
        root - root of the OAI service endpoint, e.g. http://dspace.mit.edu/oai/request
        '''
        self.root = root
        self.logger = logger
        return
    
    def list_sets(self):

        sets = []
        resumptionToken = ''
        #e.g. http://dspace.mit.edu/oai/request?verb=ListSets
        params = {'verb' : 'ListSets'}

        while True:

            if resumptionToken:
                params['resumptionToken'] = resumptionToken

            qstr = urllib.urlencode(params)

            url = self.root + '?' + qstr
            self.logger.debug('OAI request URL: {0}'.format(url))
            start_t = time.time()
            try:
                content = urllib2.urlopen(url).read()
            except urllib2.URLError as e:
                raise OAIHTTPError("list_sets could not make request: %s" % \
                                   e.reason)
            except urllib2.HTTPError as e:
                raise OAIHTTPError("list_sets got status %d: %s" % \
                                   (e.code, e.reason))
            retrieved_t = time.time()
            self.logger.debug('Retrieved in {0}s'.format(retrieved_t - start_t))

            paths = [
                u'string(o:setDescription/oai_dc:dc/dc:description)',
                u'string(o:setDescription/o:oclcdc/dc:description)',
                u'string(o:setDescription/dc:description)',
                u'string(o:setDescription)'
            ]
            def receive_nodes(n):
                setSpec = n.xml_select(u'string(o:setSpec)', prefixes=PREFIXES)
                setName = n.xml_select(u'string(o:setName)', prefixes=PREFIXES)
                #TODO better solution is to traverse setDescription amara tree
                for p in paths:
                    setDescription = n.xml_select(p, prefixes=PREFIXES)
                    if setDescription:
                        break
                sets.append(dict([('setSpec', setSpec), ('setName', setName), ('setDescription', setDescription)]))

            pushtree(content, u"o:OAI-PMH/o:ListSets/o:set", receive_nodes, namespaces=PREFIXES)
            try:
                xml_content = XML_PARSE(content)

                resumptionToken = \
                    xml_content["OAI-PMH"]["ListSets"].get("resumptionToken","")
            except KeyError:
                try:
                    error = xml_content["OAI-PMH"]["error"]
                    raise OAIError(error)
                except KeyError:
                    raise OAIParseError("Could not parse %s:\n%s" % (url, xml_content))
            if isinstance(resumptionToken, dict):
                resumptionToken = resumptionToken.get("#text", "")

            # Apply resumptionToken to sets
            if not resumptionToken:
                break

        return sets

    def list_records(self, set_id=None, resumption_token="", metadataPrefix="",
                     frm=None, until=None):
        '''
        List records. Use either the resumption token or set id.
        '''
        error = None
        records = []

        params = {
                    'verb' : 'ListRecords',
                    'metadataPrefix': metadataPrefix
        }
        if set_id:
            params["set"] = set_id
        metadataPrefix = metadataPrefix.lower()
        if frm:
            params["from"] = frm
        if until:
            params["until"] = until

        # Override params if we have a resumption_token
        if resumption_token:
            params = {'verb' : 'ListRecords', 'resumptionToken': resumption_token}
        
        qstr = urllib.urlencode(params)
        url = self.root + '?' + qstr
        self.logger.debug('OAI request URL: {0}'.format(url))
        start_t = time.time()
        try:
            content = urllib2.urlopen(url).read()
        except urllib2.URLError as e:
            raise OAIHTTPError("list_records could not make request: %s" % \
                               e.reason)
        except urllib2.HTTPError as e:
            raise OAIHTTPError("list_records got status %d: %s" % \
                               (e.code, e.reason))
        retrieved_t = time.time()
        self.logger.debug('Retrieved in {0}s'.format(retrieved_t - start_t))

        try:
            xml_content = XML_PARSE(content)
        except:
            self.logger.error("Could not parse:\n%s" % content)
            raise

        try:
            resumption_token = \
                xml_content["OAI-PMH"]["ListRecords"].get("resumptionToken",
                                                          "")
        except KeyError:
            try:
                error = xml_content["OAI-PMH"]["error"]
                raise OAIError(error)
            except KeyError:
                raise OAIParseError("Could not parse %s:\n%s" % (url, content))
        if isinstance(resumption_token, dict):
            resumption_token = resumption_token.get("#text", "")
        if isinstance(xml_content['OAI-PMH']['ListRecords']['record'], dict):
            lr_records = [xml_content['OAI-PMH']['ListRecords']['record']]
        else:
            lr_records = xml_content['OAI-PMH']['ListRecords']['record']
        for full_rec in lr_records:
            try:
                if not 'deleted' in full_rec['header'].get('status', ''):
                    header = full_rec['header']
                    rec_id = full_rec['header']['identifier']
                    # Due to the way this function used to be written, code for
                    # different metadata formats still expect the data to be
                    # formatted differently.
                    if metadataPrefix in ['marc', 'marc21', 'mods', 'untl']:
                        md = full_rec['metadata']
                        if metadataPrefix in ('marc', 'marc21'):
                            rec_field = md.get('record') or md.get('marc:record')
                        elif metadataPrefix == 'untl':
                            rec_field = md['untl:metadata']
                        else:
                            rec_field = md.get('mods:mods') or md.get('mods')
                        status = rec_field.get('status', '')
                        if not 'deleted' in status:
                            records.append((rec_id, full_rec))
                    else:
                        # (This is the condition that we eventually want to make
                        # the only one, doing away with this if/else. This will
                        # require refactoring the enrichment modules for the MARC,
                        # MODS, and UNTL providers.)
                        #
                        # The following key (element 0) should be the only one, and
                        # will be something like "oai_dc:dc" or "mods:mods"
                        k = full_rec['metadata'].keys()[0]
                        orig = dict(full_rec['metadata'][k])
                        record = self.record_for_prefix(metadataPrefix,
                                                        orig,
                                                        header)
                        if not 'deleted' in record.get('status', ''):
                            records.append((rec_id, record))
            except Exception as e:
                logger.error("Unable to process record in #list_records(): \n\t%s" % e)

        return {'records': records, 'resumption_token': resumption_token,
                'error': error}

    def record_for_prefix(self, prefix, orig, header):
        """
        Given an OAI-PMH metadata prefix value (indicating particular XML
        element names), return a dictionary of the given "orig" dictionary
        mapped to new keys.

        If the given metadata prefix isn't represented in our mapping, just
        return the original, because it doesn't have to be modified (our
        fetcher or enrichment code being written to work with it as-is).

        If we're using one of our mappings, map fields that are defined in the
        mapping, but silently ignore fields that are not.
        """
        map_key = prefix.lower()
        if map_key in ["oai_qdc", "oai_qdc_imdpla", "dpla_dc"]:
            map_key = "qdc"
        mfm = metadata_field_map.get(map_key)
        hfm = header_field_map.get(map_key)
        if mfm or hfm:
            if mfm:
                record = {}
                for k in orig.keys():
                    if k.startswith("xmlns:") or k.startswith("xsi:"):
                        continue
                    f = mfm.get(k, lambda (v): {}) # ignore if undefined
                    v = orig[k]
                    record.update(f(v))
            else:
                record = orig
            if hfm:
                for k in header.keys():
                    f = hfm.get(k, lambda (v): {}) # ignore if undefined
                    v = header[k]
                    record.update(f(v))
            record["status"] = record.get("status", [])
            return record
        else:
            return orig

