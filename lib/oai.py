
# -*- encoding: utf-8 -*-

# Copyright 2008-2009 Zepheira LLC

'''
OAI tools - evolution of oaitools.py in akara demos

Adapted 2012 for DPLA by Jeffrey Licht
'''
import sys
import time, logging
import urllib

from amara import bindery
from amara.bindery.model import examplotron_model, generate_metadata, metadata_dict
from amara.lib import U
from amara.pushtree import pushtree
from amara.thirdparty import httplib2
from akara import logger
from dplaingestion.utilities import iterify
import xmltodict
from dplaingestion.selector import getprop

OAI_NAMESPACE = u"http://www.openarchives.org/OAI/2.0/"

XML_PARSE = lambda doc: xmltodict.parse(doc,xml_attribs=True,attr_prefix='',force_cdata=False,ignore_whitespace_cdata=True)

#OAI-PMH verbs:
# * Identify
# * ListMetadataFormats
# * ListSets
# * GetRecord
# * ListIdentifiers
# * ListRecords

#Useful:
# http://www.nostuff.org/words/tag/oai-pmh/
# http://libraries.mit.edu/dspace-mit/about/faq.html
# http://wiki.dspace.org/index.php/OaiInstallations - List of OAI installations harvested by DSpace
#Examples:
# http://eprints.sussex.ac.uk/perl/oai2?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:eprints.sussex.ac.uk:67
# http://dspace.mit.edu/oai/request?verb=Identify
# http://dspace.mit.edu/oai/request?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:dspace.mit.edu:1721.1/5451

#Based on: http://dspace.mit.edu/oai/request?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:dspace.mit.edu:1721.1/5451

#http://dspace.mit.edu/search?scope=%2F&query=stem+cells&rpp=10&sort_by=0&order=DESC&submit=Go

DSPACE_SEARCH_PATTERN = u"http://dspace.mit.edu/search?%s"

DSPACE_ARTICLE = u"http://www.dspace.com/index/details.stp?ID="

RESULTS_DIV = u"aspect_artifactbrowser_SimpleSearch_div_search-results"

DSPACE_OAI_ENDPOINT = u"http://dspace.mit.edu/oai/request"

DSPACE_ARTICLE_BASE = u"http://dspace.mit.edu/handle/"

DSPACE_ID_BASE = u"oai:dspace.mit.edu:"

PREFIXES = {
    u'o': u'http://www.openarchives.org/OAI/2.0/',
    u'dc': u'http://purl.org/dc/elements/1.1/',
    u'oai_dc': u'http://www.openarchives.org/OAI/2.0/oai_dc/',
    u'qdc': u'http://epubs.cclrc.ac.uk/xmlns/qdc/',
    u'marc': u'http://www.loc.gov/MARC21/slim'
}

class oaiservice(object):
    """
    >>> from zen.oai import oaiservice
    >>> remote = oaiservice('http://dspace.mit.edu/oai/request')
    >>> sets = remote.list_sets()
    >>> sets[0]
    >>> first_set = sets[0][0]
    >>> records = remote.list_records(first_set)
    >>> records

    If you want to see the debug messages, just do (before calling read_contentdm for the first time):

    >>> import logging; logging.basicConfig(level=logging.DEBUG)

    """
    def __init__(self, root, logger=logging, cachedir='/tmp/.cache'):
        '''
        root - root of the OAI service endpoint, e.g. http://dspace.mit.edu/oai/request
        '''
        self.root = root
        self.logger = logger
        self.h = httplib2.Http(cachedir)
        return
    
    def list_sets(self):
        #e.g. http://dspace.mit.edu/oai/request?verb=ListSets
        qstr = urllib.urlencode({'verb' : 'ListSets'})
        url = self.root + '?' + qstr
        self.logger.debug('OAI request URL: {0}'.format(url))
        start_t = time.time()
        resp, content = self.h.request(url)
        retrieved_t = time.time()
        self.logger.debug('Retrieved in {0}s'.format(retrieved_t - start_t))
        sets = []

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
        return sets

    def get_record(self, id):
        params = {'verb': 'GetRecord', 'metadataPrefix': 'oai_dc', 'identifier': id}
        qstr = urllib.urlencode(params)
        url = self.root + '?' + qstr
        self.logger.debug('OAI request URL: {0}'.format(url))
        start_t = time.time()
        resp, content = self.h.request(url)
        retrieved_t = time.time()
        self.logger.debug('Retrieved in {0}s',format(retrieved_t - start_t))
        doc = bindery.parse(url, model=OAI_GETRECORD_MODEL)

        record, rid = metadata_dict(generate_metadata(doc), nesteddict=False)
        for id_, props in (record if isinstance(record, list) else [record]):
            for k, v in props.iteritems():
                props[k] = [ U(item) for item in v ]

        return {'record' : record}

    def search(self, term):
        qstr = urllib.urlencode({'verb' : 'GetRecord', 'metadataPrefix': 'oai_dc', 'identifier': dspace_id})
        url = DSPACE_OAI_ENDPOINT + '?' + qstr
        logger.debug('DSpace URL: ' + str(url))
        #keywords = [ (k.strip(), JOVE_TAG) for k in unicode(row.xml_select(u'string(.//*[@class="keywords"])')).split(',') ]

        doc = bindery.parse(url, model=OAI_MODEL)
        #print >> sys.stderr, list(generate_metadata(doc))
        resources, first_id = metadata_dict(generate_metadata(doc), nesteddict=False)
        record = doc.OAI_PMH

        resource = resources[first_id]

    def list_records(self, set="", resumption_token="", metadataPrefix="",
                     frm=None, until=None):
        '''
        List records. Use either the resumption token or set id.
        '''
        error = None

        params = {
                    'set': set,
                    'verb' : 'ListRecords',
                    'metadataPrefix': metadataPrefix
        }
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
        resp, content = self.h.request(url)
        retrieved_t = time.time()
        self.logger.debug('Retrieved in {0}s'.format(retrieved_t - start_t))

        resumption_token = ''
        if metadataPrefix in ["mods", "marc", "untl"]:
            xml_content = XML_PARSE(content)
            records = []
            error = getprop(xml_content, "OAI-PMH/error/#text", True)
            if error is None:
                for record in xml_content["OAI-PMH"]["ListRecords"]["record"]:
                    id = record["header"]["identifier"]
                    if "null" not in id:
                        records.append((id, record))
                if "resumptionToken" in xml_content["OAI-PMH"]["ListRecords"]:
                    resumption_token = xml_content["OAI-PMH"]["ListRecords"]["resumptionToken"]
                    if isinstance(resumption_token, dict):
                        resumption_token = resumption_token.get("#text", "")
        else:
            doc = bindery.parse(url, model=LISTRECORDS_MODELS[metadataPrefix])
            records, first_id = metadata_dict(generate_metadata(doc),
                                            nesteddict=False)
          
            for id_, props in records:
                for k, v in props.iteritems():
                    props[k] = [ U(item) for item in v ]
            if (doc.OAI_PMH.ListRecords is not None) and (doc.OAI_PMH.ListRecords.resumptionToken is not None):
                resumption_token = U(doc.OAI_PMH.ListRecords.resumptionToken)

        return {'records': records, 'resumption_token': resumption_token,
                'error': error}

#
OAI_LISTSETS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2011-03-14T18:26:05Z</responseDate>
  <request verb="ListSets">http://dspace.mit.edu/oai/request</request>
  <ListSets>
    <set>
      <setSpec>hdl_1721.1_18193</setSpec>
      <setName>1. Reports</setName>
    </set>
    <set>
      <setSpec>hdl_1721.1_18194</setSpec>
      <setName>2. Working Papers</setName>
    </set>
    <set>
      <setSpec>hdl_1721.1_18195</setSpec>
      <setName>3. Theses</setName>
    </set>
  </ListSets>
</OAI-PMH>
"""

OAI_DC_LISTRECORDS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd" xmlns:o="http://www.openarchives.org/OAI/2.0/"
  xmlns:eg="http://examplotron.org/0/" xmlns:ak="http://purl.org/xml3k/akara/xmlmodel">
  <responseDate>2011-03-14T21:29:34Z</responseDate>
  <request verb="ListRecords" set="hdl_1721.1_18193" metadataPrefix="oai_dc">http://dspace.mit.edu/oai/request</request>
  <ListRecords>
    <record ak:resource="o:header/o:identifier">
      <header>
        <identifier>oai:dspace.mit.edu:1721.1/27225</identifier>
        <datestamp ak:rel="local-name()" ak:value=".">2008-03-10T16:34:16Z</datestamp>
        <setSpec ak:rel="local-name()" ak:value=".">hdl_1721.1_18193</setSpec>
      </header>
      <metadata>
        <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
          <dc:title ak:rel="local-name()" ak:value=".">A methodology for the assessment of the proliferation resistance of nuclear power systems: topical report</dc:title>
          <dc:creator ak:rel="local-name()" ak:value=".">Papazoglou, Ioannis Agamennon</dc:creator>
          <dc:subject ak:rel="local-name()" ak:value=".">Nuclear disarmament</dc:subject>
          <dc:description ak:rel="local-name()" ak:value=".">A methodology for the assessment of the differential resistance of various nuclear power systems to ...</dc:description>
          <dc:date>2005-09-15T14:12:55Z</dc:date>
          <dc:date ak:rel="local-name()" ak:value=".">2005-09-15T14:12:55Z</dc:date>
          <dc:date>1978</dc:date>
          <dc:type ak:rel="local-name()" ak:value=".">Technical Report</dc:type>
          <dc:audience ak:rel="local-name()" ak:value=".">elementary school students</dc:audience>
          <dc:audience>ESL teachers</dc:audience>
          <dc:format ak:rel="local-name()" ak:value=".">6835289 bytes</dc:format>
          <dc:format>7067243 bytes</dc:format>
          <dc:format>application/pdf</dc:format>
          <dc:format>application/pdf</dc:format>
          <dc:coverage ak:rel="local-name()" ak:value=".">Providence</dc:coverage>
          <dc:coverage>Dresden</dc:coverage>
          <dc:source ak:rel="local-name()" ak:value=".">Image from page 54 of the 1922 edition of Romeo and Juliet</dc:source>
          <dc:publisher ak:rel="local-name()" ak:value=".">MIT Press</dc:publisher>
          <dc:contributor ak:rel="local-name()" ak:value=".">Betty</dc:contributor>
          <dc:contributor>John</dc:contributor>
          <dc:provenance ak:rel="local-name()" ak:value=".">Estate of Hunter Thompson</dc:provenance>
          <dc:provenance>This copy once owned by Benjamin Spock</dc:provenance>
          <dc:accrualmethod ak:rel="local-name()" ak:value=".">Purchase</dc:accrualmethod>
          <dc:instructionalmethod ak:rel="local-name()" ak:value=".">Experiential learning</dc:instructionalmethod>
          <dc:rightsholder ak:rel="local-name()" ak:value=".">MIT</dc:rightsholder>
          <dc:rights ak:rel="local-name()" ak:value=".">Collection may be protected under Title 17 of the U.S. Copyright Law.</dc:rights>
          <dc:rights>Access limited to members</dc:rights>
          <dc:identifier ak:rel="'handle'" ak:value=".">04980676</dc:identifier>
          <dc:identifier>http://hdl.handle.net/1721.1/27225</dc:identifier>
          <dc:language ak:rel="local-name()" ak:value=".">en_US</dc:language>
          <dc:relation ak:rel="local-name()" ak:value=".">MIT-EL</dc:relation>
          <dc:relation>78-021</dc:relation>
          <dc:relation>MIT-EL</dc:relation>
          <dc:relation>78-022</dc:relation>
        </oai_dc:dc>
      </metadata>
    </record>
    <resumptionToken expirationDate="2011-03-14T22:29:39Z">0001-01-01T00:00:00Z/9999-12-31T23:59:59Z/hdl_1721.1_18193/oai_dc/100</resumptionToken>
  </ListRecords>
</OAI-PMH>
"""

QDC_LISTRECORDS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2013-03-12T20:06:26Z</responseDate>
  <request verb="ListRecords" metadataPrefix="qdc" set="maps">http://imagesearch.library.illinois.edu/cgi-bin/oai.exe</request>
  <ListRecords>
    <record>
      <header>
        <identifier>oai:imagesearch.library.illinois.edu:maps/2243</identifier>
        <datestamp>2013-02-26</datestamp>
        <setSpec>maps</setSpec>
      </header>
      <metadata>
        <qdc:qualifieddc xmlns:qdc="http://epubs.cclrc.ac.uk/xmlns/qdc/"
           xmlns:dc="http://purl.org/dc/elements/1.1/"
           xmlns:dcterms="http://purl.org/dc/terms/"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://epubs.cclrc.ac.uk/xmlns/qdc/
           http://epubs.cclrc.ac.uk/xsd/qdc.xsd">
          <dc:title>Illinois (Cook County), Chicago quadrangle : topographic sheet</dc:title>
          <dcterms:alternative>Chicago quadrangle, Illinois (Cook County)</dcterms:alternative>
          <dc:creator>Geological Survey (U.S.)</dc:creator>
          <dcterms:spatial>Illinois</dcterms:spatial>
          <dc:subject>Illinois; Maps; Topographic maps; Topography; Chicago (Ill.); Cook County (Ill.)</dc:subject>
          <dc:type>Maps</dc:type>
          <dc:description>1 map : col. ; 45 x 34 cm. -- Relief indicated by contours. -- &quot;Surveyed in 1889, 1897 and 1899.&quot; -- &quot;Contour interval 5 feet.&quot;</dc:description>
          <dc:date>1901</dc:date>
          <dc:contributor>Gannett, Henry, 1846-1914; Harrison, D.C.; Renshawe, John H.; United States. Lake Survey</dc:contributor>
          <dc:description>Scale 1:62,500</dc:description>
          <dcterms:isPartOf>Historical Maps Online</dcterms:isPartOf>
          <dc:publisher>[Washington, D.C.] : U.S. Geological Survey, 1901.</dc:publisher>
          <dc:rights>http://images.library.uiuc.edu/projects/maps/terms.html</dc:rights>
          <dc:relation>http://histmapimages.grainger.illinois.edu/jp2files/Chicago1901.jp2</dc:relation>
          <dc:identifier>http://imagesearch.library.illinois.edu/u?/maps,2243</dc:identifier>
        </qdc:qualifieddc>
      </metadata>
    </record>
    <resumptionToken>maps:::qdc:1000</resumptionToken>
  </ListRecords>
</OAI-PMH>
"""

MODS_LISTRECORDS_XML = """<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
                http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"
                xmlns:ak="http://purl.org/xml3k/akara/xmlmodel"
                xmlns:o="http://www.openarchives.org/OAI/2.0/">
  <responseDate>2013-03-21T20:02:50Z</responseDate>
  <request metadataPrefix="mods" verb="ListRecords" set="dag">http://vcoai.lib.harvard.edu/vcoai/vc</request>
  <ListRecords>
    <record ak:resource="o:header/o:identifier">
      <header>
      <identifier>oai:vc.harvard.edu:dag.HUAM286037</identifier>
      <datestamp ak:rel="local-name()" ak:value=".">2012-08-04</datestamp>
      <setSpec ak:rel="local-name()" ak:value=".">dag</setSpec>
      </header>
      <metadata>
        <mods xmlns="http://www.loc.gov/mods/v3"
              xmlns:cdwalite="http://www.getty.edu/research/conducting_research/standards/cdwa/cdwalite"
              xmlns:xlink="http://www.w3.org/TR/xlink"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
              xsi:schemaLocation="http://www.loc.gov/mods/v3
              http://www.loc.gov/standards/mods/v3/mods-3-4.xsd"
              version="3.4">
          <typeOfResource ak:rel="local-name()" ak:value=".">still image</typeOfResource>
          <genre ak:rel="local-name()" ak:value=".">photograph</genre>
          <abstract ak:rel="local-name()" ak:value=".">An 1895 newspaper taped to the verso of the plate may indicate the date of a previous rehousing or rebinding.</abstract>
          <note ak:rel="local-name()" ak:value=".">Credit Line: Harvard Art Museums/Fogg Museum, Transfer from Countway Library, Harvard Medical School</note>
          <identifier type="Object Number" ak:rel="local-name()" ak:value=".">P1973.54</identifier>
          <accessCondition displayLabel="copyright" type="useAndReproduction" ak:rel="local-name()" ak:value=".">President and Fellows of Harvard College</accessCondition>
          <tableOfContents ak:rel="local-name()" ak:value=".">1. Les Croniques / Nicholas Trivet, in French (433-459).</tableOfContents>
          <titleInfo ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <nonSort ak:rel="local-name()">The </nonSort>
            <title ak:rel="local-name()">Oliver Wendell Holmes (1809-1894)</title>
            <subTitle ak:rel="local-name()">From the Caledonian Mercury</subTitle>
          </titleInfo>
          <name ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <namePart ak:rel="local-name()">Hawes, Josiah Johnson</namePart>
            <namePart type="date">1808-1901</namePart>
            <namePart>American</namePart>
            <role>
              <roleTerm ak:rel="local-name()">creator</roleTerm>
            </role>
          </name>
          <originInfo ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <place>
              <placeTerm ak:rel="local-name()">United States</placeTerm>
            </place>
            <issuance ak:rel="local-name()">monographic</issuance>
            <publisher ak:rel="local-name()">s.n.</publisher>
            <dateIssued ak:rel="local-name()">1788</dateIssued>
            <dateOther keyDate="yes" ak:rel="local-name()">c. 1850 - c. 1856</dateOther>
            <dateCreated ak:rel="local-name()">c. 1850 - c. 1856</dateCreated>
          </originInfo>
          <physicalDescription ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <form authority="marcform" ak:rel="local-name()">print</form>
            <extent ak:rel="local-name()">actual: 24.1 x 19.1 cm (9 1/2 x 7 1/2 in.)</extent>
          </physicalDescription>
          <subject ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <topic ak:rel="local-name()">Photographs</topic>
            <temporal ak:rel="local-name()">1822-1832</temporal>
            <geographic ak:rel="local-name()">Scotland</geographic>
            <hierarchicalGeographic ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
              <country ak:rel="local-name()">England</country>
              <city ak:rel="local-name()">Newcastle</city>
            </hierarchicalGeographic>
          </subject>
          <relatedItem ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <partName ak:rel="local-name()">Women and work</partName>
            <note ak:rel="local-name()">Villasana, A.R.  Ensayo repertorio bib. venezolano, v. 6, p. 523</note>
            <location>
              <url ak:rel="local-name()">http://nrs.harvard.edu/urn-3:RAD.SCHL:sch00140</url>
            </location>
          </relatedItem>
          <extension ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <cdwalite:cultureWrap>
              <cdwalite:culture ak:rel="local-name()">American</cdwalite:culture>
            </cdwalite:cultureWrap>
          </extension>
          <extension ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <cdwalite:indexingMaterialsTechSet>
              <cdwalite:termMaterialsTech ak:rel="local-name()">Whole plate daguerreotype</cdwalite:termMaterialsTech>
            </cdwalite:indexingMaterialsTechSet>
          </extension>
          <location ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <url displayLabel="Full Image" note="unrestricted" ak:rel="local-name()">http://nrs.harvard.edu/urn-3:HUAM:88783_dynmc</url>
            <url displayLabel="Thumbnail">http://nrs.harvard.edu/urn-3:HUAM:88783_dynmc</url>
          </location>
          <location ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <physicalLocation displayLabel="repository" type="current" ak:rel="local-name()">Harvard Art Museums</physicalLocation>
          </location>
          <location ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <url usage="primary display" access="object in context">http://preserve.harvard.edu/daguerreotypes/view.html?uniqueId=HUAM286037</url>
          </location>
          <recordInfo ak:resource="concat(ancestor::o:record/o:header/o:identifier, '--', local-name())">
            <recordContentSource authority="marcorg" ak:rel="local-name()">MH</recordContentSource>
            <recordCreationDate encoding="marc" ak:rel="local-name()">940610</recordCreationDate>
            <recordChangeDate encoding="iso8601" ak:rel="local-name()">20080115115022.0</recordChangeDate>
            <recordIdentifier source="VIA" ak:rel="local-name()">HUAM286037</recordIdentifier>
            <languageOfCataloging>
              <languageTerm ak:rel="local-name()">eng</languageTerm>
            </languageOfCataloging>
          </recordInfo>
        </mods>
      </metadata>
    </record>
    <resumptionToken>RC|101|0001-01-01|9999-12-31|dag|mods|7731602|dag</resumptionToken>
  </ListRecords>
</OAI-PMH>
"""

OAI_GETRECORD_XML = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:o="http://www.openarchives.org/OAI/2.0/"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"
         xmlns:eg="http://examplotron.org/0/" xmlns:ak="http://purl.org/xml3k/akara/xmlmodel">
  <responseDate>2009-03-30T06:09:23Z</responseDate>
  <request verb="GetRecord" identifier="oai:dspace.mit.edu:1721.1/5451" metadataPrefix="oai_dc">http://dspace.mit.edu/oai/request</request>
  <GetRecord>
    <record ak:resource="o:header/o:identifier">
      <header>
        <identifier>oai:dspace.mit.edu:1721.1/5451</identifier>
        <datestamp ak:rel="local-name()" ak:value=".">2006-09-20T00:15:44Z</datestamp>
        <setSpec ak:rel="local-name()" ak:value=".">hdl_1721.1_18193</setSpec>
      </header>
      <metadata>
        <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
          <dc:title ak:rel="local-name()" ak:value=".">A methodology for the assessment of the proliferation resistance of nuclear power systems: topical report</dc:title>
          <dc:creator ak:rel="local-name()" ak:value=".">Papazoglou, Ioannis Agamennon</dc:creator>
          <dc:subject ak:rel="local-name()" ak:value=".">Nuclear disarmament</dc:subject>
          <dc:description ak:rel="local-name()" ak:value=".">A methodology for the assessment of the differential resistance of various nuclear power systems to ...</dc:description>
          <dc:date>2005-09-15T14:12:55Z</dc:date>
          <dc:date ak:rel="local-name()" ak:value=".">2005-09-15T14:12:55Z</dc:date>
          <dc:date>1978</dc:date>
          <dc:type ak:rel="local-name()" ak:value=".">Technical Report</dc:type>
          <dc:audience ak:rel="local-name()" ak:value=".">elementary school students</dc:audience>
          <dc:audience>ESL teachers</dc:audience>
          <dc:format ak:rel="local-name()" ak:value=".">6835289 bytes</dc:format>
          <dc:format>7067243 bytes</dc:format>
          <dc:format>application/pdf</dc:format>
          <dc:format>application/pdf</dc:format>
          <dc:coverage ak:rel="local-name()" ak:value=".">Providence</dc:coverage>
          <dc:coverage>Dresden</dc:coverage>
          <dc:source ak:rel="local-name()" ak:value=".">Image from page 54 of the 1922 edition of Romeo and Juliet</dc:source>
          <dc:publisher ak:rel="local-name()" ak:value=".">MIT Press</dc:publisher>
          <dc:contributor ak:rel="local-name()" ak:value=".">Betty</dc:contributor>
          <dc:contributor>John</dc:contributor>
          <dc:provenance ak:rel="local-name()" ak:value=".">Estate of Hunter Thompson</dc:provenance>
          <dc:provenance>This copy once owned by Benjamin Spock</dc:provenance>
          <dc:accrualmethod ak:rel="local-name()" ak:value=".">Purchase</dc:accrualmethod>
          <dc:instructionalmethod ak:rel="local-name()" ak:value=".">Experiential learning</dc:instructionalmethod>
          <dc:rightsholder ak:rel="local-name()" ak:value=".">MIT</dc:rightsholder>
          <dc:rights ak:rel="local-name()" ak:value=".">Collection may be protected under Title 17 of the U.S. Copyright Law.</dc:rights>
          <dc:rights>Access limited to members</dc:rights>
          <dc:identifier ak:rel="'handle'" ak:value=".">04980676</dc:identifier>
          <dc:identifier>http://hdl.handle.net/1721.1/27225</dc:identifier>
          <dc:language ak:rel="local-name()" ak:value=".">en_US</dc:language>
          <dc:relation ak:rel="local-name()" ak:value=".">MIT-EL</dc:relation>
          <dc:relation>78-021</dc:relation>
          <dc:relation>MIT-EL</dc:relation>
          <dc:relation>78-022</dc:relation>
        </oai_dc:dc>
      </metadata>
    </record>
  </GetRecord>
</OAI-PMH>
"""

OAI_GETRECORD_MODEL = examplotron_model(OAI_GETRECORD_XML)
QDC_LISTRECORDS_MODEL = examplotron_model(QDC_LISTRECORDS_XML)
MODS_LISTRECORDS_MODEL = examplotron_model(MODS_LISTRECORDS_XML)
OAI_DC_LISTRECORDS_MODEL = examplotron_model(OAI_DC_LISTRECORDS_XML)

LISTRECORDS_MODELS = {
    "qdc": QDC_LISTRECORDS_MODEL,
    "mods": MODS_LISTRECORDS_MODEL,
    "oai_dc": OAI_DC_LISTRECORDS_MODEL
}
