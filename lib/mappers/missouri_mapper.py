
"""
Missouri Hub Mapper 
"""

from akara import logger
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper
from dplaingestion.textnode import textnode, NoTextNodeError
from dplaingestion.utilities import iterify
from dplaingestion.spectype import valid_spec_types


class MissouriMapper(OAIMODSMapper):

    def map_data_provider(self):
        """Map dataProvider

        In feed XML:
            //record/metadata/mods/note[@type='ownership']
        In body of mapper request JSON:
            .note
        """
        note = iterify(self.provider_data.get('note'))
        if note:
            try:
                ownership = [e for e in note
                             if type(e) == dict and e['type'] == 'ownership']
                if ownership:
                    provider = ownership[0]
                    self.mapped_data.update({'dataProvider': textnode(e)})
            except (KeyError, NoTextNodeError):
                # There was no note with a 'type' attribute, or there was, but
                # it was an XML element lacking a text node.
                pass

    def map_creator(self):
        """Map sourceResource.creator

        In feed XML:
            //record/metadata/mods/name/namePart[./role/roleTerm='creator']
        In body of mapper request JSON:
            .name like this:
            {'namePart': 'Creator Name',
             'role': {
                 'roleTerm': {'#text': 'creator', 'type': 'text'}
             }
            }
        """            
        # only in MHM, SLU, and WUSTL collections
        def creator_names(names):
            """Creator names from name elements with creator role

            Can pass a TypeError or KeyError.
            """
            for n in names:
                try:
                    if n['role']['roleTerm'] == 'creator' \
                            or textnode(n['role']['roleTerm']) == 'creator':
                        yield n['namePart']
                except (KeyError, NoTextNodeError):
                    # No name with a roleTerm of "creator," but it's not
                    # required.
                    pass
        name = iterify(self.provider_data.get('name', []))
        if name:
            creators = [n for n in creator_names(name)]
            if creators:
                self.update_source_resource({'creator': creators})

    def map_date(self):
        """Map sourceResource.date

        In feed XML:
            //record/metadata/mods/originInfo/dateCreated[@keyDate='yes']
            or
            //record/metadata/mods/originInfo/dateIssued[@point='start']
            or
            //record/metadata/mods/originInfo/dateOther
        In body of mapper request JSON:
            .originInfo[].dateIssued like this:
                {'point': 'start', '#text': '2014'}
            or .originInfo[].dateCreated like this:
                {'keyDate': 'yes', '#text': '2014'}
            or .originInfo[].dateOther like this:
                {'#text': '2014'}
        """
        # (FRBSTL uses <originInfo><dateIssued>;
        # SLU, MDH, WUSTL use <originInfo><dateOther>
        def first_date(els):
            """Return first date string from originInfo elements"""
            for el in els:
                # Allow for each of the following elements to be an array of
                # dicts or strings, or one on its own.
                date_created = iterify(el.get('dateCreated', []))
                date_issued = iterify(el.get('dateIssued', []))
                date_other = iterify(el.get('dateOther', []))
                sort_date = iterify(el.get('sortDate', []))
                try:
                    for d in date_created:
                        if type(d) == dict and d.get('keyDate') == 'yes':
                            return textnode(d)
                    for d in date_issued:
                        if type(d) == dict and d.get('point') == 'start':
                            return textnode(d)
                    # Nothing yet?  Take first dateOther, ignoring attributes:
                    if date_other:
                        return textnode(date_other[0])
                    # OK, then take the first dateIssued we can get, ignoring
                    # attribute ...
                    if date_issued:
                        return textnode(date_issued[0])
                    # Still nothing?  Try sortDate:
                    if sort_date:
                        return textnode(sort_date[0])
                except NoTextNodeError:
                    # Weird, but date is not required.
                    pass
        origin_info = iterify(self.provider_data.get('originInfo', []))
        if origin_info:
            date = first_date(origin_info)
            if date:
                self.update_source_resource({'date': date})

    def map_description(self):
        """Map sourceResource.description

        In feed XML:
            //record/metadata/mods/note
            ... where there's no 'type' attribute of 'ownership'
        In body of mapper request JSON:
            .note
            ... with conditions above
        """
        def is_ownership_note(n):
            """The given //note has attribute "type" == "ownership" """
            try:
                return n.get('type') == 'ownership'
            except:
                return False
        def desc_string(n):
            """The text node of the given //note or '' if it's empty"""
            try:
                return textnode(n)
            except NoTextNodeError:
                return ''
        note = iterify(self.provider_data.get('note', []))
        if note:
            try:
                desc = [desc_string(n) for n in note
                        if not is_ownership_note(n) and desc_string(n)]
                if desc:
                    self.update_source_resource({'description': desc})
            except IndexError:
                # No appropriate notes.  Not required, so pass.
                pass

    def map_extent(self):
        """Map sourceResource.extent

        In feed XML:
            //record/metadata/mods/physicalDescription/extent
        In body of mapper request JSON:
            .physicalDescription.extent, as in:
            "physicalDescription": {
                "extent": "3 files",
                "xmlns:default": "http://www.loc.gov/mods/v3"
            }
        """
        phys_desc = self.provider_data.get('physicalDescription')
        if phys_desc and 'extent' in phys_desc:
            self.update_source_resource({'extent': phys_desc['extent']})

    def map_format(self):
        """Map sourceResource.format

        In feed XML:
            //record/metadata/mods/physicalDescription/note
            or
            //record/metadata/mods/genre
        In body of mapper request JSON:
            "physicalDescription": {
                "note": "the description"
            }
            ... That being theoretical; I haven't encountered it yet.
            ... if there are attributes, note will have a text node, and
                that will be used.
            or
            "genre": {
                "#text": "book",
                "xmlns:default": "http://www.loc.gov/mods/v3"
            }
            ... and "genre" could be a string if the element has no attribute.
        """
        def fmt_from_physdesc(phys_descs):
            """Yield format strings from a list of physicalDescriptions"""
            for pd in phys_descs:
                if 'note' in pd:
                    try:
                        yield textnode(pd['note'])
                    except NoTextNodeError:
                        pass
        genre = iterify(self.provider_data.get('genre', []))
        phys_desc = iterify(self.provider_data.get('physicalDescription', []))
        format = None
        if genre:
            format = [f for f in _genre_strings(genre)]
        elif phys_desc:
            format = [f for f in fmt_from_physdesc(phys_desc)]
        if format:
            self.update_source_resource({'format': format})

    def map_identifier(self):
        """Map sourceResource.identifier

        In feed XML:
            //record/metadata/mods/identifier
        In body of mapper request JSON:
            "identifier": [
              {
                "#text": "11504366",
                "type": "oclc",
                "xmlns:default": "http://www.loc.gov/mods/v3"
              },
              {
                "#text": "27303118",
                "type": "oclc",
                "xmlns:default": "http://www.loc.gov/mods/v3"
              }
            ]
            or
            "identifier": {
                "#text": "..."
                ... etc ...
            }
            ... where "type" is optional, but if it's "oclc" we'll prepend
            "OCLC:" to the identifier.
        """
        def idstrings(els):
            for el in els:
                try:
                    if type(el) == dict and el.get('type') == 'oclc':
                        t = 'OCLC:' + textnode(el).strip()
                    else:
                        t = textnode(el).strip()
                    yield t
                except NoTextNodeError:
                    pass
        identifier_el = iterify(self.provider_data.get('identifier', []))
        identifiers = [i for i in idstrings(identifier_el)]
        if identifiers:
            self.update_source_resource({'identifier': identifiers})

    def map_language(self):
        """Map sourceResource.language

        In feed XML:
            //record/metadata/mods/language/languageTerm
            ... or language without LanguageTerm
        In body of mapper request JSON:
            "language": {
              "languageTerm": {
                "#text": "Language Name",
                "type": "text"
              }
            }
            ... where "type" can be "code" with "#text" being a code
            ... or
            "language": {
                "#text": "language code"
                "xmlns:default": "..."
            }
        Language codes are sometimes semicolon-delimited, like "jpn; eng"
        """
        def lang_terms(els):
            """Extract language terms from different elements"""
            for el in els:
                try:
                    s = el.get('#text') or el['languageTerm'].get('#text')
                    for t in s.split(';'):
                        yield t.strip()
                except KeyError:
                    pass
        language = iterify(self.provider_data.get('language', []))
        if language:
            languages = [t for t in lang_terms(language)]
            if languages:
                self.update_source_resource({'language': languages})

    def map_publisher(self):
        """Map sourceResource.publisher

        In feed XML:
            //record/metadata/mods/originInfo/publisher
        In body of mapper request JSON:
            "originInfo": [
              {
                "publisher": "Publisher One"
              },
              {
                "publisher": "Publisher Two"
              },
              {
                "someOtherField": "..."
              }
            ]
        """
        def pub_strings(els):
            for el in els:
                try:
                    rv = textnode(el['publisher']).strip(' ,\n')
                    if rv:
                        yield rv
                except (KeyError, NoTextNodeError):
                    pass
        origin_info = iterify(self.provider_data.get('originInfo', []))
        if origin_info:
            publishers = [p for p in pub_strings(origin_info)]
            if publishers:
                self.update_source_resource({'publisher': publishers})

    def map_relation(self):
        """Map sourceResource.relation

        In feed XML:
            //record/metadata/mods/relatedItem/location/url
            or
            //record/metadata/mods/relatedItem/titleInfo/title
        In body of mapper request JSON:
        {
            "relatedItem": [
                {
                    "location": {
                        "url": "http://fraser.stlouisfed.org/example/"
                    },
                    "recordInfo": {
                        "recordIdentifier": "63"
                    },
                    "titleInfo": {
                        "title": "The Title"
                    },
                    "type": "similarTo",
                    "typeOfResource": "text",
                    "xmlns:default": "http://www.loc.gov/mods/v3"
                },
                ...
        }
        """
        def relation_strings(els):
            """Yield all relation strings from a list of relatedItems"""
            for el in els:
                try:
                    if 'location' in el and el['location']['url']:
                        yield el['location']['url'].strip(' \n')
                    if 'titleInfo' in el and el['titleInfo']['title']:
                        yield el['titleInfo']['title'].strip(' \n')
                except (KeyError, TypeError):
                    # Not required, so pass
                    pass
        related_item = iterify(self.provider_data.get('relatedItem', []))
        if related_item:
            relations = [r for r in relation_strings(related_item)]
            if relations:
                self.update_source_resource({'relation': relations})

    def map_rights(self):
        """Map sourceResource.rights

        In feed XML:
            //record/metadata/mods/accessCondition
        In body of mapper request JSON:
            .accessCondition
        """
        # missing from MHM and FRBSTL collections
        acc_cond = iterify(self.provider_data.get('accessCondition', []))
        if acc_cond:
            try:
                rights = [textnode(r) for r in acc_cond]
                if rights:
                    self.update_source_resource({'rights': rights})
            except NoTextNodeError:
                # No text node (empty)?  Deal with it later in validation.
                pass

    def map_spec_type(self):
        """Map sourceResource.genre

        See map_format() and _genre_strings()
        """
        genre = iterify(self.provider_data.get('genre', []))
        if genre:
            spec_types = [g for g in _genre_strings(genre)
                          if g.lower() in valid_spec_types]
            if spec_types:
                self.update_source_resource({'specType': spec_types})

    def map_subject(self):
        """Map sourceResource.subject

        In feed XML:
            //record/metadata/mods/subject/topic
            or
            //record/metadata/mods/subject/theme
        In mapper request JSON:
        {
            "subject": [
                {"topic": "Missouri--St. Louis"},
                ...
            ]
        }
        or
        {
            "subject": [
                {
                    "recordInfo": { ... },
                    "theme": {
                        "#text": "Bureau of Labor Statistics Publications",
                        "xmlns:default": "http://www.loc.gov/mods/v3"
                    }
                }
                ...
            ]
        }
        """
        def subject_strings(els):
            """Yield all subject strings from given list of elements"""
            for el in els:
                try:
                    s = None
                    if 'topic' in el:
                        s = textnode(el['topic'])
                    elif 'theme' in el:
                        s = textnode(el['theme'])
                    if s:
                        for subj in s.split(';'):
                            yield subj.strip()
                except NoTextNodeError:
                    pass
        subject = iterify(self.provider_data.get('subject', []))
        if subject:
            subjects = [s for s in subject_strings(subject)]
            if subjects:
                self.update_source_resource({'subject': subjects})

    def map_spatial(self):
        """Map sourceResource.spatial

        See map_subject().
        The JSON is formatted as follows:
        {
            "subject": [
                {
                    "hierarchicalGeographic": {
                        "country": "United States",
                        "state": "MO",
                        "continent": "North America",
                        "city": "St. Louis"
                    },
                    "cartographics": {
                        "coordinates": "38.6277,-90.1995"
                    }
                }
            ]
        }
        """
        def coordinates(el):
            """Return the coordinates string from the given dict"""
            try:
                return textnode(el.get('coordinates'))
            except:
                return ''
        subjects = iterify(self.provider_data.get('subject', []))
        if subjects:
            ok_spatial = set(['city', 'county', 'state', 'country',
                              'coordinates'])
            spatial = []
            # This logic of append() calls comes from oai_mods_mapper.
            # The assignment of coordinate strings as their own list elements
            # is by design.
            # For the treatment of 'continent', see
            # https://github.com/dpla/ingestion/pull/37
            for s in subjects:
                for hg in iterify(s.get('hierarchicalGeographic', [])):
                    keys = set.intersection(ok_spatial, hg.keys())
                    clean_hg = dict([(k, hg[k]) for k in keys])
                    # 'continent' is not allowed in MAP v3.1 but it can be used
                    # for spatial.name.
                    if 'continent' in hg and len(hg) == 1:
                        clean_hg['name'] = hg['continent']
                    if clean_hg and clean_hg not in spatial:
                        spatial.append(clean_hg)
                for carto in iterify(s.get('cartographics', [])):
                    c = coordinates(carto)
                    if c and c not in spatial:
                        spatial.append(c)
            if spatial:
                self.update_source_resource({'spatial': spatial})

    def map_temporal(self):
        """Map sourceResource.temporal

        See map_subject() and map_spatial()
        """
        def temporal_strings(els):
            for el in els:
                try:
                    yield textnode(el.get('temporal'))
                except:
                    pass
        subjects = iterify(self.provider_data.get('subject', []))
        temporal = [t for t in temporal_strings(subjects)]
        if temporal:
            self.update_source_resource({'temporal': temporal})

    def location_url(self, attr_name, attr_value):
        """URL for a //location/url with the given attribute

        Exceptions: Could pass KeyError, IndexError, or TypeError, all
                    indicating an expected attribute or text node was not
                    found.
        """
        def first_url_string(location):
            url_el = location.get('url')
            urls = url_el if type(url_el) == list else [url_el]
            for u in urls:
                try:
                    if attr_name in u and u[attr_name] == attr_value:
                        return u['#text']
                except (TypeError, KeyError):
                    # Not a dict, or no text node
                    pass
        location = iterify(self.provider_data.get('location', []))
        if location:
            url_strings = (first_url_string(loc) for loc in location)
            defined_url_strings = [s for s in url_strings if s]
            if defined_url_strings:
                return defined_url_strings[0]

    def map_is_shown_at(self):
        """Map isShownAt

        In feed XML:
            //record/metadata/mods/location/url[@access='object in context']
        In body of mapper request JSON:
            See the test, test_map_is_shown_at() in test_missouri_mapper.py
            We can encounter multiple data structures.
        """
        is_shown_at = self.location_url('access', 'object in context')
        if is_shown_at:
            self.mapped_data['isShownAt'] = is_shown_at

    def map_object(self):
        """Map object

        In feed XML:
            //record/metadata/mods/location/url[@access='preview']
        In body of mapper request JSON:
            {
              "location": {
                "url": [
                  {
                    "#text": "http://digital.wustl.edu/example.gif",
                    "access": "preview"
                  }
                ]
              }
            }
        """
        obj = self.location_url('access', 'preview')
        if obj:
            self.mapped_data['object'] = obj

    def map_has_view(self):
        """Map hasView

        See location_url() and map_is_shown_at().
        Use //record/metadata/mods/location/url[@access='object in context']
        for hasView.@id.
        Use //record/metadata/mods/physicalDescription/internetMediaType
        for hasView.format.
        """
        def first_media_type(phys_descs):
            """First internetMediaType string from physicalDescription list"""
            for pd in phys_descs:
                imt = pd.get('internetMediaType')
                if imt:
                    try:
                        return textnode(imt)
                    except NoTextNodeError:
                        pass
            return None
        try:
            if not 'hasView' in self.mapped_data:
                self.mapped_data['hasView'] = {}
            self.mapped_data['hasView'].update({
                '@id': self.location_url('access', 'object in context')
                })
            phys_desc = iterify(self.provider_data.get('physicalDescription',
                                                       []))
            if phys_desc:
                media_type = first_media_type(phys_desc)
                if media_type:
                    self.mapped_data['hasView'].update({'format': media_type})
        except (KeyError, IndexError, TypeError):
            # Not required
            pass

    def map_type(self):
        """Map sourceResource.type

        In feed XML:
            //record/metadata/mods/typeOfResource
        In body of mapper request JSON:
            {
                "typeOfResource": {
                    "#text": "the type",
                    "xmlns:default": "http://www.loc.gov/mods/v3"
                }
            }
        """
        # missing from MDH and WUSTL collections
        def type_strings(els):
            for el in els:
                try:
                    yield textnode(el)
                except NoTextNodeError:
                    pass
        tor = iterify(self.provider_data.get('typeOfResource', []))
        if tor:
            types = [t for t in type_strings(tor)]
            self.update_source_resource({'type': types})

    def map_title(self):
        """Map sourceResource.title

        In feed XML:
            //record/metadata/mods/titleInfo/title
        In body of mapper request JSON:
            {
                'titleInfo': [
                    {'title': 'the title'}
                    ...
                ]
            }
        ... where titleInfo could also be a dict with key 'title'
        """
        ti = iterify(self.provider_data.get('titleInfo', []))
        if ti:
            self.update_source_resource({'title': [textnode(t['title'])
                                                   for t in ti]})


def _genre_strings(els):
    """Yield genre strings from list of elements"""
    for el in els:
        try:
            yield textnode(el).strip()
        except NoTextNodeError:
            pass

