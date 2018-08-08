"""
Michigan Hub Mapper 
"""

from akara import logger
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.textnode import textnode


class MichiganMapper(OAIMODSMapper):
    def map_collection(self):
        ret_dict = {"collection": getprop(self.provider_data, "collection")}

        prop = self.root_key + "relatedItem"

        if exists(self.provider_data, prop):
            related_items = iterify(getprop(self.provider_data, prop))
            host_types = [item for item in related_items if
                          item.get("type") == "host"]
            if host_types:
                title = getprop(host_types[-1], "titleInfo/title", True)
                if title:
                    ret_dict = {"collection": {"title": title, "@id": "",
                                               "id": "", "description": ""}}

        if ret_dict["collection"]:
            self.update_source_resource(ret_dict)

    def map_creator_and_contributor(self):
        prop = self.root_key + "name"
        _dict = {
            "creator": [],
            "contributor": []
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                name = s.get("namePart")
                if name:
                    rt = []
                    try:
                        # Get all the roleTerm values for a given mods:name
                        # entity
                        rt = [textnode(r.get("roleTerm")) for r in
                              iterify(s.get("role"))]
                    except Exception as e:
                        logger.error("Error getting name/role/roleTerm for " +
                                     "record %s" % self.provider_data["_id"])
                        continue

                    # If mods:roleTerm is empty or if it contains 'Creator'
                    # then map the namePart value to creator. If roleTerm
                    # contains 'Contributor' map to contributor
                    if not rt or "creator" in map(unicode.lower, rt):
                        if isinstance(name, list):
                            for n in name:
                                clean_name = textnode(n)
                                if isinstance(clean_name, basestring):
                                    _dict["creator"].append(clean_name)
                        else:
                            _dict["creator"].append(textnode(name))
                    elif "contributor" in map(unicode.lower, rt):
                        if isinstance(name, list):
                            for n in name:
                                clean_name = textnode(n)
                                if isinstance(clean_name, basestring):
                                    _dict["contributor"].append(clean_name)
                        else:
                            _dict["contributor"].append(textnode(name))

            self.update_source_resource(self.clean_dict(_dict))


    # Data == first <mods:recordInfo><mods:recordContentSource>
    # Intermediate == second <mods:recordInfo><mods:recordContentSource>
    def map_data_provider_and_intermediate_provider(self):
        prop = self.root_key + "recordInfo/recordContentSource"

        if exists(self.provider_data, prop):
            record_content_source = getprop(self.provider_data, prop)
            data_provider = ""
            intermediate_provider = ""
            if isinstance(record_content_source, list):
                data_provider = textnode(record_content_source[0])
                if len(data_provider) > 1:
                    intermediate_provider = textnode(record_content_source[1])
            elif isinstance(record_content_source, dict):
                data_provider = textnode(record_content_source)
            else:
                data_provider = record_content_source

            if data_provider:
                self.mapped_data.update({"dataProvider": data_provider})
            if intermediate_provider:
                self.mapped_data.update(
                        {"intermediateProvider": intermediate_provider})

    # any <mods:originInfo><mods:dateIssued>
    # <mods:originInfo><mods:publisher>
    def map_publisher(self):
        prop = self.root_key + "originInfo"
        _dict = {
            "publisher": []
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "publisher" in s:
                    _dict["publisher"].append(s.get("publisher"))

            self.update_source_resource(self.clean_dict(_dict))

    def map_date(self):
        originInfoPath = self.root_key + "originInfo"
        dateCreated = []
        dateIssued = []
        date_begin, date_end = None, None

        if exists(self.provider_data, originInfoPath):
            for date in iterify(getprop(self.provider_data, originInfoPath)):
                if "dateCreated" in date:
                    dateCreated.append(textnode(date["dateCreated"]))

                if "dateIssued" in date:
                    t = date["dateIssued"]
                    try:
                        if "point" not in t:
                            dateIssued.append(textnode(t))
                        elif "point" in t and t["point"] == "start":
                            date_begin = textnode(t)
                        elif "point" in t and t["point"] == "end":
                            date_end = textnode(t)
                    except Exception as e:
                        logger.error("Exception when trying to map date "
                                     "values. for record %s \n\n%s" %
                                     (self.provider_data % e.message))

        # If there are no dateIssued or dateCreated properties then construct
        # a date range from begin and end points (if they exist).
        if date_begin and date_end and not dateCreated and not dateIssued:
            dateIssued.append(date_begin + "-" + date_end)

        if dateCreated:
            self.update_source_resource({"date": dateCreated})
        elif dateIssued:
            self.update_source_resource({"date": dateIssued})

    # <mods:note> AND
    # <mods:abstract>
    def map_description(self):
        props = (self.root_key + "note", self.root_key + "abstract")

        desc = []
        for desc_prop in props:
            if exists(self.provider_data, desc_prop):
                for s in iterify(getprop(self.provider_data, desc_prop)):
                    if isinstance(s, dict):
                        desc.append(s.get("#text"))
                    else:
                        desc.append(s)
        desc = filter(None, desc)

        if desc:
            self.update_source_resource({"description": desc})

    # <mods:physicalDescription><mods:extent>
    def map_extent(self):
        prop = self.root_key + "physicalDescription/extent"

        if exists(self.provider_data, prop):
            self.update_source_resource({
                "extent":
                    getprop(self.provider_data,
                            prop)
            })

    def map_type(self):
        prop = self.root_key + "typeOfResource"

        if exists(self.provider_data, prop):
            self.update_source_resource({
                "type":
                    getprop(self.provider_data,
                            prop)
            })

    # <mods:genre> AND <mods:physicalDescription><mods:form>
    def map_format(self):
        props = (self.root_key + "genre",
                 self.root_key + "physicalDescription/form")

        formats = []

        for prop in props:
            if exists(self.provider_data, prop):
                for s in iterify(getprop(self.provider_data, prop)):
                    if isinstance(s, dict):
                        formats.append(s.get("#text"))
                    else:
                        formats.append(s)

        formats = filter(None, formats)

        if formats:
            self.update_source_resource({"format": formats})

    def map_genre(self):
        props = (self.root_key + "genre",
                 self.root_key + "physicalDescription/form")

        genres = []

        for prop in props:
            if exists(self.provider_data, prop):
                for s in iterify(getprop(self.provider_data, prop)):
                    genres.append(s)

        genres = filter(None, genres)

        if genres:
            self.update_source_resource({"genre": genres})

    # <mods:identifer>
    def map_identifier(self):
        prop = self.root_key + "identifier"
        ids = []

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, dict):
                    ids.append(s.get("#text"))
                else:
                    ids.append(s)
            ids = filter(None, ids)

        if ids:
            self.update_source_resource({"identifier": ids})

    # <mods:language><mods:languageTerm>
    def map_language(self):
        prop = self.root_key + "language/languageTerm"

        languages = []

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, dict):
                    languages.append(s.get("#text"))
                else:
                    languages.append(s)

        languages = filter(None, languages)
        if languages:
            self.update_source_resource({"language": languages})

    def map_rights(self):
        prop = self.root_key + "accessCondition"
        rights = []
        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, dict):
                    rights.append(s.get("#text"))
                else:
                    rights.append(s)
            rights = filter(None, rights)
        if rights:
            self.update_source_resource({"rights": rights})

    # <mods:subject><mods:geographic>
    def map_spatial(self):
        prop = self.root_key + "subject"
        geography = []

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if exists(s, "geographic") and isinstance(s.get('geographic'),
                                                          dict):
                    geography.append(s.get('geographic').get("#text"))
                elif exists(s, "geographic") and isinstance(s.get('geographic'),
                                                            list):
                    geography = geography + [textnode(g) for g in s.get('geographic')]
                elif exists(s, "geographic"):
                    geography.append(textnode(s.get('geographic')))

        geography = filter(None, geography)

        if geography:
            self.update_source_resource({"spatial": geography})

    # <mods:subject> AND
    # <mods:subject><mods:topic> AND
    # <mods:subject><mods:name><mods:namePart> AND
    # <mods:subject><mods:genre> AND
    # <mods:subject><mods:titleInfo><mods:title>
    def map_subject(self):
        prop = (self.root_key + "subject")
        props = ('topic', 'genre', 'name/namePart', 'titleInfo/title')
        subject = []

        if exists(self.provider_data, prop):

            prov_subjects = getprop(self.provider_data, prop)

            for subj_prop in props:
                for s in iterify(prov_subjects):
                    if exists(s, subj_prop):
                        if isinstance(getprop(s, subj_prop), dict):
                            subject.append(textnode(getprop(s, subj_prop)))
                        elif isinstance(getprop(s, subj_prop), list):
                            subject = subject + [textnode(s) for s in getprop(s, subj_prop)]
                        else:
                            subject.append(getprop(s, subj_prop))
        subject = filter(None, subject)

        if subject:
            self.update_source_resource({"subject": subject})

    def map_temporal(self):
        prop = self.root_key + "subject/temporal"

        if exists(self.provider_data, prop):
            self.update_source_resource({
                "temporal":
                    getprop(self.provider_data,
                            prop)
            })

    def map_title(self, unsupported_types=[], unsupported_subelements=[]):
        prop = self.root_key + "titleInfo/title"

        if exists(self.provider_data, prop):
            self.update_source_resource({
                "title":
                    getprop(self.provider_data,
                            prop)
            })

    def map_object_and_is_shown_at(self):
        prop = self.root_key + "location"
        ret_dict = {}

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    url = getprop(s, "url", True)
                    if url:
                        if "usage" in url and url.get("usage") == "primary":
                            ret_dict["isShownAt"] = url.get("#text")
                        elif "access" in url and url.get("access") == "preview":
                            ret_dict["object"] = url.get("#text")
                except:
                    logger.error("No dictionaries in prop %s of record %s" %
                                 (prop, self.provider_data["_id"]))
                    continue

        self.mapped_data.update(ret_dict)

    def map_multiple_fields(self):
        self.map_creator_and_contributor()
        self.map_collection()
        self.map_data_provider_and_intermediate_provider()
        self.map_object_and_is_shown_at()