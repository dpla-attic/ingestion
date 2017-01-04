"""
Michigan Hub Mapper 
"""

from akara import logger
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop


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
                    ret_dict = {"collection": {"title": title}}

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
                    try:
                        role_terms = [r.get("roleTerm") for r in
                                      iterify(s.get("role"))]
                    except:
                        logger.error("Error getting name/role/roleTerm for " +
                                     "record %s" % self.provider_data["_id"])
                        continue

                    if "contributor" not in role_terms:
                        _dict["creator"].append(name)
                    elif "contributor" in role_terms:
                        _dict["contributor"].append(name)

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
                data_provider = record_content_source[0].get("#text")
                if len(data_provider) > 1:
                    intermediate_provider = record_content_source[1].get(
                            "#text")
            elif isinstance(record_content_source, dict):
                data_provider = record_content_source.get("#text")
            else:
                data_provider = record_content_source

            if data_provider:
                self.mapped_data.update({"dataProvider": data_provider})
            if intermediate_provider:
                self.mapped_data.update(
                        {"intermediateProvider": intermediate_provider})

    # any <mods:originInfo><mods:dateIssued>
    # <mods:originInfo><mods:publisher>
    def map_date_and_publisher(self):
        prop = self.root_key + "originInfo"
        _dict = {
            "date": [],
            "publisher": []
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "dateIssued" in s:
                    try:
                        date_begin, date_end = None, None
                        for i in self.get_date(s):
                            _dict["date"].append(i)
                        if "point" in s:
                            if s["point"] == "start":
                                date_begin = self.get_date(s)
                            elif s["point"] == "end":
                                date_end = self.get_date(s)

                        if date_begin and date_end:
                            _dict["date"] = date_begin[0] + "-" + date_end[0]
                    except Exception as e:
                        logger.error("Exception when trying to map date "
                                     "values. %s" % e.message)

                if "publisher" in s:
                    _dict["publisher"].append(s.get("publisher"))

            self.update_source_resource(self.clean_dict(_dict))

    # <mods:physicalDescription><mods:note> AND 
    # <mods:note> AND 
    # <mods:abstract>
    def map_description(self):
        props = (self.root_key + "physicalDescription/note",
                 self.root_key + "note", self.root_key + "abstract")

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
                    geography = geography + s.get('geographic')
                elif exists(s, "geographic"):
                    geography.append(s.get('geographic'))

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
                            subject.append(getprop(s, subj_prop).get("#text"))
                        elif isinstance(getprop(s, subj_prop), list):
                            subject = subject + getprop(s, subj_prop)
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
        self.map_date_and_publisher()
        self.map_data_provider_and_intermediate_provider()
        self.map_object_and_is_shown_at()

    def get_date(self, record):
        """ Get date values from provided record and returns them as list of
        values"""
        # TODO this could be rewritten using textnode.textnode()
        dates = []
        if not isinstance(getprop(record, "dateIssued"), basestring):
            # Iterate through dicts and lists appending their values to dates
            for d in iterify(getprop(record, "dateIssued")):
                if isinstance(d, dict) and "#text" in d:
                    dates.append(getprop(d, "#text"))
                else:
                    dates.append(d)
        else:
            dates.append(getprop(record, "dateIssued"))
        return dates
