from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper

class DigitalNCMapper(OAIMODSMapper):
    def __init__(self, provider_data):
        super(DigitalNCMapper, self).__init__(provider_data)

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

                    if "creator" in role_terms:
                       _dict["creator"].append(name)
                    elif "contributor" in role_terms:
                       _dict["contributor"].append(name)

            self.update_source_resource(self.clean_dict(_dict))

    def map_date_and_publisher(self):
        prop = self.root_key + "originInfo"
        _dict = {
            "date": None,
            "publisher": []
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "dateCreated" in s:
                    date_list = iterify(s.get("dateCreated"))
                    date = [t.get("#text") for t in date_list if
                            t.get("keyDate") == "yes"]
                    # Check if last date is already a range
                    if "-" in date[-1] or "/" in date[-1]:
                        _dict["date"] = date[-1]
                    elif len(date) > 1:
                        _dict["date"] = "%s-%s" % (date[0], date[-1])
                    else:
                        _dict["date"] = date[0]

                if "publisher" in s:
                    _dict["publisher"].append(s.get("publisher"))

            self.update_source_resource(self.clean_dict(_dict))

    def map_format(self):
        pass

    def map_format_and_spec_type(self):
        prop = self.root_key + "physicalDescription"
        _dict = {
            "format": [],
            "specType": []
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "form" in s:
                    for f in iterify(s.get("form")):
                        if (f.lower() in ["books", "government records"] and
                            f.capitalize() not in _dict["specType"]):
                            _dict["specType"].append(f.capitalize())
                        elif f not in _dict["format"]:
                            _dict["format"].append(f)

            self.update_source_resource(self.clean_dict(_dict))

    def map_subject_and_spatial(self):
        prop = self.root_key + "subject"
        _dict = {
            "subject": [],
            "spatial": []
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "geographic" in s:
                    _dict["spatial"].append(s.get("geographic"))
                elif "topic" in s:
                    _dict["subject"].append(s.get("topic"))

            self.update_source_resource(self.clean_dict(_dict))

    def map_description(self):
        prop = self.root_key + "note"

        if exists(self.provider_data, prop):
            description = []
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    if s.get("type") == "content":
                        description.append(s.get("#text"))
                except:
                    logger.error("Error getting note/type from record %s" %
                                 self.provider_data["_id"])

            self.update_source_resource({"description": description})

    def map_title(self):
        prop = self.root_key + "titleInfo"
        _dict = {"title": []}

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                _dict["title"].append(s.get("title"))

            self.update_source_resource(self.clean_dict(_dict))

    def map_identifier(self):
        prop = self.root_key + "identifier"

        if exists(self.provider_data, prop):
            self.update_source_resource({"identifier":
                                         getprop(self.provider_data, prop)})

    def map_language(self):
        prop = self.root_key + "language/languageTerm"

        if exists(self.provider_data, prop):
            self.update_source_resource({"language":
                                         getprop(self.provider_data, prop)})

    def map_rights(self):
        prop = self.root_key + "accessCondition"

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    if s.get("type") == "local rights statement":
                        self.update_source_resource({"rights": s.get("#text")})
                except:
                    logger.error("Error getting rights statement from record %s" %
                                 self.provider_data["_id"])

    def map_type(self):
        prop = self.root_key + "genre"

        if exists(self.provider_data, prop):
            self.update_source_resource({"type":
                                         getprop(self.provider_data, prop)})

    def map_is_part_of(self):
        prop = self.root_key + "relatedItem"
        _dict = {"isPartOf": []}

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                for k in ("location/url", "titleInfo/title"):
                    try:
                        _dict["isPartOf"].append(getprop(s, k))
                    except:
                        pass

            self.update_source_resource(self.clean_dict(_dict))

    def map_data_provider(self):
        prop = self.root_key + "note"
        _dict = {"dataprovider": None}

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    if s.get("type") == "ownership":
                        _dict["dataProvider"] = s.get("#text")
                        break
                except:
                    logger.error("Error getting note/type for record %s" %
                                 self.provider_data["_id"])

            self.mapped_data.update(self.clean_dict(_dict))

    def map_object_and_is_shown_at(self):
        prop = self.root_key + "location"
        ret_dict = {}

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    url =  getprop(s, "url/#text", True)
                except:
                    logger.error("No dictionaries in prop %s of record %s" %
                                 (prop, self.provider_data["_id"]))
                    continue

                if url:
                    usage = getprop(s, "url/usage", True)
                    access = getprop(s, "url/access", True)
                    if (usage == "primary display" and
                        access == "object in context"):
                        ret_dict["isShownAt"] = url
                    elif access == "preview":
                        ret_dict["object"] = url

        self.mapped_data.update(ret_dict)

    def map_multiple_fields(self):
        self.map_creator_and_contributor()
        self.map_date_and_publisher()
        self.map_format_and_spec_type()
        self.map_subject_and_spatial()
        self.map_object_and_is_shown_at()
