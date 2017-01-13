from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mods_mapper import MODSMapper


class TNMapper(MODSMapper):
    def __init__(self, provider_data):
        super(TNMapper, self).__init__(provider_data)

    def map_multiple_fields(self):
        super(MODSMapper, self).map_multiple_fields()

    def map_is_part_of(self):
        path = "/metadata/mods/relatedItem"
        if exists(self.provider_data, path):
            for relatedItem in getprop(self.provider_data, path):
                if "type" in relatedItem \
                        and "displayLabel" in relatedItem \
                        and relatedItem["type"] == "host" \
                        and relatedItem["displayLabel"] == "Project":
                    title = relatedItem["titleInfo"]["title"]
                    self.update_source_resource({"isPartOf": title})

    # helper function for the next two functions
    def name_part(self, type):
        prop = "/metadata/mods/name"
        results = []
        if exists(self.provider_data, prop):
            for name in getprop(self.provider_data, prop):
                if "role" in name and "namePart" in name:
                    for role in iterify(name["role"]):
                        role_prop = "roleTerm/#text"
                        if exists(role, role_prop) \
                                and getprop(role, role_prop) == type:
                            results.append(name["namePart"])

        return results

    def map_contributor(self):
        contributor = self.name_part("Contributor")
        if len(contributor) > 0:
            self.update_source_resource({"contributor": contributor})

    def map_creator(self):
        creators = self.name_part("Creator")
        if len(creators) > 0:
            self.update_source_resource({"creator": creators})

    def map_date(self):
        path = "/metadata/mods/originInfo/dateCreated/#text"
        if exists(self.provider_data, path):
            date_created = getprop(self.provider_data, path)
            self.update_source_resource({"date": {"displayDate": date_created}})

    def map_description(self):
        path = "/metadata/mods/abstract"
        if exists(self.provider_data, path):
            description = getprop(self.provider_data, path)
            self.update_source_resource({"description": description})

    def map_extent(self):
        path = "/metadata/mods/physicalDescription/extent"
        if exists(self.provider_data, path):
            extents = self.get_value(
                getprop(self.provider_data, path)
            )
            self.update_source_resource({"extent": extents})

    def map_format(self):
        path = "/metadata/mods/physicalDescription/form"
        if exists(self.provider_data, path):
            forms = self.get_value(
                getprop(self.provider_data, path)
            )
            self.update_source_resource({"format": forms})

    def map_identifier(self):
        path = "/metadata/mods/identifier"
        if exists(self.provider_data, path):
            identifier = self.get_value(
                getprop(self.provider_data, path)
            )
            self.update_source_resource({"identifier": identifier})

    def map_language(self):
        path = "/metadata/mods/language/languageTerm"
        if exists(self.provider_data, path):
            language_term = getprop(self.provider_data, path)
            if "type" in language_term \
                    and "authority" in language_term \
                    and language_term["type"] == "code" \
                    and language_term["authority"] == "iso639-2b":
                self.update_source_resource(
                    {"language": {"name": language_term["#text"]}}
                )

    def map_publisher(self):
        path = "/metadata/mods/originInfo/publisher"
        if exists(self.provider_data, path):
            publisher = self.get_value(
                getprop(self.provider_data, path)
            )
            self.update_source_resource(
                {"publisher": publisher}
            )

    def map_rights(self):
        path = "/metadata/mods/accessCondition"
        if exists(self.provider_data, path):
            rights = self.get_value(
                getprop(self.provider_data, path)
            )
            self.update_source_resource(
                {"rights": rights}
            )

    def map_subject(self):
        path = "/metadata/mods/subject"
        if exists(self.provider_data, path):
            for subject in getprop(self.provider_data, path):
                if isinstance(subject, dict) and "topic" in subject:
                    self.update_source_resource(
                        {"subject": subject["topic"]}
                    )

    def map_temporal(self):
        path = "/metadata/mods/subject"
        if exists(self.provider_data, path):
            for subject in getprop(self.provider_data, path):
                if isinstance(subject, dict) and "temporal" in subject:
                    if isinstance(subject["temporal"], str):
                        self.update_source_resource(
                            {"temporal": subject["temporal"]}
                        )
                    elif isinstance(subject["temporal"], dict):
                        self.update_source_resource(
                            {"temporal": subject["temporal"]["#text"]}
                        )

    def map_title(self):
        path = "/metadata/mods/titleInfo/title"
        if exists(self.provider_data, path):
            self.update_source_resource(
                {"title": getprop(self.provider_data, path)}
            )

    def map_type(self):
        path = "/metadata/mods/physicalDescription/form"
        if exists(self.provider_data, path):
            type = self.get_value(
                getprop(self.provider_data, path)
            )
            self.update_source_resource({"type": type})

    def map_data_provider(self, prop="source"):
        path = "/metadata/mods/recordInfo/recordContentSource"
        if exists(self.provider_data, path):
            data_provider = getprop(self.provider_data, path)
            self.mapped_data.update({"dataProvider": data_provider})

    def map_is_shown_at(self):
        path = "/metadata/mods/location/url"
        if exists(self.provider_data, path):
            for location_url in iterify(getprop(self.provider_data, path)):
                if "usage" in location_url \
                        and "access" in location_url \
                        and location_url["usage"] == "primary" \
                        and location_url["access"] == "object in context":
                    self.mapped_data.update(
                        {"isShownAt": location_url["#text"]}
                    )

    def map_object(self):
        path = "/metadata/mods/location/url"
        if exists(self.provider_data, path):
            for url in iterify(getprop(self.provider_data, path)):
                if "access" in url and url["access"] == "preview":
                    self.mapped_data.update({"object": url["#text"]})

    def map_spatial(self):
        # "<subject><geographic authority="""" valueURI="""">[text term]</geographic>"
        path = "/metadata/mods/subject"
        spatial = {}

        if exists(self.provider_data, path):
            for subject in iterify(getprop(self.provider_data, path)):
                if "cartographics" in subject and "coordinates" in subject["cartographics"]:
                    spatial["coordinates"] = subject["cartographics"]["coordinates"]

                if "geographic" in subject and isinstance(subject["geographic"], dict):
                    if "authority" in subject["geographic"] and "valueURI" in subject["geographic"]:
                        spatial["name"] = subject["geographic"].get("#text")

        if spatial:
            self.update_source_resource( {"spatial": spatial})

    def map_intermediate_provider(self):
        path = "/metadata/mods/note"
        intermediate_providers = []
        if exists(self.provider_data, path):
            for note in iterify(getprop(self.provider_data, path)):
                if "displayLabel" in note \
                        and "#text" in note \
                        and note["displayLabel"] == "Intermediate Provider":
                    intermediate_providers.append(note["#text"])
        if len(intermediate_providers) > 0:
            self.mapped_data.update(
                {"intermediateProvider": intermediate_providers}
            )

    def get_value(self, _value):
        if isinstance(_value, dict) and "#text" in _value:
            return _value.get("#text")
        else:
            return _value

    def log(self, label, obj):
        logger.error(label + ": " + str(obj))
