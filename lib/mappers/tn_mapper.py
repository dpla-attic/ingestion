from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mods_mapper import MODSMapper


class TNMapper(MODSMapper):
    def __init__(self, provider_data):
        super(TNMapper, self).__init__(provider_data)

    def map_multiple_fields(self):
        super(MODSMapper, self).map_multiple_fields()
        self.map_alternative()
        self.map_edm_has_type()

    def map_alternative(self):
        prop = "/metadata/mods/titleInfo"
        if exists(self.provider_data, prop):
            for titleInfoDict in iterify(getprop(self.provider_data, prop)):
                titleInfo = titleInfoDict['title']
                if isinstance(titleInfo, dict) \
                        and titleInfo['type'] == "alternative" \
                        and "#text" in titleInfo:
                    self.update_source_resource(
                        {"alternative": titleInfo["#text"]}
                    )

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
                    self.mapped_data.update({"collection": {"title": title}})

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
        self.log("CONTRIBUTOR", contributor)
        if len(contributor) > 0:
            self.update_source_resource({"contributor": contributor})

    def map_creator(self):
        creators = self.name_part("Creator")
        self.log("CREATOR", creators)
        if len(creators) > 0:
            self.update_source_resource({"creator": creators})

    def map_date(self):
        path = "/metadata/mods/originInfo/dateCreated/#text"
        if exists(self.provider_data, path):
            date_created = getprop(self.provider_data, path)
            self.update_source_resource({"date": date_created})

    def map_description(self):
        path = "/metadata/mods/abstract"
        if exists(self.provider_data, path):
            description = getprop(self.provider_data, path)
            self.update_source_resource({"description": description})

    def map_extent(self):
        path = "/metadata/mods/physicalDescription/extent"
        if exists(self.provider_data, path):
            extents = getprop(self.provider_data, path)
            self.update_source_resource({"extent": extents})

    def map_format(self):
        path = "/metadata/mods/physicalDescription/form"
        if exists(self.provider_data, path):
            forms = getprop(self.provider_data, path)
            self.update_source_resource({"format": forms})

    def map_edm_has_type(self):
        path = "/metadata/mods/genre"
        if exists(self.provider_data, path):
            for genre in iterify(getprop(self.provider_data, path)):
                if "authority" in genre \
                        and "valueURI" in genre \
                        and genre["authority"] == "aat":
                    self.update_source_resource(
                        {"hasType": genre["valueURI"]}
                    )

    def map_identifier(self):
        path = "/metadata/mods/identifier"
        if exists(self.provider_data, path):
            identifer = getprop(self.provider_data, path)
            self.update_source_resource({"identifier": identifer})

    def map_language(self):
        path = "/metadata/mods/language/languageTerm"
        if exists(self.provider_data, path):
            language_term = getprop(self.provider_data, path)
            if "type" in language_term \
                    and "authority" in language_term \
                    and language_term["type"] == "code" \
                    and language_term["authority"] == "iso639-2b":
                self.update_source_resource(
                    {"language": language_term["#text"]}
                )

    def map_publisher(self):
        path = "/metadata/mods/originInfo/publisher"
        if exists(self.provider_data, path):
            self.update_source_resource(
                {"publisher": getprop(self.provider_data, path)}
            )

    def map_rights(self):
        path = "/metadata/mods/accessCondition"
        if exists(self.provider_data, path):
            self.update_source_resource(
                {"rights": getprop(self.provider_data, path)}
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
            type = getprop(self.provider_data, path)
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
                if "access" in url and url["access"] == "raw object":
                    self.mapped_data.update({"object": url["#text"]})

    def map_preview(self):
        path = "/metadata/mods/location/url"
        if exists(self.provider_data, path):
            for url in iterify(getprop(self.provider_data, path)):
                if "access" in url and url["access"] == "preview":
                    self.mapped_data.update({"preview": url["#text"]})

    def map_provider(self, prop="provider"):
        self.mapped_data.update({"provider": "Tennessee Digital Library"})

    def map_spatial(self):
        # "<subject><geographic authority="""" valueURI="""">[text term]</geographic>"
        path = "/metadata/mods/subject"
        spatial = {}

        if exists(self.provider_data, path):
            for subject in getprop(self.provider_data, path):
                # self.log("SUBJ GEO", subject)

                if "cartographics" in subject and "coordinates" in subject["cartographics"]:
                    spatial["coordinates"] = subject["cartographics"]["coordinates"]
                    # self.update_source_resource({"spatial": subject["cartographics"]["coordinates"]})
                if "geographic" in subject:
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

    def log(self, label, obj):
        logger.error(label + ": " + str(obj))
