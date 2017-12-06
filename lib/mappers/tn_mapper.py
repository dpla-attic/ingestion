from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mods_mapper import MODSMapper
from dplaingestion.textnode import textnode


class TNMapper(MODSMapper):
    def __init__(self, provider_data):
        super(TNMapper, self).__init__(provider_data)

    def map_multiple_fields(self):
        self.map_spatial_and_subject_and_temporal()

    def map_is_part_of(self):
        path = "/metadata/mods/relatedItem"
        collections = []

        if exists(self.provider_data, path):
            for relatedItem in iterify(getprop(self.provider_data, path)):
                title, description = "", ""
                if "displayLabel" in relatedItem \
                        and "titleInfo" in relatedItem:
                    title = relatedItem["titleInfo"]["title"]

                if "abstract" in relatedItem:
                    description = relatedItem["abstract"]

                if title:
                    collections.append({"title": title,
                                    "description": description,
                                    "@id": "",
                                    "id": ""})

        if collections:
            self.update_source_resource({"collection": collections})

    # helper function for the next two functions
    def name_part(self, role_type):
        prop = "/metadata/mods/name"
        results = []
        if exists(self.provider_data, prop):
            for name in getprop(self.provider_data, prop):
                if "role" in name and "namePart" in name:
                    for role in iterify(name["role"]):
                        role_prop = "roleTerm/#text"
                        if exists(role, role_prop) \
                                and getprop(role, role_prop) == role_type:
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

            if date_created:
                self.update_source_resource({"date": date_created})

    def map_description(self):
        description = []

        path = "/metadata/mods/abstract"
        for d in iterify(getprop(self.provider_data, path, True)):
            description.append(textnode(d))

        if description:
            self.update_source_resource({"description": description})

    def map_extent(self):
        path = "/metadata/mods/physicalDescription/extent"
        extents = []
        if exists(self.provider_data, path):
            for e in iterify(getprop(self.provider_data, path)):
                extents.append(textnode(e))

            if extents:
                self.update_source_resource({"extent": extents})

    def map_format(self):
        path = "/metadata/mods/physicalDescription/form"
        formats = []
        if exists(self.provider_data, path):
            for f in iterify(getprop(self.provider_data, path)):
                formats.append(textnode(f))

            if formats:
                self.update_source_resource({"format": formats})

    def map_identifier(self):
        path = "/metadata/mods/identifier"
        identifiers = []
        if exists(self.provider_data, path):
            for tn_id in iterify(getprop(self.provider_data, path)):
                identifiers.append(textnode(tn_id))
        if identifiers:
            self.update_source_resource({"identifier": identifiers})

    def map_language(self):
        path = "/metadata/mods/language/languageTerm"
        lang = []
        if exists(self.provider_data, path):
            for l in iterify(getprop(self.provider_data, path)):
                if "type" in l and "authority" in l \
                        and l["type"] == "code" \
                        and l["authority"] == "iso639-2b":
                    lang.append(textnode(l))

            if lang:
                self.update_source_resource({"language": lang})

    def map_publisher(self):
        path = "/metadata/mods/originInfo/publisher"
        pub = []
        if exists(self.provider_data, path):
            for p in iterify(getprop(self.provider_data, path)):
                pub.append(textnode(p))

            if pub:
                self.update_source_resource({"publisher": pub})

    def map_rights(self):
        path = "/metadata/mods/accessCondition"
        rights = []
        if exists(self.provider_data, path):
            for r in iterify(getprop(self.provider_data, path)):
                t = getprop(r, "type", True)
                if t and t == "local rights statement":
                    rights.append(textnode(r))

            if rights:
                self.update_source_resource({"rights": rights})

    def map_edm_rights(self):
        path = "/metadata/mods/accessCondition"
        edm_rights = ""
        if exists(self.provider_data, path):
            for r in iterify(getprop(self.provider_data, path)):
                t = getprop(r, "type", True)
                rs = getprop(r, "xlink:href", True)
                if t and rs and t == "use and reproduction":
                    edm_rights = textnode(rs)

        if edm_rights:
            self.mapped_data.update({"rights": edm_rights})

    def map_spatial_and_subject_and_temporal(self):
        path = "/metadata/mods/subject"
        subject_props = ['topic', 'genre', 'occupation', "/titleInfo/title"]
        spatials = []
        temporals = []
        subjects = []

        if exists(self.provider_data, path):
            for subject in iterify(getprop(self.provider_data, path)):
                if "cartographics" in subject and \
                                "coordinates" in subject["cartographics"]:
                    coord = subject["cartographics"]["coordinates"]
                    spatials.append({"name": coord })

                if "geographic" in subject:
                    for g in iterify(getprop(subject, "geographic")):
                        spatials.append({"name": textnode(g)})

                if "temporal" in subject:
                    for t in iterify(getprop(subject, "temporal")):
                        temporals.append(textnode(t))

                for s_path in subject_props:
                    for s in iterify(getprop(subject, s_path, True)):
                        subjects.append(s)

        if spatials:
            self.update_source_resource({"spatial": spatials})
        if temporals:
            self.update_source_resource({"temporal": temporals})
        if subjects:
            self.update_source_resource({"subject": subjects})

    def map_title(self):

        path = "/metadata/mods/titleInfo"
        titles = []
        if exists(self.provider_data, path):
            for t in iterify(getprop(self.provider_data, path)):
                if exists(t, "title") and not exists(t, "title/type"):
                    titles.append(textnode(getprop(t, "title")))
            if titles:
                self.update_source_resource({"title": titles})

    def map_type(self):
        path = "/metadata/mods/typeOfResource"
        path_form = "/metadata/mods/physicalDescription/form"

        if not exists(self.provider_data, path):
            path = path_form

        if exists(self.provider_data, path):
            types = []
            for t in iterify(getprop(self.provider_data, path)):
                types.append(textnode(t))
            if types:
                self.update_source_resource({"type": types})

    def map_data_provider(self, prop="source"):
        path = "/metadata/mods/recordInfo/recordContentSource"
        data_provider = []
        if exists(self.provider_data, path):
            for dp in iterify(getprop(self.provider_data, path)):
                data_provider.append(textnode(dp))
            self.mapped_data.update({"dataProvider": data_provider[0]})

    def map_is_shown_at(self):
        path = "/metadata/mods/location"
        if exists(self.provider_data, path):
            for locations in iterify(getprop(self.provider_data, path)):
                if exists(locations, "url"):
                    for url in iterify(getprop(locations, "url")):
                        if(exists(url, "usage") and exists(url, "access")
                           and url["usage"].lower().startswith("primary")
                           and url["access"].lower() == "object in context"):
                            self.mapped_data.update({"isShownAt": textnode(url)})

    def map_object(self):
        path = "/metadata/mods/location"
        if exists(self.provider_data, path):
            for locations in iterify(getprop(self.provider_data, path)):
                if exists(locations, "url"):
                    for url in iterify(getprop(locations, "url")):
                        if(exists(url, "access")
                           and url["access"].lower() == "preview"):
                            self.mapped_data.update({"object": textnode(url)})

    def map_intermediate_provider(self):
        path = "/metadata/mods/note"
        im_prov = []
        if exists(self.provider_data, path):
            for note in iterify(getprop(self.provider_data, path)):
                if "displayLabel" in note \
                        and "#text" in note \
                        and note["displayLabel"] == "Intermediate Provider":
                    im_prov.append(textnode(note))

            # There can be only one.
            if im_prov:
                self.mapped_data.update({"intermediateProvider": im_prov[0]})

    @classmethod
    def log(self, label, obj):
        logger.error(label + ": " + str(obj))