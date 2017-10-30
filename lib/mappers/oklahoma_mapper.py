"""
Oklahoma Hub Mapper
"""

from akara import logger
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.textnode import textnode


class OklahomaMapper(OAIMODSMapper):
    # < mods:titleInfo > < mods:title type = alternative >
    def map_title(self):
        prop = self.root_key + "titleInfo"
        allTitles = []
        altTitles, titles = [], []

        for ti in iterify(getprop(self.provider_data, prop, True)):
            for t in iterify(getprop(ti, "title", True)):
                allTitles.append(t)
        for t in allTitles:
            if isinstance(t, dict):
                tType = getprop(t, "type", True)
                if tType and tType == "alternative":
                    altTitles.append(textnode(t))
                else:
                    titles.append(textnode(t))
            else:
                titles.append(textnode(t))
        if titles:
            self.update_source_resource({"title": titles})
        if altTitles:
            self.update_source_resource({"alternative": altTitles})

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
                    ret_dict = {
                        "collection": {
                            "title": title, "@id": "",
                            "id": "", "description": ""
                        }
                    }

        if ret_dict["collection"]:
            self.update_source_resource(ret_dict)

    def map_creator(self):
        prop = self.root_key + "name"
        creators = []
        for oi in iterify(getprop(self.provider_data, prop, True)):
            for d in iterify(getprop(oi, "namePart", True)):
                creators.append(textnode(d))
        if creators:
            self.update_source_resource({"creator": creators})

    def map_data_provider(self):
        prop = self.root_key + "note"
        data_provider = self.map_value(prop, "type", "ownership")
        if data_provider:
            self.mapped_data.update({"dataProvider": data_provider})

    def map_date(self):
        prop = self.root_key + "originInfo"
        dateCreated = []
        for oi in iterify(getprop(self.provider_data, prop, True)):
            for d in iterify(getprop(oi, "dateCreated", True)):
                dateCreated.append(textnode(d))

        if dateCreated:
            self.update_source_resource({"date": dateCreated})

    def map_description(self):
        prop = self.root_key + "note"
        description = self.map_value(prop, "type", "content")
        if description:
            self.mapped_data.update({"description": description})

    def map_extent(self):
        prop = self.root_key + "physicalDescription"
        extents = []
        for oi in iterify(getprop(self.provider_data, prop, True)):
            for d in iterify(getprop(oi, "extent", True)):
                extents.append(textnode(d))

        if extents:
            self.update_source_resource({"extent": extents})

    def map_format(self):
        formats = []
        for g in iterify(getprop(self.provider_data, self.root_key+"genre",
                                 True)):
            formats.append(textnode(g))
        for d in iterify(getprop(self.provider_data,
                                 self.root_key + "physicalDescription", True)):
            for n in iterify(getprop(d, "note", True)):
                formats.append(textnode(n))
        if formats:
            self.update_source_resource({"format": formats})

    def map_identifier(self):
        prop = self.root_key + "identifier"
        ids = self.map_value(prop)
        if ids:
            self.update_source_resource({"identifier": ids})

    def map_is_shown_at(self):
        urls = []
        for i in iterify(getprop(self.provider_data, self.root_key + "location", True)):
            t = getprop(i, "url", True)
            usage = getprop(i, "url/usage", True)
            access = getprop(i, "url/access", True)
            if (usage
                and access
                and usage == "primary display"
                and access == "object in context"):

                urls.append(textnode(t))
        if urls:
            self.mapped_data.update({"isShownAt": urls[0]})

    def map_language(self):
        prop = self.root_key + "language"
        langs = []
        for oi in iterify(getprop(self.provider_data, prop, True)):
            for d in iterify(getprop(oi, "languageTerm", True)):
                langs.append(textnode(d))

        if langs:
            self.update_source_resource({"language": langs})

    def map_publisher(self):
        prop = self.root_key + "originInfo"
        publishers = []
        for oi in iterify(getprop(self.provider_data, prop, True)):
            for d in iterify(getprop(oi, "publisher", True)):
                publishers.append(textnode(d))

        if publishers:
            self.update_source_resource({"publisher": publishers})

    def map_object(self):
        urls = []
        for l in iterify(getprop(self.provider_data, self.root_key +
                "location", True)):
            for u in iterify(getprop(l, "url", True)):
                access = getprop(u, "access", True)
                if access and access == "preview":
                    urls.append(textnode(u))
        if urls:
            self.mapped_data.update({"object": urls[0]})

    def map_relation(self):
        # <mods:relatedItem><mods:titleInfo><mods:title> when @type DOES NOT
        # equal "host"
        relations = []

        for ri in iterify(getprop(self.provider_data, "relatedItem", True)):
            for ti in iterify(getprop(ri, "titleInfo", True)):
                for t in iterify(getprop(ti, "title", True)):
                    rType = getprop(t, "type", True)
                    if rType and not rType == "host":
                        relations.append(textnode(t))
        if relations:
            self.update_source_resource({"relation": relations})

    def map_edm_rights(self):
        prop = self.root_key + "accessCondition"
        urls = []
        for i in iterify(getprop(self.provider_data, prop, True)):
            rType = getprop(i, "type", True)
            rLink = getprop(i, "xlink:href", True)
            if rType and rLink and rType == "use and reproduction":
                urls.append(textnode(i))
        if urls:
            self.mapped_data.update({"rights": urls[0]})

    def map_rights(self):
        prop = self.root_key + "accessCondition"
        rights = self.map_value(prop, "type", "local rights statement")
        if rights:
            self.update_source_resource({"rights": rights})

    def map_spatial(self):
        geography = []
        for oi in iterify(getprop(self.provider_data, self.root_key + "subject", True)):
            for d in iterify(getprop(oi, "geographic", True)):
                geography.append(textnode(d))

        if geography:
            self.update_source_resource({"spatial": geography})

    def map_subject(self):
        subjects = []
        for oi in iterify(getprop(self.provider_data, self.root_key + "subject", True)):
            for d in iterify(getprop(oi, "topic", True)):
                subjects.append(textnode(d))
        if subjects:
            self.update_source_resource({"subject": subjects})

    def map_temporal(self):
        temporal = []
        for oi in iterify(getprop(self.provider_data, self.root_key + "subject", True)):
            for d in iterify(getprop(oi, "temporal", True)):
                temporal.append(textnode(d))
        if temporal:
            self.update_source_resource({"temporal": temporal})

    def map_type(self):
        prop = self.root_key + "typeOfResource"
        s = self.map_value(prop)
        if s:
            self.update_source_resource({"type": s})

    def map_value(self, prop, attribute=None, attrValue=None):
        values = []
        for v in iterify(getprop(self.provider_data, prop, True)):
            if attribute and isinstance(v, dict):
                av = getprop(v, attribute, True)
                if av and attrValue and av == attrValue:
                    values.append(textnode(v))
            # Only extract text value if not checking attributes
            elif attribute is None:
                values.append(textnode(v))
        return filter(None, values)

    def map_multiple_fields(self):
        self.map_collection()
