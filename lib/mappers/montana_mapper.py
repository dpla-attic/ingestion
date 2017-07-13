"""
Montana Mapper
"""
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper
from dplaingestion.selector import getprop
from dplaingestion.textnode import textnode
from dplaingestion.utilities import iterify


class MontanaMapper(OAIMODSMapper):
    def __init__(self, provider_data):
        super(MontanaMapper, self).__init__(provider_data)

    def map_collection(self):
        """first instance of <mods:relatedItem><mods:titleInfo><mods:title>"""
        collection = {}
        prop = self.root_key + "relatedItem"
        related_items = iterify(getprop(self.provider_data, prop, True))

        if related_items:
            title = getprop(related_items[0], "titleInfo/title", True)
            if title:
                collection = {
                        "title": title,
                        "@id": "",
                        "id": "",
                        "description": ""
                }

        if collection:
            self.update_source_resource({"collection": collection})

    def map_creator(self):
        """<mods:name><mods:namePart> when <mods:role><mods:roleTerm>
        equals Creator"""
        prop = self.root_key + "name"
        roleTypes = []
        _dict = {"creator": []}

        for s in iterify(getprop(self.provider_data, prop, True)):
            name = s.get("namePart")
            if name:
                try:
                    # Get all the roleTerm values for a given mods:name
                    # entity
                    roleTypes = [textnode(r.get("roleTerm")) for r in
                          iterify(s.get("role"))]
                except Exception as e:
                    continue

            # If mods:roleTerm is empty or if it contains 'Creator'
            # then map the namePart value to creator. If roleTerm
            # contains 'Contributor' map to contributor
            if "creator" in map(unicode.lower, roleTypes):
                if isinstance(name, list):
                    for n in name:
                        clean_name = textnode(n)
                        if isinstance(clean_name, basestring):
                            _dict["creator"].append(clean_name)
                else:
                    _dict["creator"].append(textnode(name))

        self.update_source_resource(self.clean_dict(_dict))

    def map_data_provider(self, prop="source"):
        """<mods:note> @type=ownership"""
        prop = self.root_key + "note"
        data_provider = []

        for r in iterify(getprop(self.provider_data, prop, True)):
            note_type = getprop(r, "type", True)
            if note_type and note_type == "ownership":
                data_provider.append(textnode(r))

        if data_provider:
            self.mapped_data.update({"dataProvider": data_provider})

    def map_date(self):
        """<mods:originInfo><mods:dateCreated>"""
        prop = self.root_key + "originInfo"
        dates = []

        for oi in iterify(getprop(self.provider_data, prop,True)):
            for d in iterify(getprop(oi, "dateCreated", True)):
                dates.append(textnode(d))
        if dates:
            self.update_source_resource({"date": dates})

    def map_description(self):
        """<mods:note> @type=content"""
        prop = self.root_key + "note"
        desc = []

        for d in iterify(getprop(self.provider_data, prop, True)):
            desc_type = getprop(d, "type", True)
            if desc_type and desc_type == "content":
                desc.append(textnode(d))

        if desc:
            self.update_source_resource({"description": desc})

    def map_edm_rights(self):
        """<mods:accessCondition>@type=use and reproduction
            @xlink:href =[this is the value to be mapped]"""

        prop = self.root_key + "accessCondition"
        edm_rights = []

        for r in iterify(getprop(self.provider_data, prop, True)):
            rights_type = getprop(r, "type", True)
            edm_value = getprop(r, "xlink:href", True)
            if rights_type and rights_type == "use and reproduction" and edm_value:
                edm_rights.append(textnode(edm_value))

        if edm_rights:
            self.mapped_data.update({"rights": edm_rights})

    def map_extent_format(self):
        """<mods:physicalDescription><extent>"""
        prop = self.root_key + "physicalDescription/"
        extent = []

        for pd in iterify(getprop(self.provider_data, prop, True)):
            for e in iterify(getprop(pd, "extent", True)):
                extent.append(textnode(e))

        if extent:
            self.update_source_resource({"extent": extent, "format": extent})

    def map_is_shown_at(self):
        """<mods:location><mods:url> @access=object in context @usage=primary display"""
        prop = self.root_key + "location"
        link = []

        for l in iterify(getprop(self.provider_data, prop, True)):
            for r in iterify(getprop(l, "url", True)):
                access_type = getprop(r, "access", True)
                usage_type = getprop(r, "usage", True)
                if (access_type and access_type == "object in context")\
                        and (usage_type and usage_type == "primary display"):
                    link.append(textnode(r))

        if link:
            self.mapped_data.update({"isShownAt": link[0]})

    def map_object(self):
        """<mods:location><mods:url> @access=preview"""
        prop = self.root_key + "location"
        link = []

        for l in iterify(getprop(self.provider_data, prop, True)):
            for r in iterify(getprop(l, "url", True)):
                access_type = getprop(r, "access", True)
                if access_type and access_type == "preview":
                    link.append(textnode(r))

        if link:
            self.mapped_data.update({"object": link[0]})

    def map_publisher(self):
        """<mods:originInfo><mods:publisher>"""
        prop = self.root_key + "originInfo"
        publishers = []

        for oi in iterify(getprop(self.provider_data, prop, True)):
            for p in iterify(getprop(oi, "publisher", True)):
                publishers.append(textnode(p))

        if publishers:
            self.update_source_resource({"publisher": publishers})

    def map_rights(self):
        """<mods:accessCondition> @type=local rights statements"""
        prop = self.root_key + "accessCondition"
        rights = []

        for r in iterify(getprop(self.provider_data, prop, True)):
            rights_type = getprop(r, "type", True)
            if rights_type and rights_type == "local rights statements":
                rights.append(textnode(r))

        if rights:
            self.update_source_resource({"rights": rights})

    def map_spatial(self):
        """<mods:subject><mods:geographic>"""
        prop = self.root_key + "subject"
        geo = []
        for s in iterify(getprop(self.provider_data, prop, True)):
            for g in iterify(getprop(s, "geographic", True)):
                geo.append(textnode(g))

        if geo:
            self.update_source_resource({"spatial": geo})

    def map_subject(self):
        """<mods:subject><mods:topic>"""
        prop = self.root_key + "subject"
        subjects = []

        for s in iterify(getprop(self.provider_data, prop, True)):
            for t in iterify(getprop(s, "topic", True)):
                subjects.append(textnode(t))

        if subjects:
            self.update_source_resource({"subject": subjects})

    def map_title(self):
        """<mods:titleInfo><mods:title>"""
        prop = self.root_key + "titleInfo"
        titles = []

        for ti in iterify(getprop(self.provider_data, prop, True)):
            for t in iterify(getprop(ti, "title", True)):
                titles.append(textnode(t))

        if titles:
            self.update_source_resource({"title": titles})

    def map_type(self):
        """<mods:typeofresource>"""
        prop = self.root_key + "typeofresource"
        types = []

        for t in iterify(getprop(self.provider_data, prop, True)):
            types.append(textnode(t))

        if types:
            self.update_source_resource({"type": types})

    def map_format(self):
        pass

    def map_multiple_fields(self):
        self.map_extent_format()