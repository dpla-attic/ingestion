from dplaingestion.mappers.mapper import Mapper

class NARAMapper(Mapper):
    def __init__(self, provider_data):
        super(NARAMapper, self).__init__(provider_data)

    def map_extent(self):
        extent = []
        for s in self.extract_xml_items("physical-occurrences",
                                        "physical-occurrence", "extent"):
            [extent.append(e) for e in iterify(s) if e not in extent]

        if extent:
            self.update_source_resource({"extent": extent})

    def map_creator(self):
        for s in self.extract_xml_items("creators", "creator"):
            if isinstance(s, dict) and s.get("creator-type") == "Most Recent":
                self.update_source_resource({"creator": s["creator-display"]})
                break

    def map_relation(self):
        relation = []
        lods = ("series", "file unit")
        for s in self.extract_xml_items("hierarchy", "hierarchy-item"):
            if s.get("hierarchy-item-lod", "").lower() in lods:
                relation.append(s.get("hierarchy-item-title"))

        if relation:
            self.update_source_resource({"relation": relation})

    def map_title(self):
        prop = "title-only"

        if prop in self.provider_data:
            self.update_source_resource({"title": self.provider_data[prop]})

    def map_language(self):
        language = self.extract_xml_items("languages", "language")
        if language:
            self.update_source_resource({"language": language})

    def map_identifier(self):
        identifier = [self.provider_data.get("arc-id")]
        for s in self.extract_xml_items("variant-control-numbers",
                                        "variant-control-number"):
            identifier.append(s.get("variant-type"))
            identifier.append(s.get("variant-number-desc"))

        identifier = list(set(filter(None, identifier)))
        if identifier:
            self.update_source_resource({"identifier": identifier})

    def map_rights(self):
        rights = []
        for s in self.extract_xml_items("access-restriction",
                                        "restriction-status"):
            if s not in rights:
                rights.append(s)

        if rights:
            self.update_source_resource({"rights": rights})

    def map_type(self):
        _type = self.extract_xml_items("general-records-types",
                                       "general-records-type",
                                       "general-records-type-desc")
        
        prop = "physical-occurrences/physical-occurrence"
        if exists(self.provider_data, prop):
            type_key = "media-occurrences/media-occurrence/media-type"
            for s in iterify(getprop(self.provider_data, prop)):
                if exists(s, type_key):
                    _type.append(getprop(s, type_key))

        if _type:
            self.update_source_resource({"type": "; ".join(_type)})

    def map_date(self):
        group_keys = ("coverage-dates", "copyright-dates", "production-dates",
                     "broadcast-dates", "release-dates")
        for key in group_keys:
            if key in self.provider_data:
                if key == "coverage-dates":
                    begin = self.extract_xml_items(key, "cov-start-date")
                    end = self.extract_xml_items(key, "cov-end-date")
                    date = "%s-%s" % (begin, end)
                else:
                    date = self.extract_xml_items(key, key[:-1])

                if date:
                    self.update_source_resource({"date": date})
                    break

    def map_description(self):
        description = self.extract_xml_items("general-notes", "general-note")

        if description:
            self.update_source_resource({"description": description})

    def map_state_located_in(self):
        for phys in self.extract_xml_items("physical-occurrences",
                                           "physical-occurrence"):
            state_located_in = self.extract_xml_items("reference-units",
                                                      "reference-unit",
                                                      "state")
            if state_located_in:
                self.update_source_resource({"stateLocatedIn":
                                             state_located_in})
                break

    def map_has_view(self):
        def _add_views(has_view, url, format=None):
            for i in range(0, len(url)):
                view = {"@id": url[i]}
                if format:
                    view["format"] = format[i]
                has_view.append(view)
            return has_view

        has_view = []

        if "objects" in self.provider_data:
            group_key = "objects"
            item_key = "object"
            url_key = "file-url"
            format_key = "mime-type"
            url = self.extract_xml_items(group_key, item_key, url_key)
            format = self.extract_xml_items(group_key, item_key, format_key)
            has_view = _add_views(has_view, url, format)

        if "online-resources" in self.provider_data:
            group_key = "online-resources"
            item_key = "online-resource"
            url_key = "online-resource-url"
            url = self.extract_xml_items(group_key, item_key, url_key)
            has_view = _add_views(has_view, url)

        if has_view:
            self.mapped_data.update({"hasView": has_view})

    def map_data_provider(self):
        data_provider = None
        phys_occur = self.extract_xml_items("physical-occurrences",
                                            "physical-occurrence")

        # Use "Reproduction-Reference" copy-status
        p = [phys for phys in phys_occur if "copy-status" in phys and
             phys.get("copy-status") == "Reproduction-Reference"]
        if not p:
            # Use "Preservation" copy-status
            p = [phys for phys in phys_occur if "copy-status" in phys and
                 phys.get("copy-status") == "Preservation"]

        if p:
            ref = self.extract_xml_items("reference-units", "reference-unit",
                                         data=p[0])
            if ref and ref[0]["num"] == "1":
                self.mapped_data.update({"dataProvider": ref[0]["name"]})

    def map_is_shown_at(self):
        prefix = "http://research.archives.gov/description/%s"

        self.mapped_data.update({"isShownAt": prefix %
                                 self.provider_data["arc-id-desc"]})

    def map_object(self):
        for obj in self.extract_xml_items("objects", "object"):
            if "thumbnail-url" in obj:
                self.mapped_data.update({"object": obj["thumbnail-url"]})
                break

    def map_subject_and_spatial(self):
        _dict = {"subject": [], "spatial": []}

        for s in self.extract_xml_items("subject-references",
                                        "subject-reference"):
            _dict["subject"].append(s["display-name"])
            if s["subject-type"] == "TGN":
                _dict["spatial"].append(s["display-name"])

        self.update_source_resource(self.clean_dict(_dict))

    def map_contributor_and_publisher(self):
        _dict = {"contributor": [], "publisher": []}

        for s in self.extract_xml_items("contributors", "contributor"):
            _type = s.get("contributor-type")
            display = s.get("contributor-display")
            if _type == "Publisher":
                _dict["publisher"].append(display)
            else:
                # If contributor-type is "Most Recent" use only this
                # contributor
                if _type == "Most Recent":
                    _dict["contributor"] = display
                elif isinstance(_dict["contributor"], list):
                    _dict["contributor"].append(display)

        self.update_source_resource(self.clean_dict(_dict))

    def map_multiple_fields(self):
        self.map_subject_and_spatial()
        self.map_contributor_and_publisher()
