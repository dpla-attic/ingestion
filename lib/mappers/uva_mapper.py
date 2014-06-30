from dplaingestion.mappers.mods_mapper import MODSMapper

class UVAMapper(MODSMapper):
    def __init__(self, provider_data):
        super(UVAMapper, self).__init__(provider_data)

    def map_subject(self):
        def _join_on_double_hyphen(subject_list):
            if any(["--" in s for s in subject_list]):
                return subject_list
            else:
                return ["--".join(subject_list)]

        prop = "subject"

        if exists(self.provider_data, prop):
            subject = []
            for _dict in iterify(getprop(self.provider_data, prop)):
                # Extract subject from both "topic" and "name" fields
                if "topic" in _dict:
                    topic = _dict["topic"]
                    if isinstance(topic, list):
                        subject.extend(_join_on_double_hyphen(topic))
                    else:
                        subject.append(topic)
                if "name" in _dict:
                    name_part = getprop(_dict, "name/namePart")
                    if isinstance(name_part, list):
                        subj = [None, None, None]
                        for name in name_part:
                            if isinstance(name, basestring):
                                subj[0] = name
                            elif isinstance(name, dict):
                                if name.get("type") == "termsOfAddress":
                                    subj[1] = name["#text"]
                                elif name.get("type") == "date":
                                    subj[2] = name["#text"]
                        if subj:
                            subject.append(" ".join(filter(None, subj)))
                            
                    else:
                        subject.append(name_part)

            if subject:
                self.update_source_resource({"subject": subject})

    def map_creator(self):
        prop = "name"

        if exists(self.provider_data, prop):
            personal_creator = []
            corporate_creator = []
            for s in iterify(getprop(self.provider_data, prop)):
                creator = [None, None, None]
                for name in iterify(s.get("namePart")):
                    if isinstance(name, basestring):
                            creator[0] = name
                    elif isinstance(name, dict):
                        type = name.get("type")
                        if type == "family":
                            creator[0] = name.get("#text")
                        elif type == "given":
                            creator[1] = name.get("#text")
                        elif type == "termsOfAddress":
                            creator[1] = name.get("#text")
                        elif type == "date":
                            creator[2] = name.get("#text")

                creator = ", ".join(filter(None, creator))

                if (s.get("type") == "personal" and creator not in
                    personal_creator):
                    personal_creator.append(creator)
                elif (s.get("type") == "corporate" and creator not in
                      corporate_creator):
                    corporate_creator.append(creator)

            if personal_creator:
                self.update_source_resource({"creator": personal_creator})
            elif corporate_creator:
                self.update_source_resource({"creator": corporate_creator})

    def map_title(self):
        prop = "titleInfo"

        if exists(self.provider_data, prop):
            title = []
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, basestring):
                    title.append(s)
                elif isinstance(s, dict):
                    t = s.get("title")
                    if t and "nonSort" in s:
                        t = s.get("nonSort") + t
                    title.append(t)
            title = filter(None, title)

            if title:
                self.update_source_resource({"title": title[-1]})

    def map_date(self):
        prop = "originInfo"

        if exists(self.provider_data, prop):
            date = []
            date_prop = prop + "/dateIssued"
            if not exists(self.provider_data, date_prop):
                date_prop = prop + "/dateCreated"

            for s in iterify(getprop(self.provider_data, date_prop)):
                if isinstance(s, basestring):
                    date.append(s)
                elif isinstance(s, dict):
                    date.append(s.get("#text"))
            date = filter(None, date)

            if date:
                self.update_source_resource({"date": date})

    def map_identifier(self):
        prop = "identifier"

        if exists(self.provider_data, prop):
            identifier = []
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, dict) and s.get("type") == "uri":
                    identifier.append(s.get("#text"))
            identifier = "-".join(filter(None, identifier))

            if identifier:
                self.update_source_resource({"identifier": identifier})

    def map_spatial(self):
        spatial = []
        prop = "subject"
        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "hierarchicalGeographic" in s:
                    spatial = s["hierarchicalGeographic"]
                    name = ", ".join(filter(None, [spatial.get("city"),
                                                   spatial.get("county"),
                                                   spatial.get("state"),
                                                   spatial.get("country")]))
                    spatial["name"] = name
                    spatial = [spatial]

        prop = "originInfo/place"
        if not spatial and exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "placeTerm" in s:
                    for place in iterify(s["placeTerm"]):
                        if "type" in place and place["type"] != "code":
                            spatial.append(place["#text"])

        if spatial:
            self.update_source_resource({"spatial": spatial})

    def map_publisher(self):
        prop = "originInfo/publisher"

        if exists(self.provider_data, prop):
            self.update_source_resource({"publisher":
                                         getprop(self.provider_data, prop)})

    def map_rights(self):
        prop = "accessCondition"

        if exists(self.provider_data, prop):
            rights = filter(None, [s.get("#text") for s in
                                   getprop(self.provider_data, prop) if
                                   isinstance(s, dict)])

            if rights:
                self.update_source_resource({"rights": rights})

    def map_type(self):
        prop = "typeOfResource/#text"

        if exists(self.provider_data, prop):
            self.update_source_resource({"type":
                                         getprop(self.provider_data, prop)})

    def map_state_located_in(self):
        self.update_source_resource({"stateLocatedIn": "Virginia"})

    def map_description_extent_and_format(self):
        prop = "physicalDescription"

        if exists(self.provider_data, prop):
            out = {}
            for _dict in iterify(getprop(self.provider_data, prop)):
                note = getprop(_dict, "note")
                if note:
                    for sub_dict in note:
                        if (isinstance(sub_dict, dict) and "displayLabel" in
                            sub_dict):
                            if sub_dict["displayLabel"] == "size inches":
                                out["extent"] = sub_dict.get("#text")
                            elif sub_dict["displayLabel"] == "condition":
                                out["description"] = sub_dict.get("#text")
                if "form" in _dict:
                    for form in iterify(_dict["form"]):
                        if "#text" in form:
                            out["format"] = form["#text"]
                            break

            if out:
                self.update_source_resource(out)

    def map_is_show_at_object_has_view_and_dataprovider(self):
        def _get_media_type(d):
            pd = iterify(getprop(d, "physicalDescription"))
            for _dict in pd:
                if exists(_dict, "internetMediaType"):
                    return getprop(_dict, "internetMediaType")

        prop = "location"
        if exists(self.provider_data, prop):
            location = iterify(getprop(self.provider_data, prop))
            format = _get_media_type(self.provider_data)
            out = {}
            try:
                for _dict in location:
                    if "url" in _dict:
                        for url_dict in _dict["url"]:
                            if url_dict and "access" in url_dict:
                                if url_dict["access"] == "object in context":
                                    out["isShownAt"] = url_dict.get("#text")
                                elif url_dict["access"] == "preview":
                                    out["object"] = url_dict.get("#text")
                                elif url_dict["access"] == "raw object":
                                    has_view = {"@id": url_dict.get("#text"),
                                                "format": format}
                                    out["hasView"] = has_view
                    if ("physicalLocation" in _dict and
                        isinstance(_dict["physicalLocation"], basestring)):
                        out["dataProvider"] = _dict["physicalLocation"]
            except Exception as e:
                logger.error(e)

            if out:
                self.mapped_data.update(out)

    def map_multiple_fields(self):
        self.map_description_extent_and_format()
        self.map_is_show_at_object_has_view_and_dataprovider()
