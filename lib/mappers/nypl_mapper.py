from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mods_mapper import MODSMapper

class NYPLMapper(MODSMapper):
    def __init__(self, provider_data):
        super(NYPLMapper, self).__init__(provider_data)
        self.creator_roles = [
            "architect", "artist", "author", "cartographer", "composer",
            "creator", "designer", "director", "engraver", "interviewer",
            "landscape architect", "lithographer", "lyricist",
            "musical director", "photographer", "performer",
            "project director", "singer", "storyteller", "surveyor",
            "technical director", "woodcutter"
            ]

    def map_title(self):
        prop = "titleInfo"

        if exists(self.provider_data, prop):
            title_info = iterify(getprop(self.provider_data, prop))
            # Title is in the last titleInfo element
            title = None
            try:
                title = self._striptags(title_info[-1].get("title"))
            except:
                logger.error("Error setting sourceResource.title for %s" %
                             self.provider_data["_id"])
            
            if title:
                self.update_source_resource({"title": title})

    def map_identifier(self):
        prop = "identifier"

        if exists(self.provider_data, prop):
            id_values = ("local_imageid", "isbn", "isrc", "isan", "ismn",
                         "iswc", "issn", "uri", "urn")
            identifier = []
            for v in iterify(getprop(self.provider_data, prop)):
                for id_value in id_values:
                    if (id_value in v.get("displayLabel", "") or
                        id_value in v.get("type", "")):
                        identifier.append(v.get("#text"))
                        break

            if identifier:
                self.update_source_resource({"identifier": identifier})

    def map_description(self):
        def txt(n):
            if not n:
                return ""
            elif type(n) == dict:
                return n.get("#text") or ""
            elif isinstance(n, basestring):
                return n
            else:
                return "" 

        note = txt(getprop(self.provider_data, "note", True))
        pd = getprop(self.provider_data, "physicalDescription", True)
        pnote = None
        if type(pd) == list:
            pnote = [e["note"] for e in pd if "note" in e] # Yes, a list.
        elif type(pd) == dict and "note" in pd:
            pnote = txt(pd["note"]) # Yes, a string.

        desc = note or pnote
        if desc:
            self.update_source_resource({"description": desc})

    def map_is_shown_at(self):
        prop = "tmp_item_link"

        if exists(self.provider_data, prop):
            self.mapped_data.update({"isShownAt":
                                     getprop(self.provider_data, prop)})

    def map_state_located_in(self):
        self.update_source_resource({"stateLocatedIn": "New York"})

    def map_subject(self):
        # Mapped from subject and genre 
        #
        # Per discussion with Amy on 10 April 2014, don't worry about
        # checking whether heading maps to authority file. Amy simplified the
        # crosswalk.
        #
        # TODO: When present, we should probably pull in the valueURI and
        # authority values into the sourceResource.subject - this would
        # represent an index/API change, however. 
        subject = []

        if exists(self.provider_data, "subject"):
            for v in iterify(getprop(self.provider_data, "subject")):
                if "topic" in v:
                    if isinstance(v, basestring):
                        subject.append(v["topic"])
                    elif isinstance(v["topic"], dict):
                        subject.append(v["topic"].get("#text"))
                    else:
                        logger.error("Topic is not a string nor a dict; %s" %
                                     self.provider_data["_id"])
                if exists(v, "name/namePart"):
                    subject.append(getprop(v, "name/namePart"))

        if exists(self.provider_data, "genre"): 
            for v in iterify(getprop(self.provider_data, "genre")):
                if isinstance(v, basestring):
                    subject.append(v)
                elif isinstance(v, dict):
                    subject.append(v.get("#text"))
                else:
                    logger.error("Genre is not a string nor a dict; %s" %
                                 self.provider_data["_id"])

        if subject:
            self.update_source_resource({"subject": subject})

    def map_type(self):
        prop = "typeOfResource"

        if exists(self.provider_data, prop):
            self.update_source_resource({"type":
                                         getprop(self.provider_data, prop)})

    def map_format(self):
        prop = "physicalDescription/form"

        if exists(self.provider_data, prop):
            format = []
            for v in iterify(getprop(self.provider_data, prop)):
                if isinstance(v, dict):
                    f = v.get("$")
                    if f:
                        format.append(f)

            if format:
                self.update_source_resource({"format": format})

    def map_rights(self):
        prop = "tmp_rights_statement"
        if exists(self.provider_data, prop):
            self.update_source_resource({"rights":
                                         getprop(self.provider_data, prop)})

    def map_contributor_and_creator(self):
        prop = "name"
        if exists(self.provider_data, prop):
            ret_dict = {}
            creator = set()
            contributor = set()
            for v in iterify(getprop(self.provider_data, prop)):
                for role in iterify(v.get("role", [])):
                    for role_term in iterify(role.get("roleTerm", [])):
                        rt = role_term.get("#text").lower().strip(" .")
                        if rt in self.creator_roles:
                            creator.add(v["namePart"])
                        elif rt != "publisher":
                            contributor.add(v["namePart"])

            if creator:
                ret_dict["creator"] = list(creator)
            cont = contributor - creator
            if cont:
                ret_dict["contributor"] = list(cont)

            self.update_source_resource(ret_dict)

    def map_extent(self):
        prop = "physicalDescription/extent"

        if exists(self.provider_data, prop):
            self.update_source_resource({"extent":
                                         getprop(self.provider_data, prop)})

    def map_date_publisher_and_spatial(self):
        """
        Examine the many possible originInfo elements and pick out date,
        spatial, and publisher information.

        Dates may come in multiple originInfo elements, in which case we take
        the last one.
        """
        ret_dict = {"date": [], "spatial": [], "publisher": []}
        date_fields = ("dateIssued", "dateCreated", "dateCaptured",
                       "dateValid", "dateModified", "copyrightDate",
                       "dateOther")
        date_origin_info = []

        def datestring(date_data):
            """
            Given a "date field" element from inside an originInfo, return a
            string representation of the date or dates represented.
            """
            if type(date_data) == dict:
                # E.g. single dateCaptured without any attributes; just take
                # it
                return date_data.get("#text")
            elif type(date_data) == unicode:
                return date_data
            keyDate, startDate, endDate = None, None, None
            for _dict in date_data:
                if _dict.get("keyDate") == "yes":
                    keyDate = _dict.get("#text")
                if _dict.get("point") == "start":
                    startDate = _dict.get("#text")
                if _dict.get("point") == "end":
                    endDate = _dict.get("#text")
            if startDate and endDate:
                return "%s - %s" % (startDate, endDate)
            elif keyDate:
                return keyDate
            else:
                return None

        origin_infos = filter(None, iterify(getprop(self.provider_data,
                                                    "originInfo", True)))
        for origin_info in origin_infos:
            # Put aside date-related originInfo elements for later ...
            for field in date_fields:
                if field in origin_info:
                    date_origin_info.append(origin_info)
                    break
            # Map publisher
            if ("publisher" in origin_info and origin_info["publisher"] not in
                ret_dict["publisher"]):
                ret_dict["publisher"].append(origin_info["publisher"])
            # Map spatial
            if exists(origin_info, "place/placeTerm"):
                for place_term in iterify(getprop(origin_info,
                                                   "place/placeTerm")):
                    if isinstance(place_term, basestring):
                        pass
                    elif isinstance(place_term, dict):
                        place_term = place_term.get("#text")

                    if (place_term and place_term not in ret_dict["spatial"]):
                        ret_dict["spatial"].append(place_term)

        # Map dates. Only use the last date-related originInfo element
        try:
            last_date_origin_info = date_origin_info[-1]
            for field in date_fields:
                if field in last_date_origin_info:
                    s = datestring(last_date_origin_info[field])
                    if s and s not in ret_dict["date"]:
                        ret_dict["date"].append(s)
        except Exception as e:
            logger.info("Can not get date from %s" %
                        self.provider_data["_id"])

        for k in ret_dict.keys():
            if not ret_dict[k]:
                del ret_dict[k]

        self.update_source_resource(ret_dict)

    def map_data_provider(self):
        prop = "location"
        data_provider = None

        if exists(self.provider_data, prop):
            for v in iterify(getprop(self.provider_data, prop)):
                for p in iterify(v.get("physicalLocation", [])):
                    if (p.get("type") == "division" and
                        p.get("authority") != "marcorg"):
                        phys_location = p.get("#text", "")
                        while phys_location.endswith("."):
                            phys_location = phys_location[:-1]
                        if phys_location:
                             data_provider = phys_location.strip() + \
                                             ". The New York Public Library"

        if data_provider:
            self.mapped_data.update({"dataProvider": data_provider})

    def map_collection_and_relation(self):
        ret_dict = {"collection":  getprop(self.provider_data, "collection")}
        if exists(self.provider_data, "relatedItem"):
            related_items = iterify(getprop(self.provider_data, 
                                    "relatedItem"))
            # Map relation
            relation = filter(None, [getprop(item, "titleInfo/title", True) for
                                     item in related_items])
            if relation:
                relation.reverse()
                relation = ". ".join(relation).replace("..", ".")
                ret_dict["relation"] = relation

            # Map collection title
            host_types = [item for item in related_items if
                          item.get("type") == "host"]
            if host_types:
                title = getprop(host_types[-1], "titleInfo/title", True)
                if title:
                    ret_dict["collection"]["title"] = title

        self.update_source_resource(ret_dict)

    def map_multiple_fields(self):
        self.map_contributor_and_creator()
        self.map_date_publisher_and_spatial()
        self.map_collection_and_relation()
