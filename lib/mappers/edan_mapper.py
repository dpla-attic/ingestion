from dplaingestion.mappers.mapper import *
import dplaingestion.itemtype as itemtype

class NoTypeError(Exception):
    pass

class EDANMapper(Mapper):
    def __init__(self, data):
        super(EDANMapper, self).__init__(data)

        # Map of "format" or "physical description" substring to
        # sourceResource.type. This format field is considered first, and these
        # values should be as specific as possible, to avoid false assignments,
        # because this field is usually pretty free-form, unlike the type
        # fields.
        self.type_for_phys_keyword = [
            ('holiday card', 'image'),
            ('christmas card', 'image'),
            ('mail art', 'image'),
            ('postcard', 'image'),
        ]

        # Map of type-related substring to desired sourceResource.type.
        # For simple "if substr in str" matching. Place more specific
        # patterns higher up, before more general ones.
        # Items labeled as physical objects are "images of physical objects."
        self.type_for_ot_keyword = [
            ('photograph', 'image'),
            ('sample book', 'image'),
            ('book', 'text'),
            ('specimen', 'image'),
            ('electronic resource', 'interactive resource'),
            # Keep "textile" above "text"
            ('textile', 'image'),
            ('text', 'text'),
            ('frame', 'image'),
            ('costume', 'image'),
            ('object', 'image'),
            ('statue', 'image'),
            ('sculpture', 'image'),
            ('container', 'image'),
            ('jewelry', 'image'),
            ('furnishing', 'image'),
            ('furniture', 'image'),
            ('image', 'image'),
            ('drawing', 'image'),
            ('print', 'image'),
            ('painting', 'image'),
            ('illumination', 'image'),
            ('poster', 'image'),
            ('appliance', 'image'),
            ('tool', 'image'),
            ('electronic component', 'image'),
            ('publication', 'text'),
            ('magazine', 'text'),
            ('journal', 'text'),
            ('postcard', 'image'),
            ('correspondence', 'text'),
            ('writing', 'text'),
            ('manuscript', 'text'),
            # keep "equipment" above "audio" ("Audiovisual equipment")
            ('equipment', 'image'),
            ('cartographic', 'image'),
            ('notated music', 'image'),
            ('mixed material', 'image'),
            ('audio', 'sound'),
            ('sound recording', 'sound'),
            ('oral history recording', 'sound'),
            ('finding aid', 'collection'),
            ('online collection', 'collection'),
            ('online exhibit', 'interactive resource'),
            ('motion picture', 'moving image'),
            ('film', 'moving image'),
            ('video game', 'interactive resource'),
            ('video', 'moving image')
        ]

        self.creator_field_names = [
            "Architect",
            "Artist",
            "Artists/Makers",
            "Attributed to",
            "Author",
            "Cabinet Maker",
            "Ceramist",
            "Circle of",
            "Co-Designer",
            "Creator",
            "Decorator",
            "Designer",
            "Draftsman",
            "Editor",
            "Embroiderer",
            "Engraver",
            "Etcher",
            "Executor",
            "Follower of",
            "Graphic Designer",
            "Instrumentiste",
            "Inventor",
            "Landscape Architect",
            "Landscape Designer",
            "Maker",
            "Model Maker/maker",
            "Modeler",
            "Painter",
            "Photographer",
            "Possible attribution",
            "Possibly",
            "Possibly by",
            "Print Maker",
            "Printmaker",
            "Probably",
            "School of",
            "Sculptor",
            "Studio of",
            "Workshop of",
            "Weaver",
            "Writer",
            "animator",
            "architect",
            "artist",
            "artist.",
            "artist?",
            "artist attribution",
            "author",
            "author.",
            "author?",
            "authors?",
            "caricaturist",
            "cinematographer",
            "composer",
            "composer, lyricist",
            "composer; lyrcist",
            "composer; lyricist",
            "composer; performer",
            "composer; recording artist",
            "composer?",
            "creator",
            "creators",
            "designer",
            "developer",
            "director",
            "editor",
            "engraver",
            "ethnographer",
            "fabricator",
            "filmmaker",
            "filmmaker, anthropologist",
            "garden designer",
            "graphic artist",
            "illustrator",
            "inventor",
            "landscape Architect",
            "landscape architect",
            "landscape architect, photographer",
            "landscape designer",
            "lantern slide maker",
            "lithographer",
            "lyicist",
            "lyicrist",
            "lyricist",
            "lyricist; composer",
            "maker",
            "maker (possibly)",
            "maker or owner",
            "maker; inventor",
            "original artist",
            "performer",
            "performer; composer; lyricist",
            "performer; recording artist",
            "performers",
            "performing artist; recipient",
            "performing artist; user",
            "photgrapher",
            "photograher",
            "photographer",
            "photographer and copyright claimant",
            "photographer and/or colorist",
            "photographer or collector",
            "photographer?",
            "photographerl",
            "photographerphotographer",
            "photographers",
            "photographers?",
            "photographer}",
            "photographic firm",
            "photogrpaher",
            "playwright",
            "poet",
            "possible maker",
            "printer",
            "printmaker",
            "producer",
            "recordig artist",
            "recording artist",
            "recording artist; composer",
            "recordist",
            "recordng artist",
            "sculptor",
            "shipbuilder",
            "shipbuilders",
            "shipping firm",
            "weaver",
            "weaver or owner",
        ]

    def map_temporal(self):
        temporal = []
        for s in self.extract_xml_items("freetext", "date"):
            if "@label" in s and "#text" in s:
                temporal.append(s["#text"])

        if temporal:
            self.update_source_resource({"temporal": temporal})

    def map_description(self):
        description = [s for s in self.extract_xml_items("freetext", "notes",
                                                         "#text")]
        if description:
            self.update_source_resource({"description": description})
   
    def map_identifier(self):
        identifier = []
        labels = ("Catalog", "Accession")
        for s in self.extract_xml_items("freetext", "identifier"):
            if isinstance(s, dict):
                for label in labels:
                    if s.get("@label", "").startswith(label):
                        identifier.append(s.get("#text"))

        if identifier:
            self.update_source_resource({"identifier": identifier})

    def map_language(self):
        language = []
        for s in self.extract_xml_items("indexedStructured", "language"):
            try:
                language.append(s.replace(" language", ""))
            except:
                pass

        if language:
            self.update_source_resource({"language": language})

    def map_publisher(self):
        publisher = []
        for s in self.extract_xml_items("freetext", "publisher"):
            if isinstance(s, dict) and s.get("@label") == "Publisher":
                publisher.append(s["#text"])

        if publisher:
            self.update_source_resource({"publisher": publisher})

    def map_title(self):
        title = None
        labels = ("title", "Title", "Object Name", "Title (Spanish)")
        for s in self.extract_xml_items("descriptiveNonRepeating",
                                        "title"):
            if isinstance(s, dict) and s.get("@label") in labels:
                title = s.get("#text")

        if title:
            self.update_source_resource({"title": title})

    def map_rights(self):
        rights = []

        prop = "descriptiveNonRepeating/online_media/media"
        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "@rights" in s and s["@rights"] not in rights:
                    rights.append(s["@rights"])

        if not rights:
            for s in self.extract_xml_items("freetext", "creditLine"):
                if (isinstance(s, dict) and
                    s.get("@label") == "Credit Line" and
                    s.get("#text") not in rights):
                    rights.append(s.get("#text"))

            for s in self.extract_xml_items("freetext", "objectRights"):
                if (isinstance(s, dict) and
                    s.get("@label") == "Rights" and
                    s.get("#text") not in rights):
                    rights.append(s.get("#text"))

        rights = filter(None, rights)
        if rights:
            self.update_source_resource({"rights": rights})

    def map_subject(self):
        subject = []
        topic_labels = ("Topic", "subject", "event")
        for s in self.extract_xml_items("freetext", "topic"):
            if isinstance(s, dict) and s.get("@label") in topic_labels:
                subject.append(s["#text"])

        props = ("topic", "name", "culture", "tax_kingdom", "tax_phylum",
                 "tax_division", "tax_class", "tax_order", "tax_family",
                 "tax_sub-family", "scientific_name", "common_name",
                 "strat_group", "strat_formation", "strat_member")
        for prop in props:
            prop = "indexedStructured/" + prop
            if exists(self.provider_data, prop):
                for s in iterify(getprop(self.provider_data, prop)):
                    if isinstance(s, basestring):
                        subject.append(s)
                    elif isinstance(s, dict) and "#text" in s:
                        subject.append(s["#text"])

        subject = list(set(subject))
        if subject:
            self.update_source_resource({"subject": subject})

    def map_spatial(self):
        def _get_spatial(_dict):
            """
            Given a geolocation dictionary, extracts
            city/state/county/region/country/coordinates
            """
            tag_place_types = {
                "L5": ["city", ("City", "Town")],
                "L3": ["state", ("State", "Province")],
                "L4": ["county", ("County", "Island")],
                "L2": ["country", ("Country", "Nation")],
                "Other": ["region", ()],
                "points": ["coordinates", ("decimal", "degrees")]
            }

            spatial = {}
            for tag in _dict:
                if tag in tag_place_types:
                    place, types = tag_place_types[tag]
                else:
                    continue

                if tag == "points":
                    lat_type = getprop(_dict,
                                       tag + "/point/latitude/@type")
                    lon_type = getprop(_dict,
                                       tag + "/point/longitude/@type")
                    lat_value = getprop(_dict,
                                        tag + "/point/latitude/#text")
                    lon_value = getprop(_dict,
                                        tag + "/point/longitude/#text")
                    correct_type = (lat_type in types and lon_type in
                                    types)

                    value = ",".join(filter(None, [lat_value, lon_value]))
                else:
                    if tag == "Other":
                        correct_type = True
                    else:
                        try:
                            geo_type = getprop(_dict, tag + "/@type")
                            correct_type = geo_type in types
                        except:
                            continue

                    value = getprop(_dict, tag + "/#text")

                if correct_type and value:
                    spatial[place] = value

            return spatial

        spatial = []
        for s in self.extract_xml_items("indexedStructured", "geoLocation"):
            if isinstance(s, dict):
                spatial_dict = {}
                spatial_dict.update(_get_spatial(s))
                if spatial_dict:
                    spatial.append(spatial_dict)
        if not spatial:
            for s in self.extract_xml_items("freetext", "place"):
                if (isinstance(s, dict) and "#text" in s and
                    s["#text"] not in spatial):
                    spatial.append(s["#text"])

        if spatial:
            self.update_source_resource({"spatial": spatial})

    def map_type(self):
        """Get type from objectType or object_type element

        Specifically, look at freetext/objectType[label=Type] and
        indexedStructured/object_type.
        """
        object_type_strings = []
        phys_type_strings = []
        ot_ccase = self.extract_xml_items("freetext", "objectType")
        phys_desc = self.extract_xml_items("freetext",
                                           "physicalDescription")
        ot_uscore = self.extract_xml_items("indexedStructured",
                                           "object_type")
        for pd in phys_desc:
            pd_text = pd.get("#text", "").strip()
            if pd_text:
                phys_type_strings.append(pd_text.lower())
        for ot in ot_ccase:
            if ot.get("@label", "") == "Type":
                s = ot.get("#text", "").strip()
                if s:
                    object_type_strings.append(s.lower())
        for ot in ot_uscore:
            s = ot.strip()
            if s:
                object_type_strings.append(s.lower())
        try:
            new_type = itemtype.type_for_strings_and_mappings([
                (phys_type_strings, self.type_for_phys_keyword),
                (object_type_strings, self.type_for_ot_keyword)
                ])
        except itemtype.NoTypeError:
            id_for_msg = self.provider_data.get("_id", "[no _id]")
            logger.warning("Can not deduce type for item with _id: %s" %
                           id_for_msg)
            new_type = 'image'

        self.update_source_resource({"type": new_type})

    def map_is_shown_at(self):
        prop = "descriptiveNonRepeating/record_ID"
        if exists(self.provider_data, prop):
            prefix = "http://collections.si.edu/search/results.htm?" + \
                     "q=record_ID%%3A%s&repo=DPLA"
            obj = getprop(self.provider_data, prop)

            self.mapped_data.update({"isShownAt": prefix % obj})

    def map_object(self):
        prop = "descriptiveNonRepeating/online_media/media"
        if exists(self.provider_data, prop):
            obj = getprop(self.provider_data, prop)
            if isinstance(obj, list):
                obj = obj[0]

            if "@thumbnail" in obj:
                self.mapped_data.update({"object": obj["@thumbnail"]})

    def map_data_provider(self):
        dp = self.extract_xml_items("descriptiveNonRepeating", "data_source")
        if dp:
            self.mapped_data.update({"dataProvider": dp[0]})

    def map_extent_and_format(self):
        _dict = {}
        extent_labels = ("Dimensions")
        format_labels = ("Physical description", "Medium")
        for s in self.extract_xml_items("freetext", "physicalDescription"):
            if isinstance(s, dict):
                key = None
                label = s.get("@label")
                if label in extent_labels:
                    key = "extent"
                elif label in format_labels:
                    key = "format"

                if key and "#text" in s:
                    try:
                        _dict[key].append(s["#text"])
                    except:
                        _dict[key] = [s["#text"]]

        self.update_source_resource(_dict)

    def map_creator_and_contributor(self):
        _dict = {}
        for s in self.extract_xml_items("freetext", "name"):
            if isinstance(s, dict):
                key = None
                label = s.get("@label")
                if label in self.creator_field_names:
                    key = "creator"
                elif label == "associated person":
                    key = "contributor"

                if key and "#text" in s:
                    try:
                        _dict[key].append(s["#text"])
                    except:
                        _dict[key] = [s["#text"]]

        self.update_source_resource(_dict)

    def map_multiple_fields(self):
        self.map_extent_and_format()
        self.map_creator_and_contributor()
