from akara import logger
from dplaingestion.mappers.mods_mapper import MODSMapper
from dplaingestion.selector import exists, getprop
from dplaingestion.utilities import iterify


class NYPLMapper(MODSMapper):
    def __init__(self, provider_data):
        super(NYPLMapper, self).__init__(provider_data)

        self.contributor_roles = [
            "actor",
            "adapter",
            "addressee",
            "analyst",
            "animator",
            "annotator",
            "applicant",
            "arranger",
            "art director",
            "artistic director",
            "assignee",
            "associated name",
            "auctioneer",
            "author in quotations or text extracts",
            "author of afterword, colophon, etc.",
            "author of dialog",
            "author of introduction, etc.",
            "bibliographic antecedent",
            "blurbwriter",
            "book designer",
            "book producer",
            "bookseller",
            "censor",
            "cinematographer",
            "client",
            "collector",
            "collotyper",
            "colorist",
            "commentator",
            "commentator for written text",
            "compiler",
            "complainant",
            "complainant-appellant",
            "complainant-appellee",
            "compositor",
            "conceptor",
            "conductor",
            "conservator",
            "consultant",
            "consultant to a project",
            "contestant",
            "contestant-appellant",
            "contestant-appellee",
            "contestee",
            "contestee-appellant",
            "contestee-appellee",
            "contractor",
            "contributor",
            "copyright claimant",
            "copyright holder",
            "corrector",
            "curator",
            "curator of an exhibition",
            "dancer",
            "data contributor",
            "data manager",
            "dedicatee",
            "defendant",
            "defendant-appellant",
            "defendant-appellee",
            "degree granting institution",
            "degree grantor",
            "delineator",
            "depositor",
            "distributor",
            "donor",
            "draftsman",
            "editor",
            "editor of compilation",
            "editor of moving image work",
            "electrician",
            "electrotyper",
            "engineer",
            "expert",
            "field director",
            "film distributor",
            "film editor",
            "film producer",
            "first party",
            "former owner",
            "funder",
            "geographic information specialist",
            "honoree",
            "host",
            "illuminator",
            "inscriber",
            "instrumentalist",
            "issuing body",
            "judge",
            "laboratory",
            "laboratory director",
            "lead",
            "lender",
            "libelant",
            "libelant-appellant",
            "libelant-appellee",
            "libelee",
            "libelee-appellant",
            "libelee-appellee",
            "licensee",
            "licensor",
            "manufacturer",
            "marbler",
            "markup editor",
            "metadata contact",
            "metal-engraver",
            "moderator",
            "monitor",
            "music copyist",
            "musical director",
            "musician",
            "narrator",
            "opponent",
            "organizer",
            "originator",
            "other",
            "owner",
            "panelist",
            "patent applicant",
            "patent holder",
            "patron",
            "permitting agency",
            "plaintiff",
            "plaintiff-appellant",
            "plaintiff-appellee",
            "platemaker",
            "presenter",
            "printer",
            "printer of plates",
            "printmaker",
            "process contact",
            "producer",
            "production company",
            "production manager",
            "production personnel",
            "programmer",
            "project director",
            "proofreader",
            "publisher",
            "publishing director",
            "puppeteer",
            "radio producer",
            "recording engineer",
            "redaktor",
            "fenderer",
            "feporter",
            "research team head",
            "research team member",
            "researcher",
            "respondent",
            "respondent-appellant",
            "respondent-appellee",
            "responsible party",
            "restager",
            "reviewer",
            "rubricator",
            "scenarist",
            "scientific advisor",
            "screenwriter",
            "scribe",
            "second party",
            "secretary",
            "signer",
            "speaker",
            "sponsor",
            "stage director",
            "stage manager",
            "standards body",
            "stereotyper",
            "storyteller",
            "supporting host",
            "surveyor",
            "teacher",
            "technical director",
            "television director",
            "television producer",
            "thesis advisor",
            "transcriber",
            "translator",
            "type designer",
            "typographer",
            "videographer",
            "voice actor",
            "witness",
            "writer of accompanying material"
        ]

        self.creator_roles = [
            "architect",
            "art copyist",
            "artist",
            "attributed name",
            "author",
            "binder",
            "binding designer",
            "book jacket designer",
            "bookplate designer",
            "calligrapher",
            "cartographer",
            "choreographer",
            "composer",
            "correspondent",
            "costume designer",
            "cover designer",
            "creator",
            "dedicator",
            "designer",
            "director",
            "dissertant",
            "dubious author",
            "engraver",
            "etcher",
            "facsimilist",
            "film director",
            "forger",
            "illustrator",
            "interviewee",
            "interviewer",
            "inventor",
            "landscape architect",
            "librettist",
            "lighting designer",
            "lithographer",
            "lyricist",
            "papermaker",
            "performer",
            "photographer",
            "sculptor",
            "set designer",
            "singer",
            "sound designer",
            "wood engraver",
            "woodcutter"
            ]

    def txt(self, n):
        if not n:
            return ""
        elif type(n) == dict:
            return n.get("#text") or ""
        elif isinstance(n, basestring):
            return n
        else:
            return ""

    def map_title(self):
        prop = "titleInfo"
        if exists(self.provider_data, prop):
            primary = None
            alternates = []

            for title_info in iterify(getprop(self.provider_data, prop)):
                if "primary" == title_info.get("usage"):
                    primary = title_info.get("title")
                else:
                    alternates.append(title_info.get("title"))

            if primary:
                self.update_source_resource({"title": primary})
            else:
                logger.error(
                    "Error setting sourceResource.title for %s" %
                    self.provider_data["_id"]
                )

            if alternates:
                self.update_source_resource({"alternative": alternates})

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
                        identifier.append(self.txt(v))
                        break

            if identifier:
                self.update_source_resource({"identifier": identifier})

    def map_description(self):

        desc = []

        note_data = iterify(getprop(self.provider_data, "note", True))

        if note_data:
            for note in note_data:
                note_type = getprop(note, "type", True)
                if "content" == note_type:
                    desc.append(self.txt(note))

        abstracts = iterify(getprop(self.provider_data, "abstract", True))

        if abstracts:
            for abstract in abstracts:
                desc.append(self.txt(abstract))

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
        subjects = set()
        subject_keys = ["topic", "geographic", "temporal",
                        "name", "occupation", "titleInfo"]
        if exists(self.provider_data, "subject"):
            for v in iterify(getprop(self.provider_data, "subject")):
                for subject_key in subject_keys:
                    subject = self.extract_subject(v, subject_key)
                    if subject:
                        subjects.add(subject)

        if subjects:
            self.update_source_resource({"subject": list(subjects)})

    # helper function for map_subject
    def extract_subject(self, subject_info, key):
        if key in subject_info:
            subject_type_info = subject_info[key]
            if isinstance(subject_type_info, dict):
                if key == "name" and "namePart" in subject_type_info:
                    return subject_type_info.get("namePart")
                if key == "titleInfo" and "title" in subject_type_info:
                    return subject_type_info.get("title")
                return self.txt(subject_type_info)
            elif isinstance(subject_type_info, list):
                subject_texts = []
                for s in subject_type_info:
                    subject_texts.append(self.txt(s))
                return " -- ".join(subject_texts)
            elif isinstance(subject_type_info, basestring):
                return subject_type_info

    def map_temporal(self):
        temporals = set()
        if exists(self.provider_data, "subject"):
            for v in iterify(getprop(self.provider_data, "subject")):
                temporal = self.extract_subject(v, "temporal")
                if temporal:
                    temporals.add(temporal)
        if temporals:
            self.update_source_resource({"temporal": list(temporals)})

    def map_type(self):
        prop = "typeOfResource"

        if exists(self.provider_data, prop):
            self.update_source_resource({"type":
                                         getprop(self.provider_data, prop)})

    def map_format(self):
        formats = set()

        for physical_description in iterify(
                getprop(self.provider_data, "physicalDescription", True)):
            if exists(physical_description, "form"):
                for form in iterify(
                        getprop(physical_description, "form", True)):
                    format = self.txt(form)
                    if format:
                        formats.add(format)

        for genre_data in iterify(getprop(self.provider_data, "genre", True)):
            genre = self.txt(genre_data)
            if genre:
                formats.add(genre)

        if formats:
            self.update_source_resource({"format": list(formats)})

    def map_extent(self):
        extents = set()
        for physical_description in iterify(
                getprop(self.provider_data, "physicalDescription", True)):
            if exists(physical_description, "extent"):
                for extent in iterify(
                        getprop(physical_description, "extent", True)):
                    extents.add(extent)
        if extents:
            self.update_source_resource({"extent": list(extents)})

    def map_contributor_and_creator(self):
        prop = "name"
        if exists(self.provider_data, prop):
            ret_dict = {}
            creator = set()
            contributor = set()
            for v in iterify(getprop(self.provider_data, prop)):
                if isinstance(v, dict) and "role" in v:
                    for role in iterify(v.get("role", [])):
                        if isinstance(role, dict):
                            for role_term in iterify(role.get("roleTerm", [])):
                                rt = self.txt(role_term).lower().strip(" .")
                                if rt in self.creator_roles:
                                    creator.add(v["namePart"])
                                elif rt in self.contributor_roles:
                                    contributor.add(v["namePart"])

            if creator:
                ret_dict["creator"] = list(creator)
            cont = contributor - creator
            if cont:
                ret_dict["contributor"] = list(cont)

            self.update_source_resource(ret_dict)

    def map_date_publisher(self):
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
                return self.txt(date_data)
            elif type(date_data) == unicode:
                return date_data
            keyDate, startDate, endDate = None, None, None
            for _dict in date_data:
                if _dict.get("keyDate") == "yes":
                    keyDate = self.txt(_dict)
                if _dict.get("point") == "start":
                    startDate = self.txt(_dict)
                if _dict.get("point") == "end":
                    endDate = self.txt(_dict)
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
                ret_dict["publisher"].append(
                    self.txt(origin_info["publisher"]))
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
                        phys_location = self.txt(p)
                        while phys_location.endswith("."):
                            phys_location = phys_location[:-1]
                        if phys_location:
                            data_provider = phys_location.strip() + \
                                             ". The New York Public Library"

        if data_provider:
            self.mapped_data.update({"dataProvider": data_provider})

    # helper method for map_collection_and_relation that recurse through nested
    # relatedItems.
    def recurse_relations(self, node, relations, collection_titles):
        if exists(node, "relatedItem"):
            related_items = iterify(getprop(node, "relatedItem"))
            relation = filter(None, [getprop(item, "titleInfo/title", True)
                                     for item in related_items])
            if relation:
                relation.reverse()
                relation = ". ".join(relation).replace("..", ".")
                relations.append(relation)

            # Map collection title
            host_types = [item for item in related_items if
                          item.get("type") == "host"]
            if host_types:
                title = getprop(host_types[-1], "titleInfo/title", True)
                if title:
                    collection_titles.append(title)

            for related_item in related_items:
                self.recurse_relations(
                    related_item,
                    relations,
                    collection_titles
                )

    def map_collection_and_relation(self):
        ret_dict = {"collection":  getprop(self.provider_data, "collection")}
        collection_titles = []
        relations = []
        self.recurse_relations(
            self.provider_data, relations, collection_titles)
        if relations:
            ret_dict["relation"] = relations
        if collection_titles:
            ret_dict["collection"]["title"] = collection_titles[-1]

        self.update_source_resource(ret_dict)

    # defeating the default implementation of this in deference to map_rights
    # which handles more than it would normally
    def map_edm_rights(self):
        pass

    # catchall method for mapping rights since nypl will have 3 places to grab
    # rights info that are interdependent
    def map_rights(self):
        edm_rights_prop = "rightsStatementURI"
        tmp_rights_prop = "tmp_rights_statement"
        map_tmp = True

        if exists(self.provider_data, edm_rights_prop):
            self.mapped_data.update(
                {"rights": getprop(self.provider_data, edm_rights_prop)}
            )
            map_tmp = False

        if map_tmp and exists(self.provider_data, tmp_rights_prop):
            self.update_source_resource(
                {"rights": getprop(self.provider_data, tmp_rights_prop)}
            )

    def map_edm_has_type(self):
        prop = "genre"
        if exists(self.provider_data, prop):
            genre = self.txt(getprop(self.provider_data, prop))
            self.update_source_resource({"hasType": genre})

    def map_spatial(self):
        geographics_list = []
        # because of cardnality and xml -> json issues, this might be a dict
        # with k = type and value = subject, or it might just be a subject
        # so this works only for the former type.
        subjects = iterify(getprop(self.provider_data, "subject", True))
        for subject in subjects:
            geographic_info = getprop(subject, "geographic", True)
            if geographic_info:
                text = self.txt(geographic_info)
                if text:
                    geographics_list.append(text)

        geographics = list(set(geographics_list))
        if geographics:
            self.update_source_resource({"spatial": geographics})

    def map_is_part_of(self):
        pass

    def map_language(self):
        languages = set()
        for language_data in iterify(
                getprop(self.provider_data, "language", True)):
            for language_term in iterify(
                    getprop(language_data, "languageTerm", True)):
                language = self.txt(language_term)
                if language:
                    languages.add(language)
        if languages:
            self.update_source_resource({"language": list(languages)})

    def map_multiple_fields(self):
        self.map_contributor_and_creator()
        self.map_date_publisher()
        self.map_collection_and_relation()
        self.map_edm_has_type()
