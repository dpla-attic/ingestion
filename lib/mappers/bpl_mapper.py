from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper

class BPLMapper(OAIMODSMapper):
    def __init__(self, provider_data):
        super(BPLMapper, self).__init__(provider_data)

    def map_date_and_publisher(self):
        prop = self.root_key + "originInfo"

        if exists(self.provider_data, prop):
            origin_info = getprop(self.provider_data, prop)

            date = []
            for key in ["dateCreated", "dateIssued", "dateOther",
                        "copyrightDate"]:
                _dict = getprop(origin_info, key, True)
                if _dict is not None:
                    start_date = None
                    end_date = None
                    for item in iterify(_dict):
                        if isinstance(item, basestring):
                            date.append(item)
                        elif item.get("encoding") == "w3cdtf":
                            text = item.get("#text")
                            if item.get("point") == "start":
                                start_date = text
                            elif item.get("point") == "end":
                                end_date = text
                            else:
                                date.append(text)
                    if start_date is not None and end_date is not None:
                        date.append("%s-%s" %(start_date, end_date))

            publisher = []
            if "publisher" in origin_info:
                publisher.append(origin_info["publisher"])
            if "place" in origin_info:
                for p in iterify(origin_info["place"]):
                    if getprop(p, "placeTerm/type", True) == "text":
                        publisher.append(getprop(p, "placeTerm/#text"))

            date_and_publisher = {}
            if date:
                date_and_publisher["date"] = date
            if publisher:
                date_and_publisher["publisher"] = publisher

            if date_and_publisher:
                self.update_source_resource(date_and_publisher)


    def map_is_shown_at_has_view_object_and_data_provider(self):
        def _get_media_type():
            pd = iterify(getprop(self.provider_data,
                         self.root_key + "physicalDescription", True))
            for _dict in filter(None, pd):
                try:
                    return getprop(_dict, "internetMediaType", True)
                except KeyError:
                    pass

            return None

        location = iterify(getprop(self.provider_data,
                                   self.root_key + "location", True))
        format = _get_media_type()
        phys_location = None
        out = {}
        try:
            for _dict in location:
                if "url" in _dict:
                    for url_dict in iterify(_dict["url"]):
                        if url_dict and "access" in url_dict:
                            if url_dict["access"] == "object in context" and \
                               url_dict.get("usage") == "primary":
                                out["isShownAt"] = url_dict.get("#text")
                            elif url_dict["access"] == "preview":
                                out["object"] = url_dict.get("#text")
                            elif url_dict["access"] == "raw object":
                                out["hasView"] = {"@id": url_dict.get("#text"),
                                                  "format": format}
                if phys_location is None:
                    phys_location = getprop(_dict, "physicalLocation", True)
            if phys_location is not None:
                out["dataProvider"] = phys_location
        except Exception as e:
            logger.error(e)

        if out:
            self.mapped_data.update(out)

    def map_type(self):
        prop = self.root_key + "typeOfResource"

        if exists(self.provider_data, prop):
            self.update_source_resource({"type":
                                         getprop(self.provider_data, prop)})

    def map_extent(self):
        prop = self.root_key + "physicalDescription/extent"

        if exists(self.provider_data, prop):
            self.update_source_resource({"extent":
                                         getprop(self.provider_data, prop)})

    def map_description(self):
        props = (self.root_key + "abstract", self.root_key + "note")

        desc = []
        for desc_prop in props:
            if exists(self.provider_data, desc_prop):
                for s in iterify(getprop(self.provider_data, desc_prop)):
                    if isinstance(s, dict):
                        desc.append(s.get("#text"))
                    else:
                        desc.append(s)
        desc = filter(None, desc)

        if desc:
            self.update_source_resource({"description": desc})

    def map_format(self):
        return super(BPLMapper, self).map_format(authority_condition=False)

    def map_title(self):
        return super(BPLMapper, self).map_title(
                    unsupported_types=["alternative"],
                    unsupported_subelements=["partNumber", "partName",
                                             "nonSort"]
                    )

    def map_identifier(self):
        prop = self.root_key + "identifier"

        if exists(self.provider_data, prop):
            identifier = []
            types = ["local-accession", "local-other", "local-call",
                     "local-barcode", "isbn", "ismn", "isrc", "issn",
                     "issue-number", "lccn", "matrix-number", "music-plate",
                     "music-publisher", "sici", "videorecording-identifier"]
            for item in iterify(getprop(self.provider_data, prop)):
                type = item.get("type")
                if type in types:
                    type = " ".join(type.split("-"))
                    identifier.append("%s: %s" % (type.capitalize(),
                                                  item.get("#text")))

            if identifier:
                self.update_source_resource({"identifier": identifier})

    def map_language(self):
        prop = self.root_key + "language/languageTerm"

        if exists(self.provider_data, prop):
            language = []
            authority_uri = "http://id.loc.gov/vocabulary/iso639-2"
            for s in iterify(getprop(self.provider_data, prop)):
                if (s.get("type") == "text" and
                    s.get("authority") == "iso639-2b" and
                    s.get("authorityURI") == authority_uri):
                    language.append(s.get("#text"))

            if language:
                self.update_source_resource({"language": language})

    def map_relation(self):
        prop = self.root_key + "relatedItem"

        if exists(self.provider_data, prop):
            relation = []
            host = None
            series = None
            for s in iterify(getprop(self.provider_data, prop)):
                title = getprop(s, "titleInfo/title", True)
                if title is not None:
                    if s.get("type") == "host":
                        host = title
                    if s.get("type") == "series":
                        series = title

                if host:
                    val = host
                    if series:
                        val += ". " + series
                    relation.append(val)

            relation = relation[0] if len(relation) == 1 else relation

            if relation:
                self.update_source_resource({"relation": relation})

    def map_rights(self):
        prop = self.root_key + "accessCondition"

        if exists(self.provider_data, prop):
            rights = []
            for s in iterify(getprop(self.provider_data, prop)):
                rights.append(s.get("#text"))

            rights = ". ".join(filter(None, rights)).replace("..", ".")

            if rights:
                self.update_source_resource({"rights": rights})

    def update_relation(self):
        # Join dataProvider with relation
        try:
            relation = getprop(self.mapped_data, "dataProvider") + ". " + \
                       getprop(self.mapped_data, "sourceResource/relation")
            self.update_source_resource({"relation":
                                          relation.replace("..", ".").strip()})
        except:
            pass

    def map_multiple_fields(self):
        self.map_creator_and_contributor()
        self.map_subject_spatial_and_temporal(geographic_subject=False)
        self.map_date_and_publisher()
        self.map_is_shown_at_has_view_object_and_data_provider()

    def update_mapped_fields(self):
        self.update_relation()
