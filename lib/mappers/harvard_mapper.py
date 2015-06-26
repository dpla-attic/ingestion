from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper

class HarvardMapper(OAIMODSMapper):
    def __init__(self, provider_data):
        self.set_to_data_provider = {
            "lap": "Widener Library, Harvard University",
            "crimes": "Harvard Law School Library, Harvard University",
            "scarlet": "Harvard Law School Library, Harvard University",
            "manuscripts": "Houghton Library, Harvard University",
            "eda": "Emily Dickenson Archive",
            "cna": "Harvard University Archives"
        }
        super(HarvardMapper, self).__init__(provider_data)

    def map_date_and_publisher(self):
        prop = self.root_key + "originInfo"

        if exists(self.provider_data, prop):
            origin_info = getprop(self.provider_data, prop)
            date_and_publisher = {}

            date = None
            if "dateCreated" in origin_info:
                date = origin_info["dateCreated"]
            if not date and \
                getprop(origin_info, "dateOther/keyDate", True) == "yes":
                date = getprop(origin_info, "dateOther/#text", True)

            if isinstance(date, list):
                dd = {}
                for i in date:
                    if is_display_date(i):
                        dd["displayDate"] = display_date(i)
                    elif "point" in i:
                        if i["point"] == "start":
                            dd["begin"] = i["#text"]
                        else:
                            dd["end"] = i["#text"]
                    else:
                        # Odd date? Log error and investigate
                        logger.error("Invalid date in record %s" %
                                     self.provider_data["_id"])
                date = dd if dd else None
            else:
                date = display_date(date)

            if date and date != "unknown":
                date_and_publisher["date"] = date
            
            if "publisher" in origin_info:
                publisher = []
                pub = origin_info["publisher"]

                di = origin_info.get("dateIssued", None)
                di = di[0] if isinstance(di, list) else di

                # Get all placeTerms of type "text"
                terms = []
                if "place" in origin_info:
                    for p in iterify(origin_info["place"]):
                        if getprop(p, "placeTerm/type", True) == "text":
                            terms.append(getprop(p, "placeTerm/#text", True))

                for t in filter(None, terms):
                    if di:
                        publisher.append("%s: %s, %s" % (t, pub, di))
                    else:
                        publisher.append("%s: %s" % (t, pub))
                if len(publisher) == 1:
                    publisher = publisher[0]

                if publisher:
                    date_and_publisher["publisher"] = publisher

            if date_and_publisher:
                self.update_source_resource(date_and_publisher)

    def map_is_shown_at_has_view_and_object(self):
        prop = self.root_key + "location"
        if exists(self.provider_data, prop):
            iho = {}
            location = getprop(self.provider_data, prop)
            for loc in iterify(location):
                urls = getprop(loc, "url", True)
                if urls:
                    for url in iterify(urls):
                        if isinstance(url, dict):
                            usage = getprop(url, "usage", True)
                            if usage == "primary display":
                                # Account for bad 'url' elements that are
                                # not URLs.  Seen in Emily Dickenson collection
                                # ('eda' set).
                                # E.g. item b1b694dc65aeb685e7e4b5199bf8dd8f
                                s = getprop(url, "#text")
                                if s.startswith('http://'):
                                    iho["isShownAt"] = s

                            # Most sets use the 'displayLabel' attribute to
                            # indicate thumbnails and full images, but the
                            # Colonial North America set uses 'access'.
                            label = getprop(url, "displayLabel", True)
                            access = getprop(url, "access", True)
                            # ... being paranoid about the potential for the
                            # coexistence of 'displayLabel' and 'access' ...
                            if label:
                                if label == "Full Image":
                                    iho["hasView"] = {"@id": getprop(url,
                                                                     "#text")}
                                if label == "Thumbnail":
                                    iho["object"] = getprop(url, "#text")
                            elif access:
                                if access == "preview":
                                    iho["object"] = getprop(url, "#text")

            if iho:
                self.mapped_data.update(iho)

    def map_data_provider(self):
        dp = None
        set_spec = getprop(self.provider_data, "header/setSpec", True)
        location = getprop(self.provider_data, self.root_key + "location",
                           True)
        if set_spec == "dag" and location is not None:
            for loc in iterify(location):
                phys = getprop(loc, "physicalLocation", True)
                if phys and is_repository(phys):
                    dp = getprop(phys, "#text").split(";")[0] \
                         + ", Harvard University"

        if set_spec in self.set_to_data_provider:
            dp = self.set_to_data_provider[set_spec]

        if dp is not None:
            self.mapped_data.update({"dataProvider": dp})

    def map_rights(self):
        set_spec = getprop(self.provider_data, "header/setSpec", True)
        if set_spec == 'eda':
            rights = 'CC BY-NC-ND 3.0 http://www.edickenson.org/terms'
        elif set_spec == 'cna':
            rights = getprop(self.provider_data,
                             self.root_key + 'accessCondition', True)
        else:
            rights = 'Held in the collections of Harvard University.'
        self.update_source_resource({'rights': rights})

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
        prop = self.root_key + "note"
        out_desc = []
        for desc in iterify(getprop(self.provider_data, prop, True)):
            if isinstance(desc, dict):
                desc = desc["#text"] if "#text" in desc else None
            if desc:
                out_desc.append(desc)

        if out_desc:
            self.update_source_resource({"description": out_desc})

    def map_format(self):
        return super(HarvardMapper, self).map_format(authority_condition=True)

    def map_title(self):
        return super(HarvardMapper, self).map_title(
                    unsupported_types=["alternative"],
                    unsupported_subelements=[]
                    )

    def map_identifier(self):
        id = []
        obj = getprop(self.provider_data, self.root_key + "identifier", True)
        if obj and "type" in obj and obj["type"] == "Object Number":
            id.append(obj["#text"])

        record_id = getprop(self.provider_data,
                            self.root_key + "recordInfo/recordIdentifier",
                            True)
        if (record_id and "source" in record_id and
            record_id["source"] == "VIA"):
            id.append(record_id["#text"])

        id = filter(None, id)

        if id:
            self.update_source_resource({"identifier" : "; ".join(id)})

    def map_language(self):
        prop = self.root_key + "language/languageTerm"

        if exists(self.provider_data, prop):
            language = []
            for s in iterify(getprop(self.provider_data, prop)):
                if "#text" in s and s["#text"] not in language:
                    language.append(s["#text"])

            if language:
                self.update_source_resource({"language": language})

    def map_relation(self):
        prop = self.root_key + "relatedItem"

        if exists(self.provider_data, prop):
            relation = []
            for el in iterify(getprop(self.provider_data, prop)):
                if is_relation(el):
                    relation.append(getprop(el, "titleInfo/title", True))

            relation = filter(None, relation)
            relation = relation[0] if len(relation) == 1 else relation

            if relation:
                self.update_source_resource({"relation": relation})

    def map_multiple_fields(self):
        self.map_creator_and_contributor()
        self.map_subject_spatial_and_temporal()
        self.map_date_and_publisher()
        self.map_is_shown_at_has_view_and_object()


def is_display_date(element):
    return isinstance(element, basestring) or 'qualifier' in element

def is_relation(element):
    return (element.get('type') == 'series') or \
           (element.get('type') == 'host' and \
            element.get('displayLabel') == 'collection')

def display_date(element):
    """Return a display date string, given a suitable object or string"""
    # Accept either a `getprop` hash for an XML element with only a text node,
    # or an element with a 'qualifier' attribute.
    try:
        if isinstance(element, basestring):
            date = element
        elif 'qualifier' in element:
            date = element['#text']
            if element['qualifier'] == 'questionable':
                date = 'ca. ' + date
    except (KeyError, TypeError):  # shouldn't happen, but be paranoid.
        date = None
    return date

def is_repository(element):
    s = getprop(element, 'displayLabel', True) \
            or getprop(element, 'type', True)
    return s == 'repository'
