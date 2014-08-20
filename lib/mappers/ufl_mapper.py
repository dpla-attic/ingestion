import re
from akara import logger
from dplaingestion.selector import setprop, getprop
from dplaingestion.mappers.marc_mapper import MARCMapper

class UFLMapper(MARCMapper):                                                       
    def __init__(self, provider_data):                                                   
        super(UFLMapper, self).__init__(provider_data)

        # Mapping dictionary for use with datafield
        # Keys are used to check if there is a tag match. If so, the value
        # provides a list of (property, code) tuples. In the case where certain
        # tags have prominence over others, an index is used and the tuples
        # will be of the form (property, index, code). To exclude a code,
        # prefix it with a "!": [("format", "!cd")] will exclude the "c"
        # "d" codes (see method _get_values).
        self.mapping_dict = {
            lambda t: t == "856":               [(self.map_is_shown_at, "u")],
            lambda t: t == "041":               [(self.map_language, "a")],
            lambda t: t == "260":               [(self.map_display_date, "c"),
                                                 (self.map_publisher, "ab")],

            lambda t: t == "300":               [(self.map_extent, "ac")],
            lambda t: t in ("337", "338"):      [(self.map_format, "a")],

            lambda t: t == "340":               [(self.map_extent, "b"),
                                                 (self.map_format, "a")],

            lambda t: t == "050":               [(self.map_identifier, "ab")],
            lambda t: t in ("020", "022",
                            "035"):             [(self.map_identifier, "a")],

            lambda t: t in ("100", "110",
                            "111"):             [(self.map_creator, None)],
            lambda t: (760 <= int(t) <= 787):   [(self.map_relation, None)],
            lambda t: (t.startswith("5") and
                       t not in
                       ("510", "533",
                        "535", "538")):         [(self.map_description, "a")],
            lambda t: t in ("506", "540"):      [(self.map_rights, None)],
            lambda t: t == "648":               [(self.map_temporal, None)],

            lambda t: t in ("700", "710",
                            "711", "720"):      [(self.map_contributor, None)],

            lambda t: t == "245":               [(self.map_title, 0, "!c")],
            lambda t: t == "242":               [(self.map_title, 1, None)],
            lambda t: t == "240":               [(self.map_title, 2, None)],
            lambda t: t == "651":               [(self.map_spatial, "a")],
            lambda t: (int(t) in
                       set([600, 630, 650, 651] +
                           range(610, 620) +
                           range(653, 659) +
                           range(690, 700))):   [(self.map_subject, None),
                                                 (self.map_format, "v"),
                                                 (self.map_temporal, "y"),
                                                 (self.map_spatial, "z")],
            lambda t: t == "752":               [(self.map_spatial, None)],
            lambda t: t == "830":           [(self.map_collection_title, "a")],
            lambda t: t == "535":           [(self.map_data_provider, "a")],
            lambda t: t == "992":           [(self.map_object, "a")],
        }

    def map_spatial(self, _dict, tag, codes):
        prop = "sourceResource/spatial"
        values = [re.sub("\.$", "", v) for v in
                  self._get_values(_dict, codes)]
        non_dupes = []
        [non_dupes.append(v) for v in values if v not in non_dupes]
        if tag == "752":
            non_dupes = "--".join(non_dupes)
        self.extend_prop(prop, _dict, codes, values=non_dupes)

    def map_collection_title(self, _dict, tag, codes):
        prop = "sourceResource/collection"
        values = self._get_values(_dict, codes)
        if values:
            collection = getprop(self.provider_data, "collection", True)
            if not collection:
                collection = {}
            collection["title"] = values
            if len(values) > 1:
                logger.error("More than one collection title for %s" %
                             self.provider_data["_id"])
            setprop(self.mapped_data, prop, collection)

    def map_data_provider(self, _dict, tag, codes):
        values = self._get_values(_dict, codes)
        if values:
            setprop(self.mapped_data, "dataProvider", values)

    def map_object(self, _dict, tag, codes):
        values = self._get_values(_dict, codes)
        if values:
            setprop(self.mapped_data, "object", values)

    def map_provider(self):
        setprop(self.mapped_data, "provider",
                "University of Florida Libraries")

    def map(self):
        super(UFLMapper, self).map()
        self.map_provider()
