import re
import datetime
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, setprop, delprop
from dplaingestion.selector import getprop
from collections import OrderedDict
from dplaingestion.mappers.mapper import Mapper
from dplaingestion.utilities import strip_unclosed_brackets

class MARCMapper(Mapper):                                                       

    def __init__(self, provider_data, key_prefix=None):
        super(MARCMapper, self).__init__(provider_data, key_prefix)

        # Fields controlfield, datafield, and leader may be nested within the
        # metadata/record field
        prop = "metadata/record"
        if exists(self.provider_data, prop):
            self.provider_data.update(getprop(self.provider_data, prop))
            delprop(self.provider_data, prop)

        self.control_001 = ""
        self.control_007_01 = ""
        self.control_008_18 = ""
        self.control_008_21 = ""
        self.control_008_28 = ""
        self.control_format_char = ""
        self.datafield_086_or_087 = False

        self.identifier_tag_labels = {
            "020": "ISBN:",
            "022": "ISSN:",
            "050": "LC call number:"
        }

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

            lambda t: (t != "538" and
                       t.startswith("5")):      [(self.map_description, "a")],

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
        }

        self.type_mapping = {
            "datafield": OrderedDict([
                ("AJ", ("Journal", "Text")),
                ("AN", ("Newspaper", "Text")),
                ("BI", ("Biography", "Text")),
                ("BK", ("Book", "Text")),
                ("CF", ("Computer File", "Interactive Resource")),
                ("CR", ("CDROM", "Interactive Resource")),
                ("CS", ("Software", "Software")),
                ("DI", ("Dictionaries", "Text")),
                ("DR", ("Directories", "Text")),
                ("EN", ("Encyclopedias", "Text")),
                ("HT", ("HathiTrust", None)),
                ("MN", ("Maps-Atlas", "Image")),
                ("MP", ("Map", "Image")),
                ("MS", ("Musical Score", "Text")),
                ("MU", ("Music", "Text")),
                ("MV", ("Archive", "Collection")),
                ("MW", ("Manuscript", "Text")),
                ("MX", ("Mixed Material", "Collection")),
                ("PP", ("Photograph/Pictorial Works", "Image")),
                ("RC", ("Audio CD", "Sound")),
                ("RL", ("Audio LP", "Sound")),
                ("RM", ("Music", "Sound")),
                ("RS", ("Spoken word", "Sound")),
                ("RU", (None, "Sound")),
                ("SE", ("Serial", "Text")),
                ("SX", ("Serial", "Text")),
                ("VB", ("Video (Blu-ray)", "Moving Image")),
                ("VD", ("Video (DVD)", "Moving Image")),
                ("VG", ("Video Games", "Moving Image")),
                ("VH", ("Video (VHS)", "Moving Image")),
                ("VL", ("Motion Picture", "Moving Image")),
                ("VM", ("Visual Material", "Image")),
                ("WM", ("Microform", "Text")),
                ("XC", ("Conference", "Text")),
                ("XS", ("Statistics", "Text"))
            ]),
            "leader": OrderedDict([
                ("am", ("Book", "Text")),
                ("asn", ("Newspapers", "Text")),
                ("as", ("Serial", "Text")),
                ("aa", ("Book", "Text")),
                ("a(?![mcs])", ("Serial", "Text")),
                ("[cd].*", ("Musical Score", "Text")),
                ("t.*", ("Manuscript", "Text")),
                ("[ef].*", ("Maps", "Image")),
                ("g.[st]", ("Photograph/Pictorial Works", "Image")),
                ("g.[cdfo]", ("Film/Video", "Moving Image")),
                ("g.*", (None, "Image")),
                ("k.*", ("Photograph/Pictorial Works", "Image")),
                ("i.*", ("Nonmusic", "Sound")),
                ("j.*", ("Music", "Sound")),
                ("r.*", (None, "Physical object")),
                ("p[cs].*", (None, "Collection")),
                ("m.*", (None, "Interactive Resource")),
                ("o.*", (None, "Collection"))
            ])
        }

    def extend_prop(self, prop, _dict, codes, label=None, values=None):
        if values is None:
            values = self._get_values(_dict, codes)

        if values:
            if label:
                values.insert(0, label)
            prop_value = self._get_mapped_value(prop)
            prop_value.extend(self._join_values(prop, values))
            setprop(self.mapped_data, prop, prop_value)

    def _get_mapped_value(self, prop):
        v = getprop(self.mapped_data, prop, True)
        if v is None:
            v = []
        else:
            v = iterify(v)

        return v

    def _join_values(self, prop, values):
        """Joins the values on a prop-specific delimiter"""
        join_props = (["sourceResource/subject"], ""), \
                     (["sourceResource/relation"], ". "), \
                     (["sourceResource/contributor",
                       "sourceResource/creator",
                       "sourceResource/publisher",
                       "sourceResource/extent",
                       "sourceResource/identifier"], " ")

        for prop_list, delim in join_props:
            if prop in prop_list:
                if delim == ". ":
                    # Remove any existing periods at end of values, except for
                    # last value
                    values = [re.sub("\.$", "", v) for v in values]
                    values[-1] += "."
                if values:
                    values = [delim.join(values)]

        # Remove any double periods (excluding those in ellipsis)
        values = [re.sub("(?<!\.)\.{2}(?!\.)", ".", v) for v in values]

        return values

    def _get_subfields(self, _dict):
        if "subfield" in _dict:
            for subfield in iterify(_dict["subfield"]):
                yield subfield
        else:
            return

    def _get_values(self, _dict, codes):
        """
        Extracts the appropriate "#text" values from _dict given a string of
        codes. If codes is None, all "#text" values are extracted. If codes
        starts with "!", codes are excluded.
        """
        values = []
        exclude = False

        if codes and codes.startswith("!"):
            exclude = True
            codes = codes[1:]

        for subfield in self._get_subfields(_dict):
            if not codes:
                pass
            elif not exclude and ("code" in subfield and subfield["code"] in
                                  codes):
                pass
            elif exclude and ("code" in subfield and subfield["code"] not in
                              codes):
                pass
            else:
                continue

            if "#text" in subfield:
                values.append(subfield["#text"])

        return values

    def _get_one_subfield(self, _dict, code):
        """Get one MARC subfield having the given code

        _dict: a dictionary of one element of the "datafield" list
        code:  one MARC subfield character code
        """
        try:
            subfields = [sf["#text"] for sf in self._get_subfields(_dict)
                         if sf["code"] == code]
            return subfields[0]  # assume there's just one
        except (KeyError, IndexError):
            return None

    def _get_subject_values(self, _dict, tag):
        """
        Extracts the "#text" values from _dict for the subject field and
        incrementally joins the values by the tag/code dependent delimiter
        """
        def _delimiters(tag, code):
            """
            Returns the appropriate delimiter(s) based on the tag and code
            """
            if tag == "658":
                if code == "b":
                    return [":"]
                elif code == "c":
                    return [" [", "]"]
                elif code == "d":
                    return ["--"]
            elif ((tag == "653") or
                  (int(tag) in range(690, 700)) or
                  (code == "b" and tag in ("654", "655")) or
                  (code in ("v", "x", "y", "z"))):
                return ["--"]
            elif code == "d":
                return [", "]

            return [". "]

        values = []
        for subfield in self._get_subfields(_dict):
            code = subfield.get("code", "")
            if not code or code.isdigit():
                # Skip codes that are numeric
                continue

            if "#text" in subfield:
                values.append(subfield["#text"].rstrip(", "))
                delimiters = _delimiters(tag, code)
                for delim in delimiters:
                    values = [delim.join(values)]
                    if delim != delimiters[-1]:
                        # Append an empty value for subsequent joins
                        values.append("")

        return values

    def _get_contributor_values(self, _dict, codes):
        """
        Extracts the appropriate "#text" values from _dict for the contributor
        field. If subfield "e" #text value is "aut" or "cre", the _dict is not
        used.
        """
        values = []
        for subfield in self._get_subfields(_dict):
            if not codes or ("code" in subfield and subfield["code"] in codes):
                if "#text" in subfield:
                    values.append(subfield["#text"])

            # Do not any _dict subfield values if the _dict contains #text of
            # "aut" or "cre" for code "e"
            if (subfield.get("code") == "e" and subfield.get("#text") in
                ("aut", "cre")):
                return []

        return values

    def map_is_shown_at(self, _dict, tag, codes):
        prop = "isShownAt"
        self.extend_prop(prop, _dict, codes)

    def map_language(self, _dict, tag, codes):
        def _extract_codes(values):
            """Splits the language values every third character"""
            language = []
            for lang_str in values:
                language.extend([lang_str[i:i+3] for i in
                                 range(0, len(lang_str), 3)])
            return language

        prop = "sourceResource/language"
        values = self._get_values(_dict, codes)
        if tag == "041":
          values = _extract_codes(values)
        self.extend_prop(prop, _dict, codes, values=values)

    def map_display_date(self, _dict, tag, codes):
        """Map what will be the displayDate to sourceResource/date.

        This will be further processed down the pipeline, or recreated as
        a dictionary by the Control Field 008 mapping.
        """
        date_given = self._get_one_subfield(_dict, codes) or ""
        semi_stripped = date_given.strip(";. ")
        date = strip_unclosed_brackets(semi_stripped)
        self.mapped_data["sourceResource"]["date"] = date

    def map_publisher(self, _dict, tag, codes):
        prop = "sourceResource/publisher"
        self.extend_prop(prop, _dict, codes)

    def map_extent(self, _dict, tag, codes):
        prop = "sourceResource/extent"
        self.extend_prop(prop, _dict, codes)

    def map_format(self, _dict, tag, codes):
        prop = "sourceResource/format"
        self.extend_prop(prop, _dict, codes)

    def map_identifier(self, _dict, tag, codes):
        prop = "sourceResource/identifier"
        label = self.identifier_tag_labels.get(tag)
        self.extend_prop(prop, _dict, codes, label)

    def map_creator(self, _dict, tag, codes):
        prop = "sourceResource/creator"
        self.extend_prop(prop, _dict, codes)

    def map_relation(self, _dict, tag, codes):
        prop = "sourceResource/relation"
        self.extend_prop(prop, _dict, codes)

    def map_description(self, _dict, tag, codes):
        prop = "sourceResource/description"
        self.extend_prop(prop, _dict, codes)

    def map_rights(self, _dict, tag, codes):
        prop = "sourceResource/rights"
        self.extend_prop(prop, _dict, codes)

    def map_temporal(self, _dict, tag, codes):
        prop = "sourceResource/temporal"
        self.extend_prop(prop, _dict, codes)

    def map_spatial(self, _dict, tag, codes):
        prop = "sourceResource/spatial"
        values = [re.sub("\.$", "", v) for v in
                  self._get_values(_dict, codes)]
        non_dupes = []
        [non_dupes.append(v) for v in values if v not in non_dupes]
        self.extend_prop(prop, _dict, codes, values=non_dupes)
    
    def map_subject(self, _dict, tag, codes):
        prop = "sourceResource/subject"
        values = self._get_subject_values(_dict, tag)
        self.extend_prop(prop, _dict, codes, values=values)

    def map_contributor(self, _dict, tag, codes):
        prop = "sourceResource/contributor"
        values = self._get_contributor_values(_dict, codes)
        self.extend_prop(prop, _dict, codes, values=values)
   
    def map_title(self, _dict, tag, index, codes):
        prop = "sourceResource/title"
        if not exists(self.mapped_data, prop):
            setprop(self.mapped_data, prop, [None, None, None])

        values = self._get_values(_dict, codes)
        if values:
            title = getprop(self.mapped_data, prop)
            t = title[index]
            if t:
                t.extend(values)
                t[0] = re.sub("\.$", "", t[0].strip())
                values = ["; ".join(t)]
            title[index] = values
            setprop(self.mapped_data, prop, title)

    def map_type_and_spec_type(self, _dict, tag, codes):
        ret_dict = {"type": None, "specType": None}
        for v in self._get_values(_dict, codes):
            if v in self.type_mapping["datafield"]:
                spec_type, _type = self.type_mapping["datafield"][v]
                if spec_type:
                    ret_dict["specType"] = spec_type
                if _type:
                    ret_dict["type"] = _type
                break

        self.update_source_resource(self.clean_dict(ret_dict))

    def update_title(self):
        prop = "sourceResource/title"
        title_list = filter(None, getprop(self.mapped_data, prop))
        if title_list:
            title = [" ".join(t) for t in title_list]
            setprop(self.mapped_data, prop, title)
        else:
            delprop(self.mapped_data, prop)

    def update_format(self):
        control = {
            "a": "Map",
            "c": "Electronic resource",
            "d": "Globe",
            "f": "Tactile material",
            "g": "Projected graphic",
            "h": "Microform",
            "k": "Nonprojected graphic",
            "m": "Motion picture",
            "o": "Kit",
            "q": "Notated music",
            "r": "Remote-sensing image",
            "s": "Sound recording",
            "t": "Text",
            "v": "Videorecording",
            "z": "Unspecified"
        }

        leader = {
            "a": "Language material",
            "c": "Notated music",
            "d": "Manuscript",
            "e": "Cartographic material",
            "f": "Manuscript cartographic material",
            "g": "Projected medium",
            "i": "Nonmusical sound recording"
        }

        format = []
        format.append(control.get(self.control_format_char))
        try:
            leader_format_char = self.provider_data.get("leader", "")[6]
            format.append(leader.get(leader_format_char))
        except:
            pass

        format = filter(None, format)
        if format:
            prop = "sourceResource/format"
            new_format = self._get_mapped_value(prop)
            new_format.extend(format)
            setprop(self.mapped_data, prop, new_format)

    def update_type_and_spec_type(self):
        """
        Overrides type and specType if values can be mapped using leader and
        controlfield, then adds "Government Document" to specType if
        applicable
        """
        _type = self._get_mapped_value("sourceResource/type")
        spec_type = self._get_mapped_value("sourceResource/specType")

        leader = getprop(self.provider_data, "leader")
        mapping_key = leader[6] + leader[7] + self.control_007_01 + \
                      self.control_008_21

        for key in self.type_mapping["leader"]:
            if re.match("^{0}".format(key), mapping_key):
                spec_type = iterify(self.type_mapping["leader"][key][0])
                _type = self.type_mapping["leader"][key][1]
                break

        if (self.control_008_28 in ("a", "c", "f", "i", "l", "m", "o", "s") or
            self.datafield_086_or_087):
            spec_type.append("Government Document")

        ret_dict = {}
        if _type:
            ret_dict["type"] = _type
        if spec_type:
            ret_dict["specType"] = spec_type

        self.update_source_resource(ret_dict)

    def update_is_shown_at(self):
        pass
        
    def add_identifier(self, value):
        pass

    def map_datafield_tags(self):
        for item in iterify(getprop(self.provider_data, "datafield")):
            for _dict in iterify(item):
                tag = _dict.get("tag", None)
                # Skip cases where there is no tag or where tag == "ERR"
                try:
                    int(tag)
                except:
                    continue

                if tag == "086" or tag == "087":
                    self.datafield_086_or_087 = True

                if tag == "264" and _dict.get("ind2") != "1":
                    continue

                for match, func_tuples in self.mapping_dict.items():
                    if match(tag):
                        for func_tuple in func_tuples:
                            if len(func_tuple) == 2:
                                func, codes = func_tuple
                                func(_dict, tag, codes)
                            elif len(func_tuple) == 3:
                                func, index, codes = func_tuple
                                func(_dict, tag, index, codes)

    ### MARC control field 008 date-parsing functions
    #   http://www.loc.gov/marc/archive/2000/concise/ecbd008s.html

    def cf8_multiple_dates(self, s):
        """Begin and end dates for MARC control field 008, Type of Date "m" """
        begin = s[7:11]
        end   = s[11:15]
        return (begin, end)

    def cf8_detailed_date(self, s):
        """Begin and end dates for MARC control field 008, Type of Date "e"

        Since this contains one date, begin and end are the same.
        """
        year  = s[7:11]
        month = s[11:13]
        day   = s[13:15]
        date  = "%s-%s-%s" % (year, month, day)
        return (date, date)

    def cf8_single_date(self, s):
        """Begin and end dates for MARC control field 008, Type of Date "s"

        Since this contains one date, begin and end are the same.
        """
        year = s[7:11]
        return (year, year)

    def cf8_reissue_date(self, s):
        """Begin and end dates for MARC control field 008, Type of Date "r"

        Reissue date contains date reissued, and original date, if known.
        Use the reissue date for both begin and end, because we're representing
        one date.
        """
        year = s[7:11]
        return (year, year)

    def cf8_pub_copy_date(self, s):
        """Begin and end dates for MARC control field 008, Type of Date "t"

        Publication and copyright date.  We only represent the publication
        date.
        """
        year = s[7:11]
        return (year, year)

    def cf8_serial_item_current(self, s):
        """Begin and end dates, MARC control field 008, type "c"

        Serial item in current publication
        """
        begin = s[7:11]
        # The MARC spec says the end year is supposed to be "9999", but I've
        # seen otherwise, and the current year looks better.  Since "9999" is
        # a bogus value, anyway, I'm using the current year.
        end   = str(datetime.datetime.today().year)
        return (begin, end)

    def cf8_serial_item_ceased_pub(self, s):
        """Begin and end dates, MARC control field 008, type "d"

        Serial item that has ceased publication
        """
        begin = s[7:11]
        end   = s[11:15]
        return (begin, end)

    def display_date_for_none_given(self, begin, end):
        """Construct a display date if none was given in subfield 260"""
        if begin != end:
            return "%s-%s" % (begin, end)
        else:
            return begin

    def set_begin_end_dates(self, begin, end):
        """Given begin and end, set sourceResource/date properties"""
        display_date = getprop(self.mapped_data, "sourceResource/date", True)
        if isinstance(display_date, dict):
            display_date = display_date.get("displayDate")
        date = {
                "displayDate": display_date or \
                               self.display_date_for_none_given(begin, end),
                "begin": begin,
                "end": end
               }
        setprop(self.mapped_data, "sourceResource/date", date)

    def map_controlfield_tags(self):
        date_func = {
                     "m": "cf8_multiple_dates",
                     "q": "cf8_multiple_dates",
                     "s": "cf8_single_date",
                     "e": "cf8_detailed_date",
                     "r": "cf8_reissue_date",
                     "t": "cf8_pub_copy_date",
                     "d": "cf8_serial_item_ceased_pub",
                     "c": "cf8_serial_item_current"
                    }
        for item in iterify(getprop(self.provider_data, "controlfield")):
            if "#text" in item and "tag" in item:
                if item["tag"] == "001":
                    self.control_001 = item["#text"]
                    self.add_identifier(item["#text"])
                elif item["tag"] == "007":
                    self.control_format_char = item["#text"][0]
                    try:
                        self.control_007_01 = item["#text"][1]
                    except:
                        pass
                elif item["tag"] == "008":
                    text = item["#text"]
                    type_of_date = text[6]
                    if type_of_date in date_func:
                        f = getattr(self, date_func[type_of_date])
                        (begin, end) = f(text)
                        self.set_begin_end_dates(begin, end)
                    if len(text) > 18:
                        self.control_008_18 = text[18]
                    if len(text) > 21:
                        self.control_008_21 = text[21]
                    if len(text) > 28:
                        self.control_008_28 = text[28]
                    if len(text) > 37:
                        prop = "sourceResource/language"
                        language = self._get_mapped_value(prop)
                        language.append(text[35:38])
                        setprop(self.mapped_data, prop, language)

    def update_mapped_fields(self):
        self.update_title()
        self.update_format()
        self.update_is_shown_at()
        self.update_type_and_spec_type()

    def map(self):
        self.map_base()
        self.map_datafield_tags()
        self.map_controlfield_tags()
        self.update_mapped_fields()
