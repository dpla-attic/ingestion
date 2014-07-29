import re
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.marc_mapper import MARCMapper

class GPOMapper(MARCMapper):
    def __init__(self, provider_data, key_prefix="marc:"):
        super(GPOMapper, self).__init__(provider_data, key_prefix)

        self.exclude_rights = [
            "Access may require a library card.",

            "Access restricted to U.S. military service members and Dept. " + \
            "of Defense employees; the only issues available are those " + \
            "from Monday of the prior week through Friday of current week.",

            "Access to data provided for a fee except for requests " + \
            "generated from Internet domains of .gov, .edu, .k12, .us, " + \
            "and .mil.",

            "Access to issues published prior to 2001 restricted to " + \
            "Picatinny Arsenal users.",

            "Access to some volumes or items may be restricted",
            "Document removed from the GPO Permanent Access Archive at " + \
            "agency request",

            "FAA employees only.",
            "First nine issues (Apr.-Dec. 2002) were law enforcement " + \
            "restricted publications and are not available to the general " + \
            "public.",

            "Free to users at U.S. Federal depository libraries; other " + \
            "users are required to pay a fee.",

            "Full text available to subscribers only.",
            "Login and password required to access web page where " + \
            "electronic files may be downloaded.",

            "Login and password required to access web page where " + \
            "electronic formats may be downloaded.",

            "Not available for external use as of Monday, Oct. 20, 2003.",
            "Personal registration and/or payment required to access some " + \
            "features.",

            "Restricted access for security reasons",
            "Restricted to Federal depository libraries and other users " + \
            "with valid user accounts.",

            "Restricted to federal depository libraries with valid user " + \
            "IDs and passwords.",

            "Restricted to institutions with a site license to the USA " + \
            "trade online database. Free to users at federal depository " + \
            "libraries.",

            "Some components of this directory may not be publicly " + \
            "accessible.",

            "Some v. are for official use only, i.e. distribution of Oct. " + \
            "1998 v. 2 is restricted.",

            "Special issue for Oct./Dec. 2007 for official use only.",
            "Subscription required for access."
        ]

        self.leader = getprop(self.provider_data, "leader")
        self.date = {
            "260": [],
            "264": [],
            "362": []
        }
        self.description = {
            "310": [],
            "5xx": [],
            "583": []
        }
        self.is_shown_at = {
            "856u": [],
            "856z3": []
        }

        # Mapping dictionary for use with datafield
        # Keys are used to check if there is a tag match. If so, the value
        # provides a list of (property, code) tuples. In the case where certain
        # tags have prominence over others, an index is used and the tuples
        # will be of the form (property, index, code). To exclude a code,
        # prefix it with a "!": [("format", "!cd")] will exclude the "c"
        # "d" codes (see method _get_values).
        self.mapping_dict = {
            lambda t: t in ("700", "710",
                            "711"):         [(self.map_contributor, None)],
            lambda t: t in ("100", "110",
                            "111"):         [(self.map_creator, None)],
            lambda t: t in ("260", "264"):  [(self.map_date, "c"),
                                             (self.map_publisher, "ab")],
            lambda t: t == "362":           [(self.map_date, None)],
            lambda t: t == "300":           [(self.map_extent, "a")],
            lambda t: t in ("001", "020",
                            "022"):         [(self.map_identifier, None)],
            lambda t: t in ("035", "050",
                            "074", "082",
                            "086"):         [(self.map_identifier, "a")],
            lambda t: t == "506":           [(self.map_rights, None)],
            lambda t: t in ("600", "610",
                            "611", "630",
                            "650", "651"):  [(self.map_subject, None)],
            lambda t: t in ("600", "610",
                            "650", "651"):  [(self.map_temporal, "y")],
            lambda t: t == "611":           [(self.map_temporal, "d")],
            lambda t: t in ("255", "310"):  [(self.map_description, None)],
            lambda t: t == "583":           [(self.map_description, "z")],
            lambda t: (int(t) in
                       (range(500, 538) +
                        range(539, 583) +
                        range(584, 600))):  [(self.map_description, None)],
            lambda t: t in ("337", "338",
                            "340"):         [(self.map_format, "a")],
            lambda t: t in ("041", "546"):  [(self.map_language, None)],
            lambda t: t == "650":           [(self.map_spatial, "z")],
            lambda t: t == "651":           [(self.map_spatial, "a")],
            lambda t: (int(t) in
                       (range(760, 787) +
                        ["490", "730",
                         "740", "830"])):   [(self.map_relation, None)],
            lambda t: t == "337":           [(self.map_type, "a")],
            lambda t: t == "655":           [(self.map_type, None)],
            lambda t: t == "245":           [(self.map_title, None)],
        }

        self.desc_frequency = {
            "a": "Annual",
            "b": "Bimonthly",
            "c": "Semiweekly",
            "d": "Daily",
            "e": "Biweekly",
            "f": "Semiannual",
            "g": "Biennial",
            "h": "Triennial",
            "i": "Three times a week",
            "j": "Three times a month",
            "k": "Continuously updated",
            "m": "Monthly",
            "q": "Quarterly",
            "s": "Semimonthly",
            "t": "Three times a year",
            "u": "Unknown",
            "w": "Weekly",
            "z": "Other"
        }

    def _get_subfield_e(self, _dict):
        """
        Returns the '#text' value for subfield with code 'e', if it exists
        """
        for subfield in self._get_subfields(_dict):
            if ("code" in subfield and "#text" in subfield and
                subfield["code"] == "e"):
                return subfield["#text"]
        return None

    def map_contributor(self, _dict, tag, codes):
        prop = "sourceResource/contributor"
        values = self._get_values(_dict, codes)
        subfield_e = self._get_subfield_e(_dict)
        # Use values only if subfield 'e' exists and its value is not
        # 'author'
        if subfield_e and subfield_e != "author":
            self.extend_prop(prop, _dict, codes, values=values)

    def map_creator(self, _dict, tag, codes):
        prop = "sourceResource/creator"
        values = self._get_values(_dict, codes)
        if tag in ("700", "710", "711"):
            subfield_e = self._get_subfield_e(_dict)
            # Use values only if subfield 'e' exists and its value is 'author'
            if not subfield_e or subfield_e != "author":
                return
        self.extend_prop(prop, _dict, codes, values=values)

    def map_date(self, _dict, tag, codes):
        values = self._get_values(_dict, codes)
        self.date[tag].extend(values)

    def map_description(self, _dict, tag, codes):
        values = self._get_values(_dict, codes)
        if tag in ("310", "583"):
            self.description[tag].extend(values)
        else:
            self.description["5xx"].extend(values)

    def map_type(self, _dict, tag, codes):
        prop = "sourceResource/type"
        values = self._get_contributor_values(_dict, codes)
        self.extend_prop(prop, _dict, codes, values=values)

    def map_title(self, _dict, tag, codes):
        prop = "sourceResource/title"
        values = self._get_contributor_values(_dict, codes)
        self.extend_prop(prop, _dict, codes, values=values)

    def map_extent(self, _dict, tag, codes):
        prop = "sourceResource/extent"
        values = [re.sub(":$", "", v) for v in self._get_values(_dict, codes)]
        self.extend_prop(prop, _dict, codes, values=values)

    def update_title(self):
        prop = "sourceResource/title"
        title = self._get_mapped_value(prop)
        if title:
            self.update_source_resource({"title": " ".join(title)})
  
    def update_is_shown_at(self):
        uri = "http://catalog.gpo.gov/F/?func=direct&doc_number=%s"
        if self.control_001:
            self.mapped_data.update({"isShownAt": uri % self.control_001})

    def update_date(self):
        date = None
        if self.date["362"]:
            date = self.date["362"]
        elif self.leader[7] == "m":
            if self.date["260"]:
                date = self.date["260"]
            elif self.date["264"]:
                date = self.date["264"]
        if date:
            self.update_source_resource({"date": date})

    def update_description(self):
        description = []
        if (not self.description["310"] and self.leader[7] == "s" and
            self.control_008_18 in self.desc_frequency):
            description.append(self.desc_frequency.get(self.control_008_18))
        for values in self.description.values():
            for v in iterify(values):
                if v not in description:
                    description.append(v)
        description = filter(None, description)
        if description:
            self.update_source_resource({"description": description})

    def update_rights(self):
        rights = self._get_mapped_value("sourceResource/rights")
        if not rights:
            r = "Pursuant to Title 17 Section 105 of the United States " + \
                "Code, this file is not subject to copyright protection " + \
                "and is in the public domain. For more information " + \
                "please see http://www.gpo.gov/help/index.html#" + \
                "public_domain_copyright_notice.htm"
            self.update_source_resource({"rights": r})

    def remove_record_if_excluded_rights(self):
        rights = self._get_mapped_value("sourceResource/rights")
        if any(right in self.excluded_rights for right in rights):
            logger.debug("Rights value excluded. Unsetting _id %s" %
                         self.mapped_data["_id"])
            del self.mapped_data["_id"]

    def update_mapped_fields(self):
        super(GPOMapper, self).update_mapped_fields()
        self.update_date()
        self.update_description()
        self.update_rights()

    def map(self):
        self.map_base()
        self.map_provider()
        self.map_datafield_tags()
        self.map_controlfield_tags()
        self.update_mapped_fields()
        self.remove_record_if_excluded_rights()
