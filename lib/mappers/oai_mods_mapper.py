import re
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mapper import Mapper

class OAIMODSMapper(Mapper):
    def __init__(self, provider_data, key_prefix="mods:"):
        super(OAIMODSMapper, self).__init__(provider_data, key_prefix)
        if exists(provider_data, "metadata/mods"):
            self.root_key = "metadata/mods/"
        else:
            self.root_key = ""

    # Common methods
    def name_from_name_part(self, name_part):
        type_exceptions = ("affiliation", "displayForm", "description",
                           "role")
        name = []
        for n in iterify(name_part):
            if isinstance(n, basestring):
                name.append(n)
            elif isinstance(n, dict) and "#text" in n:
                if not (set(n["type"]) & set(type_exceptions)):
                    name.append(n["#text"])

        return ", ".join(name)

    def map_creator_and_contributor(self):
        prop = self.root_key + "name"

        if exists(self.provider_data, prop):
            creator_and_contributor = {}
            names = []
            for s in iterify(getprop(self.provider_data, prop)):
                name = {}
                name["name"] = self.name_from_name_part(
                                getprop(s, "namePart", True)
                                )
                if name["name"]:
                    name["type"] = getprop(s, "type", True)
                    name["roles"] = []
                    if "role" in s:
                        roles = getprop(s, "role")
                        for r in iterify(roles):
                            role = r["roleTerm"]
                            if isinstance(role, dict):
                                role = role["#text"]
                            name["roles"].append(role)

                    names.append(name)

            # Set creator
            creator = [name for name in names if "creator" in name["roles"]]
            creator = creator[0] if creator else names[0]
            names.remove(creator)
            creator_and_contributor["creator"] = creator["name"]

            # Set contributor
            contributor = [name["name"] for name in names]
            if contributor:
                creator_and_contributor["contributor"] = contributor

            self.update_source_resource(creator_and_contributor)

    def map_subject_spatial_and_temporal(self, geographic_subject=True):
        prop = self.root_key + "subject"

        if exists(self.provider_data, prop):
            ret_dict = {
                "subject": [],
                "spatial": [],
                "temporal": []
            }
            for s in iterify(getprop(self.provider_data, prop)):
                subject = []
                if "name" in s:
                    namepart = getprop(s, "name/namePart", True)
                    name = self.name_from_name_part(namepart)
                    if name and name not in subject:
                        subject.append(name)

                if "topic" in s:
                    for t in iterify(s["topic"]):
                        if t and t not in subject:
                            subject.append(t)

                if "geographic" in s:
                    for g in iterify(s["geographic"]):
                        if g:
                            if geographic_subject and g not in subject:
                                subject.append(g)
                            if g not in ret_dict["spatial"]:
                                ret_dict["spatial"].append(g)

                if "hierarchicalGeographic" in s:
                    for h in iterify(s["hierarchicalGeographic"]):
                        if isinstance(h, dict):
                            for k in h.keys():
                                if k not in ["city", "county", "state",
                                             "country", "coordinates"]:
                                    del h[k]
                            if h not in ret_dict["spatial"]:
                                ret_dict["spatial"].append(h)
                            if "country" in h:
                                ret_dict["spatial"].append(h["country"])

                coords = getprop(s, "cartographics/coordinates", True)
                if coords and coords not in ret_dict["spatial"]:
                    ret_dict["spatial"].append(coords)

                if "temporal" in s:
                    ret_dict["temporal"].append(s["temporal"])

                ret_dict["subject"].append("--".join(subject))

            for k in ret_dict.keys():
                if not ret_dict[k]:
                    del ret_dict[k]

            self.update_source_resource(ret_dict)

    def map_format(self, authority_condition):
        prop = self.root_key + "genre"

        if exists(self.provider_data, prop):
            format = []
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, basestring):
                    format.append(s)
                else:
                    if authority_condition:
                        if "authority" in s and (s["authority"] == "marc" or
                                                 s["authority"] == ""):
                            format.append(s["#text"])
                    else:
                        format.append(s["#text"])
            format = filter(None, format)

            if format:
                self.update_source_resource({"format": format})

    def map_title(self, unsupported_types=[], unsupported_subelements=[]):
        prop = self.root_key + "titleInfo"

        if exists(self.provider_data, prop):
            title = ""
            for t in iterify(getprop(self.provider_data, prop)):
                if "type" in t and t["type"] in unsupported_types:
                    continue

                title = t["title"]
                if ("nonSort" in t and "nonSort" not in
                    unsupported_subelements):
                    title = t["nonSort"] + title
                if ("subTitle" in t and "subTitle" not in
                    unsupported_subelements):
                    title = title + ": " + t["subTitle"]
                if ("partNumber" in t and "partNumber" not in
                    unsupported_subelements):
                    part = t["partNumber"]
                    if not isinstance(part, list):
                        part = [part]
                    part = ", ".join(part)
                    title = title + ". " + part + "."
                if ("partName" in t and "partName" not in
                    unsupported_subelements):
                    title = title + ". " + t["partName"]

            title = re.sub("\.\.", "\.", title)
            title = re.sub(",,", ",", title)

            if title:
                self.update_source_resource({"title": title})
