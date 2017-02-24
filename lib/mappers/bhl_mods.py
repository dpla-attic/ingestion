import re
from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper
from dplaingestion.textnode import textnode, NoTextNodeError


class BHLMapper(OAIMODSMapper):
    def __init__(self, provider_data):
        super(BHLMapper, self).__init__(provider_data)

    def map_date_and_publisher(self):
        prop = self.root_key + "originInfo"

        _dict = {
            "date": None,
            "publisher": []
        }

        if exists(self.provider_data, prop):
            for or_info in iterify(getprop(self.provider_data, prop)):
                dates = _date_values(or_info)  # They come sorted
                if dates:
                    if '-' in dates[-1] or '/' in dates[-1]:
                        # Date is already a range, so just use it
                        _dict['date'] = dates[-1]
                    elif len(dates) > 1:
                        _dict['date'] = "%s-%s" % (dates[0], dates[-1])
                    else:
                        _dict['date'] = dates[0]

                if "publisher" in or_info:
                    _dict["publisher"].append(or_info["publisher"])

            self.update_source_resource(self.clean_dict(_dict))

    def map_description(self):
        prop = self.root_key + "note"

        if exists(self.provider_data, prop):
            description = []
            for s in iterify(getprop(self.provider_data, prop)):
                if "type" in s and s.get("type") == "content":
                    description.append(s.get("#text"))

            if description:
                self.update_source_resource({"description": description})

    # OAI MODS Mapper is blowing up on this method because of arg
    # count issues,
    def map_format(self):
        pass

    def map_format_and_spec_type(self):
        prop = self.root_key + "physicalDescription"

        _dict = {
            "format": [],
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "form" in s:
                    for f in iterify(s.get("form")):
                        if f.get("authority") == "marcform":
                            _dict["format"].append(f["#text"])

            self.update_source_resource(self.clean_dict(_dict))

    def map_identifier(self):
        prop = self.root_key + "identifier"
        identifiers = []
        if exists(self.provider_data, prop):
            for identifier in iterify(getprop(self.provider_data, prop)):
                identifiers.append(identifier["#text"])
        self.update_source_resource({"identifier": identifiers})

    def map_language(self):
        prop = self.root_key + "language/languageTerm"

        if exists(self.provider_data, prop):
            language = []

            for s in iterify(getprop(self.provider_data, prop)):
                if (s.get("type") == "text" and
                        s.get("authority") == "iso639-2b"):
                    language.append(s.get("#text"))

            if language:
                self.update_source_resource({"language": language})

    def map_subject_and_spatial_and_temporal(self):
        prop = self.root_key + "subject"
        _dict = {
            "subject": [],
            "spatial": [],
            "temporal": []
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "geographic" in s:
                    _dict["spatial"].append(s["geographic"])
                elif "topic" in s:
                    _dict["subject"].append(s["topic"])
                elif "genre" in s:
                    _dict["subject"].append(s["genre"])
                elif "temporal" in s:
                    _dict["temporal"].append(s["temporal"])

            self.update_source_resource(self.clean_dict(_dict))

    def map_is_part_of(self):
        prop = self.root_key + "relatedItem"
        _dict = {"relation": []}

        if exists(self.provider_data, prop):
            for relatedItem in iterify(getprop(self.provider_data, prop)):
                title_prop = "titleInfo/title"
                if exists(relatedItem, title_prop):
                    _dict["relation"].append(getprop(relatedItem, title_prop))

            self.update_source_resource(self.clean_dict(_dict))

    def map_rights(self):
        prop = self.root_key + "accessCondition/#text"
        if exists(self.provider_data, prop):
            self.update_source_resource(
                {"rights": getprop(self.provider_data, prop)}
            )

    def map_type(self):
        prop = self.root_key + "typeOfResource"

        if exists(self.provider_data, prop):
            self.update_source_resource(
                {"type": getprop(self.provider_data, prop)}
            )

    def map_edm_has_type(self):
        prop = self.root_key + "genre"
        genre = {}
        if exists(self.provider_data, prop):
            genre = getprop(self.provider_data, prop)
        if "authority" in genre and genre["authority"] == "marcgt":
            self.update_source_resource({"hasType": genre["#text"]})

    def map_data_provider(self, prop="source"):
        prop = self.root_key + "note"
        _dict = {"dataprovider": None}

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "type" in s and s.get("type") == "ownership":
                    _dict["dataProvider"] = s.get("#text")
                    break

            self.mapped_data.update(self.clean_dict(_dict))

    def map_preview_and_is_shown_at(self):
        prop = self.root_key + "location/url"
        ret_dict = {}

        if exists(self.provider_data, prop):
            for url in iterify(getprop(self.provider_data, prop)):
                usage = getprop(url, "usage", True)
                access = getprop(url, "access", True)
                if usage == "primary" and access == "raw object":
                    ret_dict["isShownAt"] = url["#text"]
                elif (access == "object in context"
                      and usage == "primary display"):
                    ret_dict["object"] = url["#text"]

        self.mapped_data.update(ret_dict)

    def map_title(self):
        """Update title with concatenated title and volume number fields

        Use //titleinfo/title (where there is no "type" attribute), appended
        with //part/detail/number if it's defined.  Do this for each "good"
        title encountered.
        """
        ti_path = self.root_key + "titleInfo"
        title_infos = iterify(getprop(self.provider_data, ti_path, True))
        titles = _good_titles(title_infos)  # generator
        part_path = self.root_key + "part"
        number = _part_num_str(getprop(self.provider_data, part_path, True))
        def t_string(t):
            if number:
                return "%s, %s" % (t, number)
            else:
                return t
        # Titles are concatenated with volume numbers, and have various
        # unwanted characters and whitespace removed.
        full_titles = [t_string(re.sub(r'[\s:\\]+$', '', t)) for t in titles]
        if full_titles:
            self.update_source_resource({"title": full_titles})

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
            creator = [name["name"] for name in names if "creator" in name["roles"]]
            if creator:
                creator_and_contributor["creator"] = creator

            # Set contributor
            contributor = [name["name"] for name in names if "contributor" in name["roles"]]
            if contributor:
                creator_and_contributor["contributor"] = contributor

            self.update_source_resource(creator_and_contributor)

    def map_collection(self):
        """Skip mapping `collection', per G.G. 2017-02-17"""
        pass

    def map_multiple_fields(self):
        self.map_format_and_spec_type()
        self.map_creator_and_contributor()
        self.map_subject_and_spatial_and_temporal()
        self.map_preview_and_is_shown_at()
        self.map_edm_has_type()
        self.map_date_and_publisher()

class DateString(unicode):
    """Class for date strings that has custom sorting rules

    Strings with range delimiters ('-' and '/') are sorted to the end of the
    list.

    See how BHLMapper.map_date_and_publisher() expects to encounter date
    ranges.
    """
    def __lt__(a, b):
        if '-' in a or '/' in a:
            if '-' not in b and '/' not in b:
                return False
            else:
                return unicode.__lt__(a, b)
        elif '-' in b or '/' in b:
            return True
        else:
            return unicode.__lt__(a, b)
    def __gt__(a, b):
        if '-' in a or '/' in a:
            if '-' not in b and '/' not in b:
                return True
            else:
                return unicode.__lt__(a, b)
        elif '-' in b or '/' in b:
            return False
        else:
            return unicode.__lt__(a, b)


def _date_values(element):
    """Pick out the child element of the given XML element that is our date
    field, and return the date values from it as a list of strings.
    """
    prop = [k for k in element if k in ['dateOther', 'dateIssued']]
    if not prop:
        return None
    date_list = iterify(element.get(prop[0]))
    return [DateString(textnode(e))
            for e in _date_elements(date_list, prop[0])]

def _date_elements(el_list, el_name):
    """Given a list of candidate elements, yield those suitable as dates"""
    for e in el_list:
        if isinstance(e, dict) and e.get('keyDate') == 'yes':
            if (el_name == 'dateOther' and e.get('type') == 'issueDate') \
                    or el_name == 'dateIssued':
                yield e

def _good_titles(elements):
    """Given a list of elements, yield the ones that are "real" titles.

    The real ones don't have a "type" attribute, like "type=abbreviation".
    This function tries to handle a variety of cases where elements have or
    dont' have attributes, where we might get strings or dicts with '#text'
    properties.
    """
    for el in elements:
        if isinstance(el, dict) and 'type' not in el:
            try:
                if 'title' in el:
                    yield textnode(el.get('title'))
                else:
                    yield textnode(el)
            except NoTextNodeError:
                # swallow textnode.NoTextNodeError
                pass
        elif isinstance(el, basestring):
            yield el

def _part_num_str(part_elements):
    """Given a list of //part elements, return a string of their concatenated
    values
    """
    try:
        return ", ".join([textnode(e['detail']['number'])
                          for e in iterify(part_elements)])
    except NoTextNodeError:
        # There was probably an issue determining the string value of one of
        # the elements.  See textnode.textnode().
        return ""
