from itertools import cycle
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.mapper import Mapper


intermediate_providers = {
    "medicalheritagelibrary": "Medical Heritage Library",
    "blc": "Boston Library Consortium"
}


class IAMapper(Mapper):
    def __init__(self, provider_data):
        super(IAMapper, self).__init__(provider_data)
        self.meta_key = "metadata/"

    def _add_to_list(self, lst, item):
        """Allows a list to be appended or extended conditionally in place.

        In Python, the `list.extend()` method can be used to combine two
        lists. However, if `list.extend()` is used to extend a list with 
        a string, the string will be iterated over as if it were a list. 
        For example, consider the following:

        >>> lst = ["1", "2", "3"]
        >>> lst.extend("abc")
        >>> lst
        ["1", "2", "3", "a", "b", "c"]

        When working with strings, `list.append()` uses the desired behavior.
        """
        if isinstance(item, list):
            lst.extend(item)
        else:
            lst.append(item)

    def _generate_map_dict(self, to_prop, from_props=None,
                           prevent_single_item_lists=True):
        """Generates a dict used for mapping metadata.

        to_prop specifies the name of the key in the resulting dict.
        from_props specifies the name of the key(s) in the incoming data.
        If from_props is None, it is set to the value of to_prop.
        prevent_single_item_lists will modify prop_values (the returned 
        property values from from_props) to return a single string if it is
        a single item list.
        """
        if from_props is None:
            from_props = [to_prop]

        prop_values = []
        for prop in iterify(from_props):
            from_prop = self.meta_key + prop
            if exists(self.provider_data, from_prop):
                self._add_to_list(prop_values, getprop(self.provider_data,
                                                       from_prop))
        if len(prop_values) == 1 and prevent_single_item_lists:
            prop_values = prop_values[0]
        if prop_values:
            return {to_prop: prop_values}
        else:
            return {}

    def _map_meta(self, to_prop, from_props=None, source_resource=True):
        """Helper function to update the `mapped_data` in a IAMapper object.

        to_prop and from_props are described in IAMapper_generate_map_dict().
        source_resource indicates whether to_prop should be updated in the
        context of source_resource (if True) or the top-level object (if
        False). 
        """
        _dict = self._generate_map_dict(to_prop, from_props)
        if _dict:
            if source_resource:
                self.update_source_resource(_dict)
            else:
                self.mapped_data.update(_dict)

    def map_creator(self):
        self._map_meta("creator")

    def map_date(self):
        self._map_meta("date")

    def map_description(self):
        self._map_meta("description")

    def map_language(self):
        self._map_meta("language")

    def map_publisher(self):
        self._map_meta("publisher")

    def map_subject(self):
        self._map_meta("subject")

    def map_rights(self):
        self._map_meta("rights", "possible-copyright-status")

    def map_type(self):
        self._map_meta("type", "mediatype")

    def map_identifier(self):
        self._map_meta("identifier", ["identifier", "call_number"])

    def map_title(self):
        ia_titles = self._generate_map_dict("title", from_props=None,
                                            prevent_single_item_lists=False)
        volumes = self._generate_map_dict("volume", from_props=None,
                                            prevent_single_item_lists=False)
        if volumes:
            if len(ia_titles.get("title")) > len(volumes.get("volume")):
                titles = [", ".join(t) for t in zip(ia_titles["title"],
                                                    cycle(volumes["volume"]))]
            else:
                titles = [", ".join(t) for t in zip(cycle(ia_titles["title"]),
                                                    volumes["volume"])]
            self.update_source_resource({"title": titles})
        else:
            self.update_source_resource(ia_titles)

    def map_data_provider(self):
        self._map_meta("dataProvider", "contributor", False)

    def map_is_shown_at(self):
        self._map_meta("isShownAt", "identifier-access", False)

    def map_intermediate_provider(self):
        coll_key = self.meta_key + "collection"
        # Get all of the "collection" elements from the provider's record.
        # We only want the first one that matches one of our identifiers.
        cols = getprop(self.provider_data, coll_key, keyErrorAsNone=True)
        if cols is not None:
            for c in list(cols):
                if c in intermediate_providers:
                    p = intermediate_providers[c]
                    self.mapped_data.update({"intermediateProvider": p})
                    return

    def map_has_view(self):
        file_name = None
        _id = None
        if exists(self.provider_data, "originalRecord/_id"):
            _id = getprop(self.provider_data, "originalRecord/_id", True)
        if exists(self.provider_data, "files/pdf"):
            file_name = getprop(self.provider_data, "files/pdf", True)
        if _id and file_name:
            _format = "application/pdf"
            url_prefix = "http://www.archive.org/download/{0}/{1}"
            rights = getprop(self.provider_data,
                             "metadata/possible-copyright-status", True)
            has_view = {"@id": url_prefix.format(_id, file_name),
                        "rights": rights,
                        "format": _format}

            self.mapped_data.update({"hasView": has_view})

    def map_marc(self):
        def _update_field(d, k, v):
            if k in d and v:
                if isinstance(d[k], list) and v not in d[k]:
                    d[k].append(v)
                elif isinstance(d[k], dict):
                    d[k].update(v)
                elif isinstance(d[k], basestring) and d[k] != v:
                    d[k] = [d[k], v]
            else:
                d[k] = v

        def _extract_sub_field(d, subfield_name="subfield", codes=tuple(),
                               exclude_codes=tuple(), content_key="#text",
                               concat_str=", "):
            if subfield_name in d:
                if exclude_codes:
                    values = [v[content_key] for v in d[subfield_name] if
                              isinstance(v, dict) and "code" in v and
                              content_key in v and v["code"] not in
                              exclude_codes]
                else:
                    values = [v[content_key] for v in d[subfield_name] if
                              isinstance(v, dict) and "code" in v and
                              content_key in v and (not codes or v["code"] in
                              codes)]
                return concat_str.join(values)
            else:
                return None

        prop = "record/datafield"
        if exists(self.provider_data, prop):
            data_field_dicts = getprop(self.provider_data, prop)
            out = {"contributor": [], "extent": [], "format": [],
                   "spatial": [], "isPartOf": []}
            first_is_part_of_set = False
            for _dict in data_field_dicts:
                if isinstance(_dict, dict) and "tag" in _dict:
                    tag = _dict["tag"]
                    if tag in ("300",):
                        value = _extract_sub_field(_dict, codes=("c",))
                        _update_field(out, "extent", value)
                    elif tag.startswith("6") and len(tag) == 3:
                        value = _extract_sub_field(_dict, codes=("z",))
                        _update_field(out, "spatial", value)
                    elif (tag in
                          ("440", "490", "800", "810", "830", "785") and not
                          first_is_part_of_set):
                        first_is_part_of_set = True
                        value = _extract_sub_field(_dict,
                                                   exclude_codes=("w",),
                                                   concat_str=". ")
                        _update_field(out, "isPartOf", value)
                    elif tag in ("780",):
                        value = _extract_sub_field(_dict,
                                                   exclude_codes=("w",),
                                                   concat_str=". ")
                        _update_field(out, "isPartOf", value)

            for k, v in out.items():
                if not v:
                    del out[k]
                elif len(v) == 1:
                    out[k] = v[0]

            self.update_source_resource(out)

    def map_ia_provider(self):
        self.mapped_data.update({"provider": {
                "@id": "http://dp.la/api/contributor/internet_archive",
                "name": "Internet Archive"
            }})

    def map_multiple_fields(self):
        self.map_marc()
        self.map_ia_provider()
