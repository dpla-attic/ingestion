from akara import logger
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper
from dplaingestion.textnode import textnode


class EsdnMapper(OAIMODSMapper):
    def __init__(self, provider_data):
        super(EsdnMapper, self).__init__(provider_data)

    def map_collection(self):
        ret_dict = {"collection": getprop(self.provider_data, "collection")}

        prop = self.root_key + "relatedItem"

        if exists(self.provider_data, prop):
            related_items = iterify(getprop(self.provider_data, prop))
            host_collections = [item for item in related_items
                                if item.get("type") == "host" and
                                item.get("displayLabel") == "Collection"]

            for host_collection in host_collections:
                title = getprop(host_collection, "titleInfo/title", True)
                if title:
                    ret_dict["collection"]["title"] = title

                description = getprop(host_collection, "abstract", True)
                if description:
                    ret_dict["collection"]["description"] = description

        self.update_source_resource(ret_dict)

    def map_creator_and_contributor(self):
        prop = self.root_key + "name"
        mapped_props = {
            "creator": [],
            "contributor": []
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                name = s.get("namePart")
                if name:
                    role_terms = []
                    try:
                        for r in iterify(s.get("role")):
                            role_term = r.get("roleTerm")
                            if isinstance(role_term, dict):
                                role_terms.append(
                                        role_term.get("#text").lower())
                            elif isinstance(role_term, list):
                                for rt in role_term:
                                    role_terms.append(rt.lower())
                            else:
                                role_terms.append(role_term.lower())
                    except Exception as e:
                        logger.error("Error getting name/role/roleTerm for " +
                                     "record %s\nException:%\n%s" %
                                     (self.provider_data["_id"], e))
                        continue

                    if "creator" in role_terms:
                        mapped_props["creator"].append(name)
                    elif "contributor" in role_terms:
                        mapped_props["contributor"].append(name)

            self.update_source_resource(self.clean_dict(mapped_props))

    def map_data_provider(self):
        prop = self.root_key + "note"
        data_provider = None

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    if s.get("type") == "ownership" and "#text" in s:
                        data_provider = s.get("#text")
                        break
                except Exception as e:
                    logger.error("Error getting mapping dataProvider "
                                 "from note/type for record "
                                 "%s\nException:\n%s" %
                                 (self.provider_data["_id"], e))

        if data_provider:
            self.mapped_data.update({"dataProvider": data_provider})

    def map_description(self):
        prop = self.root_key + "note"
        description = []
        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    if s.get("type") == "content":
                        description.append(s.get("#text"))
                except Exception as e:
                    logger.error("Error getting mapping description "
                                 "fro record %s\nException:\n%s" %
                                 (self.provider_data["_id"], e))

        if description:
            self.update_source_resource({"description": description})

    def map_date_and_publisher(self):
        prop = self.root_key + "originInfo"
        mapped_props = {
            "date": "",
            "publisher": []
        }

        if exists(self.provider_data, prop):
            dates = {
                "date": None,
                "early_date": None,
                "late_date": None
            }
            for s in iterify(getprop(self.provider_data, prop)):
                if "dateCreated" in s:
                    date_list = iterify(s.get("dateCreated"))
                    # Ist ein uber kludge
                    try:
                        dates['date'] = [t.get("#text") for t in date_list if
                                         isinstance(t, dict) and
                                         t.get("keyDate") == "yes"
                                         and "point" not in t]

                        dates['early_date'] = [t.get("#text") for t in date_list
                                               if isinstance(t, dict) and
                                               t.get("keyDate") == "yes" and
                                               t.get("point") == "start"]

                        dates['late_date'] = [t.get("#text") for t in date_list
                                              if isinstance(t, dict) and
                                              t.get("point") == "end"]

                    except Exception as e:
                        logger.error(
                                "Unable to map date data:\n\t %s" % date_list)
                        logger.error(e)

                if "publisher" in s:
                    mapped_props["publisher"].append(s.get("publisher"))

            # Remove Time component from date
            for k in dates.keys():
                if dates.get(k):
                    dates[k] = dates[k][0]
                    if dates.get(k) and 'T' in dates.get(k):
                        date = dates[k]
                        dates[k] = date[:date.index('T')]

            if dates.get("date"):
                mapped_props["date"] = dates.get("date")
            elif dates.get("early_date") and dates.get("late_date"):
                mapped_props["date"] = dates.get("early_date") + "-" + \
                                       dates.get("late_date")
            elif dates.get("early_date"):
                mapped_props["date"] = dates.get("early_date")

            # Remove None values
            mapped_props["publisher"] = filter(None, mapped_props["publisher"])
            mapped_props["date"] = filter(None, mapped_props["date"])

            self.update_source_resource(self.clean_dict(mapped_props))

    def map_extent(self):
        prop = self.root_key + "physicalDescription/extent"
        if exists(self.provider_data, prop):
            self.update_source_resource({
                "extent":
                    getprop(self.provider_data,
                            prop)
            })

    def map_format(self):
        formats = []
        props = [self.root_key + "physicalDescription/form",
                 self.root_key + "physicalDescription/genre"]
        for prop in props:
            if exists(self.provider_data, prop):
                vals = getprop(self.provider_data, prop)
                if isinstance(vals, list):
                    for v in vals:
                        formats.append(textnode(v))
                else:
                    for i in iterify(vals):
                        formats.append((textnode(i)))

        formats = filter(None, formats)

        if formats:
            self.update_source_resource({"format": formats})

    def map_identifier(self):
        prop = self.root_key + "identifier"
        identifier = None
        if exists(self.provider_data, prop):
            identifier = getprop(self.provider_data, prop)
            if isinstance(identifier, dict):
                identifier = identifier.get("#text")

        if identifier:
            self.update_source_resource({"identifier": identifier})

    def map_intermediate_provider(self):
        prop = self.root_key + "note"
        intermediate_provider = None

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "type" in s and s.get("type") == "regional council":
                    intermediate_provider = s.get("#text")

        if intermediate_provider:
            self.mapped_data.update({
                "intermediateProvider":
                    intermediate_provider
            })

    def map_is_part_of(self):
        prop = self.root_key + "relatedItem"
        _dict = {"isPartOf": []}

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if "type" in s and s.get("type") == "host" and \
                        s.get("displayLevel") == "collection":
                    _dict["isPartOf"].append(getprop(s, "titleInfo/title"))

            self.update_source_resource(self.clean_dict(_dict))

    def map_is_shown_at_and_preview(self):
        prop = self.root_key + "location"
        mapped_props = {
            "isShownAt": "",
            "object": ""
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    url = s.get("url")
                    if url.get("usage") == "primary display" and \
                            url.get("access") == "object in context":
                        mapped_props["isShownAt"] = url.get("#text")
                    elif url.get("access") == "preview":
                        mapped_props["object"] = url.get("#text")
                except Exception as e:
                    logger.error("Error getting mapping isShownAt and object" +
                                 " %s\nException:\n%s" %
                                 (self.provider_data["_id"], e))

            self.mapped_data.update(self.clean_dict(mapped_props))

    def map_language(self):
        prop = self.root_key + "language/languageTerm"
        lang = None
        if exists(self.provider_data, prop):
            lang = getprop(self.provider_data, prop)
            if isinstance(lang, dict):
                lang = lang.get("#text")

        if lang:
            self.update_source_resource({"language": filter(None, lang)})

    def map_relation(self):
        prop = self.root_key + "relatedItem/titleInfo/title"

        if exists(self.provider_data, prop):
            self.update_source_resource({
                "relation":
                    getprop(self.provider_data,
                            prop)
            })

    def map_rights(self):
        prop = self.root_key + "accessCondition"
        rights = []
        edm_rights = None
        if exists(self.provider_data, prop):
            for right in iterify(getprop(self.provider_data, prop)):
                if isinstance(right, dict):
                    _type = getprop(right, "type", True)
                    if _type and _type == "use and reproduction":
                        edm_rights = right.get("xlink:href")
                    else:
                        rights.append(right.get("#text"))
                elif isinstance(right, list):
                    for r in right:
                        rights.append(r)
                else:
                    rights.append(right)
        if rights:
            self.update_source_resource({"rights": filter(None, rights)})
        if edm_rights:
            self.mapped_data.update({"rights": filter(None, edm_rights)})

    def map_edm_rights(self):
        # defer to map_rights for edm:rights implementation
        pass

    def map_subject_and_spatial_and_temporal(self):
        prop = self.root_key + "subject"
        mapped_props = {
            "subject": [],
            "spatial": [],
            "temporal": []
        }

        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                try:
                    if "geographic" in s and s.get("geographic"):
                        mapped_props["spatial"].append(s.get("geographic"))
                    elif "topic" in s and s.get("topic"):
                        mapped_props["subject"].append(s.get("topic"))
                    elif "temporal" in s and s.get("temporal"):
                        mapped_props["temporal"].append(s.get("temporal"))
                except Exception as e:
                    logger.error("Error mapping geo/subject/temporal"
                                 "for record %s\nException:\n%s" %
                                 (self.provider_data["_id"], e))

            for k in mapped_props.keys():
                mapped_props[k] = self.unnest_list(mapped_props.get(k), [])

            if mapped_props:
                self.update_source_resource(self.clean_dict(
                        mapped_props))

    def map_title(self):
        prop = self.root_key + "titleInfo/title"
        title = None
        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, basestring):
                    title = s
                elif isinstance(s, list):
                    title = s[0]
            if title:
                self.update_source_resource({"title": title})

    def map_type(self):
        prop = self.root_key + "typeOfResource"
        types = []
        if exists(self.provider_data, prop):
            v = iterify(getprop(self.provider_data, prop))
            logger.error("v = %s" % v)
            for i in v:
                logger.error(("i = %s" % i))
                types.append(textnode(i))
            logger.error(("types = %s" % types))
            self.update_source_resource({"type": types})

    def unnest_list(self, nested_array, array=[]):
        for i in nested_array:
            if isinstance(i, list):
                self.unnest_list(i, array)
            else:
                array.append(i)
        return array

    def map_multiple_fields(self):
        self.map_creator_and_contributor()
        self.map_date_and_publisher()
        self.map_subject_and_spatial_and_temporal()
        self.map_is_shown_at_and_preview()
        self.map_collection()
