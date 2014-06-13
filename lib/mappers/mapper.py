import re
from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists
from dplaingestion.selector import getprop as get_prop, setprop, delprop

def getprop(data, prop):
    return get_prop(data, prop, True)

class Mapper(object):
    """The base class for all mappers.

       Defines attributes and methods that are common to all mappers. Methods
       to map a specific field are named accordingly, for example:

            Method to map provider: map_provider
            Method to map isShownAt: map_is_shown_at

       These methods map fields from the provider_data dictionary to the 
       mapped_data dictionary one-to-one. For mapping one-to-many fields or
       many-to-one fields, you can create methods to be called in method
       map_multiple_fields.

       If any logic needs to run after mapping is complete (for example,
       field1, once mapped, needs to be updated based on the mapping of
       field2), you can create methods to be called in method
       update_mapped_fields.
    """

    def __init__(self, data, key_prefix=None):
        self.provider_data = data
        self.context = {
           "begin": {
               "@id": "dpla:dateRangeStart",
               "@type": "xsd:date"
           },
           "@vocab": "http://purl.org/dc/terms/",
           "hasView": "edm:hasView",
           "name": "xsd:string",
           "object": "edm:object",
           "dpla": "http://dp.la/terms/",
           "collection": "dpla:aggregation",
           "edm": "http://www.europeana.eu/schemas/edm/",
           "end": {
               "@id": "dpla:end",
               "@type": "xsd:date"
           },
           "state": "dpla:state",
           "aggregatedDigitalResource": "dpla:aggregatedDigitalResource",
           "coordinates": "dpla:coordinates",
           "isShownAt": "edm:isShownAt",
           "stateLocatedIn": "dpla:stateLocatedIn",
           "sourceResource": "edm:sourceResource",
           "dataProvider": "edm:dataProvider",
           "originalRecord": "dpla:originalRecord",
           "provider": "edm:provider",
           "LCSH": "http://id.loc.gov/authorities/subjects"
        }
        #self.context = {
        #    "@context": "http://rawgithub.com/anarchivist/8718510/raw/" + \
        #                "dpla-mapv3-context.json",
        #    "aggregatedCHO": "#sourceResource",
        #    "@type": "ore:Aggregation"
        #}
        self.mapped_data = {"sourceResource": {}}
        if key_prefix is not None:
            self.remove_key_prefix(self.provider_data, key_prefix)

    def extract_xml_items(self, group_key, item_key, name_key=None,
                          data=None):
        """
        Generalization of what proved to be an idiom in XML information
        extraction, e.g. in the XML structure;
        <creators>
        <creator>Alice</creator>
        <creator>Bob</creator>
        <creator type="editor">Eve</creator>
        </creators>

        "group_key" is the name of the containing property, e.g "creators"
        "item_key" is the name of the contained property, e.g. "creator"
        "name_key" is the property of the item_key-named resource to be
        extracted, if present, otherwise the value of the name_key property
        """
        if not data:
            data = self.provider_data

        items = []
        group = data.get(group_key)
        if group:
            # xmltodict converts what would be a list of length 1 into just
            # that lone dictionary, so we iterify.
            # FIXME: We then pick the first in the list, although this is
            # unlikely to be optimal and possibly incorrect in some cases.
            item = iterify(group)[0]

            subitem = item.get(item_key)
            for s in iterify(subitem):
                if isinstance(s, dict):
                    if not name_key:
                        items.append(s)
                    elif name_key in s:
                        items.append(s.get(name_key, None))
                elif s not in data:
                    items.append(s)

        return filter(None, items)

    def remove_key_prefix(self, data, key_prefix):
        for key in data.keys():
            if not key == "originalRecord":
                new_key = key.replace(key_prefix, "")
                if new_key != key:
                    data[new_key] = data[key]
                    del data[key]
                for item in iterify(data[new_key]):
                    if isinstance(item, dict):
                        self.remove_key_prefix(item, key_prefix)

        self.provider_data = data

    def clean_dict(self, _dict):
        return {i:j for i,j in _dict.items() if j}

    def map(self):
        """
        Maps fields from provider_data to fields in mapped_data by:

        1. Mapping, one-to-one, provider_data fields to mapped_data root fields
           via map_root
        2. Mapping, one-to-one, provider_data fields to mapped_data
           sourceResource fields via map_source_resource
        3. Mapping, one-to-many or many-to-one, provider_data fields to
           mapped_data fields
        4. Running any post-mapping logic via update_mapped_fields 
        """
        self.map_root()
        self.map_source_resource()
        self.map_multiple_fields()
        self.update_mapped_fields()

    def map_root(self):
        """Maps the mapped_data root fields."""
        self.map_base()
        self.map_provider()
        self.map_data_provider()
        self.map_is_shown_at()
        self.map_has_view()
        self.map_object()

    def map_base(self):
        """Maps base fields shared by all Mappers"""
        self.map_context()
        self.map_original_record()
        self.map_ids()
        self.map_ingest_fields()

    def map_source_resource(self):
        """Mapps the mapped_data sourceResource fields."""
        #self.mapped_data["sourceResource"] = {"@id": "#sourceResource"}
        self.map_collection()
        self.map_contributor()
        self.map_creator()
        self.map_date()
        self.map_description()
        self.map_extent()
        self.map_format()
        self.map_identifier()
        self.map_is_part_of()
        self.map_language()
        self.map_publisher()
        self.map_relation()
        self.map_rights()
        self.map_spatial()
        self.map_spec_type()
        self.map_state_located_in()
        self.map_subject()
        self.map_temporal()
        self.map_title()
        self.map_type()

    def update_source_resource(self, _dict):
        """
        Updates the mapped_data sourceResource field with the given dictionary.
        """
        self.mapped_data["sourceResource"].update(_dict)

    # base mapping functions
    def map_context(self):
        #self.mapped_data.update(self.context)
        self.mapped_data.update({"@context": self.context})

    def map_original_record(self):
        prop = "originalRecord"
        self.mapped_data["originalRecord"] = self.provider_data.get(prop)

    def map_ids(self):
        id = self.provider_data.get("id", "")
        _id = self.provider_data.get("_id")
        at_id = "http://dp.la/api/items/" + id
        self.mapped_data.update({"id": id, "_id": _id, "@id": at_id})

    # root mapping functions
    def map_provider(self, prop="provider"):
        if exists(self.provider_data, prop):
            self.mapped_data.update({"provider":
                                     self.provider_data.get(prop)})

    def map_data_provider(self, prop="source"):
        if exists(self.provider_data, prop):
            self.mapped_data.update({"dataProvider":
                                     self.provider_data.get(prop)})

    def map_ingest_fields(self):
        for prop in ("ingestDate", "ingestType", "ingestionSequence"):
            self.mapped_data.update({prop: self.provider_data.get(prop)})

    def map_is_shown_at(self):
        pass

    def map_has_view(self):
        pass

    def map_object(self):
        pass

    # sourceResource mapping functions
    def map_collection(self):
        prop = "collection"
        if exists(self.provider_data, prop):
            self.update_source_resource({"collection":
                                         self.provider_data.get(prop)})

    def map_contributor(self):
        pass

    def map_creator(self):
        pass

    def map_date(self):
        pass

    def map_description(self):
        pass

    def map_extent(self):
        pass

    def map_format(self):
        pass

    def map_identifier(self):
        pass

    def map_is_part_of(self):
        pass

    def map_language(self):
        pass

    def map_publisher(self):
        pass

    def map_relation(self):
        pass

    def map_rights(self):
        pass

    def map_subject(self):
        pass

    def map_temporal(self):
        pass

    def map_title(self):
        pass

    def map_type(self):
        pass

    def map_state_located_in(self):
        pass

    def map_spatial(self):
        pass

    def map_spec_type(self):
        pass

    def map_multiple_fields(self):
        pass

    def update_mapped_fields(self):
        pass
