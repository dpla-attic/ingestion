class Mapper(object):
    """The base class for all mappers.

       Includes attributes and methods that are common to all types
    """
    def __init__(self, data):
        self.data = data
        self.context = {
            "@context": "http://rawgithub.com/anarchivist/8718510/raw/" + \
                        "dpla-mapv3-context.json",
            "aggregatedCHO": "#sourceResource",
            "@type": "ore:Aggregation"
        }
        self.mapped_data = {}

    def map(self):
        self.map_root()
        self.map_source_resource()

    def map_root(self):
        self.map_context()
        self.map_ids()
        self.map_ingest_fields()
        self.map_provider()
        self.map_data_provider()
        self.map_is_shown_at()

    def map_source_resource(self):
        self.mapped_data["sourceResource"] = {"@id": "#sourceResource"}
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

    def update_source_resource(_dict):
        self.mapped_data["sourceResource"].update(_dict)

    # root mapping functions
    def map_context(self):
        self.mapped_data.update(self.context)

    def map_ids(self):
        id = self.data.get("id", "")
        _id = self.data.get("_id")
        at_id = "http://dp.la/api/item/" + id
        self.mapped_data.update({"id": id, "_id": _id, "@id": at_id})

    def map_provider(self):
        prop = "provider"
        self.mapped_data.update({prop: self.data.get(prop)})

    def map_data_provider(self):
        prop = "source"
        self.mapped_data.update({"dataProvider": self.data.get(prop)})

    def map_is_shown_at(self):
        pass

    def map_ingest_fields(self):
        for prop in ("ingestDate", "ingestType", "ingestionSequence"):
            self.mapped_data.update({prop: self.data.get(prop)})

    # sourceResource mapping functions
    def map_collection(self):
        prop = "collection"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_contributor(self):
        prop = "contributor"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_creator(self):
        prop = "creator"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_date(self):
        prop = "date"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_description(self):
        prop = "description"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_extent(self):
        prop = "extent"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_format(self):
        prop = "format"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_identifier(self):
        prop = "identifier"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_is_part_of(self):
        prop = "relation" # I think
        self.update_source_resource({prop: self.data.get(prop)})

    def map_language(self):
        prop = "language"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_publisher(self):
        prop = "publisher"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_relation(self):
        prop = "relation" # I think
        self.update_source_resource({prop: self.data.get(prop)})

    def map_rights(self):
        prop = "rights"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_spatial(self):
        prop = "coverage"
        self.update_source_resource({"spatial": self.data.get(prop)})

    def map_spec_type(self):
        prop = "spec_type"
        self.update_source_resource({"specType": self.data.get(prop)})

    def map_state_located_in(self):
        prop = "state_located_in"
        self.update_source_resource({"stateLocatedIn": self.data.get(prop)})

    def map_subject(self):
        prop = "subject"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_temporal(self):
        self.update_source_resource({prop: self.data.get(prop)})

    def map_title(self):
        prop = "title"
        self.update_source_resource({prop: self.data.get(prop)})

    def map_type(self):
        prop = "type"
        self.update_source_resource({prop: self.data.get(prop)})
