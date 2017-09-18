from dplaingestion.utilities import iterify
from dplaingestion.selector import getprop
from dplaingestion.mappers.mapv3_json_mapper import MAPV3JSONMapper
from akara import logger


class FloridaMapper(MAPV3JSONMapper):
    def __init__(self, provider_data):
        super(FloridaMapper, self).__init__(provider_data)

    def map_field(self, prop, subProp=None):
        sr = self.provider_data["sourceResource"]
        values = []
        for c in iterify(getprop(sr, prop, True)):
            if subProp:
                values.append(getprop(c, subProp, True))
            elif c:
                values.append(c)

        return self.dropEmpty(values)

    def map_collection(self):
        collections = []
        titles = self.map_field("collection", "name")

        for title in titles:
            collections.append({
                "title": title,
                "description": ""
            })

        if collections:
            self.overwrite_source_resource({"collection": collections})

    def update_contributor(self):
        v = self.map_field("contributor", "name")
        if v:
            self.overwrite_source_resource({"contributor": v})

    def update_creator(self):
        v = self.map_field("creator", "name")
        if v:
            self.overwrite_source_resource({"creator": v})

    def update_format(self):
        genres = self.map_field("genre", "name")
        formats = self.map_field("format")

        if formats and genres:
            self.overwrite_source_resource({"format": genres + formats})
        elif genres:
            self.overwrite_source_resource({"format": genres})
        elif formats:
            self.overwrite_source_resource({"format": formats})

    def update_language(self):
        l = self.map_field("language", "name")
        l = l + self.map_field("language", "iso_639_3")
        if l:
            self.overwrite_source_resource({"language": l})

    def update_rights(self):
        rights = self.map_field("rights", "text")
        edmRights = self.map_field("rights", "@id")

        if rights:
            self.overwrite_source_resource({"rights": rights})
        if edmRights:
            self.mapped_data.update({"rights": edmRights[0]})

    def update_spatial(self):
        places = self.map_field("spatial")
        myPlaces = []
        for place in places:
            myPlaces.append( {
                "name": getprop(place, "name", True),
                "lat": getprop(place, "lat", True),
                "long": getprop(place, "long", True)
            })

        if places:
            self.overwrite_source_resource({"spatial": myPlaces})

    def update_subject(self):
        subjects = self.map_field("subject", "name")

        if subjects:
            self.overwrite_source_resource({"subject": subjects})

    def update_mapped_fields(self):
        self.update_contributor()
        self.update_creator()
        self.update_format()
        self.update_language()
        self.update_rights()
        self.update_spatial()
        self.update_subject()

    def dropEmpty(self, l):
        return filter(None, l)
