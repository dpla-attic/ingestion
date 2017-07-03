from dplaingestion.utilities import iterify
from dplaingestion.selector import getprop, exists
from dplaingestion.mappers.mapv3_json_mapper import MAPV3JSONMapper
from akara import logger


class LibraryOfCongressMapper(MAPV3JSONMapper):
    def __init__(self, provider_data):
        super(LibraryOfCongressMapper, self).__init__(provider_data)

    '''
    [partof['title'] for partof in item['partof'] 
        if '/collections/' in partof['url']]
    '''

    def map_collection(self):
        title = ""
        for partof in iterify(getprop(self.provider_data, "partof", True)):
            if exists(partof, "url") and "/collections/" in partof["url"]:
                title = partof["title"]
                break

        if title:
            self.update_source_resource({"collection": {
                "title": title, "id": "", "_id": "", "description": ""
            }})

    def map_contributor(self):
        contributors = []
        values = iterify(getprop(self.provider_data, "contributor_names", True))
        if values:
            contributors = list(set(values + contributors))
        else:
            names = iterify(getprop(self.provider_data, "contributors", True))
            for name in names:
                contributors.append(name.keys())

        if contributors:
            self.update_source_resource({"contributor": contributors})

    def map_description(self):
        props = ['description', 'created_published']
        description = []

        for prop in props:
            values = iterify(getprop(self.provider_data, prop, True))
            if values:
                description = list(set(values+description))

        if description:
            self.update_source_resource({"description": description})

    def map_format_genre_type(self, dest):
        formats = []

        # Either-Or mapping
        values = iterify(getprop(self.provider_data, "type", True))
        if values:
            formats = list(set(values + formats))
        else:
            values = iterify(getprop(self.provider_data, "format", True))
            formats = values.keys()

        # Include mapping from 'genre' if the destination is not 'type'
        if dest != "type":
            values = iterify(getprop(self.provider_data, "genre", True))
            if values:
                formats = list(set(values + formats))

        if formats:
            self.update_source_resource({dest: formats})

    def map_language(self):
        languages = []
        values = iterify(getprop(self.provider_data, "language", True))
        for v in values:
            languages = list(set(v.keys() + languages))
        if languages:
            self.update_source_resource({"language": languages})

    def map_spatial(self):
        places = set()
        values = iterify(getprop(self.provider_data, "location", True))
        for v in values:
            places = places.add(v.keys())
        if places:
            self.update_source_resource({"spatial": places})

    def map_rights(self):
            rights = ("For rights relating to this resource, visit %s" %
                      getprop(self.provider_data, "url", True))
            self.update_source_resource({"rights": rights})

    def map_source_resource(self):
        super(LibraryOfCongressMapper, self).map_source_resource()
        """
        Many of the mappings for Library of Congress are conditional (e.g. if
        A exists then map A, else map B, else map C etc.  
        """
        maps = {
            "other-title,other-titles,alternate_ title": "alternative",
            "date,dates": "date",
            "id": "identifier",
            "subject_headings": "subject",
            "title": "title",
            "mime_type": "format",
            "medium": "extent"
        }
        for (sources, dest) in maps.iteritems():
            for source in sources:
                values = iterify(getprop(self.provider_data, source, True))
                if values:
                    existing_values = \
                        getprop(self.mapped_data["sourceResource"], dest, True)
                    if existing_values:
                        values = list(set(values + existing_values))
                    self.update_source_resource({dest: values})
                    break

    def map_edm_rights(self):
        pass

    def map_is_shown_at(self):
        is_shown_at = getprop(self.provider_data, "url", True)

        if is_shown_at:
            self.mapped_data.update({"isShownAt": is_shown_at})

    def map_data_provider(self):
        pass

    def map_object(self):
        prop = "image_url"
        object_url = ("https:%s" % iterify(getprop(self.provider_data, prop,
                                                   True))[-1])
        if object_url:
            self.mapped_data.update({"object": object_url})

    def map_multiple_fields(self):
        # The same mapping applies for both genre, format and type**
        for dest in ["format", "genre", "type"]:
            self.map_format_genre_type(dest)

