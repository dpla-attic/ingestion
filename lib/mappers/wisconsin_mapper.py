from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper
from dplaingestion.utilities import iterify
from dplaingestion.textnode import textnode

class WIMapper(QDCMapper):
    def __init__(self, provider_data):
        super(WIMapper, self).__init__(provider_data)

    def map_data_provider(self):
        """//edm:dataProvider -> .dataProvider (required property)"""
        data_provider = getprop(self.provider_data, 'dataProvider', True)
        if data_provider:
            self.mapped_data.update({'dataProvider': data_provider})


    def map_is_shown_at(self):
        """//edm:isShownAt -> .isShownAt (required property)"""
        is_shown_at = getprop(self.provider_data, 'isShownAt', True)
        if is_shown_at:
            self.mapped_data.update({'isShownAt': is_shown_at})

    def map_object(self):
        if exists(self.provider_data, 'preview'):
            self.mapped_data.update(
                {'object': getprop(self.provider_data, 'preview')})

    def map_collection(self):
        """//dct:isPartOf -> sourceResource.collection"""
        collection = []
        if exists(self.provider_data, 'isPartOf'):
            collection = [{'title': c} for c in
                          iterify(getprop(self.provider_data, 'isPartOf', []))]
        if collection:
            self.update_source_resource({"collection": collection})



    def map_format(self):
        """//dc:format or //dc:medium -> .sourceResource.format"""
        formats = []
        self.extend_collection(formats, 'medium')
        self.extend_collection(formats, 'format')
        if formats:
            self.update_source_resource({'format': formats})

    def map_identifier(self):
        """//dc:identifier -> .sourceResource.identifier"""
        if exists(self.provider_data, "dc:identifier"):
            identifiers = []
            for identifier in iterify(self.provider_data.get("identifier")):
                identifiers.append(textnode(identifier))
            identifiers = filter(None, identifiers)
            if identifiers:
                self.update_source_resource({"identifier": identifiers})

    def map_language(self):
        lang = []
        if exists(self.provider_data, "language"):
            for s in iterify(getprop(self.provider_data, "language")):
                lang.append(textnode(s))
            lang = filter(None, lang)

        if lang:
            self.update_source_resource({"language": lang})

    def map_publisher(self):
        """//dc:publisher -> .sourceResource.publisher"""
        if exists(self.provider_data, "publisher"):
            self.update_source_resource(
                {"publisher": self.provider_data.get("publisher")})

    def map_relation(self):
        """//relation -> .sourceResource.relation"""
        if exists(self.provider_data, "relation"):
            relation = []
            self.extend_collection(relation, "relation")
            if relation:
                self.update_source_resource({"relation": relation})

    def map_rights(self):
        """//dc:rights or //dct:accessRights -> .sourceResource.rights
        (required property)
        """
        rights = []
        self.extend_collection(rights, 'accessRights')
        self.extend_collection(rights, 'rights')
        self.extend_collection(rights, 'rightsHolder')
        if rights:
            self.update_source_resource({'rights': rights})

    def map_subject(self):
        subjects = []
        if exists(self.provider_data, "subject"):
            for s in iterify(getprop(self.provider_data, "subject")):
                if isinstance(s, dict):
                    subjects.append(s.get("#text"))
                else:
                    subjects.append(s)
                    subjects = filter(None, subjects)

        if subjects:
            self.update_source_resource({"subject": subjects})

    def map_type(self):
        types = []
        if exists(self.provider_data, "type"):
            for s in iterify(getprop(self.provider_data, "type")):
                if isinstance(s, dict):
                    types.append(s.get("#text"))
                else:
                    types.append(s)
                    types = filter(None, types)

        if types:
            self.update_source_resource({"type": types})

    def extend_collection(self, collection, prop):
        results = []
        if exists(self.provider_data, prop):
            for s in iterify(getprop(self.provider_data, prop)):
                if isinstance(s, dict):
                    results.append(s.get("#text"))
                elif isinstance(s, list):
                    for t in s:
                        results.append(t)
                else:
                    results.append(s)

        results = filter(None, results)

        if results:
            collection.extend(results)
