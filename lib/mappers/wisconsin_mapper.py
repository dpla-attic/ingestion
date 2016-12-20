from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper


class WIMapper(QDCMapper):
    def __init__(self, provider_data):
        super(WIMapper, self).__init__(provider_data)

    def map_data_provider(self):
        """//edm:dataProvider -> .dataProvider (required property)"""
        self.mapped_data.update(
            {'dataProvider': getprop(self.provider_data, 'dataProvider')})

    def map_is_shown_at(self):
        """//edm:isShownAt -> .isShownAt (required property)"""
        self.mapped_data.update(
            {'isShownAt': getprop(self.provider_data, 'isShownAt')})

    def map_collection(self):
        """//dct:isPartOf -> sourceResource.collection"""
        if exists(self.provider_data, 'isPartOf'):
            self.update_source_resource(
                {'collection': iterify(getprop(self.provider_data,
                                               'isPartOf'))})

    def map_format(self):
        """//dc:format or //dc:medium -> .sourceResource.format"""
        self.source_resource_prop_from_many('format', ('medium', 'format'))

    def map_spec_type(self):
        """//dc:format -> .sourceResource.specType (a.k.a. genre in MAPv4)"""
        if exists(self.provider_data, 'format'):
            self.update_source_resource(
                {'specType': iterify(getprop(self.provider_data, 'format'))})

    def map_identifier(self):
        """//dc:identifier -> .sourceResource.identifier"""
        self.source_resource_prop_to_prop('identifier')

    def map_publisher(self):
        """//dc:publisher -> .sourceResource.publisher"""
        self.source_resource_prop_to_prop('publisher')

    def map_rights(self):
        """//dc:rights or //dct:accessRights -> .sourceResource.rights
        (required property)
        """
        self.source_resource_prop_from_many('rights',
                                            ('rights', 'accessRights'))
