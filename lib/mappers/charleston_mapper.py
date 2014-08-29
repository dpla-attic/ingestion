"""
SCDL Charleston Mapper
"""


from dplaingestion.mappers.scdl_mapper import SCDLMapper


class SCDLCharlestonMapper(SCDLMapper):

    def map_object(self):
        """Map .object, based on the record header identifier"""
        # _id includes the value of what was //header/identifier in the
        # OAI feed, and reliably gives the identifier that's needed to
        # construct the thumbnail url.
        id_prop = self.provider_data['_id']
        identifier = id_prop.split('/')[-1]
        obj_url = 'http://fedora.library.cofc.edu:8080' \
                  '/fedora/objects/%s/datastreams' \
                  '/THUMB1/content' % identifier
        self.mapped_data.update({'object': obj_url})
