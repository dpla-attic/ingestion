from akara import logger
from amara.lib.iri import is_absolute
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.dublin_core_mapper import DublinCoreMapper
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists

class PAMapper(DublinCoreMapper):

    def __init__(self, provider_data):
        super(PAMapper, self).__init__(provider_data)
