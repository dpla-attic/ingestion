from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.qdc_mapper import QDCMapper

class MarylandMapper(QDCMapper):

    def __init__(self, provider_data):
        super(MarylandMapper, self).__init__(provider_data)

