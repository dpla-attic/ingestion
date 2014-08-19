from dplaingestion.selector import setprop
from dplaingestion.mappers.marc_mapper import MARCMapper

class UFLMARCMapper(MARCMapper):                                                       
    def __init__(self, provider_data):                                                   
        super(UFLMapper, self).__init__(provider_data)

    def is_description_tag(self, tag):
        return tag.startswith("5") and tag not in ("510", "533", "535", "538")
