from dplaingestion.selector import exists, getprop
from dplaingestion.mappers.dublin_core_mapper import DublinCoreMapper
from dplaingestion.utilities import iterify
from dplaingestion.selector import exists, setprop
from urlparse import urlparse
from akara import logger


class PAMapper(DublinCoreMapper):

    dcmi_types = {
        "Collection",
        "Dataset",
        "Event",
        "Image",
        "InteractiveResource",
        "MovingImage",
        "PhysicalObject",
        "Service",
        "Software",
        "Sound",
        "StillImage",
        "Text"
    }

    def __init__(self, provider_data):
        super(PAMapper, self).__init__(provider_data)

    def map_relation(self):
        prop = "relation"
        if exists(self.provider_data, prop):
            relations = iterify(self.provider_data.get(prop))
            self.update_source_resource({"collection": {"title": relations[0]}})
            if len(relations) > 1:
                self.update_source_resource({"relation": relations[1:]})

    def map_rights(self):
        prop = "rights"
        if exists(self.provider_data, prop):
            rights_uri = ""
            rights = self.provider_data.get(prop)
            try:
                if rights.startswith("http"):
                    rights_uri = urlparse(rights).geturl()
            except Exception as e:
                logger.warn("Unable to parse rights URI: %s\n%s" % (rights, e))

            if rights_uri:
                self.mapped_data.update({"rights": rights_uri})
            else:
                self.update_source_resource({"rights": rights})

    def map_type(self):
        prop = "type"
        if exists(self.provider_data, prop):
            types = iterify(self.provider_data.get(prop))
            non_dcmi_types = [type for type in types if type not in self.dcmi_types]
            if len(non_dcmi_types) > 0:
                self.update_source_resource({"format": non_dcmi_types})
            dcmi_types = [type for type in types if type in self.dcmi_types]
            if (len(dcmi_types)) > 0:
                self.update_source_resource({"type": dcmi_types})

    def map_spatial(self):
        prop = "coverage"
        if exists(self.provider_data, prop):
            coverage = self.provider_data.get(prop)
            self.update_source_resource({"spatial": coverage})

    def map_contributor(self):
        prop = "contributor"
        if exists(self.provider_data, prop):
            contributors = iterify(self.provider_data.get(prop))
            setprop(self.mapped_data, "dataProvider", contributors[-1])
            if len(contributors) > 1:
                self.update_source_resource({"contributor": contributors[:-1]})

    def map_identifier(self):
        """
        * If there are three identifiers the last identifier is the <object>
        and the second is <isShownAt>
        * If there are two identifier, the last is (or, still, the second) is
        <isShownAt>
        :return:
        """
        prop = "handle"
        if exists(self.provider_data, prop):
            identifiers = iterify(getprop(self.provider_data, prop))
            if len(identifiers) >= 2:
                setprop(self.mapped_data, "object", identifiers[-1])
            if len(identifiers) > 2:
                setprop(self.mapped_data, "isShownAt", identifiers[1])

    def map_subject(self):
        prop = "subject"
        subject = []

        if exists(self.provider_data, prop):
            s = getprop(self.provider_data, prop)
            if isinstance(s, dict):
                subject.append(s.get("#text"))
            elif isinstance(s, list):
                subject += s
            else:
                subject.append(s)
        subject = filter(None, subject)

        if subject:
            self.update_source_resource({"subject": subject})

    def map_intermediate_provider(self):
        prop = "source"
        if exists(self.provider_data, prop):
            setprop(self.mapped_data, "intermediateProvider", getprop(self.provider_data, prop))

    def map_is_shown_at(self):
        """
        Override map_is_shown_at to do nothing because it is incorrectly
        implemented for PA in dublin_core_mapper. isShownAt is mapped in
        map_identifier
        """
        pass
