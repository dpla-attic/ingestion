from dplaingestion.mappers.marc_mapper import *                                      

class HathiMapper(MARCMapper):                                                       
    def __init__(self, data):
        super(HathiMapper, self).__init__(data)

        self.mapping_dict.update({
            lambda t: t == "973": [(self.map_provider, "ab")],
            lambda t: t == "974": [(self.map_data_provider, "u"),
                                   (self.map_rights, "r")],
            lambda t: t == "970": [(self.map_type_and_spec_type, "a")]
        })

        self.data_provider_mapping = {
            "bc": "Boston College",
            "chi": "University of Chicago",
            "coo": "Cornell University",
            "dul1": "Duke University",
            "gri": "Getty Research Institute",
            "hvd": "Harvard University",
            "ien": "Northwestern University",
            "inu": "Indiana University",
            "loc": "Library of Congress",
            "mdl": "Minnesota Digital Library",
            "mdp": "University of Michigan",
            "miua": "University of Michigan",
            "miun": "University of Michigan",
            "nc01": "University of North Carolina",
            "ncs1": "North Carolina State University",
            "njp": "Princeton University",
            "nnc1": "Columbia University",
            "nnc2": "Columbia University",
            "nyp": "New York Public Library",
            "psia": "Penn State University",
            "pst": "Penn State University",
            "pur1": "Purdue University",
            "pur2": "Purdue University",
            "uc1": "University of California",
            "uc2": "University of California",
            "ucm": "Universidad Complutense de Madrid",
            "ufl1": "University of Florida",
            "uiug": "University of Illinois",
            "uiuo": "University of Illinois",
            "umn": "University of Minnesota",
            "usu": "Utah State University Press",
            "uva": "University of Virginia",
            "wu": "University of Wisconsin",
            "yale": "Yale University"
        }

        self.rights_desc = {
            "pd":       "Public domain",
            "ic-world": "In-copyright and permitted as world viewable by "
                        "the copyright holder",
            "pdus": "Public domain only when viewed in the US",
            "cc-by": "Creative Commons Attribution license",
            "cc-by-nd": "Creative Commons Attribution-NoDerivatives license",
            "cc-by-nc-nd": "Creative Commons "
                           "Attribution-NonCommercial-NoDerivatives license",
            "cc-by-nc":    "Creative Commons Attribution-NonCommercial "
                           "license",
            "cc-by-nc-sa": "Creative Commons "
                           "Attribution-NonCommercial-ShareAlike license",
            "cc-by-sa": "Creative Commons Attribution-ShareAlike license",
            "cc-zero": "Creative Commons Zero license (implies pd)",
            "und-world": "undetermined copyright status and permitted as "
                         "world viewable by the depositor"
        }

    def map_provider(self, _dict, tag, codes):
        values = self._get_values(_dict, codes)
        if "HT" in values and "avail_ht" in values:
            provider = {
                "@id": "http://dp.la/api/contributor/hathitrust",
                "name": "HathiTrust"
            }
            setprop(self.mapped_data, "provider", provider)

    def map_data_provider(self, _dict, tag, codes):
        data_provider = []
        for v in self._get_values(_dict, codes):
            namespace = v.split(".")[0]
            data_provider.append(self.data_provider_mapping.get(namespace))

        data_provider = filter(None, data_provider)
        if data_provider:
            setprop(self.mapped_data, "dataProvider", data_provider)

    def map_rights(self, _dict, tag, codes):
        values = self._get_values(_dict, codes)
        code = values[0]
        try:
            rights = self.rights_desc[code]
            rights += ". Learn more at http://www.hathitrust.org/access_use"
            setprop(self.mapped_data, "sourceResource/rights", rights)
        except KeyError as e:
            logger.warning("Unacceptable rights code for %s: %s" %
                           (self.provider_data["_id"], e.message))
        except:
            logger.error("Could not get rights from %s" %
                         self.provider_data["_id"])

    def update_is_shown_at(self):
        prop = "sourceResource/identifier"
        if exists(self.mapped_data, prop):
            for v in iterify(getprop(self.mapped_data, prop)):
                if v.startswith("Hathi: "):
                    _id = v.split("Hathi: ")[-1]
                    is_shown_at = "http://catalog.hathitrust.org/Record/%s" % \
                                  _id
                    setprop(self.mapped_data, "isShownAt", is_shown_at)
                    break
            
    def add_identifier(self, value):
        prop = "sourceResource/identifier"
        identifier = self._get_mapped_value(prop)
        identifier.append("Hathi: " + value)
        setprop(self.mapped_data, prop, identifier)
