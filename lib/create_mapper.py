def create_mapper(mapper_type, data):
    """
    Given a mapper_type, creates imports and instanstiates the appropriate
    Mapper class with the given data.
    """

    def _create_ia_mapper(data):
        from dplaingestion.mappers.ia_mapper import IAMapper
        return IAMapper(data)

    def _create_bpl_mapper(data):
        from dplaingestion.mappers.bpl_mapper import BPLMapper
        return BPLMapper(data)

    def _create_uva_mapper(data):
        from dplaingestion.mappers.uva_mapper import UVAMapper
        return UVAMapper(data)

    def _create_mdl_mapper(data):
        from dplaingestion.mappers.mdl_mapper import MDLMapper
        return MDLMapper(data)

    def _create_cdl_json_mapper(data):
        from dplaingestion.mappers.cdl_json_mapper import CDLJSONMapper
        return CDLJSONMapper(data)

    def _create_mapv3_json_mapper(data):
        from dplaingestion.mappers.mapv3_json_mapper import MAPV3JSONMapper
        return MAPV3JSONMapper(data)

    def _create_mdl_json_mapper(data):
        from dplaingestion.mappers.mdl_json_mapper import MDLJSONMapper
        return MDLJSONMapper(data)

    def _create_gpo_mapper(data):
        from dplaingestion.mappers.gpo_mapper import GPOMapper
        return GPOMapper(data)

    def _create_scdl_mapper(data):
        from dplaingestion.mappers.scdl_mapper import SCDLMapper
        return SCDLMapper(data)

    def _create_edan_mapper(data):
        from dplaingestion.mappers.edan_mapper import EDANMapper
        return EDANMapper(data)

    def _create_nara_mapper(data):
        from dplaingestion.mappers.nara_mapper import NARAMapper
        return NARAMapper(data)

    def _create_nypl_mapper(data):
        from dplaingestion.mappers.nypl_mapper import NYPLMapper
        return NYPLMapper(data)

    def _create_untl_mapper(data):
        from dplaingestion.mappers.untl_mapper import UNTLMapper
        return UNTLMapper(data)

    def _create_uiuc_mapper(data):
        from dplaingestion.mappers.uiuc_mapper import UIUCMapper
        return UIUCMapper(data)

    def _create_mwdl_mapper(data):
        from dplaingestion.mappers.mwdl_mapper import MWDLMapper
        return MWDLMapper(data)

    def _create_hathi_mapper(data):
        from dplaingestion.mappers.hathi_mapper import HathiMapper
        return HathiMapper(data)

    def _create_getty_mapper(data):
        from dplaingestion.mappers.getty_mapper import GettyMapper
        return GettyMapper(data)

    def _create_harvard_mapper(data):
        from dplaingestion.mappers.harvard_mapper import HarvardMapper
        return HarvardMapper(data)

    def _create_digitalnc_mapper(data):
        from dplaingestion.mappers.digitalnc_mapper import DigitalNCMapper
        return DigitalNCMapper(data)

    def _create_uiuc_marc_mapper(data):
        from dplaingestion.mappers.uiuc_marc_mapper import UIUCMARCMapper
        return UIUCMARCMapper(data)

    def _create_dublin_core_mapper(data):
        from dplaingestion.mappers.dublin_core_mapper import DublinCoreMapper
        return DublinCoreMapper(data)

    def _create_missouri_mapper(data):
        from dplaingestion.mappers.missouri_mapper import MissouriMapper
        return MissouriMapper(data)

    def _create_in_mapper(data):
        from dplaingestion.mappers.in_mapper import INMapper
        return INMapper(data)

    def _create_il_mapper(data):
        from dplaingestion.mappers.il_mapper import ILMapper
        return ILMapper(data)
      
    def _create_mi_mapper(data):
        from dplaingestion.mappers.michigan_mapper import MichiganMapper
        return MichiganMapper(data)
    
    def _create_pa_mapper(data):
        from dplaingestion.mappers.pa_mapper import PAMapper
        return PAMapper(data)

    def _create_esdn_mapper(data):
        from dplaingestion.mappers.esdn_mapper import EsdnMapper
        return EsdnMapper(data)

    def _create_wi_mapper(data):
        from dplaingestion.mappers.wisconsin_mapper import WIMapper
        return WIMapper(data)

    def _create_bhl_mapper(data):
        from dplaingestion.mappers.bhl_mods import BHLMapper
        return BHLMapper(data)

    def _create_pa_mapper(data):
        from dplaingestion.mappers.pa_mapper import PAMapper
        return PAMapper(data)

    mappers = {
        'ia':           lambda d: _create_ia_mapper(d),
        'bpl':          lambda d: _create_bpl_mapper(d),
        'uva':          lambda d: _create_uva_mapper(d),
        'mdl':          lambda d: _create_mdl_mapper(d),
        'gpo':          lambda d: _create_gpo_mapper(d),
        'scdl':         lambda d: _create_scdl_mapper(d),
        'edan':         lambda d: _create_edan_mapper(d),
        'nara':         lambda d: _create_nara_mapper(d),
        'nypl':         lambda d: _create_nypl_mapper(d),
        'untl':         lambda d: _create_untl_mapper(d),
        'uiuc':         lambda d: _create_uiuc_mapper(d),
        'mwdl':         lambda d: _create_mwdl_mapper(d),
        'hathi':        lambda d: _create_hathi_mapper(d),
        'getty':        lambda d: _create_getty_mapper(d),
        'harvard':      lambda d: _create_harvard_mapper(d),
        'digitalnc':    lambda d: _create_digitalnc_mapper(d),
        'uiuc_marc':    lambda d: _create_uiuc_marc_mapper(d),
        'dublin_core':  lambda d: _create_dublin_core_mapper(d),
        'missouri':     lambda d: _create_missouri_mapper(d),
        'mapv3_json':   lambda d: _create_mapv3_json_mapper(d),
        'mdl_json':     lambda d: _create_mdl_json_mapper(d),
        'cdl_json':     lambda d: _create_cdl_json_mapper(d),
        'in':           lambda d: _create_in_mapper(d),
        'il':           lambda d: _create_il_mapper(d),
        'mi':           lambda d: _create_mi_mapper(d),
        'esdn':         lambda d: _create_esdn_mapper(d),
        'wisconsin':    lambda d: _create_wi_mapper(d),
        'bhl':          lambda d: _create_bhl_mapper(d),
        'mi':           lambda d: _create_mi_mapper(d),
        'pa':           lambda d: _create_pa_mapper(d)
    }

    return mappers.get(mapper_type)(data)
