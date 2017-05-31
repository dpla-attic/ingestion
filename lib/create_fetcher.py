from amara.thirdparty import json

def create_fetcher(profile_path, uri_base, config_file):
    """
    Given a fetcher type, creates, imports, and instantiates the appropriate
    Fetcher subclass.
    """

    def _create_ia_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.ia_fetcher import IAFetcher
        return IAFetcher(profile, uri_base, config_file)

    def _create_uva_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.uva_fetcher import UVAFetcher
        return UVAFetcher(profile, uri_base, config_file)

    def _create_nypl_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.nypl_fetcher import NYPLFetcher
        return NYPLFetcher(profile, uri_base, config_file)

    def _create_nara_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.nara_fetcher import NARAFetcher
        return NARAFetcher(profile, uri_base, config_file)

    def _create_edan_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.edan_fetcher import EDANFetcher
        return EDANFetcher(profile, uri_base, config_file)

    def _create_mwdl_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.mwdl_fetcher import MWDLFetcher
        return MWDLFetcher(profile, uri_base, config_file)

    def _create_getty_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.getty_fetcher import GettyFetcher
        return GettyFetcher(profile, uri_base, config_file)

    def _create_hathi_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.hathi_fetcher import HathiFetcher
        return HathiFetcher(profile, uri_base, config_file)

    def _create_oai_verbs_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.oai_verbs_fetcher import OAIVerbsFetcher
        return OAIVerbsFetcher(profile, uri_base, config_file)

    def _create_mdl_api_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.mdl_api_fetcher import MDLAPIFetcher
        return MDLAPIFetcher(profile, uri_base, config_file)

    def _create_cdl_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.cdl_fetcher import CDLFetcher
        return CDLFetcher(profile, uri_base, config_file)

    def _create_loc_fetcher(profile, uri_base, config_file):
        from dplaingestion.fetchers.loc_fetcher import LOCFetcher
        return LOCFetcher(profile, uri_base, config_file)

    fetchers = {
        'ia':           lambda p, u, c: _create_ia_fetcher(p, u, c),
        'uva':          lambda p, u, c: _create_uva_fetcher(p, u, c),
        'nypl':         lambda p, u, c: _create_nypl_fetcher(p, u, c),
        'nara':         lambda p, u, c: _create_nara_fetcher(p, u, c),
        'edan':         lambda p, u, c: _create_edan_fetcher(p, u, c),
        'mwdl':         lambda p, u, c: _create_mwdl_fetcher(p, u, c),
        'getty':        lambda p, u, c: _create_getty_fetcher(p, u, c),
        'hathi':        lambda p, u, c: _create_hathi_fetcher(p, u, c),
        'oai_verbs':    lambda p, u, c: _create_oai_verbs_fetcher(p, u, c),
        'mdl':          lambda p, u, c: _create_mdl_api_fetcher(p, u, c),
        'cdl':          lambda p, u, c: _create_cdl_fetcher(p, u, c),
        'loc':          lambda p, u, c: _create_loc_fetcher(p, u, c)
    }

    with open(profile_path, "r") as f:
        profile = json.load(f)
    type = profile.get("type")

    return fetchers.get(type)(profile, uri_base, config_file)
