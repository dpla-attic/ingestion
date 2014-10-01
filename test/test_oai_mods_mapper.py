from nose.tools import assert_equals
from dplaingestion.mappers.oai_mods_mapper import OAIMODSMapper


def test_map_subject_spatial_and_temporal():
    provider_data = {
        'subject': {
            'temporal': '1901-1910'
        }
    }
    m = OAIMODSMapper(provider_data)
    m.map_subject_spatial_and_temporal()
    expected = {
        'sourceResource': {
            'subject': [''],
            'temporal': ['1901-1910']
        }
    }
    assert_equals(expected, m.mapped_data)
