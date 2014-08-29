from dplaingestion.mappers.charleston_mapper import SCDLCharlestonMapper


def test_map_object():
    """SCDLCharlestonMapper derives object from ID"""
    provider_data = {
        '_id': 'scdl-charleston--http://lcdl.library.cofc.edu'
               '/lcdl/catalog/lcdl:4633'
    }
    cm = SCDLCharlestonMapper(provider_data)
    cm.map_object()
    expected = {
        'sourceResource': {},
        'object': 'http://fedora.library.cofc.edu:8080'
                  '/fedora/objects/lcdl:4633/datastreams/THUMB1/content'
    }
    assert cm.mapped_data == expected
