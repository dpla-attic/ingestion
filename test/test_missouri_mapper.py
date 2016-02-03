from nose.tools import assert_equals, nottest
from dplaingestion.mappers.missouri_mapper import MissouriMapper

empty_result = {'sourceResource': {}}

def test_map_data_provider():
    """MissouriMapper gets dataProvider from note"""
    # Normal case
    provider_data = {
        'note': [
            u'Here is the description ...',
            {
                u'#text': u'Washington University in St. Louis',
                u'type': u'ownership'
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_data_provider()
    expected = {
        'sourceResource': {},
        'dataProvider': u'Washington University in St. Louis'
    }
    assert_equals(expected, mm.mapped_data)
    # Empty ownership note:
    #     <note>Description here...</note>
    #     <note type="ownership"></note>
    provider_data = {
        'note': [
                 u'Description here...',
                 {u'type': 'ownership'}
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_data_provider()
    assert_equals(empty_result, mm.mapped_data)

def test_map_creator():
    """MissouriMapper gets sourceResource.creator from name"""
    # Normal case
    provider_data = {
        'name': {
            'namePart': 'John Kern', 
            'role': {
                'roleTerm': {'type': 'text', '#text': 'creator'}
            }
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_creator()
    assert_equals({'sourceResource': {'creator': ['John Kern']}},
                  mm.mapped_data)
    # Theoretical case of no role element
    provider_data = {
        'name': {
            'namePart': 'John Kern'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_creator()
    assert_equals(empty_result, mm.mapped_data)
    # Theoretical case of other role
    provider_data = {
        'name': {
            'namePart': 'John Kern',
            'role': {
                'roleTerm': {'type': 'text', '#text': 'something else'}
            }
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_creator()
    assert_equals(empty_result, mm.mapped_data)
    # Multiple creators
    provider_data = {
        'name': [
            {
                'namePart': 'Creator One',
                'role': {
                    'roleTerm': {'type': 'text', '#text': 'creator'}
                }
            },
            {
                'namePart': 'Creator Two',
                'role': {
                    'roleTerm': {'type': 'text', '#text': 'creator'}
                }
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_creator()
    expected = {'sourceResource': {'creator': ['Creator One', 'Creator Two']}}
    assert_equals(expected, mm.mapped_data)

def test_map_date():
    """MissouriMapper gets date from originInfo"""
    # Normal cases
    provider_data = {
        'originInfo': [
            {
                'publisher': {'#text': 'Some Publisher'}
            },
            {
                'dateOther': {'#text': '1963-09-22'}
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_date()
    assert_equals({'sourceResource': {'date': '1963-09-22'}}, mm.mapped_data)
    provider_data = {
        'originInfo': [
            {
                'publisher': {'#text': 'Some Publisher'}
            },
            {
                'dateCreated': {'keyDate': 'yes', '#text': '1963-09-22'}
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_date()
    assert_equals({'sourceResource': {'date': '1963-09-22'}}, mm.mapped_data)
    provider_data = {
        'originInfo': [
            {
                'publisher': {'#text': 'Some Publisher'}
            },
            {
                'dateIssued': {'point': 'start', '#text': '1963-09-22'}
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_date()
    assert_equals({'sourceResource': {'date': '1963-09-22'}}, mm.mapped_data)
    # Empty element w/out text node?
    provider_data = {
        'originInfo': [
            {
                'publisher': {'#text': 'Some Publisher'}
            },
            {
                'dateIssued': {'point': 'start'}
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_date()
    assert_equals({'sourceResource': {}}, mm.mapped_data)
    # there is a list of a possible date element
    provider_data = {
        'originInfo': {
            'dateCreated': [
                {
                    'point': 'end',
                    '#text': '1904-12-31',
                    'encoding': 'iso8601'
                },
                {
                    'point': 'start',
                    'keyDate': 'yes',
                    '#text': '1904-01-01',
                    'encoding': 'iso8601'
                }
            ]
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_date()
    assert_equals({'sourceResource': {'date': '1904-01-01'}}, mm.mapped_data)
    # sortDate is the only field available
    provider_data = {'originInfo': {'sortDate': '2013-05-03'}}
    mm = MissouriMapper(provider_data)
    mm.map_date()
    assert_equals({'sourceResource': {'date': '2013-05-03'}}, mm.mapped_data)

def test_map_description():
    """MissouriMapper gets description from first note string"""
    provider_data = {
        'note': [
            'Here is the description.',
            {
              '#text': 'Washington University in St. Louis',
              'type': 'ownership'
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_description()
    expected = {
        'sourceResource': {
            'description': ['Here is the description.']
        }
    }
    assert_equals(expected, mm.mapped_data)
    provider_data = {
        'note': [
            {
                '#text': 'Washington University in St. Louis',
                'type': 'ownership'
            },
            {
                '#text': 'The description.',
                'type': 'content'
            },
            'Secondary description.'
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_description()
    expected = {
        'sourceResource': {
            'description': ['The description.', 'Secondary description.']
        }
    }
    assert_equals(expected, mm.mapped_data)

@nottest
def test_map_extent():
    """MissouriMapper gets extent from physicalDescription"""
    # Normal case
    provider_data = {
        'physicalDescription': {
            'extent': '285 pages',
            'xmlns:default': 'http://www.loc.gov/mods/v3'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_extent()
    assert_equals({'sourceResource': {'extent': '285 pages'}}, mm.mapped_data)
    # physicalDescription could have no 'extent' property.  Real example:
    # "physicalDescription": {
    #     "xmlns:default": "http://www.loc.gov/mods/v3"
    # }
    provider_data = {
        'physicalDescription': {
            'xmlns:default': 'http://www.loc.gov/mods/v3'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_extent()
    assert_equals({'sourceResource': {}}, mm.mapped_data)

def test_map_format():
    """MissouriMapper gets format from physicalDescription or genre"""
    provider_data = {
        'genre': {
            '#text': 'book',
            'xmlns:default': 'http://www.loc.gov/mods/v3'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_format()
    assert_equals({'sourceResource': {'format': ['book']}}, mm.mapped_data)
    provider_data = {
        'physicalDescription': {
            'note': 'book'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_format()
    assert_equals({'sourceResource': {'format': ['book']}}, mm.mapped_data)
    # Not sure if something like this will ever be the case ...
    provider_data = {
        'physicalDescription': [
            {'note': 'book'},
            {'note': 'scrapbook'}
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_format()
    expected = {'sourceResource': {'format': ['book', 'scrapbook']}}
    assert_equals(expected, mm.mapped_data)

def test_map_is_shown_at():
    """MissouriMapper gets isShownAt from location url for object in context"""
    provider_data = {
        'location': {
            'url': [
                {
                    '#text': 'http://digital.wustl.edu/example',
                    'access': 'object in context'
                },
                {
                    '#text': 'http://digital.wustl.edu/example.gif',
                    'access': 'preview'
                },
                'Bogus value to test robustness',
                {
                    '#text': 'another hypothetical test',
                    'something': 'test'
                }
            ]
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_is_shown_at()
    expected = {
        'sourceResource': {},
        'isShownAt': 'http://digital.wustl.edu/example'
    }
    assert_equals(expected, mm.mapped_data)
    provider_data = {
        'location': [
            {
                'xmlns:default': 'http://www.loc.gov/mods/v3',
                'url': 'http://fraser.stlouisfed.org/publication/?pid=1272'
            },
            {
                'url': {
                    '#text': 'http://fraser.stlouisfed.org/example',
                    'access': 'object in context'
                }
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_is_shown_at()
    expected = {
        'sourceResource': {},
        'isShownAt': 'http://fraser.stlouisfed.org/example'
    }
    assert_equals(expected, mm.mapped_data)

def test_map_object():
    """MissouriMapper gets object from location url for preview"""
    provider_data = {
        'location': {
            'url': [
                {
                    '#text': 'http://digital.wustl.edu/example',
                    'access': 'object in context'
                },
                {
                    '#text': 'http://digital.wustl.edu/example.gif',
                    'access': 'preview'
                },
                'Bogus value to test robustness',
                {
                    '#text': 'another hypothetical test',
                    'something': 'test'
                }
            ]
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_object()
    expected = {
        'sourceResource': {},
        'object': 'http://digital.wustl.edu/example.gif'
    }
    assert_equals(expected, mm.mapped_data)


def test_map_rights():
    """MissouriMapper gets rights from accessCondition"""
    provider_data = {'accessCondition': 'Copyright msg'}
    mm = MissouriMapper(provider_data)
    mm.map_rights()
    assert mm.mapped_data == {'sourceResource': {'rights': ['Copyright msg']}}
    provider_data =  {
        'accessCondition': [
            'http://digital.wustl.edu/',
            'data are freely accessible'
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_rights()
    expected = {
        'sourceResource': {
            'rights': [
                'http://digital.wustl.edu/',
                'data are freely accessible'
            ]
        }
    }
    assert_equals(expected, mm.mapped_data)

def test_map_type():
    """MissouriMapper gets type from typeOfResource"""
    provider_data = {'typeOfResource': {'#text': 'the type'}}
    mm = MissouriMapper(provider_data)
    mm.map_type()
    assert_equals({'sourceResource': {'type': ['the type']}}, mm.mapped_data)

def test_map_language():
    """MissouriMapper gets language from language or languageTerm"""
    provider_data = {
        'language': {
            'languageTerm': 'jpn'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_language()
    assert_equals({'sourceResource': {'language': ['jpn']}}, mm.mapped_data)
    provider_data = {
        'language': {
            'languageTerm': 'jpn; eng'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_language()
    assert_equals({'sourceResource': {'language': ['jpn', 'eng']}},
                  mm.mapped_data)

def test_map_identifier():
    """MissouriMapper maps identifier"""
    provider_data = {
        'identifier': [
          {
            '#text': '11504366',
            'type': 'oclc',
            'xmlns:default': 'http://www.loc.gov/mods/v3'
          },
          {
            '#text': ' 27303118',
            'type': 'oclc',
            'xmlns:default': 'http://www.loc.gov/mods/v3'
          }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_identifier()
    expected = {
        'sourceResource': {
            'identifier': ['OCLC:11504366', 'OCLC:27303118']
        }
    }
    assert_equals(expected, mm.mapped_data)
    provider_data = {'identifier': '1234'}
    mm = MissouriMapper(provider_data)
    mm.map_identifier()
    assert_equals({'sourceResource': {'identifier': ['1234']}}, mm.mapped_data)

def test_map_publisher():
    """MissourMapper gets publisher from originInfo"""
    provider_data = {
        'originInfo': [
          {'publisher': 'Publisher One'},
          {'publisher': 'Publisher Two'},
          {'someOtherField': '...'}
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_publisher()
    expected = {
        'sourceResource': {
            'publisher': ['Publisher One', 'Publisher Two']
        }
    }
    assert_equals(expected, mm.mapped_data)
    provider_data = {
        "originInfo": {
            "publisher": "Hong Kong: Longmen shudian,"
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_publisher()
    expected = {
        'sourceResource': {
            'publisher': ['Hong Kong: Longmen shudian']
        }
    }
    assert_equals(expected, mm.mapped_data)
    provider_data = {
        'originInfo': {
            'publisher': '\n\n \n\n'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_publisher()
    expected = {'sourceResource': {}}
    assert_equals(expected, mm.mapped_data)
    # Hypothetical:
    provider_data = {
        'originInfo': {
            'publisher': {'#text': 'The Publisher', 'someattr': 'abc'}
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_publisher()
    expected = {
        'sourceResource': {
            'publisher': ['The Publisher']
        }
    }
    assert_equals(expected, mm.mapped_data)

def test_map_spec_type():
    """MissouriMapper gets specType from genre"""
    # A valid value is captured
    provider_data = {
        'genre': {
            '#text': 'Photograph/Pictorial Works',
            'authority': 'dct'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_spec_type()
    expected = {
        'sourceResource': {
            'specType': ['Photograph/Pictorial Works']
        }
    }
    assert_equals(expected, mm.mapped_data)
    # An invalid value is filtered out
    provider_data = {
        'genre': {
            '#text': 'image',
            'authority': 'dct'
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_spec_type()
    assert_equals(empty_result, mm.mapped_data)

def test_map_relation():
    """MissouriMapper gets relation from relatedItem list"""
    provider_data = {
        'relatedItem': [
            {
                'location': {'url': 'http://fraser.stlouisfed.org/example/'},
                'titleInfo': {'title': 'Title One'}
            },
            {
                'titleInfo': {'title': 'Title Two\n'}
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_relation()
    expected = {
        'sourceResource': {
            'relation': [
                'http://fraser.stlouisfed.org/example/',
                'Title One',
                'Title Two'
            ]
        }
    }
    assert_equals(expected, mm.mapped_data)

def test_map_subject():
    """MissouriMapper maps subject"""
    provider_data = {
        "subject": [
            {"topic": "Missouri--St. Louis"},
            {"topic": "Data and Statistical Publications"},
            {"topic": "Subject One; Subject Two"},
            {
                "recordInfo": {
                    "recordIdentifier": "42",
                    "xmlns:default": "http://www.loc.gov/mods/v3"
                },
                "theme": {
                    "#text": "Bureau of Labor Statistics Publications",
                    "xmlns:default": "http://www.loc.gov/mods/v3"
                }
            },
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_subject()
    expected = {
        'sourceResource': {
            'subject': [
                'Missouri--St. Louis',
                'Data and Statistical Publications',
                'Subject One',
                'Subject Two',
                'Bureau of Labor Statistics Publications'
            ]
        }
    }
    assert_equals(expected, mm.mapped_data)

def test_map_has_view():
    provider_data = {
        'physicalDescription': {
            'internetMediaType': 'image/JPEG2000'
        },
        'location': {
            'url': [
                {
                    '#text': 'http://digital.wustl.edu/example',
                    'access': 'object in context'
                }
            ]
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_has_view()
    expected = {
        'sourceResource': {},
        'hasView': {
            '@id': 'http://digital.wustl.edu/example',
            'format': 'image/JPEG2000'
        }
    }
    assert_equals(expected, mm.mapped_data)
    provider_data = {
        'physicalDescription': [
            {'something': 'Whatever it is'},
            {'internetMediaType': 'image/JPEG2000'}
        ],
        'location': {
            'url': [
                {
                    '#text': 'http://digital.wustl.edu/example',
                    'access': 'object in context'
                }
            ]
        }
    }
    mm = MissouriMapper(provider_data)
    mm.map_has_view()
    expected = {
        'sourceResource': {},
        'hasView': {
            '@id': 'http://digital.wustl.edu/example',
            'format': 'image/JPEG2000'
        }
    }
    assert_equals(expected, mm.mapped_data)

def test_map_title():
    """MissouriMapper deals with one or many title elements"""
    provider_data = {
        'titleInfo': [
            {
                'title': 'One'
            },
            {
                'title': 'Two'
            },
            {
                'title': 'Three'
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_title()
    expected = {'sourceResource': {'title': ['One', 'Two', 'Three']}}
    assert_equals(expected, mm.mapped_data)
    provider_data = {'titleInfo': {'title': 'The title'}}
    mm = MissouriMapper(provider_data)
    mm.map_title()
    expected = {'sourceResource': {'title': ['The title']}}
    assert_equals(expected, mm.mapped_data)

def test_map_spatial():
    """MissouriMapper assigns spatial from //subject/hierarchicalGeographic"""
    provider_data = {
        'subject': [
            {
                'topic': {
                    'xmlns:php': 'http://php.net/xsl',
                    '#text': 'St. Louis Street Scenes'
                }
            },
            {
                'hierarchicalGeographic': {
                    'country': 'United States',
                    'state': 'MO',
                    'continent': 'North America'
                },
                'cartographics': {
                    'coordinates': '38.2589,-92.4366'
                }
            },
            {
                'hierarchicalGeographic': {
                    'country': 'United States',
                    'state': 'MO',
                    'continent': 'North America',
                    'city': 'St Louis'
                },
                'cartographics': {
                    'coordinates': '38.6277,-90.1995'
                }
            },
            {
                'hierarchicalGeographic': {
                    'continent': 'North America'
                }
            }
        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_spatial()
    expected = {
        'sourceResource': {
            'spatial': [
                {
                    'country': 'United States',
                    'state': 'MO',
                },
                '38.2589,-92.4366',
                {
                    'country': 'United States',
                    'state': 'MO',
                    'city': 'St Louis'
                },
                '38.6277,-90.1995',
                {
                    'name': 'North America'
                }
            ]
        }
    }
    assert_equals(expected, mm.mapped_data)

def test_map_temporal():
    """MissouriMapper assigns temporal from subject"""
    provider_data = {
        'subject': [
            {
                'topic': {
                    'xmlns:php': 'http://php.net/xsl',
                    '#text': 'Non-temporal element'
                }
            },
            {
                'temporal': {
                    'xmlns:php': 'http://php.net/xsl',
                    '#text': '2014'
                }
            },
            {
                'temporal': 'Early 21st Century'
            }

        ]
    }
    mm = MissouriMapper(provider_data)
    mm.map_temporal()
    expected = {
        'sourceResource': {
            'temporal': ['2014', 'Early 21st Century']
        }
    }
    assert_equals(expected, mm.mapped_data)
