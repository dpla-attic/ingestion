from dplaingestion.mappers import bhl_mods
from mock import MagicMock
from nose.tools import assert_equals


def test_datestring_sort():
    """A list of DateStrings sorts with range values at the end"""
    dates = [bhl_mods.DateString('1910/1911'), bhl_mods.DateString('1999'),
             bhl_mods.DateString('1900-1901'), bhl_mods.DateString('2014')]
    assert sorted(dates) == [u'1999', u'2014', u'1900-1901', u'1910/1911']


def test_date_elements_date_issued():
    """Given a list of candidate dateIssued elements, only suitable ones are
    yielded.
    """
    elements = ['[1921]',
                {'point': 'start',
                 'encoding': 'marc',
                 '#text': '1921',
                 'keyDate': 'yes'},
                {'point': 'start',
                 'encoding': 'marc',
                 '#text': '1922'}]
    yielded = [e for e in bhl_mods._date_elements(elements, 'dateIssued')]
    assert_equals(yielded, [{'point': 'start',
                             'encoding': 'marc',
                             '#text': '1921',
                             'keyDate': 'yes'}])

def test_date_elements_date_other():
    """Given a list of candidate dateOther elements, only suitable ones are
    yielded.
    """
    elements = ['[1921]',
                {'point': 'start',
                 'encoding': 'marc',
                 '#text': '1921',
                 'keyDate': 'yes',
                 'type': 'issueDate'},
                {'point': 'start',
                 'encoding': 'marc',
                 '#text': '1922',
                 'keyDate': 'yes'},
                {'point': 'start',
                 'encoding': 'marc',
                 '#text': '1923'}]
    yielded = [e for e in bhl_mods._date_elements(elements, 'dateOther')]
    assert_equals(yielded, [{'point': 'start',
                             'encoding': 'marc',
                             '#text': '1921',
                             'keyDate': 'yes',
                             'type': 'issueDate'}])

def test_date_values():
    """Returns a list of date strings given a dict with the right properties"""
    # Note that it takes the first date-related XML element it comes across,
    # in this case, dateIssued.
    bare_date_str = "1908."
    date_issued_hash1 = {'point': 'start',
                        'encoding': 'marc',
                        '#text': '1908',
                        'keyDate': 'yes'}
    date_issued_hash2 = {'point': 'start',
                        'encoding': 'marc',
                        '#text': '1909',
                        'keyDate': 'yes'}
    incoming = {'dateIssued': [bare_date_str, date_issued_hash1,
                               date_issued_hash2],
                'dateOther': ['1900',
                              {'point': 'start',
                               'encoding': 'marc',
                               '#text': '1921',
                               'keyDate': 'yes',
                               'type': 'issueDate'}]}
    bhl_mods._date_elements = MagicMock(return_value=[date_issued_hash1,
                                                      date_issued_hash2])
    rv = bhl_mods._date_values(incoming)
    bhl_mods._date_elements.assert_called_with(
        [bare_date_str, date_issued_hash1, date_issued_hash2], 'dateIssued')
    assert_equals(rv, [u'1908', u'1909'])

def test_date_values_sorted():
    """Returns date values sorted with ranges last"""
    # I.e. values representing ranges like "2000-2001 or 2000/2001" come at the
    # end of the list.
    date_other_list = [{'point': 'start',
                        'encoding': 'marc',
                        '#text': '1901-1902',
                        'keyDate': 'yes',
                        'type': 'issueDate'},
                       {'point': 'start',
                        'encoding': 'marc',
                        '#text': '1903/1904',
                        'keyDate': 'yes',
                        'type': 'issueDate'},
                       {'point': 'start',
                        'encoding': 'marc',
                        '#text': '1910',
                        'keyDate': 'yes',
                        'type': 'issueDate'}]
    incoming = {'dateOther': date_other_list}
    bhl_mods._date_elements = MagicMock(return_value=date_other_list)
    rv = bhl_mods._date_values(incoming)
    bhl_mods._date_elements.assert_called_with(date_other_list, 'dateOther')
    assert_equals(rv, [u'1910', u'1901-1902', u'1903/1904'])

def test_map_date_range_string():
    """Date strings expressed as ranges are preferred over single years"""
    date_strs = ['1901-1902', '1903/1904']
    for d in date_strs:
        orig_rec = {'originInfo': {'dateIssued': ['stub']}}
        mapper = bhl_mods.BHLMapper(orig_rec)
        mapper.root_key = ''
        bhl_mods._date_values = MagicMock(return_value=[u'1899', unicode(d)])
        mapper.map_date_and_publisher()
        bhl_mods._date_values.assert_called_with(orig_rec['originInfo'])
        assert_equals(mapper.mapped_data['sourceResource'],
                      {'date': unicode(d)})

def test_map_date_multiple_format_as_range():
    """A number of date elements produces hyphenated range of low to high"""
    orig_rec = {'originInfo': {'dateIssued': ['stub']}}
    mapper = bhl_mods.BHLMapper(orig_rec)
    mapper.root_key = ''
    bhl_mods._date_values = MagicMock(return_value=[u'1900', u'1901', u'1902'])
    mapper.map_date_and_publisher()
    bhl_mods._date_values.assert_called_with(orig_rec['originInfo'])
    assert_equals(mapper.mapped_data['sourceResource'], {'date': u'1900-1902'})

def test_map_date_single():
    """A single date is mapped"""
    orig_rec = {'originInfo': {'dateIssued': ['stub']}}
    mapper = bhl_mods.BHLMapper(orig_rec)
    mapper.root_key = ''
    bhl_mods._date_values = MagicMock(return_value=[u'1900'])
    mapper.map_date_and_publisher()
    bhl_mods._date_values.assert_called_with(orig_rec['originInfo'])
    assert_equals(mapper.mapped_data['sourceResource'], {'date': u'1900'})

def test_map_publisher():
    """A publisher is mapped if it is given"""
    orig_rec = {'originInfo': {'publisher': 'X'}}
    mapper = bhl_mods.BHLMapper(orig_rec)
    mapper.root_key = ''
    bhl_mods._date_values = MagicMock(return_value=[u'1900'])
    mapper.map_date_and_publisher()
    assert_equals(mapper.mapped_data['sourceResource'], {'date': u'1900',
                                                         'publisher': ['X']})

def test_no_date_or_publisher_data():
    """Does not make changes, does not fail if there are no date / publisher"""
    orig_rec = {'originInfo': {}}
    mapper = bhl_mods.BHLMapper(orig_rec)
    mapper.root_key = ''
    bhl_mods._date_values = MagicMock(return_value=[])
    mapper.map_date_and_publisher()
    assert_equals(mapper.mapped_data['sourceResource'], {})

def test_map_title_append_volume():
    """Titles are appended with volumes, without unwanted characters"""
    orig_rec = {
        'titleInfo': {
            'title': "The Conchologists' exchange.\n /:"
        },
        'part': {
            'detail': {
                'type': 'volume',
                'number': '34'
            }
        }
    }
    mapper = bhl_mods.BHLMapper(orig_rec)
    mapper.root_key = ''
    mapper.map_title()
    assert_equals(mapper.mapped_data["sourceResource"],
                  {'title': ["The Conchologists' exchange, 34"]})

def test_map_title_works_w_no_part_detail():
    """Title processing works when there's no //part/detail"""
    orig_rec = {
        'titleInfo': {
            'title': "The Title"
        },
        'part': {}
    }
    mapper = bhl_mods.BHLMapper(orig_rec)
    mapper.root_key = ''
    mapper.map_title()
    assert_equals(mapper.mapped_data["sourceResource"],
                  {'title': ["The Title"]})
