import json
import internetarchive
from nose import with_setup
from nose.tools import assert_raises, assert_equals
from mock import MagicMock
from akara import logger
from dplaingestion.fetchers.ia_fetcher import IAFetcher


logger.error = MagicMock()  # Forego printing noise to `nosetests' output


def setup():
    global fetcher, item1, item2, item3

    item1 = MagicMock()
    item1.item_metadata = {'metadata': {'identifier': 'id1'}}
    item2 = MagicMock()
    item2.item_metadata = {'metadata': {'identifier': 'id2'}}
    item3 = MagicMock()
    item3.item_metadata = {'metadata': {'identifier': 'id3'}}

    items_gen_col1 = (i for i in [item1])
    items_gen_col2 = (i for i in [item2, item3])

    # Search result objects from search_items()
    search_col1 = MagicMock()
    search_col1.iter_as_items = MagicMock(return_value=items_gen_col1)
    search_col2 = MagicMock()
    search_col2.iter_as_items = MagicMock(return_value=items_gen_col2)
    search_for_col = {'collection:col1': search_col1,
                      'collection:col2': search_col2}

    def _search_items(s, max_retries, request_kwargs):
        # See internetarchive.search_items() call in IAFetcher.fetch_all_data()
        return search_for_col[s]

    internetarchive.search_items = MagicMock(side_effect=_search_items)

    uri_base = 'http://localhost:8080'
    with open('profiles/ia.pjs', 'r') as profile_fh:
        profile = json.load(profile_fh)
    fetcher = IAFetcher(profile, uri_base, '/dev/null')
    fetcher.collections = {'col1': {'title': 'Collection 1'},
                           'col2': {'title': 'Collection 2'}}

@with_setup(setup)
def test_fail_sets_arg():
    """Raises Exception if it receives a `sets' argument"""
    with assert_raises(Exception) as ctxt_mgr:
        fetcher.fetch_all_data(sets='x').next()
    assert(str(ctxt_mgr.exception).startswith('IAFetcher does not take'))

@with_setup(setup)
def test_adds_underscore_id():
    """Adds the `_id' field with to item records"""
    # Collection records get the _id added in
    # Fetcher.create_collection_records() and are outside of the domain of this
    # test.
    for batch in fetcher.fetch_all_data():
        for record in batch['records']:
            if record.get('ingestType') != 'collection':
                identifier = record['metadata']['identifier']
                assert(record['_id'] == identifier)

@with_setup(setup)
def test_adds_collection_to_item():
    """Adds a `collection' JSON object / Python dict to an item record"""
    for batch in fetcher.fetch_all_data():
        for record in batch['records']:
            if record.get('ingestType') != 'collection':
                assert(isinstance(record['collection'], dict))

@with_setup(setup)
def test_adds_error_to_response():
    """Catches exception for record, adds it to `errors' array in response"""
    # Trigger a KeyError for 'identifier'
    item1.item_metadata = {'metadata': {}}
    errors = []
    for batch in fetcher.fetch_all_data():
        errors.extend(batch['errors'])
    assert_equals(len(errors), 1)
    assert_equals(errors[0], "In IA collection col1: 'identifier'")

@with_setup(setup)
def test_adds_good_records_despite_error():
    """Catches exception for record, adds other good records to response"""
    # KeyError for 'identifier', as above, flunking out one of the records
    item1.item_metadata = {'metadata': {}}
    records = []
    for batch in fetcher.fetch_all_data():
        records.extend(batch['records'])
    assert_equals(len(records), 4)  # 2 Collections plus 2 good items

@with_setup(setup)
def test_batch_sizes():
    """Divides result into batches correctly based on `batch_size'"""
    fetcher.batch_size = 2
    total_records = 0
    for batch in fetcher.fetch_all_data():
        num_recs = len(batch['records'])
        assert(num_recs <= fetcher.batch_size)
        total_records += num_recs
    assert_equals(total_records, 5)  # 2 collections plus 3 items

if __name__ == "__main__":
    raise SystemExit("Please run these tests with the `nosetests' command.")
