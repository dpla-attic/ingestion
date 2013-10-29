import os
import sys
import base64
import ConfigParser
from nose import with_setup
from nose.tools import nottest
from nose.plugins.attrib import attr
from dplaingestion.couch import Couch
from couchdb.design import ViewDefinition
from amara.thirdparty import json, httplib2
from server_support import server, print_error_log

H = httplib2.Http()
headers = {
    "Content-Type": "application/json",
    "Pipeline-Coll": u"/oai-set-name?sets_service=/oai.listsets.json?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe",
    "Pipeline-Rec": u"/select-id,/oai-to-dpla",
    "Source": u"clemson",
    "Contributor": base64.b64encode(json.dumps({u"@id": "http://dp.la/api/contributor/scdl-clemson", u"name": "South Carolina Digital Library"}))
}

DATA_PATH = "test/test_data/"
DATA = DATA_PATH + "clemson_ctm"
DATA_ADDED = DATA_PATH + "clemson_ctm_add5"
DATA_CHANGED = DATA_PATH + "clemson_ctm_change3"
DATA_DELETED = DATA_PATH + "clemson_ctm_delete10"
DATA_LEGACY = DATA_PATH + "clemson_ctm_legacy"

PROVIDER = "clemson"

if "TRAVIS" in os.environ:
    SERVER_URL = "http://travis_user:travis_pass@127.0.0.1:5984/"
else:
    config = ConfigParser.ConfigParser()
    config.readfp(open("akara.ini"))
    SERVER_URL = config.get("CouchDb", "Server")
    BATCH_SIZE = config.get("CouchDb", "BatchSize")

TEST_DPLA_DB = "test_dpla"
TEST_DASHBOARD_DB = "test_dashboard"
VIEWS_DIRECTORY = "couchdb_views"

class CouchTest(Couch):
    def __init__(self, **kwargs):
        super(CouchTest, self).__init__(**kwargs)

    def _sync_test_views(self):
        dashboard_view = ViewDefinition("record_docs", "by_ingestion_sequence_include_status",
                                         """function(doc) { if (doc.type == 'record') { emit(doc.ingestionSequence, doc.status) } }""")
        dashboard_view.sync(self.dashboard_db)

    def _query_records_by_ingestion_sequence_include_status(self, ingestion_sequence):
        query = self.dashboard_db.view("record_docs/by_ingestion_sequence_include_status", include_docs=True, key=ingestion_sequence)
        return query.rows

    def _delete_all_test_backups(self):
        query = self.dashboard_db.view("all_ingestion_docs/by_provider_name", include_docs=True, key=PROVIDER)
        for row in query:
            if "backupDB" in row["doc"]:
                backup_db = row["doc"]["backupDB"]
                del self.server[backup_db]

    def _delete_all_test_databases(self):
        self._delete_all_test_backups()
        for db in self.server:
            if db == TEST_DPLA_DB or db == TEST_DASHBOARD_DB:
                del self.server[db]

    def _recreate_test_databases(self):
        del self.server[TEST_DPLA_DB]
        del self.server[TEST_DASHBOARD_DB]
        self.dpla_db = self.server.create(TEST_DPLA_DB)
        self.dashboard_db = self.server.create(TEST_DASHBOARD_DB)
        self._sync_views("dpla")
        self._sync_views("dashboard")

    def _setup(self):
        self._delete_all_test_backups()
        self._recreate_test_databases()
        self._sync_test_views()

    def _teardown(self):
        self._delete_all_test_backups()
        self._delete_test_datbases()

    def get_provider_backups(self):
        return [db for db in self.server if db.startswith(PROVIDER + "_")]

    def ingest(self, file, provider, json_content=None):
        if not json_content:
            with open(file) as f:
                content = json.load(f)
        else:
            content = file

        uri_base = server()[:-1]
        ingestion_doc_id = self._create_ingestion_document(provider, uri_base,
                                                           "profiles/clemson.pjs")
        ingestion_doc = self.dashboard_db[ingestion_doc_id]

        url = server() + "enrich"
        body = json.dumps(content)
        resp, content = H.request(url, "POST", body=body, headers=headers)
        data = json.loads(content)
        docs = data["enriched_records"]
        self._back_up_data(ingestion_doc)
        self.process_and_post_to_dpla(docs, ingestion_doc)
        self.process_deleted_docs(ingestion_doc)
        return ingestion_doc_id

@nottest
def couch_setup():
    global couch
    couch = CouchTest(server_url=SERVER_URL,
                      dpla_db_name=TEST_DPLA_DB,
                      dashboard_db_name=TEST_DASHBOARD_DB,
                      views_directory=VIEWS_DIRECTORY,
                      batch_size=BATCH_SIZE)
    couch._sync_views("dpla")
    couch._sync_views("dashboard")
    couch._sync_test_views()

@nottest
def couch_teardown():
    global couch
    couch._delete_all_test_databases()

@attr(travis_exclude='yes')
@with_setup(couch_setup, couch_teardown)
def test_backup():
    first_ingestion_doc_id = couch.ingest(DATA, PROVIDER)
    first_ingestion_all_docs = [doc for doc in
                                couch._query_all_dpla_provider_docs(PROVIDER)]
    second_ingestion_doc_id = couch.ingest(DATA_ADDED, PROVIDER)

    second_ingestion_doc = couch.dashboard_db.get(second_ingestion_doc_id)
    second_ingestion_backup = second_ingestion_doc["backupDB"]
    all_backup_rows = couch.server[second_ingestion_backup].view("_all_docs").rows

    all_ingestion_ids = [doc["_id"] for doc in first_ingestion_all_docs]
    all_backup_ids = [row["id"] for row in all_backup_rows]
    assert set(all_ingestion_ids) == set(all_backup_ids)

@attr(travis_exclude='yes')
@with_setup(couch_setup, couch_teardown)
def test_added_docs():
    DOCS_ADDED = ["clemson--http://repository.clemson.edu/u?/added%s" % i for i in range(1,6)]
    
    first_ingestion_doc_id = couch.ingest(DATA, PROVIDER)
    second_ingestion_doc_id = couch.ingest(DATA_ADDED, PROVIDER)

    second_ingestion_doc = couch.dashboard_db.get(second_ingestion_doc_id)

    assert second_ingestion_doc["countAdded"] == len(DOCS_ADDED)
    assert second_ingestion_doc["countChanged"] == 0
    assert second_ingestion_doc["countDeleted"] == 0

    rows = couch._query_records_by_ingestion_sequence_include_status(second_ingestion_doc["ingestionSequence"])

    docs_added_ids = [row["doc"]["id"] for row in rows if row["doc"]["status"] == "added"]
    assert len(docs_added_ids) == len(DOCS_ADDED)
    assert len(set(docs_added_ids) - set(DOCS_ADDED)) == 0

    docs_changed = [row["doc"] for row in rows if row["doc"]["status"] == "changed"]
    assert len(docs_changed) == 0

    docs_deleted = [row["doc"] for row in rows if row["doc"]["status"] == "deleted"]
    assert len(docs_deleted) == 0

@attr(travis_exclude='yes')
@with_setup(couch_setup, couch_teardown)
def test_deleted_docs():
    nums = [372, 373, 374, 375, 376, 377, 51, 68, 77, 94]
    DOCS_DELETED = ["clemson--http://repository.clemson.edu/u?/ctm,%s" % num for num in nums]

    first_ingestion_doc_id = couch.ingest(DATA, PROVIDER)
    second_ingestion_doc_id = couch.ingest(DATA_DELETED, PROVIDER)

    second_ingestion_doc = couch.dashboard_db.get(second_ingestion_doc_id)
    assert second_ingestion_doc["countDeleted"] == len(DOCS_DELETED)
    assert second_ingestion_doc["countChanged"] == 0
    assert second_ingestion_doc["countAdded"] == 0

    rows = couch._query_records_by_ingestion_sequence_include_status(second_ingestion_doc["ingestionSequence"])

    docs_deleted_ids = [row["doc"]["id"] for row in rows if row["doc"]["status"] == "deleted"]
    assert len(docs_deleted_ids) == len(DOCS_DELETED)
    assert len(set(docs_deleted_ids) - set(DOCS_DELETED)) == 0
    
    docs_changed = [row["doc"] for row in rows if row["doc"]["status"] == "changed"]
    assert len(docs_changed) == 0

    docs_added = [row["doc"] for row in rows if row["doc"]["status"] == "added"]
    assert len(docs_added) == 0

@attr(travis_exclude='yes')
@with_setup(couch_setup, couch_teardown)
def test_changed_docs():
    DOCS_CHANGED = {
        "clemson--http://repository.clemson.edu/u?/ctm,161": {"changed": ["originalRecord/title", "sourceResource/title"]},
        "clemson--http://repository.clemson.edu/u?/ctm,169": {"changed": ["originalRecord/coverage", "sourceResource/spatial"]},
        "clemson--http://repository.clemson.edu/u?/ctm,179": {"changed": ["originalRecord/subject", "sourceResource/subject"]}
    }
    
    first_ingestion_doc_id = couch.ingest(DATA, PROVIDER)
    second_ingestion_doc_id = couch.ingest(DATA_CHANGED, PROVIDER)

    second_ingestion_doc = couch.dashboard_db.get(second_ingestion_doc_id)
    assert second_ingestion_doc["countChanged"] == len(DOCS_CHANGED.keys())
    assert second_ingestion_doc["countAdded"] == 0
    assert second_ingestion_doc["countDeleted"] == 0

    rows = couch._query_records_by_ingestion_sequence_include_status(second_ingestion_doc["ingestionSequence"])

    docs_changed = dict((row["doc"]["id"], row["doc"]["fieldsChanged"]) for row in rows if row["doc"]["status"] == "changed")
    assert len(docs_changed.keys()) == len(DOCS_CHANGED.keys())
    for k, v in docs_changed.iteritems():
        assert set(docs_changed[k]["changed"]) == set(DOCS_CHANGED[k]["changed"])

    docs_added = [row["doc"] for row in rows if row["doc"]["status"] == "added"]
    assert len(docs_added) == 0

    docs_deleted = [row["doc"] for row in rows if row["doc"]["status"] == "deleted"]
    assert len(docs_deleted) == 0

@attr(travis_exclude='yes')
@with_setup(couch_setup, couch_teardown)
def test_rollback():
    first_ingestion_doc_id = couch.ingest(DATA, PROVIDER)
    first_ingestion_all_docs = couch._query_all_dpla_provider_docs(PROVIDER)
    first_ingestion_all_ids = [doc["_id"] for doc in first_ingestion_all_docs]

    second_ingestion_doc_id = couch.ingest(DATA_ADDED, PROVIDER)
    second_ingestion_all_docs = couch._query_all_dpla_provider_docs(PROVIDER)
    second_ingestion_all_ids = [doc["_id"] for doc in second_ingestion_all_docs]
    
    assert set(first_ingestion_all_ids) != set(second_ingestion_all_ids)

    first_ingestion_doc = couch.dashboard_db.get(first_ingestion_doc_id)
    couch.rollback(PROVIDER, first_ingestion_doc["ingestionSequence"])
    rollback_all_docs = couch._query_all_dpla_provider_docs(PROVIDER)
    rollback_all_ids = [doc["_id"] for doc in rollback_all_docs]
    assert set(rollback_all_ids) == set(first_ingestion_all_ids)

@attr(travis_exclude='yes')
@with_setup(couch_setup, couch_teardown)
def test_multiple_ingestions():
    import copy

    with open(DATA) as f:
        data = json.load(f)

    data_deleted = copy.deepcopy(data)
    add_later = []
    for i in range(10):
        add_later.append(data_deleted["records"].pop(2*i))

    data_changed = copy.deepcopy(data_deleted)
    for i in range(5):
        data_changed["records"][3*i]["title"] = "Changed"

    data_added = copy.deepcopy(data_changed)
    data_added["records"] += add_later

    first_ingestion_doc_id = couch.ingest(data, PROVIDER, json_content=True)
    dashboard_db_docs = [doc for doc in couch._query_all_docs(couch.dashboard_db)]
    total_dashboard_docs_first = len(dashboard_db_docs)

    second_ingestion_doc_id = couch.ingest(data, PROVIDER, json_content=True)
    dashboard_db_docs = [doc for doc in couch._query_all_docs(couch.dashboard_db)]
    total_dashboard_docs_second = len(dashboard_db_docs)

    third_ingestion_doc_id = couch.ingest(data_deleted, PROVIDER, json_content=True)
    dashboard_db_docs = [doc for doc in couch._query_all_docs(couch.dashboard_db)]
    total_dashboard_docs_third = len(dashboard_db_docs)

    fourth_ingestion_doc_id = couch.ingest(data_changed, PROVIDER, json_content=True)
    dashboard_db_docs = [doc for doc in couch._query_all_docs(couch.dashboard_db)]
    total_dashboard_docs_fourth = len(dashboard_db_docs)

    fifth_ingestion_doc_id = couch.ingest(data_added, PROVIDER, json_content=True)
    dashboard_db_docs = [doc for doc in couch._query_all_docs(couch.dashboard_db)]
    total_dashboard_docs_fifth = len(dashboard_db_docs)

    # Second ingestion should have an extra ingestion doc
    assert int(total_dashboard_docs_first) + 1 == int(total_dashboard_docs_second)
    # Third ingestion should have extra ingestion doc + 10 deleted
    assert int(total_dashboard_docs_second) + 11 == int(total_dashboard_docs_third)
    # Fourth ingestion should have extra ingestion doc + 5 changed
    assert int(total_dashboard_docs_third) + 6 == int(total_dashboard_docs_fourth)
    # Fifth ingestion should have extra ingestion doc + 10 added
    assert int(total_dashboard_docs_fourth) + 11 == int(total_dashboard_docs_fifth)
    

    assert couch.dashboard_db.get(first_ingestion_doc_id)["countAdded"] == 244
    assert couch.dashboard_db.get(first_ingestion_doc_id)["countChanged"] == 0
    assert couch.dashboard_db.get(first_ingestion_doc_id)["countDeleted"] == 0

    assert couch.dashboard_db.get(second_ingestion_doc_id)["countAdded"] == 0
    assert couch.dashboard_db.get(second_ingestion_doc_id)["countChanged"] == 0
    assert couch.dashboard_db.get(second_ingestion_doc_id)["countDeleted"] == 0
    

    assert couch.dashboard_db.get(third_ingestion_doc_id)["countAdded"] == 0
    assert couch.dashboard_db.get(third_ingestion_doc_id)["countChanged"] == 0
    assert couch.dashboard_db.get(third_ingestion_doc_id)["countDeleted"] == 10

    assert couch.dashboard_db.get(fourth_ingestion_doc_id)["countAdded"] == 0
    assert couch.dashboard_db.get(fourth_ingestion_doc_id)["countChanged"] == 5
    assert couch.dashboard_db.get(fourth_ingestion_doc_id)["countDeleted"] == 0

    assert couch.dashboard_db.get(fifth_ingestion_doc_id)["countAdded"] == 10
    assert couch.dashboard_db.get(fifth_ingestion_doc_id)["countChanged"] == 0
    assert couch.dashboard_db.get(fifth_ingestion_doc_id)["countDeleted"] == 0

@attr(travis_exclude='yes')
@with_setup(couch_setup, couch_teardown)
def test_legacy():
    # Ingest legacy data. Records will not have an "ingestionSequence" field
    with open(DATA_LEGACY) as f:
        data = f.readlines()
    content = json.loads("".join(data))
    docs = [c["doc"] for c in content]
    # The test legacy documents contain revision so we pass new_edits=False
    # to avoid collision errors
    couch._bulk_post_to(couch.dpla_db, docs, new_edits=False)

    # Run legacy_as_first_ingestion script
    sys.path.append(os.path.join(os.getcwd(), "scripts"))
    from legacy_as_first_ingestion import main
    main(couch=couch, provider_name=PROVIDER)

    # Get last Clemson ingestion document
    ingest_doc = couch._get_last_ingestion_doc_for(PROVIDER)
    assert ingest_doc["ingestionSequence"] == 1
    assert ingest_doc["countAdded"] == 244 # 243 records plus 1 collection
    assert ingest_doc["countDeleted"] == 0
    assert ingest_doc["countChanged"] == 0

    # Get all provider documents in database
    docs = [doc for doc in couch._query_all_dpla_provider_docs(PROVIDER)]
    assert len(docs) == 244
    for doc in docs:
        assert doc["ingestionSequence"] == 1

    # Ingest to override data
    couch.ingest(DATA, PROVIDER)
    ingest_doc = couch._get_last_ingestion_doc_for(PROVIDER)
    assert ingest_doc["ingestionSequence"] == 2
    assert ingest_doc["countAdded"] == 0
    assert ingest_doc["countDeleted"] == 0
    assert ingest_doc["countChanged"] == 243 # The collection does not change

    # Get all provider documents in database
    docs = [doc for doc in couch._query_all_dpla_provider_docs(PROVIDER)]
    assert len(docs) == 244
    for doc in docs:
        assert doc["ingestionSequence"] == 2

@attr(travis_exclude='yes')
@with_setup(couch_setup, couch_teardown)
def test_get_last_ingestion_document():
    with open(DATA) as f:
        data = json.load(f)

    couch.ingest(data, PROVIDER, json_content=True)
    ingestion_doc = couch._get_last_ingestion_doc_for(PROVIDER)
    assert ingestion_doc["ingestionSequence"] == 1

    couch.ingest(data, PROVIDER, json_content=True)
    ingestion_doc = couch._get_last_ingestion_doc_for(PROVIDER)
    assert ingestion_doc["ingestionSequence"] == 2

    couch.ingest(data, PROVIDER, json_content=True)
    ingestion_doc = couch._get_last_ingestion_doc_for(PROVIDER)
    assert ingestion_doc["ingestionSequence"] == 3

    couch.ingest(data, PROVIDER, json_content=True)
    ingestion_doc = couch._get_last_ingestion_doc_for(PROVIDER)
    assert ingestion_doc["ingestionSequence"] == 4

    couch.rollback(PROVIDER, 2)
    ingestion_doc = couch._get_last_ingestion_doc_for(PROVIDER)
    assert ingestion_doc["ingestionSequence"] == 4

    couch.ingest(data, PROVIDER, json_content=True)
    ingestion_doc = couch._get_last_ingestion_doc_for(PROVIDER)
    assert ingestion_doc["ingestionSequence"] == 5
