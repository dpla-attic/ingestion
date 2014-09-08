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
    "Pipeline-Item": u"/select-id,/dpla_mapper?mapper_type=scdl",
    "Source": u"clemson"
}


DATA_PATH = "test/test_data/"
DATA = DATA_PATH + "clemson_ctm"
DATA_ADDED = DATA_PATH + "clemson_ctm_add5"
DATA_CHANGED = DATA_PATH + "clemson_ctm_change3"
DATA_DELETED = DATA_PATH + "clemson_ctm_delete10"

PROVIDER = "clemson"

if "TRAVIS" in os.environ:
    SERVER_URL = "http://travis_user:travis_pass@127.0.0.1:5984/"
else:
    config = ConfigParser.ConfigParser()
    config.readfp(open("akara.ini"))
    SERVER_URL = config.get("CouchDb", "Url")

TEST_DPLA_DB = "test_dpla"
TEST_DASHBOARD_DB = "test_dashboard"

# Dummy thresholds so that alert emails are not sent
THRESHOLDS = {
    "added": 100000,
    "changed": 100000,
    "deleted": 100000
}

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
                if backup_db in self.server:
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
        self.sync_views("dpla", True)
        self.sync_views("dashboard", True)

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
                                                           "profiles/clemson.pjs",
                                                           THRESHOLDS)
        ingestion_doc = self.dashboard_db[ingestion_doc_id]

        url = server() + "enrich"
        body = json.dumps(content)
        resp, content = H.request(url, "POST", body=body, headers=headers)
        data = json.loads(content)
        docs = data["enriched_records"]
        self._back_up_data(ingestion_doc)
        self.process_and_post_to_dpla(docs, ingestion_doc)
        self.process_deleted_docs(ingestion_doc)
        resp, total_deleted = self.dashboard_cleanup(ingestion_doc)
        print >> sys.stderr, "Dashboard cleanup, deleted %s" % total_deleted
        return ingestion_doc_id

@nottest
def couch_setup():
    global couch
    couch = CouchTest(server_url=SERVER_URL,
                      dpla_db_name=TEST_DPLA_DB,
                      dashboard_db_name=TEST_DASHBOARD_DB)
    couch.sync_views("dpla", True)
    couch.sync_views("dashboard", True)
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
        "clemson--http://repository.clemson.edu/u?/ctm,169": {"changed": ["originalRecord/coverage"]},
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

    # Verify first ingestion "record" type documents were removed from
    # dashboard database
    first_ingestion_dashboard_docs = [doc for doc in
                                      couch._query_all_dashboard_prov_docs_by_ingest_seq(PROVIDER, 2) if
                                      doc.get("type") == "record"]
    

@attr(travis_exclude='yes')
@with_setup(couch_setup, couch_teardown)
def test_multiple_ingestions():
    import copy

    with open(DATA) as f:
        data = json.load(f)

    data_deleted = copy.deepcopy(data)
    add_later = []
    for i in range(10):
        add_later.append(data_deleted.pop(2*i))

    data_changed = copy.deepcopy(data_deleted)
    for i in range(5):
        data_changed[3*i]["title"] = "Changed"

    data_added = copy.deepcopy(data_changed)
    data_added += add_later

    # Verify only item-level documents for the most recent three ingestions
    # exist in the dashboard database
    first_ingestion_doc_id = couch.ingest(data, PROVIDER, json_content=True)
    dashboard_docs = [doc for doc in
                      couch._query_records_by_ingestion_sequence_include_status(1)]
    first_ingestion_dashboard_items = len(dashboard_docs)
    ingestions = len([doc for doc in couch._query_all_provider_ingestion_docs(PROVIDER)])
    # Exclude design docs
    total_dashboard_records = len([doc for doc in
                                   couch._query_all_docs(couch.dashboard_db) if
                                   doc.get("type")])

    assert total_dashboard_records == first_ingestion_dashboard_items + \
                                      ingestions
    
    # Verify only item-level documents for the most recent three ingestions
    # exist in the dashboard database
    second_ingestion_doc_id = couch.ingest(data, PROVIDER, json_content=True)
    dashboard_docs = [doc for doc in
                      couch._query_records_by_ingestion_sequence_include_status(2)]
    second_ingestion_dashboard_items = len(dashboard_docs)
    ingestions = len([doc for doc in couch._query_all_provider_ingestion_docs(PROVIDER)])
    # Exclude design docs
    total_dashboard_records = len([doc for doc in
                                   couch._query_all_docs(couch.dashboard_db) if
                                   doc.get("type")])

    assert total_dashboard_records == first_ingestion_dashboard_items + \
                                      second_ingestion_dashboard_items + \
                                      ingestions
    # Verify second backup exists
    second_backup = couch.dashboard_db[second_ingestion_doc_id]["backupDB"]
    assert second_backup in couch.server

    # Verify only item-level documents for the most recent three ingestions
    # exist in the dashboard database
    third_ingestion_doc_id = couch.ingest(data_deleted, PROVIDER, json_content=True)
    dashboard_docs = [doc for doc in
                      couch._query_records_by_ingestion_sequence_include_status(3)]
    third_ingestion_dashboard_items = len(dashboard_docs)
    ingestions = len([doc for doc in couch._query_all_provider_ingestion_docs(PROVIDER)])
    # Exclude design docs
    total_dashboard_records = len([doc for doc in
                                   couch._query_all_docs(couch.dashboard_db) if
                                   doc.get("type")])

    assert total_dashboard_records == first_ingestion_dashboard_items + \
                                      second_ingestion_dashboard_items + \
                                      third_ingestion_dashboard_items + \
                                      ingestions
    # Verify second and third backups exist
    third_backup = couch.dashboard_db[third_ingestion_doc_id]["backupDB"]
    assert second_backup in couch.server
    assert third_backup in couch.server

    # Verify only item-level documents for the most recent three ingestions
    # exist in the dashboard database
    fourth_ingestion_doc_id = couch.ingest(data_changed, PROVIDER, json_content=True)
    dashboard_docs = [doc for doc in
                      couch._query_records_by_ingestion_sequence_include_status(4)]
    fourth_ingestion_dashboard_items = len(dashboard_docs)
    ingestions = len([doc for doc in couch._query_all_provider_ingestion_docs(PROVIDER)])
    # Exclude design docs
    total_dashboard_records = len([doc for doc in
                                   couch._query_all_docs(couch.dashboard_db) if
                                   doc.get("type")])

    assert total_dashboard_records == second_ingestion_dashboard_items + \
                                      third_ingestion_dashboard_items + \
                                      fourth_ingestion_dashboard_items + \
                                      ingestions
    # Verify second, third, and fourth backups exist
    fourth_backup = couch.dashboard_db[fourth_ingestion_doc_id]["backupDB"]
    assert second_backup in couch.server
    assert third_backup in couch.server
    assert fourth_backup in couch.server

    # Verify only item-level documents for the most recent three ingestions
    # exist in the dashboard database
    fifth_ingestion_doc_id = couch.ingest(data_added, PROVIDER, json_content=True)
    dashboard_docs = [doc for doc in
                      couch._query_records_by_ingestion_sequence_include_status(5)]
    fifth_ingestion_dashboard_items = len(dashboard_docs)
    ingestions = len([doc for doc in couch._query_all_provider_ingestion_docs(PROVIDER)])
    # Exclude design docs
    total_dashboard_records = len([doc for doc in
                                   couch._query_all_docs(couch.dashboard_db) if
                                   doc.get("type")])

    assert total_dashboard_records == third_ingestion_dashboard_items + \
                                      fourth_ingestion_dashboard_items + \
                                      fifth_ingestion_dashboard_items + \
                                      ingestions
    # Verify second backup was removed
    assert second_backup not in couch.server
    # Verify third, fourth, and fifth backups exist
    fifth_backup = couch.dashboard_db[fifth_ingestion_doc_id]["backupDB"]
    assert third_backup in couch.server
    assert fourth_backup in couch.server
    assert fifth_backup in couch.server

    # Verify count fields for each ingestion document
    assert couch.dashboard_db.get(first_ingestion_doc_id)["countAdded"] == 243
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
