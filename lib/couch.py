import os
import sys
import json
import logging
import ConfigParser
from copy import deepcopy
from couchdb import Server
from datetime import datetime
from dplaingestion.dict_differ import DictDiffer

config = ConfigParser.ConfigParser()
try:
    config.readfp(open("akara.ini"))
except Exception:
    sys.exit("Cannot find akara.ini")

couch_server = config.get("CouchDb", "Server")
couch_database_dpla = config.get("CouchDb", "DPLADatabase")
couch_database_dashboard = config.get("CouchDb", "DashboardDatabase")
couch_views_directory = config.get("CouchDb", "ViewsDirectory")

class Couch(object):
    """A class to hold the couchdb-python functionality used during ingestion.

    Includes methods to bulk post, load views from a view directory, backup
    and rollback ingestions, as well as track changes in documents between
    ingestions.
    """

    def __init__(self, server_url=couch_server,
                 dpla_db_name=couch_database_dpla,
                 dashboard_db_name=couch_database_dashboard,
                 views_directory=couch_views_directory):
        """Instantiate this class with:

        Args:
            server_url: The server url with login credentials included.
            dpla_db_name: The name of the DPLA database.
            dashboard_db_name: The name of the Dashboard database.
            views_directory: The path where the view JavaScript files
                                   are located.
        """
        self.server_url = server_url
        self.server = Server(server_url)
        self.dpla_db = self._get_db(dpla_db_name)
        self.dashboard_db = self._get_db(dashboard_db_name)
        self.views_directory = views_directory

        self.logger = logging.getLogger("couch")
        handler = logging.FileHandler("logs/couch.log")
        formatter = logging.Formatter(
            "%(asctime)s %(name)s[%(process)s]: [%(levelname)s] %(message)s",
            "%b %d %H:%M:%S")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)

    def _get_db(self, name):
        """Return a database given the database name, creating the database
           if it does not exist.
        """ 
        try:
            db = self.server.create(name)
        except Exception:
            db = self.server[name]
        return db

    def _sync_views(self):
        """Fetches views from the views_directory and saves/updates them
           in the appropriate database.
        """
        for file in os.listdir(self.views_directory):
            if file.startswith("dpla_db"):
                db = self.dpla_db
            elif file.startswith("dashboard_db"):
                db = self.dashboard_db
            else:
                continue

            fname = os.path.join(self.views_directory, file)
            with open(fname, "r") as f:
                view = json.load(f)
                previous_view = db.get(view["_id"])
                if  previous_view:
                    view["_rev"] = previous_view["_rev"]
                db[view["_id"]] = view

    def _get_doc_ids(self, docs):
        return [doc["id"] for doc in docs]

    def _is_first_ingestion(self, ingestion_doc_id):
        ingestion_doc = self.dashboard_db.get(ingestion_doc_id)
        return True if ingestion_doc["ingestion_version"] == 1 else False

    def _get_range_query_kwargs(self, doc_ids):
        """Returns a dict of keyword arguments to be used in the
           _query methods.
        """
        doc_ids = sorted(doc_ids)
        kwargs = {
            "include_docs": True,
            "startkey": doc_ids[0],
            "endkey": doc_ids[-1]
        }
        return kwargs

    def _query_all_docs(self, db, **kwargs):
        docs = db.view("_all_docs", **kwargs)
        return docs

    def _query_all_dpla_provider_docs(self, provider_name):
        docs = self.dpla_db.view("all_provider_docs/by_provider_name",
                                 include_docs=True, key=provider_name)
        return docs

    def _query_all_provider_ingestion_docs(self, provider_name):
        return self.dashboard_db.view("all_ingestion_docs/by_provider_name",
                                      include_docs=True, key=provider_name)

    def _prep_for_diff(self, doc):
        """Removes keys from document that should not be compared."""
        ignore_keys = ["_rev", "admin", "ingestDate", "ingestion_version"]
        for key in ignore_keys:
            if key in doc:
                del doc[key]
        return doc

    def _get_fields_changed(self, harvested_doc, database_doc):
        """Compares harvested_doc and database_doc and returns any changed
           fields.
        """
        fields_changed = {}
        diff = DictDiffer(harvested_doc, database_doc)
        if diff.added():
            fields_changed["added"] = diff.added()
        if diff.removed():
            fields_changed["removed"] = diff.removed()
        if diff.changed():
            fields_changed["changed"] = diff.changed()
        
        return fields_changed

    def _get_ingestion_tempfile(self, ingestion_doc_id):
        path = "/tmp"
        filename = "%s_harvested_ids" % ingestion_doc_id
        return os.path.join(path, filename)

    def _write_harvested_ids_to_tempfile(self, ingestion_doc_id, ids):
        with open(self._get_ingestion_tempfile(ingestion_doc_id), "a") as f:
            [f.write(id+"\n") for id in ids]

    def _get_all_harvested_ids_from_tempfile(self, ingestion_doc_id):
        with open(self._get_ingestion_tempfile(ingestion_doc_id), "r") as f:
            harvested_ids = f.readlines()
        return [id.replace("\n", "") for id in harvested_ids]

    def _get_last_ingestion_doc_for(self, provider_name):
        last_ingestion_doc = None
        ingestion_docs = self._query_all_provider_ingestion_docs(provider_name)
        if len(ingestion_docs):
            last_ingestion_doc = ingestion_docs.rows[-1]["doc"]
        return last_ingestion_doc

    def _update_ingestion_doc_counts(self, ingestion_doc_id, **kwargs):
        ingestion_doc = self.dashboard_db.get(ingestion_doc_id)
        for k, v in kwargs.iteritems():
            if k in ingestion_doc:
                ingestion_doc[k] += v
            else:
                self.logger.error("Key %s not in ingestion doc with ID: %s" %
                                  (k, ingestion_doc_id))
        self.dashboard_db.save(ingestion_doc)

    def _delete_documents(self, db, docs):
        """Fetches the documents givent the document ids, updates each
           document to be deleted with "_deleted: True" so that the delete
           propagates to the river, then removes the document from db via
           db.purge()
        """
        for doc in docs:
            doc["_deleted"] = True
        db.update(docs)
        db.purge(docs)

    def _backup_db(self, provider):
        """Fetches all provider docs from the DPLA database and replicates them
           to the backup database, returning the backup database name.
        """
        backup_db_name = "%s_%s" % (provider,
                                    datetime.now().strftime("%Y%m%d%H%M%S"))
        self.server.create(backup_db_name)
        provider_docs = self._query_all_dpla_provider_docs(provider)
        provider_ids = self._get_doc_ids(provider_docs)
        resp = self.server.replicate(self.server_url + self.dpla_db.name,
                                     self.server_url + backup_db_name,
                                     doc_ids=provider_ids)
        if not resp["ok"]:
            msg = "Backup failed. Response: %s" % resp
            self.logger.error(msg)
            raise Exception(msg)
        return backup_db_name

    def bulk_post_to_dpla(self, docs):
        resp = self.dpla_db.update(docs)
        self.logger.debug("DPLA database response: %s" % resp)

    def bulk_post_to_dashboard(self, docs):
        resp = self.dashboard_db.update(docs)
        self.logger.debug("Dashboard database response: %s" % resp)

    def create_ingestion_doc_and_backup_db(self, provider):
        """Syncs the views then creates the ingestion document and backs up the
           provider documents if this is not the first ingestion. Returns the
           ingestion document id.
        """
        self._sync_views()

        ingestion_doc = {
            "provider": provider,
            "type": "ingestion",
            "ingest_date": datetime.now().isoformat(),
            "count_added": 0,
            "count_changed": 0,
            "count_deleted": 0
        }

        last_ingestion_doc = self._get_last_ingestion_doc_for(provider)
        if not last_ingestion_doc:
            ingestion_version = 1
        else:
            # Since this is not the first ingestion we will back up the
            # provider documents and upate the last ingestion document with
            # the backup database name.
            ingestion_version = last_ingestion_doc["ingestion_version"] + 1
            backup_db_name = self._backup_db(provider)
            last_ingestion_doc["backup_db"] = backup_db_name
            self.dashboard_db.save(last_ingestion_doc)
            

        ingestion_doc["ingestion_version"] = ingestion_version
        ingestion_doc_id = self.dashboard_db.save(ingestion_doc)[0]
        return ingestion_doc_id

    def process_deleted_docs(self, ingestion_doc_id):
        """Deletes any provider document whose ingestion_version equals the
           previous ingestion's ingestion version, adds the deleted document id
           to the dashboard database, and updated the current ingestion
           document's deleted count.
        """
        if not self._is_first_ingestion(ingestion_doc_id):
            curr_ingest_doc = self.dashboard_db[ingestion_doc_id]
            provider = curr_ingest_doc["provider"]
            curr_ingest_version = int(curr_ingest_doc["ingestion_version"])
            prev_ingest_version = curr_ingest_version - 1

            rows  = self._query_all_dpla_provider_docs(provider)
            delete_docs = []
            dashboard_docs = []
            count = 0
            for row in rows:
                count += 1
                if int(row["doc"]["ingestion_version"]) == prev_ingest_version:
                    delete_docs.append(row["doc"])
                    dashboard_docs.append({"id": row["id"],
                                           "type": "record",
                                           "status": "deleted",
                                           "ingestion_version": curr_ingest_version})

                # So as not to use too much memory at once, do the bulk posts
                # and deletions in sets of 1000 documents
                if delete_docs and (len(delete_docs)%1000 == 0 or
                                    count == len(rows)):
                    self.bulk_post_to_dashboard(dashboard_docs)
                    self._update_ingestion_doc_counts(
                        ingestion_doc_id, count_deleted=len(delete_docs)
                        )
                    self._delete_documents(self.dpla_db, delete_docs)
                    delete_docs = []
                    dashboard_docs = []

    def process_and_post_to_dpla(self, harvested_docs, ingestion_doc_id):
        """Processes the harvested documents by:

        1. Removing unmodified docs from harvested set
        2. Counting modified docs
        3. Counting added docs
        4. Adding the ingestion_version to the harvested doc
        5. Inserting the modified and added docs to the ingestion database

        Params:
        harvested_docs - A dictionary with the doc "_id" as the key and the
                         document to be inserted in CouchDB as the value
        ingestion_doc_id -  The "_id" of the ingestion document
        """
        ingestion_doc = self.dashboard_db.get(ingestion_doc_id)
        ingestion_version = ingestion_doc["ingestion_version"]

        added_docs = []
        changed_docs = []
        for hid in harvested_docs:
            # Add ingeston_version to harvested document
            harvested_docs[hid]["ingestion_version"] = ingestion_version

            # Add the revision and find the fields changed for harvested
            # documents that were ingested in a prior ingestion
            if harvested_docs[hid]["_id"] in self.dpla_db:
                db_doc = self.dpla_db.get(harvested_docs[hid]["_id"])
                harvested_docs[hid]["_rev"] = db_doc["_rev"]
                db_doc = self._prep_for_diff(db_doc)
                harvested_doc = self._prep_for_diff(deepcopy(harvested_docs[hid]))

                fields_changed = self._get_fields_changed(harvested_doc, db_doc)
                
                if fields_changed:
                    changed_docs.append({"id": hid,
                                         "type": "record",
                                         "status": "changed",
                                         "fields_changed": fields_changed,
                                         "ingestion_version": ingestion_version})
            # New document not previousely ingested
            else:
                added_docs.append({"id": hid,
                                   "type": "record",
                                   "status": "added",
                                   "ingestion_version": ingestion_version})

        self.bulk_post_to_dashboard(added_docs + changed_docs)
        self._update_ingestion_doc_counts(ingestion_doc_id,
                                          count_added=len(added_docs),
                                          count_changed=len(changed_docs))
        self.bulk_post_to_dpla(harvested_docs.values())

    def rollback(self, provider, ingestion_version):
        """ Rolls back the provider documents by:

        1. Fetching the backup database name of an ingestion document given by
           the provider and ingestion_version
        2. Removing all provider documents from the DPLA database
        3. Replicating the backup database to the DPLA database
        """
        backup_db_name = None
        rows = self._query_all_provider_ingestion_docs(provider)
        for row in rows:
            if row["doc"]["ingestion_version"] == ingestion_version:
                backup_db_name = row["doc"]["backup_db"]
                break
        if backup_db_name:
            rows = self._query_all_dpla_provider_docs(provider)
            delete_docs = []
            # Delete in sets of 1000 so as not to use too much memory
            for row in rows:
                delete_docs.append(row["doc"])
                if len(delete_docs)%1000 == 0 or len(delete_docs) == len(rows):
                    self._delete_documents(self.dpla_db, delete_docs)
                    delete_docs = []

            # Replicate now that all provider docs have been removed from DPLA
            resp = self.server.replicate(self.server_url + backup_db_name,
                                         self.server_url + self.dpla_db.name)
            if not resp["ok"]:
                msg = "Rollback failed. Response: %s" % resp
                self.logger.error(msg)
            else:
                msg = "Rollback success! Response: %s" % resp
        else:
            msg = "Attempted to rollback but no ingestion document with " + \
                  "ingestion_version of %s was found"
            self.logger.error("msg")

        return msg
