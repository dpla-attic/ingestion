import os
import sys
import json
import time
import couchdb
import logging
import ConfigParser
from copy import deepcopy
from datetime import datetime
from dplaingestion.selector import setprop
from dplaingestion.dict_differ import DictDiffer

class Couch(object):
    """A class to hold the couchdb-python functionality used during ingestion.

    Includes methods to bulk post, load views from a view directory, backup
    and rollback ingestions, as well as track changes in documents between
    ingestions.
    """

    def __init__(self, config_file="akara.ini", **kwargs):
        """
        Default Args:
            config_file: The configuration file that includes the Couch server
                         url, dpla and dashboard database names, the views
                         directory path, and the batch size to use with
                         iterview
        Optional Args (if provided, config_file is not used:
            server_url: The server url with login credentials included.
            dpla_db_name: The name of the DPLA database.
            dashboard_db_name: The name of the Dashboard database.
            views_directory: The path where the view JavaScript files
                             are located.
            batch_size: The batch size to use with iterview
        """
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file))
        url = config.get("CouchDb", "Url")
        username = config.get("CouchDb", "Username")
        password = config.get("CouchDb", "Password")

        if not kwargs:
            dpla_db_name = "dpla"
            dashboard_db_name = "dashboard"
        else:
            dpla_db_name = kwargs.get("dpla_db_name")
            dashboard_db_name = kwargs.get("dashboard_db_name")

        bulk_download_db_name = "bulk_download"

        # Create server URL
        url = url.split("http://")
        server_url = "http://%s:%s@%s" % (username, password, url[1])
        self.server = couchdb.Server(server_url)

        self.dpla_db = self._get_db(dpla_db_name)
        self.dashboard_db = self._get_db(dashboard_db_name)
        self.bulk_download_db = self._get_db(bulk_download_db_name)
        self.views_directory = "couchdb_views"
        self.batch_size = 500

        self.logger = logging.getLogger("couch")
        handler = logging.FileHandler("logs/couch.log")
        formatter = logging.Formatter(
            "%(asctime)s %(name)s[%(process)s]: [%(levelname)s] %(message)s",
            "%b %d %H:%M:%S")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel("DEBUG")

    def _get_db(self, name):
        """Return a database given the database name, creating the database
           if it does not exist.
        """ 
        try:
            db = self.server.create(name)
        except:
            db = self.server[name]
        return db

    def dpla_view(self, viewname, **options):
        """Return the result of the given view in the "dpla" database"""
        return self.dpla_db.view(viewname, None, **options)

    def sync_views(self, db_name):
        """Fetches design documents from the views_directory, saves/updates
           them in the appropriate database, then build the views. 
        """
        build_views_from_file = ["dpla_db_all_provider_docs.js",
                                 "dpla_db_qa_reports.js",
                                 "dashboard_db_all_provider_docs.js",
                                 "dashboard_db_all_ingestion_docs.js",
                                 "dpla_db_export_database.js",
                                 "bulk_download_db_all_contributor_docs.js"]
        if db_name == "dpla":
            db = self.dpla_db
        elif db_name == "dashboard":
            db = self.dashboard_db
        elif db_name == "bulk_download":
            db = self.bulk_download_db

        for file in os.listdir(self.views_directory):
            if file.startswith(db_name):
                fname = os.path.join(self.views_directory, file)
                with open(fname, "r") as f:
                    s = f.read().replace("\n", "")
                    design_doc = json.loads(s)

                # Check if the design doc has changed
                prev_design_doc = db.get(design_doc["_id"], {})
                prev_revision = prev_design_doc.pop("_rev", None)
                diff = DictDiffer(design_doc, prev_design_doc)
                if diff.differences():
                    # Save thew design document
                    if prev_revision:
                        design_doc["_rev"] = prev_revision
                    db[design_doc["_id"]] = design_doc

                # Build views
                if file in build_views_from_file:
                    design_doc_name = design_doc["_id"].split("_design/")[-1]
                    real_views = (v for v in design_doc["views"] if v != "lib")
                    for view in real_views:
                        view_path = "%s/%s" % (design_doc_name, view)
                        start = time.time()
                        try:
                            for doc in db.view(view_path, limit=0):
                                pass
                            self.logger.debug("Built %s view %s in %s seconds"
                                              % (db.name, view_path,
                                                 time.time() - start))
                        except Exception, e:
                            self.logger.error("Error building %s view %s: %s" %
                                              (db.name, view_path, e))

    def update_ingestion_doc(self, ingestion_doc, **kwargs):
        for prop, value in kwargs.items():
            setprop(ingestion_doc, prop, value)
        self.dashboard_db.save(ingestion_doc)

    def _get_doc_ids(self, docs):
        return [doc["id"] for doc in docs]

    def _is_first_ingestion(self, ingestion_doc_id):
        ingestion_doc = self.dashboard_db.get(ingestion_doc_id)
        return True if ingestion_doc["ingestionSequence"] == 1 else False

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

    # TODO:  rename these _query* functions to remove "_query_" and remove
    # leading underscores for methods that don't have to be internal.

    def _query_all_docs(self, db):
        view_name = "_all_docs"
        for row in db.iterview(view_name, batch=self.batch_size,
                               include_docs=True):
            yield row["doc"]

    def all_dpla_docs(self):
        """Yield all documents in the dpla database"""
        for row in self.dpla_db.iterview("_all_docs",
                                         batch=self.batch_size,
                                         include_docs=True):
            yield row["doc"]

    def _query_all_prov_docs_by_ingest_seq(self, db, provider_name,
                                           ingestion_sequence):
        """Fetches all provider docs by provider name and ingestion sequence.
           The key for this view is the list [provider_name,
           ingestion_sequence, doc._id], so we supply "0" as the startkey
           doc._id and "Z" as the endkey doc._id in order to ensure proper
           sorting. See collation sequence here:
           http://docs.couchdb.org/en/latest/couchapp/views/collation.html
        """
        view_name = "all_provider_docs/by_provider_name_and_ingestion_sequence"
        startkey = [provider_name, ingestion_sequence, "0"]
        endkey = [provider_name, ingestion_sequence, "Z"]
        for row in db.iterview(view_name,
                               batch=self.batch_size,
                               include_docs=True,
                               startkey=startkey,
                               endkey=endkey):
            yield row["doc"]

    def _query_all_dpla_provider_docs(self, provider_name):
        """Yield all "dpla" database documents for the given provider"""
        # Regarding the startkey and endkey values that are used to define
        # the result, and the sort order of the documents, see the following,
        # and note that _all_docs uses an ASCII collation, such that "."
        # comes after "-":
        # http://docs.couchdb.org/en/latest/couchapp/views/collation.html#all-docs
        # ... and note that our _id values look like "provider--<somtehing>"
        for row in self.dpla_db.iterview("_all_docs",
                                         batch=self.batch_size,
                                         include_docs=True,
                                         startkey=provider_name,
                                         endkey=provider_name + "."):
            yield row["doc"]

    def _query_all_dpla_prov_docs_by_ingest_seq(self, provider_name,
                                                ingestion_sequence):
        return self._query_all_prov_docs_by_ingest_seq(self.dpla_db,
                                                      provider_name,
                                                      ingestion_sequence)
 
    def _query_all_dashboard_provider_docs(self, provider_name):
        """Yield all "dashboard" database documents for the given provider"""
        # See http://docs.couchdb.org/en/latest/couchapp/views/collation.html
        view = "all_provider_docs/by_provider_name"
        for row in self.dashboard_db.iterview(view,
                                              batch=self.batch_size,
                                              include_docs=True,
                                              startkey=[provider_name, "0"],
                                              endkey=[provider_name, "Z"]):
            yield row["doc"]

    def _query_all_dashboard_prov_docs_by_ingest_seq(self, provider_name,
                                                     ingestion_sequence):
        return self._query_all_prov_docs_by_ingest_seq(self.dashboard_db,
                                                      provider_name,
                                                      ingestion_sequence)
 
    def _query_all_provider_ingestion_docs(self, provider_name):
        view = self.dashboard_db.view("all_ingestion_docs/by_provider_name",
                                      include_docs=True, key=provider_name)
        return [row["doc"] for row in view.rows]

    def _query_prov_ingest_doc_by_ingest_seq(self, provider_name,
                                             ingestion_sequence):
        view_name = "all_ingestion_docs/by_provider_name_and_ingestion_sequence"
        view = self.dashboard_db.view(view_name, include_docs=True,
                                      key=[provider_name, ingestion_sequence])
        return view.rows[-1]["doc"]

    def _get_bulk_download_doc(self, contributor):
        view_name = "all_docs/by_contributor"
        view = self.bulk_download_db.view(view_name, include_docs=True,
                                           key=contributor)
        try:
            return view.rows[0]["doc"]
        except couchdb.http.ResourceNotFound:
            raise Exception("View all_docs/by_contributor does not exist "
                            "in bulk_download database.")
        except IndexError:
            # Nothing for this contributor yet, so start new document.
            return {"contributor": contributor}

    def _prep_for_diff(self, doc):
        """Removes keys from document that should not be compared."""
        ignore_keys = ["_rev", "admin", "ingestDate", "ingestionSequence"]
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

    def _get_sorted_ingestion_docs_for(self, provider_name):
        ingestion_docs = self._query_all_provider_ingestion_docs(provider_name)
        if len(ingestion_docs):
            # Sort by ingestionSequence
            ingestion_docs = sorted(ingestion_docs,
                                    key=lambda k: k["ingestionSequence"])
        return ingestion_docs

    def _get_last_ingestion_doc_for(self, provider_name):
        last_ingestion_doc = None
        ingestion_docs = self._get_sorted_ingestion_docs_for(provider_name)
        if ingestion_docs:
            last_ingestion_doc = ingestion_docs[-1]
        return last_ingestion_doc

    def _update_ingestion_doc_counts(self, ingestion_doc, **kwargs):
        for k, v in kwargs.iteritems():
            if k in ingestion_doc:
                ingestion_doc[k] += v
            else:
                self.logger.error("Key %s not in ingestion doc with ID: %s" %
                                  (k, ingestion_doc_id))
        self.dashboard_db.save(ingestion_doc)

    def _delete_documents(self, db, docs):
        """Fetches the documents given the document ids, updates each
           document to be deleted with "_deleted: True" so that the delete
           propagates to the river, then removes the document from db via
           db.purge()
        """
        for doc in docs:
            doc["_deleted"] = True
        db.update(docs)
        # TODO: BigCouch v0.4.2 does not currently support the couchdb-python
        # purge implementation. 
        # db.purge(docs)

        # Rebuild the database views
        self.sync_views(db.name)

    def _delete_all_provider_documents(self, provider):
        """Deletes all of a provider's documents from the DPLA and Dashboard
           databases.
        """
        count = 0
        delete_docs = []
        # Delete DPLA docs
        for doc in self._query_all_dpla_provider_docs(provider):
            delete_docs.append(doc)
            count += 1

            if len(delete_docs) == self.batch_size:
                print "%s DPLA documents deleted" % count
                self._delete_documents(self.dpla_db, delete_docs)
                delete_docs = []
        if delete_docs:
            print "%s DPLA documents deleted" % count
            self._delete_documents(self.dpla_db, delete_docs)
        elif count == 0:
            print "No DPLA documents found"

        count = 0
        delete_docs = []
        # Delete Dashboard docs
        for doc in self._query_all_provider_ingestion_docs(provider):
            # TODO: _query_all_dashboard_provider_docs should include
            # ingestion documents.
            delete_docs.append(doc)
            count += 1
        for doc in self._query_all_dashboard_provider_docs(provider):
            delete_docs.append(doc)
            count += 1

            if len(delete_docs) == self.batch_size:
                print "%s Dashboard documents deleted" % count
                self._delete_documents(self.dashboard_db, delete_docs)
                delete_docs = []
        if delete_docs:
            print "%s Dashboard documents deleted" % count
            self._delete_documents(self.dashboard_db, delete_docs)
        elif count == 0:
            print "No Dashboard documents found"

    def _backup_db(self, provider):
        """Fetches all provider docs from the DPLA database and posts them to
           the backup database, returning the backup database name.
        """
        backup_db_name = "%s_%s" % (provider,
                                    datetime.now().strftime("%Y%m%d%H%M%S"))
        backup_db = self.server.create(backup_db_name)

        msg = "Backing up %s to database %s" % (provider, backup_db_name)
        self.logger.debug(msg)
        print >> sys.stderr, msg

        count = 0
        provider_docs = []
        for doc in self._query_all_dpla_provider_docs(provider):
            count += 1
            # Revision not necessary in backup database
            if "_rev" in doc:
                del doc["_rev"]
            provider_docs.append(doc)
            # Bulk post every batch_size documents
            if len(provider_docs) == self.batch_size:
                self._bulk_post_to(backup_db, provider_docs)
                provider_docs = []
                print >> sys.stderr, "Backed up %s documents" % count

        if provider_docs:
            # Last bulk post
            self._bulk_post_to(backup_db, provider_docs)
            print >> sys.stderr, "Backed up %s documents" % count

        msg = "Backup complete"
        self.logger.debug(msg)
        print >> sys.stderr, msg

        return backup_db_name

    def _bulk_post_to(self, db, docs, **options):
        resp = db.update(docs, **options)
        self.logger.debug("%s database response: %s" % (db.name, resp))

    def _create_ingestion_document(self, provider, uri_base, profile_path,
                                   thresholds, fetcher_threads=1):
        """Creates and returns an ingestion document for the provider.
        """

        ingestion_doc = {
            "provider": provider,
            "type": "ingestion",
            "ingestionSequence": None,
            "ingestDate": datetime.now().isoformat(),
            "countAdded": 0,
            "countChanged": 0,
            "countDeleted": 0,
            "uri_base": uri_base,
            "profile_path": profile_path,
            "fetcher_threads": fetcher_threads,
            "fetch_process": {
                "status": None,
                "start_time": None,
                "end_time": None,
                "data_dir": None,
                "error": None,
                "total_items": None,
                "total_collections": None 
            },
            "enrich_process": {
                "status": None,
                "start_time": None,
                "end_time": None,
                "data_dir": None,
                "error": None,
                "total_items": None,
            "thresholdDeleted": 0,
                "total_collections": None,
                "missing_id": None,
                "missing_source_resource": None
            },
            "save_process": {
                "status": None,
                "start_time": None,
                "end_time": None,
                "error": None,
                "total_items": None,
                "total_collections": None
            },
            "delete_process": {
                "status": None,
                "start_time": None,
                "end_time": None,
                "error": None
            },
            "check_counts_process": {
                "status": None,
                "start_time": None,
                "end_time": None,
                "error": None
            },
            "dashboard_cleanup_process": {
                "status": None,
                "start_time": None,
                "end_time": None,
                "error": None
            },
            "upload_bulk_data_process": {
                "status": None,
                "start_time": None,
                "end_time": None,
                "error": None
            },

            "poll_storage_process": {
                "status": None,
                "start_time": None,
                "end_time": None,
                "error": None,
                "total_items": None,
                "total_collections": None,
                "missing_id": None,
                "missing_source_resource": None
            }
        }
        ingestion_doc.update({"thresholds": thresholds})

        # Set the ingestion sequence
        latest_ingestion_doc = self._get_last_ingestion_doc_for(provider)
        if latest_ingestion_doc is None:
            ingestion_sequence = 1
        else:
            ingestion_sequence = 1 + latest_ingestion_doc["ingestionSequence"]
        ingestion_doc["ingestionSequence"] = ingestion_sequence

        # Save the ingestion document and get its ID
        ingestion_doc_id = self.dashboard_db.save(ingestion_doc)[0]

        return ingestion_doc_id

    def _back_up_data(self, ingestion_doc):
        if ingestion_doc["ingestionSequence"] != 1:
            try:
                backup_db_name = self._backup_db(ingestion_doc["provider"])
            except couchdb.http.ServerError as e:
                self._print_couchdb_server_error(e, "backing up data")
                return -1
            except Exception as e:
                self._print_generic_exception_error(e, "backing up data")
                return -1
            ingestion_doc["backupDB"] = backup_db_name
            self.dashboard_db.save(ingestion_doc)

    def create_ingestion_doc_and_backup_db(self, provider):
        """Creates the ingestion document and backs up the provider documents
           if this is not the first ingestion, then returns the ingestion
           document id.
        """
        ingestion_doc = {
            "provider": provider,
            "type": "ingestion",
            "ingestDate": datetime.now().isoformat(),
            "countAdded": 0,
            "countChanged": 0,
            "countDeleted": 0
        }

        last_ingestion_doc = self._get_last_ingestion_doc_for(provider)
        if not last_ingestion_doc:
            ingestion_sequence = 1
        else:
            # Since this is not the first ingestion we will back up the
            # provider documents and upate the current ingestion document with
            # the backup database name.
            ingestion_sequence = last_ingestion_doc["ingestionSequence"] + 1
            backup_db_name = self._backup_db(provider)
            ingestion_doc["backupDB"] = backup_db_name
            self.dashboard_db.save(ingestion_doc)
            

        ingestion_doc["ingestionSequence"] = ingestion_sequence
        ingestion_doc_id = self.dashboard_db.save(ingestion_doc)[0]
        return ingestion_doc_id

    def process_deleted_docs(self, ingestion_doc):
        """Deletes any provider document whose ingestionSequence equals the
           previous ingestion's ingestionSequence, adds the deleted document id
           to the dashboard database, and updates the current ingestion
           document's countDeleted.

           Returns a status (-1 for error, 0 for success) along with the total
           number of documents deleted.
        """
        total_deleted = 0
        if not ingestion_doc["ingestionSequence"] == 1:
            provider = ingestion_doc["provider"]
            curr_seq = int(ingestion_doc["ingestionSequence"])
            prev_seq = curr_seq - 1

            delete_docs = []
            dashboard_docs = []
            alldocs = self._query_all_dpla_prov_docs_by_ingest_seq
            try:
                for doc in alldocs(provider, prev_seq):
                    delete_docs.append(doc)
                    dashboard_docs.append({"id": doc["_id"],
                                           "type": "record",
                                           "status": "deleted",
                                           "provider": provider,
                                           "ingestionSequence": curr_seq})

                    # So as not to use too much memory at once, do the bulk
                    # posts and deletions in batches of batch_size
                    if len(delete_docs) == self.batch_size:
                        what = "deleting documents (dashboard db)"
                        self._bulk_post_to(self.dashboard_db, dashboard_docs)
                        what = "updating ingestion doc counts"
                        self._update_ingestion_doc_counts(
                            ingestion_doc, countDeleted=len(delete_docs)
                            )
                        what = "deleting documents (dpla db)"
                        self._delete_documents(self.dpla_db, delete_docs)
                        total_deleted += len(delete_docs)
                        delete_docs = []
                        dashboard_docs = []

                if delete_docs:
                    # Last bulk post
                    what = "deleting documents (dashboard db)"
                    self._bulk_post_to(self.dashboard_db, dashboard_docs)
                    what = "syncing dashboard db views"
                    self.sync_views(self.dashboard_db.name)
                    what = "updating ingestion doc counts"
                    self._update_ingestion_doc_counts(
                        ingestion_doc, countDeleted=len(delete_docs)
                        )
                    what = "deleting documents (dpla db)"
                    self._delete_documents(self.dpla_db, delete_docs)
                    total_deleted += len(delete_docs)
            except couchdb.http.ServerError as e:
                self._print_couchdb_server_error(e, what)
                return (-1, total_deleted)
            except Exception as e:
                self._print_generic_exception_error(e, what)
                return (-1, total_deleted)
        return (0, total_deleted)

    def dashboard_cleanup(self, ingestion_doc):
        """Deletes a provider's dashboard item-level documents whose ingestion
           sequence is not in the last three ingestions along with the backup
           database for that ingesiton sequence.

           Returns a status (-1 for error, 0 for success) along with the total
           number of documents deleted.
        """
        total_deleted = 0
        provider = ingestion_doc["provider"]
        sequences = [doc["ingestionSequence"] for doc in
                     self._get_sorted_ingestion_docs_for(provider)]
        what = ""  # Description for error reporting
        if len(sequences) <= 3:
            msg = "Ingestion docs do not exceed 3. No dashboard documents " + \
                  "or backup databases will be deleted."
            print >> sys.stderr, msg
            return (0, total_deleted)
        else:
            delete_docs = []
            alldocs = self._query_all_dashboard_prov_docs_by_ingest_seq
            try:
                for seq in sequences[:len(sequences) - 3]:
                    what = "querying dashboard documents"
                    for doc in alldocs(provider, seq):
                        if not doc.get("type") == "ingestion":
                            delete_docs.append(doc)
                        else:
                            # Delete this ingestion document's backup database
                            what = "deleting backup database"
                            backup_db = doc.get("backupDB")
                            if backup_db and backup_db in self.server:
                                del self.server[backup_db]

                        # So as not to use too much memory at once, do the bulk
                        # posts and deletions in batches of batch_size
                        if len(delete_docs) == self.batch_size:
                            what = "deleting provider dashboard documents"
                            self._delete_documents(self.dashboard_db,
                                                   delete_docs)
                            total_deleted += len(delete_docs)
                            delete_docs = []
                if delete_docs:
                    # Last bulk post
                    what = "deleting provider dashboard documents"
                    self._delete_documents(self.dashboard_db, delete_docs)
                    total_deleted += len(delete_docs)
            except couchdb.http.ServerError as e:
                self._print_couchdb_server_error(e, what)
                return (-1, total_deleted)
            except Exception as e:
                self._print_generic_exception_error(e, what)
                return (-1, total_deleted)
        return (0, total_deleted)

    def process_and_post_to_dpla(self, harvested_docs, ingestion_doc):
        """Processes the harvested documents by:

        1. Removing unmodified docs from harvested set
        2. Counting changed docs
        3. Counting added docs
        4. Adding the ingestionSequence to the harvested doc
        5. Inserting the changed and added docs to the ingestion database

        Params:
        harvested_docs - A dictionary with the doc "_id" as the key and the
                         document to be inserted in CouchDB as the value
        ingestion_doc_id -  The "_id" of the ingestion document

        Returns a tuple (status, error_msg) where status is -1 for an error or
        0 otherwise
        """
        provider = ingestion_doc["provider"]
        ingestion_sequence = ingestion_doc["ingestionSequence"]

        added_docs = []
        changed_docs = []
        duplicate_doc_ids = []
        for hid in harvested_docs:
            # Add ingestonSequence to harvested document
            harvested_docs[hid]["ingestionSequence"] = ingestion_sequence

            # Add the revision and find the fields changed for harvested
            # documents that were ingested in a prior ingestion
            db_doc = self.dpla_db.get(hid)
            if db_doc:
                if db_doc.get("ingestionSequence") == ingestion_sequence:
                    # Remove duplicate documents
                    duplicate_doc_ids.append(hid)
                    continue

                harvested_docs[hid]["_rev"] = db_doc["_rev"]

                db_doc = self._prep_for_diff(db_doc)
                harvested_doc = self._prep_for_diff(deepcopy(harvested_docs[hid]))

                fields_changed = self._get_fields_changed(harvested_doc, db_doc)
                
                if fields_changed:
                    changed_docs.append({"id": hid,
                                         "type": "record",
                                         "status": "changed",
                                         "fieldsChanged": fields_changed,
                                         "provider": provider,
                                         "ingestionSequence": ingestion_sequence})
                
            # New document not previousely ingested
            else:
                added_docs.append({"id": hid,
                                   "type": "record",
                                   "status": "added",
                                   "provider": provider,
                                   "ingestionSequence": ingestion_sequence})

        # Remove duplicate documents to prevent multiple saves
        for id in duplicate_doc_ids:
            del harvested_docs[id]
        
        try:
            what = "posting to dashboard db"
            self._bulk_post_to(self.dashboard_db, added_docs + changed_docs)
            what = "updating ingestion document counts"
            self._update_ingestion_doc_counts(ingestion_doc,
                                              countAdded=len(added_docs),
                                              countChanged=len(changed_docs))
            what = "posting to dpla database"
            self._bulk_post_to(self.dpla_db, harvested_docs.values())
        except couchdb.http.ServerError as e:
            error_msg = self._couchdb_server_error_msg(e, what)
            print >> sys.stderr, self._ts_for_err(), error_msg
            return (-1, error_msg)
        except Exception as e:
            error_msg = self._generic_exception_error_msg(e, what)
            print >> sys.stderr, self._ts_for_err(), error_msg
            return (-1, error_msg)
        return (0, None)

    def rollback(self, provider, ingest_sequence):
        """ Rolls back the provider documents by:

        1. Fetching the backup database name of an ingestion document given by
           the provider and ingestion sequence
        2. Removing all provider documents from the DPLA database
        3. Fetching all the backup database documents, removing the "_rev"
           field, then posting to the DPLA database
        """
        # Since the ingestion that triggered a backup contains the backup
        # database name, we add 1 to the ingestion sequence provider to fetch
        # the appropriate ingestion document.
        ingest_sequence += 1
        ingest_doc = self._query_prov_ingest_doc_by_ingest_seq(provider,
                                                               ingest_sequence)
        backup_db_name = ingest_doc["backupDB"] if ingest_doc else None
        if backup_db_name:
            what = "Deleting DPLA provider documents"
            print what
            count = 0
            delete_docs = []
            try:
                for doc in self._query_all_dpla_provider_docs(provider):
                    delete_docs.append(doc)
                    # Delete in batches of batch_size so as not to use too much
                    # memory
                    if len(delete_docs) == self.batch_size:
                        count += len(delete_docs)
                        print "%s documents deleted" % count
                        self._delete_documents(self.dpla_db, delete_docs)
                        delete_docs = []
                # Last delete
                if delete_docs:
                        count += len(delete_docs)
                        print "%s documents deleted" % count
                        self._delete_documents(self.dpla_db, delete_docs)
            except couchdb.http.ServerError as e:
                return self._couchdb_server_error_msg(e, what)
            except Exception as e:
                return self._generic_exception_error_msg(e, what)

            # Bulk post backup database documents without revision
            what = "Retrieving documents from database %s" % backup_db_name
            print what
            count = 0
            docs = []
            try:
                for doc in self._query_all_docs(self.server[backup_db_name]):
                    if "_rev" in doc:
                        del doc["_rev"]
                    docs.append(doc)
                    if len(docs) == self.batch_size:
                        count += len(docs)
                        print "%s documents rolled back" % count
                        self._bulk_post_to(self.dpla_db, docs)
                        docs = []
                # Last POST
                if docs:
                    count += len(docs)
                    print "%s documents rolled back" % count
                    self._bulk_post_to(self.dpla_db, docs)
                    self.sync_views(self.dpla_db.name)
            except couchdb.http.ServerError as e:
                return self._couchdb_server_error_msg(e, what)
            except Exception as e:
                return self._generic_exception_error_msg(e, what)

        else:
            return "Attempted to rollback but no ingestion document with " + \
                   "ingestionSequence of %s was found" % ingest_sequence

        # Delete dashboard "record" documents for the ingestionSequence rolled
        # back from
        what = "Deleting ingestionSequence %s dashboard documents" % \
               ingest_sequence
        print what
        count = 0
        delete_docs = []
        try:
            alldocs = self._query_all_dashboard_prov_docs_by_ingest_seq
            for doc in alldocs(provider, ingest_sequence):
                if doc.get("type") == "record":
                    delete_docs.append(doc)
                if len(delete_docs) == self.batch_size:
                    count += len(delete_docs)
                    print "%s documents deleted" % count
                    self._delete_documents(self.dashboard_db, delete_docs)
                    delete_docs = []
            # Last delete
            if delete_docs:
                count += len(delete_docs)
                print "%s documents deleted" % count
                self._delete_documents(self.dashboard_db, delete_docs)
        except couchdb.http.ServerError as e:
            return self._couchdb_server_error_msg(e, what)
        except Exception as e:
            return self._generic_exception_error_msg(e, what)

        return "Rollback complete"

    def update_bulk_download_document(self, contributor, file_path,
                                      file_size):
        """Creates/updates a document for a contributor's bulk data file and
           returns the document id
        """

        bulk_download_doc = self._get_bulk_download_doc(contributor)
        bulk_download_doc.update({
            "file_path": file_path,
            "file_size": file_size,
            "last_updated": datetime.now().isoformat()
            })

        return self.bulk_download_db.save(bulk_download_doc)[0]

    def _couchdb_server_error_msg(self, e, what):
        """Return error message for couchdb.http.ServerError"""
        # couchdb.http.ServerError.message is a tuple.
        status = e.message[0]
        message = e.message[1]
        return "HTTP error %d from CouchDB while %s: %s" % \
               (status, what, message)

    def _print_couchdb_server_error(self, e, what):
        """Print message for couchdb.http.ServerError"""
        print >> sys.stderr, self._ts_for_err(), \
                 self._couchdb_server_error_msg(e, what)

    def _generic_exception_error_msg(self, e, what):
        """Return message for unexpected Exception"""
        return "Caught %s %s: %s" % (e.__class__, what, e)

    def _print_generic_exception_error(self, e, what):
        """Print message for unexpected Exception"""
        print >> sys.stderr, self._ts_for_err(), \
                 self._generic_exception_error_msg(e, what)

    def _ts_for_err(self):
        return "[%s]" % datetime.now().isoformat()

