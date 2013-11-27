import os
import sys
import json
import time
import logging
import ConfigParser
from copy import deepcopy
from couchdb import Server
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
        if not kwargs:
            config = ConfigParser.ConfigParser()
            config.readfp(open(config_file))
            server_url = config.get("CouchDb", "Server")
            dpla_db_name = config.get("CouchDb", "DPLADatabase")
            dashboard_db_name = config.get("CouchDb", "DashboardDatabase")
            views_directory = config.get("CouchDb", "ViewsDirectory")
            batch_size = config.get("CouchDb", "BatchSize")
            log_level = config.get("CouchDb", "LogLevel")
        else:
            server_url = kwargs.get("server_url")
            dpla_db_name = kwargs.get("dpla_db_name")
            dashboard_db_name = kwargs.get("dashboard_db_name")
            views_directory = kwargs.get("views_directory")
            batch_size = kwargs.get("batch_size")
            log_level = "DEBUG"

        self.server_url = server_url
        self.server = Server(server_url)
        self.dpla_db = self._get_db(dpla_db_name)
        self.dashboard_db = self._get_db(dashboard_db_name)
        self.views_directory = views_directory
        self.batch_size = int(batch_size)

        self.logger = logging.getLogger("couch")
        handler = logging.FileHandler("logs/couch.log")
        formatter = logging.Formatter(
            "%(asctime)s %(name)s[%(process)s]: [%(levelname)s] %(message)s",
            "%b %d %H:%M:%S")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(log_level)

    def _get_db(self, name):
        """Return a database given the database name, creating the database
           if it does not exist.
        """ 
        try:
            db = self.server.create(name)
        except Exception:
            db = self.server[name]
        return db

    def _sync_views(self, db_name):
        """Fetches design documents from the views_directory, saves/updates
           them in the appropriate database, then build the views. 
        """
        build_views_from_file = ["dpla_db_all_provider_docs.js",
                                 # Uncomment when QA views have been built
                                 #"dpla_db_qa_reports.js",
                                 "dashboard_db_all_provider_docs.js",
                                 "dashboard_db_all_ingestion_docs.js"]
        if db_name == "dpla":
            db = self.dpla_db
        elif db_name == "dashboard":
            db = self.dashboard_db

        for file in os.listdir(self.views_directory):
            if file.startswith(db_name):
                fname = os.path.join(self.views_directory, file)
                with open(fname, "r") as f:
                    design_doc = json.load(f)

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
                    for view in design_doc["views"]:
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

    def _query_all_docs(self, db):
        view_name = "_all_docs"
        for row in db.iterview(view_name, batch=self.batch_size,
                               include_docs=True):
            yield row["doc"]

    def _query_all_provider_docs(self, db, provider_name):
        """Fetches all provider docs by provider name. The key for this view
           is the list [provider_name, doc._id], so we supply "0" as the
           startkey doc._id and "Z" as the endkey doc._id in order to ensure
           proper sorting. See collation sequence here:
           https://wiki.apache.org/couchdb/View_collation
        """
        view_name = "all_provider_docs/by_provider_name"
        for row in db.iterview(view_name,
                               batch=self.batch_size,
                               include_docs=True,
                               startkey=[provider_name, "0"],
                               endkey=[provider_name, "Z"]):
            yield row["doc"]

    def _query_all_prov_docs_by_ingest_seq(self, db, provider_name,
                                           ingestion_sequence):
        """Fetches all provider docs by provider name and ingestion sequence.
           The key for this view is the list [provider_name,
           ingestion_sequence, doc._id], so we supply "0" as the startkey
           doc._id and "Z" as the endkey doc._id in order to ensure proper
           sorting. See collation sequence here:
           https://wiki.apache.org/couchdb/View_collation
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
        return self._query_all_provider_docs(self.dpla_db, provider_name)

    def _query_all_dpla_prov_docs_by_ingest_seq(self, provider_name,
                                                ingestion_sequence):
        return self._query_all_prov_docs_by_ingest_seq(self.dpla_db,
                                                      provider_name,
                                                      ingestion_sequence)
 
    def _query_all_dashboard_provider_docs(self, provider_name):
        return self._query_all_provider_docs(self.dashboard_db,
                                                provider_name)

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
            # Sort by ingestionSequence
            ingestion_docs = sorted(ingestion_docs,
                                    key=lambda k: k["ingestionSequence"])
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
        self._sync_views(db.name)

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
        # Rebuild database views
        self._sync_views(db.name)
        self.logger.debug("%s database response: %s" % (db.name, resp))

    def _create_ingestion_document(self, provider, uri_base, profile_path):
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
            "dashboard_cleanup_process": {
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
                "total_enriched": None,
                "total_items": None,
                "total_collections": None,
                "missing_id": None,
                "missing_source_resource": None
            }
        }

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
            except:
                print >> sys.stderr, "Error backing up data"
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
            for doc in self._query_all_dpla_prov_docs_by_ingest_seq(provider,
                                                                    prev_seq):
                delete_docs.append(doc)
                dashboard_docs.append({"id": doc["_id"],
                                       "type": "record",
                                       "status": "deleted",
                                       "provider": provider,
                                       "ingestionSequence": curr_seq})

                # So as not to use too much memory at once, do the bulk posts
                # and deletions in batches of batch_size
                if len(delete_docs) == self.batch_size:
                    try:
                        self._bulk_post_to(self.dashboard_db, dashboard_docs)
                    except:
                        print >> sys.stderr, "Error posting to dashboard db"
                        return (-1, total_deleted)
                    self._update_ingestion_doc_counts(
                        ingestion_doc, countDeleted=len(delete_docs)
                        )
                    try:
                        self._delete_documents(self.dpla_db, delete_docs)
                    except:
                        print >> sys.stderr, "Error deleting from dpla db"
                        return (-1, total_deleted)
                    total_deleted += len(delete_docs)
                    delete_docs = []
                    dashboard_docs = []

            if delete_docs:
                # Last bulk post
                try:
                    self._bulk_post_to(self.dashboard_db, dashboard_docs)
                except:
                    print >> sys.stderr, "Error posting to dashboard db"
                    return (-1, total_deleted)
                self._update_ingestion_doc_counts(
                    ingestion_doc, countDeleted=len(delete_docs)
                    )
                try:
                    self._delete_documents(self.dpla_db, delete_docs)
                except:
                    print >> sys.stderr, "Error deleting from dpla db"
                    return (-1, total_deleted)
                total_deleted += len(delete_docs)
        return (0, total_deleted)

    def dashboard_cleanup(self, ingestion_doc):
        """Deletes a provider's dashboard item-level documents whose ingestion
           sequence is not in the last three ingestions.

           Returns a status (-1 for error, 0 for success) along with the total
           number of documents deleted.
        """
        total_deleted = 0
        curr_seq = int(ingestion_doc["ingestionSequence"])
        if curr_seq <= 3:
            msg = "Current ingestion sequence %s <= 3. " % curr_seq
            msg += "No dashboard documents will be deleted."
            print >> sys.stderr, msg
            return (0, total_deleted)
        else:
            provider = ingestion_doc["provider"]
            seq_to_delete = range(1, curr_seq-2)

            delete_docs = []
            for seq in seq_to_delete:
                for doc in self._query_all_dashboard_prov_docs_by_ingest_seq(
                                                              provider, seq
                                                              ):
                    if not doc.get("type") == "ingestion":
                        delete_docs.append(doc)

                    # So as not to use too much memory at once, do the bulk
                    # posts and deletions in batches of batch_size
                    if len(delete_docs) == self.batch_size:
                        try:
                            self._delete_documents(self.dashboard_db,
                                                   delete_docs)
                        except:
                            print >> sys.stderr, "Error deleting from " + \
                                                 "dashboard db"
                            return (-1, total_deleted)
                        total_deleted += len(delete_docs)
                        delete_docs = []

            if delete_docs:
                # Last bulk post
                try:
                    self._delete_documents(self.dashboard_db, delete_docs)
                except:
                    print >> sys.stderr, "Error deleting from dashboard db"
                    return (-1, total_deleted)
                total_deleted += len(delete_docs)

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
        
        status = -1
        error_msg = None
        try:
            self._bulk_post_to(self.dashboard_db, added_docs + changed_docs)
        except:
            error_msg = "Error posting to the dashboard database"
            return (status, error_msg)
        try:
            self._update_ingestion_doc_counts(ingestion_doc,
                                              countAdded=len(added_docs),
                                              countChanged=len(changed_docs))
        except:
            error_msg = "Error updating ingestion document counts"
            return (status, error_msg)
        try:
            self._bulk_post_to(self.dpla_db, harvested_docs.values())
        except:
            error_msg = "Error posting to the dpla database"
            return (status, error_msg)

        status = 0
        return (status, error_msg)

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
            print "Deleting DPLA provider documents..."
            count = 0
            delete_docs = []
            for doc in self._query_all_dpla_provider_docs(provider):
                delete_docs.append(doc)
                count += 1
                # Delete in batches of batch_size so as not to use too much
                # memory
                if len(delete_docs) == self.batch_size:
                    print "%s documents deleted" % count
                    self._delete_documents(self.dpla_db, delete_docs)
                    delete_docs = []
            # Last delete
            if delete_docs:
                    print "%s documents deleted" % count
                    self._delete_documents(self.dpla_db, delete_docs)

            # Bulk post backup database documents without revision
            print "Retrieving documents from database %s" % backup_db_name
            count = 0
            docs = []
            for doc in self._query_all_docs(self.server[backup_db_name]):
                count += 1
                if "_rev" in doc:
                    del doc["_rev"]
                docs.append(doc)
                if len(docs) == self.batch_size:
                    print "%s documents rolled back" % count
                    self._bulk_post_to(self.dpla_db, docs)
                    docs = []
            # Last POST
            if docs:
                print "%s documents rolled back" % count
                self._bulk_post_to(self.dpla_db, docs)

            msg = "Rollback complete"
            self.logger.debug(msg)
        else:
            msg = "Attempted to rollback but no ingestion document with " + \
                  "ingestionSequence of %s was found" % ingest_sequence
            self.logger.error(msg)

        return msg
