{
   "_id": "_design/all_ingestion_docs",
   "language": "javascript",
   "views": {
       "by_provider_name": {
           "map": "function(doc) { if (doc.type == 'ingestion') { emit(doc.provider, doc.ingestionSequence) } }"
       },
       "by_provider_name_and_ingestion_sequence": {
           "map": "function(doc) { if (doc.type == 'ingestion') { emit([doc.provider, doc.ingestionSequence], null) } }"
       },
       "for_active_ingestions": {
           "map": "function(doc) { if (doc.type == 'ingestion' && (doc.fetch_process.status == 'running' || doc.enrich_process.status == 'running' || doc.save_process.status == 'running' || doc.delete_process.status == 'running' || doc.dashboard_cleanup_process.status == 'running' || doc.poll_storage_process.status == 'running')) { emit(doc.provider, null)}}"
       }
   }
}
