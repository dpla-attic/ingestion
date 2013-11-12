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
           "map": "function(doc) { if (doc.type == 'ingestion' && (doc.fetch_process.status != 'complete' || doc.enrich_process.status != 'complete' || doc.save_process.status != 'complete' || doc.delete_process.status != 'complete' || doc.dashboard_cleanup_process.status != 'complete')) { emit(doc.provider, null)}}"
       }
   }
}
