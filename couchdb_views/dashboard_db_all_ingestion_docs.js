{
   "_id": "_design/all_ingestion_docs",
   "language": "javascript",
   "views": {
       "by_provider_name": {
           "map": "function(doc) { if (doc.type == 'ingestion') { emit(doc.provider, doc.ingestionSequence) } }"
       },
       "by_provider_name_and_ingestion_sequence": {
           "map": "function(doc) { if (doc.type == 'ingestion') { emit([doc.provider, doc.ingestionSequence], null) } }"
       }
   }
}
