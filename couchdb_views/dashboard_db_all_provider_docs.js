{
   "_id": "_design/all_provider_docs",
   "language": "javascript",
   "views": {
       "by_provider_name": {
           "map": "function(doc) {
                       emit([doc.provider, doc.id], null);
                   }"
       },
       "by_provider_name_and_ingestion_sequence": {
           "map": "function(doc) {
                       emit([doc.provider, doc.ingestionSequence, doc._id], null);
                   }"
       }
   }
}
