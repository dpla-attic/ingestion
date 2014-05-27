{
   "_id": "_design/all_provider_docs",
   "language": "javascript",
   "views": {
       "by_provider_name_and_ingestion_sequence": {
           "map": "function(doc) { provider_name = doc._id.split('--').shift(); emit([provider_name, doc.ingestionSequence, doc._id], null) }"
       }
   }
}
