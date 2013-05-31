{
   "_id": "_design/all_provider_docs",
   "language": "javascript",
   "views": {
       "by_provider_name": {
           "map": "function(doc) {  emit(doc._id.split('--').shift(), null) }"
       }
   }
}
