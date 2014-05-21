{
   "_id": "_design/all_docs",
   "language": "javascript",
   "views": {
       "by_contributor": {
           "map": "function(doc) { emit(doc.contributor, null) }"
       }
   }
}
