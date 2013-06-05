{
   "_id": "_design/all_provider_docs",
   "language": "javascript",
   "views": {
       "by_provider_name": {
           "map": "function(doc) { provider_name = doc._id.split('--').shift(); if (provider_name == 'clemson') { provider_name = 'scdl-clemson'; } if (provider_name == 'ia') { provider_name = 'internet_archive'; } if (provider_name == 'kentucky') { provider_name = 'kdl'; } if (provider_name == 'minnesota') { provider_name = 'mdl'; } emit(provider_name, null) }"
       },
       "by_provider_name_and_ingestion_version": {
           "map": "function(doc) { provider_name = doc._id.split('--').shift(); if (provider_name == 'clemson') { provider_name = 'scdl-clemson'; } if (provider_name == 'ia') { provider_name = 'internet_archive'; } if (provider_name == 'kentucky') { provider_name = 'kdl'; } if (provider_name == 'minnesota') { provider_name = 'mdl'; } emit([provider_name, doc.ingestion_version], null) }"
       }
   }
}
