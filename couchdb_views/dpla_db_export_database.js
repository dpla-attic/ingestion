{
    "_id": "_design/export_database",
    "language": "javascript",
    "views": {
        "all_source_names": {
            "map": "function(doc) {
                        if (doc.ingestType == 'item') {
                            emit(doc.provider.name, null);
                        }
                    }",
            "reduce": "_count"
        },
        "profile_and_source_names": {
            "map": "function(doc) {
                        if (doc.ingestType == 'item') {
                            profile_name = doc._id.split('--').shift();
                            emit([doc.provider.name, profile_name], null);
                        }
                    }",
            "reduce": "_count"
        }
    }
}
