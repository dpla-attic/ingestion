{
    "_id": "_design/export_database",
    "language": "javascript",
    "views": {
        "all_source_names": {
            "map": "function(doc) { emit(doc.provider.name, null);}",
            "reduce": "_count"
        },
        "all_source_docs": {
            "map": "function(doc) { emit(doc.provider.name, null);}"
        }
    }
}
