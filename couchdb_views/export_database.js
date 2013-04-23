{
    "_id": "_design/export_database",
    "language": "javascript",
    "views": {
        "all_source_names": {
            "map": "function(doc) { var n = doc._id.split(\"--\"); emit(n[0], null);}",
            "reduce": "_count"
        }
    }
}
