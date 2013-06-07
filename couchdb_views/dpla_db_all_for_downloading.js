{
    "_id": "_design/thumbnails",
    "language": "javascript",
    "views": {
        "all_for_downloading": {
        "map": "function(doc) {\n  if (doc.preview_source_url && !doc.preview_file_path) {\n    emit(doc._id, doc);\n  }\n}"
        }
    }
}
