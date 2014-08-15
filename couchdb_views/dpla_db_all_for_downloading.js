{
    "_id": "_design/thumbnails",
    "language": "javascript",
    "views": {
        "all_for_downloading": {
        "map": "function(doc) {
                    if (doc.preview_source_url && !doc.preview_file_path) {
                        emit(doc._id, doc);
                    }
                }"
        }
    }
}
