/*
 * View for listing all documents which need to have the thumbnails downloaded.
 *
 * It means all that:
 *   - have the field preview_source_url
 *   - don't have the field preview_file_path
 *
 * The ordering doesn't matter, as all the documents need to be processed.
 */
{
    "_id": "_design/thumbnails",
    "_rev": "1-f244eaae163fe7dfa60f84c2bc6fa02a",
    "language": "javascript",
    "views": {
        "all_for_downloading": {
        "map": "function(doc) {\n  if (doc.preview_source_url && !doc.preview_file_path) {\n    emit(doc._id, doc);\n  }\n}"
        }
    }
}
