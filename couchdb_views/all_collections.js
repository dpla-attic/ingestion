/*
 * View for listing all collections.
 */
{
	"_id": "_design/collections",
	"_rev": "12345",
	"language": "javascript",
	"views": {
		"all_collections": {    
			"map": "function(doc) { if (doc.ingestType == "collection") emit(null, doc); }"
		}
	}
}
