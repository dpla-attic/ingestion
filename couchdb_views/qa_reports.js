{
    "_id": "_design/lists",
	"language": "javascript",
	"views": {
	"format": {
	    "map": "function(doc) {\n  if (doc.ingestType == 'item') {\n\temit(null, doc.format);\n  }\n}"
		},
	    "format_count": {
		"map": "function(doc) {\n  if (doc.ingestType == 'item') {\n\temit(doc.format, 1);\n  }\n}",
		    "reduce": "_count"
		    },
		"type": {
		    "map": "function(doc) {\n  if (doc.ingestType == 'item') {\n\temit(null, doc.type);\n  }\n}"
			},
		    "type_count": {
			"map": "function(doc) {\n  if (doc.ingestType == 'item') {\n\temit(doc.type, 1);\n  }\n}",
			    "reduce": "_count"
			    },
			"spatial": {
			    "map": "function(doc) {\n  if (doc.ingestType == 'item') {\n\tfor (var i = 0; i < doc.spatial.length; i++) \n\t{\n\t\temit(doc.id , doc.spatial[i].name);\n\t}\n  }\n}"
				},
			    "spatial_count": {
				"map": "function(doc) {\n  if (doc.ingestType == 'item') {\n\tfor (var i = 0; i < doc.spatial.length; i++) \n\t{\n\t\temit(doc.spatial[i].name, 1);\n\t}\n  }\n}",
				    "reduce": "_count"
				    },
				"subject": {
				    "map": "function(doc) {\n  if (doc.ingestType == 'item') {\n\tfor (var i = 0; i < doc.subject.length; i++) \n\t{\n\t\temit(doc.id , doc.subject[i].name);\n\t}\n  }\n}"
					},
				    "subject_count": {
					"map": "function(doc) {\n  if (doc.ingestType == 'item') {\n\tfor (var i = 0; i < doc.subject.length; i++) \n\t{\n\t\temit(doc.subject[i].name, 1);\n\t}\n  }\n}",
					    "reduce": "_count"
					    }
    },
	"lists": {
	    "csv": "function(head, req) { start({'headers': {'Content-Type': 'text/csv'}});var row; while (row = getRow()) { send(row.id + ',' + row.value + '\\n'); }}",
		"count_csv": "function(head, req) { start({'headers': {'Content-Type': 'text/csv'}});var row; while (row = getRow()) { send(row.key + ',' + row.value + '\\n'); }}"
		}
}