{
    "_id": "_design/qa_reports",
    "language": "javascript",
    "views": {
        "sourceResource.specType": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.specType; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "isShownAt": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.isShownAt; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.title": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.title; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.creator": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.creator; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.publisher": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.publisher; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.date": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.date; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i].displayDate+' ('+v[i].begin+' to '+v[i].end+')', doc['id']], 1);}} else { emit([provider, '__MISSING_DATES__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.description": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.description; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.format": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.format; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.type": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.type; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.subject.name": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.subject; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('name' in v[i]) { emit([provider, v[i].name, doc['id']], 1); } else { emit([provider, '__MISSING__', doc['id']], 1); }}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.spatial.state": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.spatial; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('state' in v[i]) { emit([provider, v[i].state, doc['id']], 1); } else { emit([provider, '__MISSING__', doc['id']], 1); }}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.spatial.name": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.spatial; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('name' in v[i]) { emit([provider, v[i].name, doc['id']], 1); } else { emit([provider, '__MISSING__', doc['id']], 1); }}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.stateLocatedIn.name": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.stateLocatedIn; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('name' in v[i]) { emit([provider, v[i].name, doc['id']], 1); } else { emit([provider, '__MISSING__', doc['id']], 1); }}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.rights": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.rights; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "provider": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.provider; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('name' in v[i]) { emit([provider, v[i].name, doc['id']], 1); } else { emit([provider, '__MISSING__', doc['id']], 1); }}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "dataProvider": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.dataProvider; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.collection.title": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.collection; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('title' in v[i]) { emit([provider, v[i].title, doc['id']], 1); } else { emit([provider, '__MISSING__', doc['id']], 1); }}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.collection.description": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.collection; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('description' in v[i]) { emit([provider, v[i].description, doc['id']], 1); } else { emit([provider, '__MISSING__', doc['id']], 1); }}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.contributor": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.contributor; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.language.name": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.language; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('name' in v[i]) { emit([provider, v[i].name, doc['id']], 1); } else { emit([provider, '__MISSING__', doc['id']], 1); }}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.language.iso639_3": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.language; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('iso639_3' in v[i]) {emit([provider, v[i].iso639_3, doc['id']], 1); } else {emit([provider, '__MISSING__', doc['id']], 1); }}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.temporal": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.temporal; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i].displayDate+' ('+v[i].begin+' to '+v[i].end+')', doc['id']], 1);}} else { emit([provider, '__MISSING_DATES__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.isPartOf": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.isPartOf; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.relation": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.relation; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.extent": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.extent; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "sourceResource.identifier": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var v = doc.sourceResource.identifier; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i], doc['id']], 1);}} else { emit([provider, '__MISSING__', doc['id']], 1); }}",
            "reduce": "_count"
        },
        "validation_invalid_record_messages": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); if (doc.admin !== undefined) { var v = doc.admin.valid_after_enrich; if (v === false) {emit([provider, doc.admin.validation_message, doc['id']], 1);}}}",
            "reduce": "_count"
        },
        "validation_status": {
            "map": "function(doc) { var provider = doc._id.split('--').shift(); var status = 'not validated'; if (doc.admin !== undefined) { var v = doc.admin.valid_after_enrich; if (typeof v === 'boolean') { status = (v ? 'valid' : 'invalid'); } else { status = 'not validated'; } } emit([provider, status, doc['id']], 1); }",
            "reduce": "_count"
        }
    },
    "lists": {
        "csv": "function(head, req) { start({'headers': {'Content-Type': 'text/csv'}});var row; while (row = getRow()) { send(row.key + ',' + row.value + '\\n'); }}",
        "count_csv": "function(head, req) { start({'headers': {'Content-Type': 'text/csv'}});var row; while (row = getRow()) { send(row.key + ',' + row.value + '\\n'); }}"
    }
}