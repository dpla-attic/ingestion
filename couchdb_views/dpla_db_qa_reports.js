{
   "_id": "_design/qa_reports",
   "language": "javascript",
   "views": {
       "spec_type": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.specType; if (v) { if (v.constructor.toString().indexOf('Array') == -1) {v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, doc['id']], v[i]); }} else { emit([provider, doc['id']], '__MISSING_SPECTYPE__'); }}"
       },
       "spec_type_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.specType; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i]], 1); }} else { emit([provider, '__MISSING_SPECTYPE__'], 1); }}",
           "reduce": "_count"
       },
       "is_shown_at": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.isShownAt; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, doc['id']], v[i]); }} else { emit([provider, doc['id']], '__MISSING_ISSHOWNAT__'); }}"
       },
       "is_shown_at_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.isShownAt; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i]], 1); }} else { emit([provider, '__MISSING_ISSHOWNAT__'], 1); }}",
           "reduce": "_count"
       },
       "title": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.title; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, doc['id']], v[i]); }} else { emit([provider, doc['id']], '__MISSING_TITLE__'); }}"
       },
       "title_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.title; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i]], 1); }} else { emit([provider, '__MISSING_TITLE__'], 1); }}",
           "reduce": "_count"
       },
       "creator": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.creator; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, doc['id']], v[i]);}} else { emit([provider, doc['id']], '__MISSING_CREATOR__'); }}"
       },
       "creator_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.creator; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i]], 1);}} else { emit([provider, '__MISSING_CREATOR__'], 1); }}",
           "reduce": "_count"
       },
       "publisher": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.publisher; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else { emit([provider, doc['id']], '__MISSING_PUBLISHER__'); }}"
       },
       "publisher_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.publisher; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_PUBLISHER__'], 1); }}",
           "reduce": "_count"
       },
       "dates": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.date; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i].displayDate+' ('+v[i].begin+' to '+v[i].end+')');}} else { emit([provider, doc['id']], '__MISSING_DATES__'); }}"
       },
       "dates_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.date; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i].displayDate+' ('+v[i].begin+' to '+v[i].end+')'], 1);}} else { emit([provider, '__MISSING_DATES__'], 1); }}",
           "reduce": "_count"
       },
       "description": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.description; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else { emit([provider, doc['id']], '__MISSING_DESCRIPTION__'); }}"
       },
       "description_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.description; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else { emit([provider, '__MISSING_DESCRIPTION__'], 1 ); }}",
           "reduce": "_count"
       },
       "format": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.format; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else {emit([provider, doc['id']], '__MISSING_FORMAT__'); }}"
       },
       "format_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.format; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_FORMAT__'], 1); }}",
           "reduce": "_count"
       },
       "type": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.type; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else { emit([provider, doc['id']], '__MISSING_TYPE__'); }}"
       },
       "type_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.type; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_TYPE__'], 1); }}",
           "reduce": "_count"
       },
       "subject": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.subject; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('name' in v[i]) {emit([provider, doc['id']], v[i].name);} else {emit([provider, doc['id']], '__MISSING_SUBJECT_NAME__'); }}} else {emit([provider, doc['id']], '__MISSING_SUBJECT__'); }}"
       },
       "subject_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.subject; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('name' in v[i]) {emit([provider, v[i].name], 1);} else {emit([provider, '__MISSING_SUBJECT_NAME__'], 1); }}} else {emit([provider, '__MISSING_SUBJECT__'], 1); }}",
           "reduce": "_count"
       },
       "spatial_state": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.spatial; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('state' in v[i]) {emit([provider, doc['id']], v[i].state);} else {emit([provider, doc['id']], '__MISSING_SPATIAL_STATE__'); }}} else {emit([provider, doc['id']], '__MISSING_SPATIAL__'); }}"
       },
       "spatial_state_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.spatial; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('state' in v[i]) {emit([provider, v[i].state], 1);} else {emit([provider, '__MISSING_SPATIAL_STATE__'], 1)}}} else { emit([provider, '__MISSING_SPATIAL__'], 1); }}",
           "reduce": "_count"
       },
       "spatial_name": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.spatial; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('name' in v[i]) {emit([provider, doc['id']], v[i].name);} else {emit([provider, doc['id']], '__MISSING_SPATIAL_NAME__'); }}} else {emit([provider, doc['id']], '__MISSING_SPATIAL__'); }}"
       },
       "spatial_name_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.spatial; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('name' in v[i]) {emit([provider, v[i].name], 1);} else {emit([provider, '__MISSING_SPATIAL_NAME__'], 1); }}} else {emit([provider, '__MISSING_SPATIAL__'], 1); }}",
           "reduce": "_count"
       },
       "state_located_in_name": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.stateLocatedIn; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('name' in v[i]) {emit([provider, doc['id']], v[i].name);} else {emit([provider, doc['id']], '__MISSING_STATELOCATEDIN_NAME__'); }}} else {emit([provider, doc['id']], '__MISSING_STATELOCATEDIN__'); }}"
       },
       "state_located_in_name_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.stateLocatedIn; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {if ('name' in v[i]) {emit([provider, v[i].name], 1);} else {emit([provider, '__MISSING_STATELOCATEDIN_NAME__'], 1); }}} else {emit([provider, '__MISSING_STATELOCATEDIN__'], 1); }}",
           "reduce": "_count"
       },
       "rights": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.rights; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else {emit([provider, doc['id']], '__MISSING_RIGHTS__'); }}"
       },
       "rights_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.rights; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_RIGHTS__'], 1); }}",
           "reduce": "_count"
       },
       "provider": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.provider; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('name' in v[i]) {emit([provider, doc['id']], v[i].name);} else { emit([provider, doc['id']], '__MISSING_PROVIDER_NAME__'); }}} else {emit([provider, doc['id']], '__MISSING_PROVIDER__'); }}"
       },
       "provider_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.provider; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('name' in v[i]) {emit([provider, v[i].name], 1);} else {emit([provider, '__MISSING_PROVIDER_NAME__'], 1); }}} else {emit([provider, '__MISSING_PROVIDER__'], 1); }}",
           "reduce": "_count"
       },
       "intermediate_provider": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.intermediateProvider; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, doc['id']], v[i]); }} else { emit([provider, doc['id']], '__MISSING_INTERMEDIATE_PROVIDER__'); }}"
       },
       "intermediate_provider_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.intermediateProvider; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { emit([provider, v[i]], 1); }} else { emit([provider, '__MISSING_INTERMEDIATE_PROVIDER__'], 1); }}",
           "reduce": "_count"
       },
       "data_provider": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.dataProvider; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else {emit([provider, doc['id']], '__MISSING_DATAPROVIDER__'); }}"
       },
       "data_provider_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.dataProvider; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_DATAPROVIDER__'], 1); }}",
           "reduce": "_count"
       },
       "collection_title": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.collection; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('title' in v[i]) {emit([provider, doc['id']], v[i].title); } else {emit([provider, doc['id']], '__MISSING_COLLECTION_TITLE__'); }}} else {emit([provider, doc['id']], '__MISSING_COLLECTION__'); }}"
       },
       "collection_title_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.collection; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('title' in v[i]) {emit([provider, v[i].title], 1);} else { emit([provider, '__MISSING_COLLECTION_TITLE__'], 1); }}} else {emit([provider, '__MISSING_COLLECTION__'], 1); }}",
           "reduce": "_count"
       },
       "collection_description": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.collection; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('description' in v[i]) {emit([provider, doc['id']], v[i].description);} else {emit([provider, doc['id']], '__MISSING_COLLECTION_DESCRIPTION__'); }}} else {emit([provider, doc['id']], '__MISSING_COLLECTION__'); }}"
       },
       "collection_description_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.collection; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('description' in v[i]) {emit([provider, v[i].description], 1);} else { emit([provider, '__MISSING_COLLECTION_DESCRIPTION__'], 1); }}} else {emit([provider, '__MISSING_COLLECTION__'], 1); }}",
           "reduce": "_count"
       },
       "contributor": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.contributor; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else {emit([provider, doc['id']], '__MISSING_CONTRIBUTOR__'); }}"
       },
       "contributor_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.contributor; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_CONTRIBUTOR__'], 1); }}",
           "reduce": "_count"
       },
       "language_name": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.language; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('name' in v[i]) {emit([provider, doc['id']], v[i].name);} else {emit([provider, doc['id']], '__MISSING_LANGUAGE_NAME__'); }}} else {emit([provider, doc['id']], '__MISSING_LANGUAGE__'); }}"
       },
       "language_name_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.language; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('name' in v[i]) {emit([provider, v[i].name], 1);} else {emit([provider, '__MISSING_LANGUAGE_NAME__'], 1); }}} else {emit([provider, '__MISSING_LANGUAGE__'], 1); }}",
           "reduce": "_count"
       },
       "language_iso": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.language; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('iso639_3' in v[i]) {emit([provider, doc['id']], v[i]['iso639_3']);} else {emit([provider, doc['id']], '__MISSING_LANGUAGE_ISO639_3__'); }}} else {emit([provider, doc['id']], '__MISSING_LANGUAGE__');}}"
       },
       "language_iso_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.language; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) { if ('iso639_3' in v[i]) {emit([provider, v[i]['iso639_3']], 1);} else {emit([provider, '__MISSING_LANGUAGE_ISO639_3__'], 1); }}} else {emit([provider, '__MISSING_LANGUAGE__'], 1);}}",
           "reduce": "_count"
       },
       "temporal": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.temporal; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i].displayDate+' ('+v[i].begin+' to '+v[i].end+')');}} else {emit([provider, doc['id']], '__MISSING_TEMPORAL__'); }}"
       },
       "temporal_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.temporal; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i].displayDate+' ('+v[i].begin+' to '+v[i].end+')'], 1);}} else {emit([provider, '__MISSING_TEMPORAL__'], 1); }}",
           "reduce": "_count"
       },
       "is_part_of": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.isPartOf; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else {emit([provider, doc['id']], '__MISSING_ISPARTOF__'); }}"
       },
       "is_part_of_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.isPartOf; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_ISPARTOF__'], 1); }}",
           "reduce": "_count"
       },
       "relation": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.relation; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else {emit([provider, doc['id']], '__MISSING_RELATION__'); }}"
       },
       "relation_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.relation; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_RELATION__'], 1); }}",
           "reduce": "_count"
       },
       "extent": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.extent; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else {emit([provider, doc['id']], '__MISSING_EXTENT__'); }}"
       },
       "extent_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.extent; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_EXTENT__'], 1); }}",
           "reduce": "_count"
       },
       "identifier": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.identifier; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, doc['id']], v[i]);}} else {emit([provider, doc['id']], '__MISSING_IDENTIFIER__'); }}"
       },
       "identifier_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); v = doc.sourceResource.identifier; if (v) { if (v.constructor.toString().indexOf('Array') == -1) { v = new Array(v); } for (i=0; i<v.length; i++) {emit([provider, v[i]], 1);}} else {emit([provider, '__MISSING_IDENTIFIER__'], 1); }}",
           "reduce": "_count"
       },
       "invalid_record_messages": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); if (doc.admin !== undefined) { v = doc.admin.valid_after_enrich; if (v === false) {emit([provider, doc['id']], doc.admin.validation_message);}}}"
       },
       "invalid_record_messages_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); if (doc.admin !== undefined) { v = doc.admin.valid_after_enrich; if (v === false) {emit([provider, doc.admin.validation_message], 1);}}}",
           "reduce": "_count"
       },
       "validation_status": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); if (doc.admin === undefined) { status = 'not validated'; } else { v = doc.admin.valid_after_enrich; if (typeof v === 'boolean') { status = (v ? 'valid' : 'invalid'); } else { status = 'not validated'; } } emit([provider, doc['id']], status); }"
       },
       "validation_status_count": {
           "map": "function(doc) { provider = doc._id.split('--').shift(); if (doc.admin === undefined) { status = 'not validated'; } else { v = doc.admin.valid_after_enrich; if (typeof v === 'boolean') { status = (v ? 'valid' : 'invalid'); } else { status = 'not validated'; } } emit([provider, status], 1); }",
           "reduce": "_count"
       }
   },
   "lists": {
       "csv": "function(head, req) { start({'headers': {'Content-Type': 'text/csv'}});var row; while (row = getRow()) { send(row.key + ',' + row.value + '\\n'); }}",
       "count_csv": "function(head, req) { start({'headers': {'Content-Type': 'text/csv'}});var row; while (row = getRow()) { send(row.key + ',' + row.value + '\\n'); }}"
   }
}
