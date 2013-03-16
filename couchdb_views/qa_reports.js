{
   "_id": "_design/lists",
   "language": "javascript",
   "views": {
       "title": {
           "map": "function(doc) { if (doc.ingestType == 'item') { title = doc.sourceResource.title; if (title.constructor.toString().indexOf('Array') == -1) { title = new Array(title); } for (i=0; i<title.length; i++) { emit(doc['id'], title[i]); }}}"
       },
       "title_count": {
           "map": "function(doc) { if (doc.ingestType == 'item') { title = doc.sourceResource.title; if (title.constructor.toString().indexOf('Array') == -1) { title = new Array(title); } for (i=0; i<title.length; i++) { emit(title[i],1); }}}",
           "reduce": "_count"
       },
       "creator": {
           "map": "function(doc) { if (doc.ingestType == 'item') { creator = doc.sourceResource.creator; if (creator.constructor.toString().indexOf('Array') == -1) { creator = new Array(creator); } for (i=0; i<creator.length; i++) { emit(doc['id'], creator[i]);}}}"
       },
       "creator_count": {
           "map": "function(doc) { if (doc.ingestType == 'item') { creator = doc.sourceResource.creator; if (creator.constructor.toString().indexOf('Array') == -1) { creator = new Array(creator); } for (i=0; i<creator.length; i++) { emit(creator[i],1);}}}",
           "reduce": "_count"
       },
       "publisher": {
           "map": "function(doc) {if (doc.ingestType == 'item') {pub = doc.sourceResource.publisher;if (pub.constructor.toString().indexOf('Array') == -1) { pub = new Array(pub); }for (i=0; i<pub.length; i++) {emit(doc['id'], pub[i]);}}}"
       },
       "publisher_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {pub = doc.sourceResource.publisher;if (pub.constructor.toString().indexOf('Array') == -1) { pub = new Array(pub); }for (i=0; i<pub.length; i++) {emit(pub[i],i);}}}",
           "reduce": "_count"
       },
       "dates": {
           "map": "function(doc) {if (doc.ingestType == 'item') {d = doc.sourceResource.date;if (d.constructor.toString().indexOf('Array') == -1) { d = new Array(d); }for (i=0; i<d.length; i++) {emit(doc['id'], d[i]['displayDate']+' ('+d[i]['begin']+' to '+d[i]['end']+')');}}}"
       },
       "dates_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {d = doc.sourceResource.date;if (d.constructor.toString().indexOf('Array') == -1) { d = new Array(d); }for (i=0; i<d.length; i++) {emit(d[i]['displayDate']+' ('+d[i]['begin']+' to '+d[i]['end']+')',1);}}}",
           "reduce": "_count"
       },
       "description": {
           "map": "function(doc) {if (doc.ingestType == 'item') {desc = doc.sourceResource.description;if (desc.constructor.toString().indexOf('Array') == -1) { desc = new Array(desc); }for (i=0; i<desc.length; i++) {emit(doc['id'], desc[i]);}}}"
       },
       "description_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {desc = doc.sourceResource.description;if (desc.constructor.toString().indexOf('Array') == -1) { desc = new Array(desc); }for (i=0; i<desc.length; i++) {emit(desc[i],1);}}}",
           "reduce": "_count"
       },
       "format": {
           "map": "function(doc) {if (doc.ingestType == 'item') {format = doc.sourceResource.format;if (format.constructor.toString().indexOf('Array') == -1) { format = new Array(format); }for (i=0; i<format.length; i++) {emit(doc['id'], format[i]);}}}"
       },
       "format_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {format = doc.sourceResource.format;if (format.constructor.toString().indexOf('Array') == -1) { format = new Array(format); }for (i=0; i<format.length; i++) {emit(format[i],1);}}}",
           "reduce": "_count"
       },
       "type": {
           "map": "function(doc) {if (doc.ingestType == 'item') {t = doc.sourceResource.type;if (t.constructor.toString().indexOf('Array') == -1) { t = new Array(t); }for (i=0; i<t.length; i++) {emit(doc['id'], t[i]);}}}"
       },
       "type_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {t = doc.sourceResource.type;if (t.constructor.toString().indexOf('Array') == -1) { t = new Array(t); }for (i=0; i<t.length; i++) {emit(t[i],1);}}}",
           "reduce": "_count"
       },
       "subject": {
           "map": "function(doc) {if (doc.ingestType == 'item') {sub = doc.sourceResource.subject;if (sub.constructor.toString().indexOf('Array') == -1) { sub = new Array(sub); }for (i=0; i<sub.length; i++) {emit(doc['id'], sub[i].name);}}}"
       },
       "subject_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {sub = doc.sourceResource.subject;if (sub.constructor.toString().indexOf('Array') == -1) { sub = new Array(sub); }for (i=0; i<sub.length; i++) {emit(sub[i].name,1);}}}",
           "reduce": "_count"
       },
       "spatial_state": {
           "map": "function(doc) {if (doc.ingestType == 'item') {spatial = doc.sourceResource.spatial;if (spatial.constructor.toString().indexOf('Array') == -1) { spatial = new Array(spatial); }for (i=0; i<spatial.length; i++) {if ('state' in spatial[i]) {emit(doc['id'], spatial[i]['state'])};}}}"
       },
       "spatial_state_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {spatial = doc.sourceResource.spatial;if (spatial.constructor.toString().indexOf('Array') == -1) { spatial = new Array(spatial); }for (i=0; i<spatial.length; i++) {if ('state' in spatial[i]) {emit(spatial[i]['state'],1)};}}}",
           "reduce": "_count"
       },
       "spatial_name": {
           "map": "function(doc) {if (doc.ingestType == 'item') {spatial = doc.sourceResource.spatial;if (spatial.constructor.toString().indexOf('Array') == -1) { spatial = new Array(spatial); }for (i=0; i<spatial.length; i++) {if ('state' in spatial[i]) {emit(doc['id'], spatial[i]['name'])};}}}"
       },
       "spatial_name_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {spatial = doc.sourceResource.spatial;if (spatial.constructor.toString().indexOf('Array') == -1) { spatial = new Array(spatial); }for (i=0; i<spatial.length; i++) {if ('state' in spatial[i]) {emit(spatial[i]['name'],1)};}}}",
           "reduce": "_count"
       },
       "rights": {
           "map": "function(doc) {if (doc.ingestType == 'item') {rights = doc.sourceResource.rights;if (rights.constructor.toString().indexOf('Array') == -1) { rights = new Array(rights); }for (i=0; i<rights.length; i++) {emit(doc['id'], rights[i]);}}}"
       },
       "rights_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {rights = doc.sourceResource.rights;if (rights.constructor.toString().indexOf('Array') == -1) { rights = new Array(rights); }for (i=0; i<rights.length; i++) {emit(rights[i],1);}}}",
           "reduce": "_count"
       },
       "provider": {
           "map": "function(doc) {if (doc.ingestType == 'item') {provider = doc.provider;if (provider.constructor.toString().indexOf('Array') == -1) { provider = new Array(provider); }for (i=0; i<provider.length; i++) {emit(doc['id'], provider[i]['name']);}}}"
       },
       "provider_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {provider = doc.provider;if (provider.constructor.toString().indexOf('Array') == -1) { provider = new Array(provider); }for (i=0; i<provider.length; i++) {emit(provider[i]['name'],1);}}}",
           "reduce": "_count"
       },
       "data_provider": {
           "map": "function(doc) {if (doc.ingestType == 'item') {dataProvider = doc.dataProvider;if (dataProvider.constructor.toString().indexOf('Array') == -1) { dataProvider = new Array(dataProvider); }for (i=0; i<dataProvider.length; i++) {emit(doc['id'], dataProvider[i]);}}}"
       },
       "data_provider_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {dataProvider = doc.dataProvider;if (dataProvider.constructor.toString().indexOf('Array') == -1) { dataProvider = new Array(dataProvider); }for (i=0; i<dataProvider.length; i++) {emit(dataProvider[i],1);}}}",
           "reduce": "_count"
       },
       "collection": {
           "map": "function(doc) {if (doc.ingestType == 'item') {cname = doc.collection.name;emit(doc['id'], cname);}}"
       },
       "collection_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {cname = doc.collection.name;emit(cname,1);}}",
           "reduce": "_count"
       },
       "contributor": {
           "map": "function(doc) {if (doc.ingestType == 'item') {contributor = doc.sourceResource.contributor;if (contributor.constructor.toString().indexOf('Array') == -1) { contributor = new Array(contributor); }for (i=0; i<contributor.length; i++) {emit(doc['id'], contributor[i]);}}}"
       },
       "contributor_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {contributor = doc.sourceResource.contributor;if (contributor.constructor.toString().indexOf('Array') == -1) { contributor = new Array(contributor); }for (i=0; i<contributor.length; i++) {emit(contributor[i],1);}}}",
           "reduce": "_count"
       },
       "language": {
           "map": "function(doc) {if (doc.ingestType == 'item') {language = doc.sourceResource.language;if (language.constructor.toString().indexOf('Array') == -1) { language = new Array(language); }for (i=0; i<language.length; i++) {emit(doc['id'], language[i]['name']);}}}"
       },
       "language_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {language = doc.sourceResource.language;if (language.constructor.toString().indexOf('Array') == -1) { language = new Array(language); }for (i=0; i<language.length; i++) {emit(language[i]['name'],1);}}}",
           "reduce": "_count"
       },
       "temporal": {
           "map": "function(doc) {if (doc.ingestType == 'item') {temporal = doc.sourceResource.temporal;if (temporal.constructor.toString().indexOf('Array') == -1) { temporal = new Array(temporal); }for (i=0; i<temporal.length; i++) {emit(doc['id'], temporal[i]['displayDate']+' ('+d[i]['begin']+' to '+d[i]['end']+')');}}}"
       },
       "temporal_count": {
           "map": "function(doc) {if (doc.ingestType == 'item') {temporal = doc.sourceResource.temporal;if (temporal.constructor.toString().indexOf('Array') == -1) { temporal = new Array(temporal); }for (i=0; i<temporal.length; i++) {emit(temporal[i]['displayDate']+' ('+d[i]['begin']+' to '+d[i]['end']+')',1);}}}",
           "reduce": "_count"
       }
   },
   "lists": {
       "csv": "function(head, req) { start({'headers': {'Content-Type': 'text/csv'}});var row; while (row = getRow()) { send(row.key + ',' + row.value + '\\n'); }}",
       "count_csv": "function(head, req) { start({'headers': {'Content-Type': 'text/csv'}});var row; while (row = getRow()) { send(row.key + ',' + row.value + '\\n'); }}"
   }
}
