The DPLA Ingestion
-------------------

Build Status
-------------------

[![Build Status](https://travis-ci.org/dpla/ingestion.png?branch=develop)](https://travis-ci.org/dpla/ingestion)

Documentation
-------------------
Prerequisites:
    Python 2.7

To install or upgrade the ingest subsystem, first install the necessary components;

    $ pip install --no-deps --ignore-installed -r requirements.txt

Configure an akara.ini file appropriately for your environment;

    [Akara]
    Port=<port for Akara to run on>

    [Bing]
    ApiKey=<your Bing Maps API key>

    [CouchDb]
    Server=http://<CouchDB username>:<CouchDB password>@<URL to CouchDB instance>:<Port>/
    DPLADatabase=dpla
    DashboardDatabase=dashboard
    ViewsDirectory=couchdb_views
    BatchSize=500
    LogLevel=<Desired CouchDB log level, ie DEBUG>

    [Bing]
    ApiKey=<Bing API key>

    [Geonames]
    Username=<Geonames username>
    Token=<Geonames token>

    [Rackspace]
    Username=<Rackspace username>
    ApiKey=<Rackspace API key>
    ContainerName=<Rackspace container>
    

The akara.conf.template and akara.ini file are merged to generate the akara.conf file by running;

    $ python setup.py install 

Set up and start the (Akara) server;

    $ akara -f akara.conf setup
    $ akara -f akara.conf start

You can test it with this set description from Clemson;

    $ curl "http://localhost:8889/oai.listrecords.json?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe&oaiset=jfb&limit=10" 

If you have the endpoint URL but not a set id, there's a separate service for listing the sets;

    $ curl "http://localhost:8889/oai.listsets.json?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe&limit=10"

To run the ingest process run the setup.py script, if not done so already, initialize the database and database views, then feed it a source profile description;

    $ python setup.py install
    $ python scripts/sync_couch_views dpla
    $ python scripts/sync_couch_views dashboard
    $ mkdir profiles && mkdir data
    $ cat <<DONE  >profiles/myprofile.pjs
    {"name":"clemsontest",
     "subresources":["gmb","ctm"],
     "endpoint_URL":"http://localhost:8889/dpla-list-records?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe&oaiset=",
     "enrichments_coll": ["http://localhost:8889/oai-set-name?sets_service=http://localhost:8889/oai.listsets.json?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe"],
     "enrichments_rec": ["http://localhost:8889/geocode?prop=coverage&newprop=coverage_geo","http://localhost:8889/shred?prop=subject&delim=%3b","http://localhost:8889/oai-to-dpla"]
    }
    DONE
    $ python scripts/ingest_provider.py profiles/myprofile.pjs

Source profiles are represented as JSON objects. Their properties include;

* endpoint_URL; the Akara-wrapped URL from which JSON representations are retrieved.
* subresources; for OAI, names individual sets in an OAI store. When used, endpoint_URL should terminate with "&oaiset=" (this may change)
* enrichments_coll; ordered list of Akara enrichment services for collections, including any service specific query parameters
* enrichments_rec; ordered list of Akara enrichment services for records, including any service specific query parameters
* type; used in distinguishing which method is used to fetch the data (acceptable values: oai_verbs, ia, nypl, mwdl, edan, nara, uva, hathi)

Enrichment pipelines are implemented through a central enrichment service which interprets the list of other services as communicated via a "Pipeline" HTTP header on a POST request. For example, given a data.sjs data document, the following request will send that data through the provided pipeline;

    $ curl -X POST -d @data.sjs -H "Pipeline: http://localhost:8889/geocode?p=location" http://localhost:8889/enrich

The provided enrichment services include;

* shred/unshred; ',' based string/list and list/string (de)construction. The "prop" parameter specifies which property is to be shredded/unshredded (support multi-properties using a period delimiter)
* geocode; creates a new property containing the lat/long of the location present in the property identified by the prop parameter. NOTE; in order to use geo lookups, the geonames sqlite file has to be created using the [instructions](https://foundry.zepheira.com/projects/zenpub/repository/entry/NOTES) and stored in the "caches" directory below the home directory of akara.conf
* select-id; creates or updates an "id" property to the value of the property named by the "prop" parameter
 
License
--------
This application is released under a AGPLv3 license.

Copyright President and Fellows of Harvard College, 2013
