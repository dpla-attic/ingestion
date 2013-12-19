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
    Url=<URL to CouchDB instance, including trailing forward-slash>
    Username=<CouchDB username>
    Passowrd=<CouchDB password>

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

To run the ingest process run the setup.py script, if not done so already, initialize the database and database views, then feed it a source profile (found in the profiles directory);

    $ python setup.py install
    $ python scripts/sync_couch_views dpla
    $ python scripts/sync_couch_views dashboard
    $ python scripts/ingest_provider.py profiles/clemson.pjs

License
--------
This application is released under a AGPLv3 license.

Copyright President and Fellows of Harvard College, 2013
