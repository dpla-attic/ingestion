The DPLA Ingestion
-------------------

Build Status
-------------------

[![Build Status](https://travis-ci.org/dpla/ingestion.png?branch=develop)](https://travis-ci.org/dpla/ingestion)

Documentation
-------------------
Setting up the ingestion server:

Install Python 2.7 if not already installed (http://www.python.org/download/releases/2.7/);

Install PIP (http://pip.readthedocs.org/en/latest/installing.html);

Install the ingestion subsystem;

    $ pip install --no-deps --ignore-installed -r requirements.txt

Configure an akara.ini file appropriately for your environment;

    [Akara]
    Port=<port for Akara to run on>
    ; Recommended LogLevel is one of DEBUG or INFO
    LogLevel=<priority>

    [Bing]
    ApiKey=<your Bing Maps API key>

    [CouchDb]
    Url=<URL to CouchDB instance, including trailing forward-slash>
    Username=<CouchDB username>
    Password=<CouchDB password>

    [Geonames]
    Username=<Geonames username>
    Token=<Geonames token>

    [Rackspace]
    Username=<Rackspace username>
    ApiKey=<Rackspace API key>
    DPLAContainer=<Rackspace container for bulk download data>
    SitemapContainer=<Rackspace container for sitemap files>

    [Sitemap]
    SitemapURI=<Sitemap URI>
    
    [Alert]
    To=<Comma-separated email addresses to receive alert email>
    From=<Email address to send alert email>

    [Enrichment]
    QueueSize=4
    ThreadCount=4

Merge the akara.conf.template and akara.ini file to create the akara.conf file;

    $ python setup.py install 

Set up and start the Akara server;

    $ akara -f akara.conf setup
    $ akara -f akara.conf start

Build the database views;

    $ python scripts/sync_couch_views.py dpla
    $ python scripts/sync_couch_views.py dashboard
    $ python scripts/sync_couch_views.py bulk_download

Testing the ingestion server:

You can test it with this set description from Clemson;

    $ curl "http://localhost:8889/oai.listrecords.json?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe&oaiset=jfb&limit=10" 

If you have the endpoint URL but not a set id, there's a separate service for listing the sets;

    $ curl "http://localhost:8889/oai.listsets.json?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe&limit=10"

To run the ingest process run the setup.py script, if not done so already, initialize the database and database views, then feed it a source profile (found in the profiles directory);

    $ python setup.py install
    $ python scripts/sync_couch_views.py dpla
    $ python scripts/sync_couch_views.py dashboard
    $ python scripts/ingest_provider.py profiles/clemson.pjs

License
--------
This application is released under a AGPLv3 license.

* Copyright President and Fellows of Harvard College, 2013
* Copyright Digital Public Library of America, 2014
