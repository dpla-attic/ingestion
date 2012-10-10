To install the ingest subsystem, first install the necessary components;

    $ pip install -r requirements.txt

Then set up and start the (Akara) server;

    $ akara -f akara.conf setup
    $ akara -f akara.conf start

You can test it with this set description from Clemson;

    $ curl "http://localhost:8889/oai.listrecords.json?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe&oaiset=jfb&limit=10" 

If you have the endpoint URL but not a set id, there's a separate service for listing the sets;

    $ curl "http://localhost:8889/oai.listsets.json?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe&limit=10"

To run the ingest process, install the script then feed it a source profile description;

    $ python setup.py install
    $ mkdir profiles && mkdir data
    $ cat <<DONE  >profiles/myprofile.pjs
    {"endpoint_URL":"http://localhost:8889/oai.listrecords.json?endpoint=http://repository.clemson.edu/cgi-bin/oai.exe&oaiset=jfb&limit=10"}
    DONE
    $ poll_profiles profiles data