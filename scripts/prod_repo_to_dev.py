import json
import couchdb
import ConfigParser
from gzip import GzipFile as zipf
from export_database import main as export_database

def confirm_deletion():
    prompt = "This will delete the dpla database on the development " + \
             "server. Are you sure you want to continue? yes | no\n"
    while True:
        ans = raw_input(prompt).lower()
        if ans == "yes":
            return True
        elif ans == "no":
            return False
        else:
            print "Please enter yes or no"

def get_dev_dpla_db():
    config = ConfigParser.ConfigParser()
    config.readfp(open("akara.ini"))
    url = config.get("CouchDb", "DevUrl")
    username = config.get("CouchDb", "Username")
    password = config.get("CouchDb", "Password")

    url = url.split("http://")
    server_url = "http://%s:%s@%s" % (username, password, url[1])
    server = couchdb.Server(server_url)

    return server["dpla"]

def main():
    if confirm_deletion():
        export_database([None, "all"])
        with zipf("dpla.gz", "r") as zf:
            data = json.loads(zf.read())
        docs = [row['doc'] for row in data['rows']]

        dev_dpla_db = get_dev_dpla_db()
        new_docs = []
        for doc in docs:
            del doc['_rev']
            new_docs.append(doc)
            if len(new_docs) == 500:
                dev_dpla_db.update(new_docs)
                new_docs = []
        dev_dpla_db.update(new_docs)

if __name__ == '__main__':
    main()
