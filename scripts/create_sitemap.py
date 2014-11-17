import os
import time
import shutil
import tarfile
from datetime import date, datetime
from dateutil.parser import parse as dateutil_parse
import ConfigParser
from dplaingestion.couch import Couch
from dplaingestion.utilities import iso_utc_with_tz, url_join
import pyrax

# TODO: Make a generalized config loader for all the scripts.
config_file = "akara.ini"
CONFIG = ConfigParser.ConfigParser()
CONFIG.readfp(open(config_file))

def send_sitemap_to_rackspace(sitemap_files_path):
    global CONFIG
    RS_USERNAME = CONFIG.get("Rackspace", "Username")
    RS_APIKEY = CONFIG.get("Rackspace", "ApiKey")
    RS_CONTAINER_NAME = CONFIG.get("Rackspace", "SitemapContainer")

    print "Loading files from %s to Rackspace CDN." % sitemap_files_path
    pyrax.set_setting("identity_type", "rackspace")
    pyrax.set_credentials(RS_USERNAME, RS_APIKEY)
    pyrax.cloudfiles.sync_folder_to_container(sitemap_files_path,
                                              RS_CONTAINER_NAME)

def create_sitemap_files(path, urls, count):
    fpath = os.path.join(path, "all_item_urls_%s.xml" % count)
    with open(fpath, "w") as f:
        line = '<?xml version="1.0" encoding="UTF-8"?>\n' + \
               '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f.write(line)
        for url in urls:
            line = "\t<url>\n" + \
                   "\t\t<loc>%s</loc>\n\t\t<lastmod>%s</lastmod>\n" % \
                   (url["loc"], url["lastmod"])
            line += "\t\t<changefreq>monthly</changefreq>\n\t</url>\n"
            f.write(line)
        f.write("</urlset>")
        print "Saving as %s ..." % fpath

def create_sitemap_index(path):
    global CONFIG
    site_map_uri = CONFIG.get("Sitemap", "SitemapURI")
    fpath = os.path.join(path, "all_item_urls.xml")
    with open(fpath, "w") as f:
        line = '<?xml version="1.0" encoding="UTF-8"?>\n' + \
               '<sitemapindex xmlns="' + \
               'http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f.write(line)
        for item in os.listdir(path):
            # Skip file being written to
            if item == "all_item_urls.xml":
                continue
            # sitemaps.dp.la is a CNAME pointing to the CDN host
            file_uri = url_join(site_map_uri, item)
            lastmod_dt = datetime.utcfromtimestamp(os.path.getmtime(
                                                os.path.join(path, item)
                                                ))
            line = "\t<sitemap>\n" + \
                   "\t\t<loc>%s</loc>\n\t\t<lastmod>%s</lastmod>\n" % \
                   (file_uri, iso_utc_with_tz(lastmod_dt)) + "\t</sitemap>\n"
            f.write(line)
        f.write("</sitemapindex>")

sitemap_path = CONFIG.get("Sitemap", "SitemapPath")

# Compress previous directory
for item in os.listdir(sitemap_path):
    item_path = os.path.join(sitemap_path, item)
    if os.path.isdir(item_path):
        with tarfile.open(item_path + ".tar.gz", "w:gz") as tar:
            tar.add(item_path, arcname=os.path.basename(item_path))
        shutil.rmtree(item_path)

# Create new directory
new_dir = os.path.join(sitemap_path, date.today().strftime("%Y%m%d"))
os.mkdir(new_dir)

# Fetch all item URLs
c = Couch()
urls = []
limit = 50000
count = 1
for doc in c._query_all_docs(c.dpla_db):
    if doc.get("ingestType") == "item":
        # Handle older ingestDates, which do not have timezone info.
        lm_dt = dateutil_parse(doc["ingestDate"])
        if lm_dt.utcoffset() is not None:
            lm = lm_dt.isoformat()
        else:
            lm = lm_dt.isoformat() + "Z"
        urls.append({"loc": "http://dp.la/item/" + doc["id"], "lastmod": lm})
    if len(urls) == limit:
        create_sitemap_files(new_dir, urls, count)
        count += 1
        urls = []
create_sitemap_files(new_dir, urls, count)

create_sitemap_index(new_dir)
send_sitemap_to_rackspace(new_dir)
