import os
import time
import shutil
import tarfile
import datetime
import ConfigParser
from dplaingestion.couch import Couch
from export_database import set_global_variables
from export_database import get_rackspace_connection
from export_database import get_rackspace_container
from export_database import file_is_in_container
from export_database import url_join

set_global_variables("SitemapContainer")
CONTAINER = get_rackspace_container()

def send_sitemap_to_rackspace(sitemap_files_path):
    global CONTAINER

    print "Loading files from %s to Rackspace CDN." % sitemap_path
    for item in os.listdir(sitemap_files_path):
        f = CONTAINER.create_object(item)
        f.load_from_filename(os.path.join(sitemap_files_path, item))

        if file_is_in_container(item, CONTAINER):
            rs_file_uri = url_join(CONTAINER.public_uri(), item)
            print "File loaded, it is available at: %s" % rs_file_uri
        else:
            print "Couldn't upload file to Rackspace CDN."

def create_sitemap_files(path, urls, count):
    lastmod = datetime.date.today().strftime("%Y-%m-%d")
    fpath = os.path.join(path, "all_item_urls_%s.xml" % count)
    with open(fpath, "w") as f:
        line = '<?xml version="1.0" encoding="UTF-8"?>\n' + \
               '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f.write(line)
        for url in urls:
            line = "\t<url>\n" + \
                   "\t\t<loc>%s</loc>\n\t\t<lastmod>%s</lastmod>\n" % \
                   (url, lastmod)
            line += "\t\t<changefreq>monthly</changefreq>\n\t</url>\n"
            f.write(line)
        f.write("</urlset>")

def create_sitemap_index(path):
    config_file = "akara.ini"
    config = ConfigParser.ConfigParser()
    config.readfp(open(config_file))
    site_map_uri = config.get("Sitemap", "SitemapURI")

    lastmod = datetime.date.today().strftime("%Y-%m-%d")
    lastmod = datetime.date.today().strftime("%Y-%m-%d")
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
            lastmod = time.strftime("%Y-%m-%dT%H:%M:%S%z",
                                    time.gmtime(os.path.getmtime(
                                        os.path.join(path, item)
                                        )))
            line = "\t<sitemap>\n" + \
                   "\t\t<loc>%s</loc>\n\t\t<lastmod>%s</lastmod>\n" % \
                   (file_uri, lastmod) + "\t</sitemap>\n"
            f.write(line)
        f.write("</sitemapindex>")

sitemap_path = "/home/dpla/data/sitemap"

# Compress previous directory
for item in os.listdir(sitemap_path):
    item_path = os.path.join(sitemap_path, item)
    if os.path.isdir(item_path):
        with tarfile.open(item_path + ".tar.gz", "w:gz") as tar:
            tar.add(item_path, arcname=os.path.basename(item_path))
        shutil.rmtree(item_path)

# Create new directory
new_dir = os.path.join(sitemap_path, datetime.date.today().strftime("%Y%m%d"))
os.mkdir(new_dir)

# Fetch all item URLs
c = Couch()
urls = []
limit = 50000
count = 1
for doc in c._query_all_docs(c.dpla_db):
    if doc.get("ingestType") == "item":
        urls.append("http://dp.la/item/" + doc["id"])
    if len(urls) == limit:
        create_sitemap_files(new_dir, urls, count)
        count += 1
        urls = []
create_sitemap_files(new_dir, urls, count)

create_sitemap_index(new_dir)
send_sitemap_to_rackspace(new_dir)
