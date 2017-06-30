from dplaingestion.selector import exists
from dplaingestion.utilities import iterify
from dplaingestion.fetchers.fetcher import getprop
from dplaingestion.fetchers.absolute_url_fetcher import AbsoluteURLFetcher


class NYPLFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(NYPLFetcher, self).__init__(profile, uri_base, config_file)
        token = self.config.get("APITokens", "NYPL")
        authorization = self.http_headers["Authorization"].format(token)
        self.http_headers["Authorization"] = authorization

    def fetch_sets(self):
        """Fetches all sets

           Returns an (error, sets) tuple
        """
        error = None
        sets = {}
        if self.sets:
            set_ids = self.sets
        else:
            sets = {}
            url = self.get_sets_url
            error, content = self.request_content_from(url)
            if error is not None:
                return error, sets
            error, content = self.extract_content(content, url)
            if error is not None:
                return error, sets
            set_ids = [uuid for uuid in
                       getprop(content, "response/uuids/uuid")]

        for set_id in set_ids:
            sets[set_id] = {
                "id": set_id
            }

        if not sets:
            error = "Error, no sets from URL %s" % url

        return error, sets

    def extract_content(self, content, url):
        error = None
        try:
            parsed_content = XML_PARSE(content)
        except Exception, e:
            error = "Error parsing content from URL %s: %s" % (url, e)
            return error, content

        content = parsed_content.get("nyplAPI")
        if content is None:
            error = "Error, there is no \"nyplAPI\" field in content from " \
                    "URL %s" % url
        elif exists(content, "response/headers/code"):
            code = getprop(content, "response/headers/code")
            if code != "200":
                error = "Error, response code %s " % code + \
                        "is not 200 for request to URL %s" % url
        return error, content

    def request_records(self, content, set_id):
        self.endpoint_url_params["page"] += 1
        error = None
        total_pages = getprop(content, "request/totalPages")
        current_page = getprop(content, "request/page")
        request_more = total_pages != current_page
        if not request_more:
            # Reset the page for the next collection
            self.endpoint_url_params["page"] = 1

        records = []
        items = getprop(content, "response/capture")
        count = 0

        for item in iterify(items):
            count += 1
            record_url = self.get_records_url.format(item["uuid"])
            error, content = self.request_content_from(record_url)
            if error is None:
                error, content = self.extract_content(content, record_url)

            if error is None:
                record = getprop(content, "response/mods")
                record["_id"] = item["uuid"]
                record["tmp_image_id"] = item.get("imageID")
                record["tmp_item_link"] = item.get("itemLink")
                record["tmp_high_res_link"] = item.get("highResLink")
                record["tmp_rights_statement"] = \
                        getprop(content, "response/rightsStatement")
                record["rightsStatementURI"] = \
                        getprop(content, "response/rightsStatementURI")
                records.append(record)

            if error is not None:
                yield error, records, request_more
                print "Error %s, " % error +\
                      "but fetched %s of %s records from page %s of %s" % \
                      (count, len(items), current_page, total_pages)

        yield error, records, request_more
