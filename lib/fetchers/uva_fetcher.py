from dplaingestion.fetchers.absolute_url_fetcher import *

class UVAFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(UVAFetcher, self).__init__(profile, uri_base, config_file)

    def uva_extract_records(self, content, url):
        error = None
        records = []

        # Handle "mods:<key>" in UVA book collection
        key_prefix = ""
        if "mods:mods" in content:
            key_prefix = "mods:"

        if key_prefix + "mods" in content:
            item = content[key_prefix + "mods"]
            for _id_dict in iterify(item[key_prefix + "identifier"]):
                if _id_dict["type"] == "uri":
                    item["_id"] = _id_dict["#text"]
                    records.append(item)
            if "_id" not in item:
                # Handle localtion url
                for _loc_dict in iterify(item[key_prefix + "location"]):
                    if "url" in _loc_dict:
                        for url in _loc_dict["url"]:
                            if ("usage" in url and
                                url["usage"] == "primary display"):
                                item["_id"] = url.get("#text")
                                records.append(item)

        if not records:
            error = "Error, no records found in content from URL %s" % url

        yield error, records

    def uva_request_records(self, content):
        error = None

        for item in content["mets:mets"]:
            if "mets:dmdSec" in item:
                records = content["mets:mets"][item]
                for rec in records:
                    if not rec["ID"].startswith("collection-description-mods"):
                        url = rec["mets:mdRef"]["xlink:href"]
                        error, cont = self.request_content_from(url)
                        if error is not None:
                            yield error, cont
                        else:
                            error, cont = self.extract_content(cont, url)
                            if error is not None:
                                yield error, cont
                            else:
                                for error, recs in \
                                    self.uva_extract_records(cont, url):
                                    yield error, recs

    def request_records(self, content):
        # UVA will not use the request_more flag
        request_more = False

        for error, records in self.uva_request_records(content):
            yield error, records, request_more
