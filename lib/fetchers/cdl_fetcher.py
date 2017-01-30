from dplaingestion.fetchers.absolute_url_fetcher import *


class CDLFetcher(AbsoluteURLFetcher):

    def __init__(self, profile, uri_base, config_file):
        super(CDLFetcher, self).__init__(profile, uri_base, config_file)
        token = self.config.get("APITokens", "CDL")
        authorization = self.http_headers["X-Authentication-Token"].format(token)
        self.http_headers["X-Authentication-Token"] = authorization

    def request_records(self, content, set_id):
        records = []
        error = None
        for item in iterify(getprop(content, "response/docs",)):
            record = item
            record["_id"] = item["id"]
            records.append(record)
            print(record["_id"])

        cursor_mark = getprop(content, "responseHeader/params/cursorMark")
        next_cursor_mark = getprop(content, "nextCursorMark")
        request_more = cursor_mark != next_cursor_mark
        self.endpoint_url_params["cursorMark"] = next_cursor_mark

        yield error, records, request_more

    def extract_content(self, content, url):
         return None, json.loads(content)
