import sys
import json
import time
from functools import wraps
import urllib2
from abc import ABCMeta
from xml.parsers import expat

import xmltodict


def run_task(task):
    try:
        task.run()
        return task.get_result()
    except Exception as e:
        return TaskResult(TaskResult.ERROR,
                          "%s: %s" % (e.__class__.__name__, str(e)),
                          task.__class__.__name__)


def with_retries(attempts_num=3, delay_sec=1):
    """
    Wrapper (decorator) that calls given func(*args, **kwargs);
    In case of exception does 'attempts_num'
    number of attempts with "delay_sec * attempt number" seconds delay
    between attempts.

    Usage:
    @with_retries(5, 2)
    def get_document(doc_id, uri): ...
    d = get_document(4444, "...") # now it will do the same logic but with retries

    Or:
    def get_document(...): ...
    get_document = with_retries(5, 2)(get_document)
    d = get_document(...) # now it will do the same logic but with retries
    """

    def apply_with_retries(func):
        assert attempts_num >= 1
        assert isinstance(attempts_num, int)
        assert delay_sec >= 0

        @wraps(func)
        def func_with_retries(*args, **kwargs):

            def pause(attempt):
                """Do pause if current attempt is not the last"""
                if attempt < attempts_num:
                    sleep_sec = delay_sec * attempt
                    print >> sys.stderr, "Sleeping for %d seconds..." % sleep_sec
                    time.sleep(sleep_sec)

            for attempt in xrange(1, attempts_num + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print >> sys.stderr, "Error [%s: %s] occurred while trying to call \"%s\". Attempt #%d failed" % (
                        e.__class__.__name__, str(e), func.__name__, attempt)
                    if attempt == attempts_num:
                        raise
                    else:
                        pause(attempt)

        return func_with_retries

    return apply_with_retries


@with_retries(10, 3)
def fetch_url(url, print_url=False):
    """Downloads data related to given url,
    checks that http response code is 200"""
    if print_url:
        print "fetching url", url, "..."
    d = None
    try:
        d = urllib2.urlopen(url, timeout=10)
        code = d.getcode()
        assert code == 200, "Bad response code = " + str(code)
        return d.read()
    finally:
        if d:
            d.close()


class TaskResult(object):
    SUCCESS = 0
    ERROR = 1
    WARN = 2
    RETRY = 3

    def __init__(self, status, result, _type):
        self.status = status
        self.result = result
        self.task_type = _type



class IngestTask(object):

    __metaclass__ = ABCMeta

    def run(self):
        pass

    def get_result(self):
        pass


class FetchIdsPageTask(IngestTask):

    def __init__(self, request_url):
        self.request_url = request_url

    def run(self):
        content = fetch_url(self.request_url)
        parsed = json.loads(content)
        response_key = "response"
        items_key = "docs"
        id_key = "identifier"
        if response_key in parsed:
            self._result = [i[id_key] for i in parsed[response_key][items_key] if id_key in i]
        else:
            raise Exception("No \"%s\" key in returned json" % response_key)

    def get_result(self):
        return TaskResult(TaskResult.SUCCESS, tuple(self._result), self.__class__.__name__)


class FetchDocumentTask(IngestTask):

    def __init__(self, identifier, collection, profile):
        self.identifier = identifier
        self.collection = collection
        self.file_url_pattern = profile["get_file_URL"]
        self.profile = profile

    def _get_parsed_xml(self, url):
        content = fetch_url(url)
        try:
            return self._parse(content)
        except expat.ExpatError as e:
            raise Exception("Error (%s) parsing data from URL: %s" % (str(e), url))

    def _parse(self, content):
        return xmltodict.parse(content,
                               xml_attribs=True,
                               attr_prefix='',
                               force_cdata=False,
                               ignore_whitespace_cdata=True)

    def _skip_doc_message(self, url, error=None, error_level="ERROR"):
        error = "%s: %s" % (error.__class__.__name__,
                            str(error)) if isinstance(error, Exception) else error
        return "%s: Document with id=\"%s\" from \"%s\" collection is skipped " \
               "or malformed while working with url=\"%s\", caused by error: %s" % (
                   error_level, self.identifier, self.collection, url, str(error)
               )

    def run(self):
        files_url = self.file_url_pattern.format(self.identifier, self.profile["prefix_files"].format(self.identifier))
        try:
            files_response = self._get_parsed_xml(files_url)["files"]
        except Exception as e:
            self._result = TaskResult(TaskResult.ERROR,
                                      self._skip_doc_message(files_url, e),
                                      self.__class__.__name__)
            return
        item_data = {"dc": None, "meta": None, "gif": None, "pdf": None,
                     "shown_at": self.profile["shown_at_URL"].format(self.identifier),
                     "marc": None}
        for file_info in files_response["file"]:
            _format = file_info["format"]
            name = file_info["name"]
            if _format == "Text PDF":
                item_data["pdf"] = name
            elif _format == "Animated GIF":
                item_data["gif"] = name
            elif _format == "Grayscale LuraTech PDF" and not item_data["pdf"]:
                item_data["pdf"] = name
            elif _format == "Metadata" and name.endswith("_meta.xml"):
                item_data["meta"] = name
            elif _format == "Dublin Core":
                item_data["dc"] = name
            elif _format == "MARC":
                item_data["marc"]= name

        assert item_data["meta"] is not None, "document \"" + self.identifier + "\" meta data is absent"

        item = {"files": item_data,
                "_id": self.identifier}
        meta_url = self.file_url_pattern.format(self.identifier, item_data["meta"])
        try:
            item.update(self._get_parsed_xml(meta_url))
        except Exception as e:
            self._result = TaskResult(TaskResult.ERROR,
                                      self._skip_doc_message(meta_url, e),
                                      self.__class__.__name__)
            return

        if item_data["marc"]:
            marc_url = self.file_url_pattern.format(self.identifier, item_data["marc"])
            try:
                item.update(self._get_parsed_xml(marc_url))
            except Exception as e:
                self._result = TaskResult(TaskResult.WARN,
                                          self._skip_doc_message(marc_url, e, "WARN"),
                                          self.__class__.__name__)
                return
        self._result = TaskResult(TaskResult.SUCCESS, item, self.__class__.__name__)

    def get_result(self):
        return self._result


class EnrichBulkTask(IngestTask):

    def __init__(self, docs_num, enrich_func, *args):
        self._enrich_func = enrich_func
        self._args_tuple = args
        self._docs_num = docs_num

    def run(self):
        self._enrich_func(*self._args_tuple)

    def get_result(self):
        return TaskResult(TaskResult.SUCCESS, {"enriched": self._docs_num}, self.__class__.__name__)
