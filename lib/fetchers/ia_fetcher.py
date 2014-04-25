from dplaingestion.fetchers.absolute_url_fetcher import *
from dplaingestion.utilities import with_retries
from amara.thirdparty import json
from collections import OrderedDict, Counter
from xml.parsers import expat
from abc import ABCMeta
from multiprocessing.dummy import Pool
from Queue import Full, Empty, Queue
from threading import Lock
import signal

class IAFetcher(AbsoluteURLFetcher):
    def __init__(self, profile, uri_base, config_file):
        super(IAFetcher, self).__init__(profile, uri_base, config_file)
        self.coll_record_count = 0
        self.retry = []
        self.get_file_url = profile.get("get_file_url")
        self.prefix_files = profile.get("prefix_files")
        self.prefix_meta = profile.get("prefix_meta")
        self.prefix_dc = profile.get("prefix_dc")
        self.shown_at_url = profile.get("shown_at_url")
        self.fetch_pool = None
        self.queue = Queue(maxsize=profile.get("endpoint_url_params")["rows"])
        self.mutex = Lock()
        self.docs_counter = Counter({"downloaded": 0})
        self._initstophook()
        self.task_args = (self.get_file_url, self.prefix_files,
                          self.shown_at_url, self.fetch_url)


    def _stophandler(self, signum, frame):
        print >> sys.stderr,"Got shutdown signal %d. Going to close pool." % \
                            signum
        print >> sys.stderr, "Sending notification to pool workers..."
        for i in reversed(range(len(self.fetch_pool._pool))):
            worker_process = self.fetch_pool._pool[i]
            if worker_process.exitcode is None and hasattr(worker_process,
                                                           "pid"):
                print >> sys.stderr, "Sending %d signal to %d pid..." % \
                                     (signal.SIGUSR1, worker_process.pid)
                os.kill(worker_process.pid, signal.SIGUSR1)
        self.fetch_pool.terminate()
        print >> sys.stderr, "Work is terminated..."
        self.fetch_pool.join()
        sys.exit(signum)

    def _initstophook(self):
        signal.signal(signal.SIGINT, self._stophandler)
        signal.signal(signal.SIGTERM, self._stophandler)
        signal.signal(signal.SIGQUIT, self._stophandler)

    def task_done(self, result):
        with self.mutex:
            if result.status == TaskResult.RETRY:
                self.retry.append(result.result)

            if result.error_message:
                result.result = result.error_message

            try:
                self.queue.put(result.result, block=False)
            except Full:
                print >> sys.stderr, "Error, queue full."

    def run_task(self, task, retry=False):
        try:
            task.run(retry)
            return task.get_result()
        except Exception as e:
            return TaskResult(TaskResult.ERROR, None, task.__class__.__name__,
                              "%s: %s" % (e.__class__.__name__, str(e)))

    @with_retries(10, 1)
    def fetch_url(self, url):
        """
        Downloads data related to the given url and checks that the response
        code is 200.
        """
        response = None
        try:
            response = urllib2.urlopen(url, timeout=10)
            code = response.getcode()
            assert code == 200, "Bad response code = " + str(code)
            return response.read()
        finally:
            if response:
                response.close()

    def extract_xml_content(self, content, url):
        error = None
        try:
            content = json.loads(content)
        except Exception, e:
            error = "Error parsing content from URL %s: %s" % (url, e)

        return error, content

    def retry_request_records(self):
        def _batch_size_chunks(_list):
            """
            Yield successive batch sized chunks of _list.
            """
            for i in xrange(0, len(_list), self.batch_size):
                yield _list[i:i+self.batch_size]

        ids = [id for id in self.retry]
        retry = False
        self.record_coll_count = 0
        error_count = 0

        for chunk in _batch_size_chunks(ids):
            self.fetch_pool = Pool(processes=10)
            for id in chunk:
                args = (FetchDocumentTask(id, *self.task_args), retry,)
                self.fetch_pool.apply_async(self.run_task,
                                            args=args,
                                            callback=self.task_done)
            self.fetch_pool.close()
            self.fetch_pool.join()

            errors = []
            records = []
            while not self.queue.empty():
                record = self.queue.get(False)

                if isinstance(record, basestring):
                    errors.append(record)
                    error_count += 1
                else:
                    records.append(record)
                    self.coll_record_count += 1

                if self.queue.empty() or \
                   (len(records) > 0 and len(records) % self.batch_size == 0):
                    print "Fetched %s of %s, %s errors" % \
                          (self.coll_record_count, len(ids), error_count)

                    yield errors, records
                    errors = []
                    records = []

    def request_records(self, content, set_id=None):
        content = content["response"]
        total_records = int(content["numFound"])
        read_records = int(content["start"])
        expected_records = self.endpoint_url_params["rows"]
        total_pages = total_records/expected_records + 1
        request_more =  total_pages != self.endpoint_url_params["page"]

        if not request_more:
            # Since we are at the last page the expected_records will not
            # be equal to self.endpoint_url_params["rows"]
            expected_records = total_records - read_records
            # Reset the page for the next collection
            self.endpoint_url_params["page"] = 1
        else:
            self.endpoint_url_params["page"] += 1

        ids = [doc["identifier"] for doc in content["docs"]]

        self.fetch_pool = Pool(processes=10)
        retry = True
        for id in ids:
            args = (FetchDocumentTask(id, *self.task_args), retry,)
            self.fetch_pool.apply_async(self.run_task,
                                        args=args,
                                        callback=self.task_done)
                                                    
        self.fetch_pool.close()
        self.fetch_pool.join()

        errors = []
        records = []
        while not self.queue.empty():
            record = self.queue.get(False)

            if isinstance(record, basestring):
                errors.append(record)
            else:
                if record["metadata"]["mediatype"] == "collection":
                    continue
                records.append(record)
                self.coll_record_count += 1

            if (self.queue.empty() or (len(records) > 0 and
                                       len(records) % self.batch_size == 0)):
                print "Fetched %s of %s, %s will be retried" % \
                      (self.coll_record_count, total_records, len(self.retry))

                yield errors, records, request_more
                errors = []
                records = []

        if not request_more:
            # Reset the collection record count
            self.coll_record_count = 0

    def retry_fetches(self):
        for errors, records in self.retry_request_records():
            yield errors, records

class TaskResult(object):
    SUCCESS = 0 # result is available, no error message
    ERROR = 1   # result is unavailable, error message presents
    RETRY = 2   # result is unavailable, error message presents, error is not
                # critical and task should be restarted

    def __init__(self, status, result, _type, error_message=None):
        self.status = status
        self.result = result
        self.task_type = _type
        self.error_message = error_message


class IngestTask(object):
    __metaclass__ = ABCMeta

    def run(self):
        pass

    def get_result(self):
        pass

class FetchDocumentTask(IngestTask):
    def __init__(self, identifier, file_url_pattern, prefix,
                 shown_at_url, fetch_url):
        self.identifier = identifier
        self.file_url_pattern = file_url_pattern
        self.prefix = prefix
        self.shown_at_url = shown_at_url
        self.fetch_url = fetch_url

    def _get_parsed_xml(self, url):
        try:
            content = self.fetch_url(url)
        except Exception, e:
            raise Exception("Error (%s) fetching data from URL: %s" %
                            (str(e), url))
        try:
            return self._parse(content)
        except expat.ExpatError as e:
            raise Exception("Error (%s) parsing data from URL: %s" %
                            (str(e), url))

    def _parse(self, content):
        return xmltodict.parse(content,
                               xml_attribs=True,
                               attr_prefix='',
                               force_cdata=False,
                               ignore_whitespace_cdata=True)

    def _skip_doc_message(self, url, error=None, error_level="ERROR"):
        if isinstance(error, Exception):
            error = "%s: %s" % (error.__class__.__name__, str(error))
        msg = ("%s: Document with id \"%s\" " %
               (error_level, self.identifier) +
               "is skipped or malformed while working with url=\"%s\", " %
               url + "caused by error: %s" % str(error))
        return msg

    def _retry_doc_message(self, url):
        return "Document with id \"%s\" and url %s will be retried." % \
               (self.identifier, url)

    def run(self, retry):
        id = self.identifier
        prefix = self.prefix
        shown_at_url = self.shown_at_url
        files_url = self.file_url_pattern.format(id, prefix.format(id))
        try:
            files_response = self._get_parsed_xml(files_url)["files"]
        except Exception, e:
            if retry:
                task_status = TaskResult.RETRY
                msg = self._retry_doc_message(files_url)
                result = id
            else:
                task_status = TaskResult.ERROR
                msg = self._skip_doc_message(files_url, e)
                result = None            

            self._result = TaskResult(task_status,
                                      result,
                                      self.__class__.__name__,
                                      msg)
            return

        item_data = {"dc": None, "meta": None, "gif": None, "pdf": None,
                     "shown_at": shown_at_url.format(id),
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
                item_data["marc"] = name

        assert item_data["meta"] is not None, ("document \"" + id +
                                               "\" meta data is absent")

        item = {"files": item_data,
                "_id": id}
        meta_url = self.file_url_pattern.format(id, item_data["meta"])
        try:
            item.update(self._get_parsed_xml(meta_url))
        except Exception as e:
            if retry:
                task_status = TaskResult.RETRY
                msg = self._retry_doc_message(meta_url)
                result = id
            else:
                task_status = TaskResult.ERROR
                msg = self._skip_doc_message(meta_url, e)
                result = None

            self._result = TaskResult(task_status,
                                      result,
                                      self.__class__.__name__,
                                      msg)
            return

        if item_data["marc"]:
            marc_url = self.file_url_pattern.format(id, item_data["marc"])
            try:
                item.update(self._get_parsed_xml(marc_url))
            except Exception as e:
                if retry:
                    task_status = TaskResult.RETRY
                    msg = self._retry_doc_message(marc_url)
                    result = id
                else:
                    task_status = TaskResult.ERROR
                    msg = self._skip_doc_message(marc_url, e)
                    result = None

                self._result = TaskResult(task_status,
                                          result,
                                          self.__class__.__name__,
                                          msg)
                return

        self._result = TaskResult(TaskResult.SUCCESS, item,
                                  self.__class__.__name__)

    def get_result(self):
        return self._result
