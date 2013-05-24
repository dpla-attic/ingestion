import sys
import json
import time
from functools import wraps
import urllib2
from abc import ABCMeta
from xml.parsers import expat
from itertools import repeat as _repeat, chain as _chain, starmap as _starmap
from operator import itemgetter as _itemgetter
import heapq as _heapq
from _abcoll import *

import xmltodict


def run_task(task):
    try:
        task.run()
        return task.get_result()
    except Exception as e:
        return TaskResult(TaskResult.ERROR,
                          None,
                          task.__class__.__name__,
                          "%s: %s" % (e.__class__.__name__, str(e)))


def with_retries(attempts_num=3, delay_sec=1, print_args_if_error=False):
    """
    Wrapper (decorator) that calls given func(*args, **kwargs);
    In case of exception does 'attempts_num'
    number of attempts with "delay_sec * attempt number" seconds delay
    between attempts.

    If 'print_args_if_error' is True, then wrapped function arguments
    will be shown in error message besides function name.

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
                    args_string = "" if not print_args_if_error else ", with arguments: %s, %s" % (args, kwargs)
                    print >> sys.stderr, "Error [%s: %s] occurred while trying to call \"%s\"%s. Attempt #%d failed" % (
                        e.__class__.__name__, str(e), func.__name__, args_string, attempt)
                    if attempt == attempts_num:
                        raise
                    else:
                        pause(attempt)

        return func_with_retries

    return apply_with_retries


@with_retries(10, 3, print_args_if_error=True)
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
    SUCCESS = 0  # result is available, no error message
    ERROR = 1  # result is unavailable, error message presents
    WARN = 2  # result is available, but error (warning) message also exists
    RETRY = 3  # result is unavailable, error message presents, error is not critical and task should be restarted

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
                                      None,
                                      self.__class__.__name__,
                                      self._skip_doc_message(files_url, e))
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
                item_data["marc"] = name

        assert item_data["meta"] is not None, "document \"" + self.identifier + "\" meta data is absent"

        item = {"files": item_data,
                "_id": self.identifier}
        meta_url = self.file_url_pattern.format(self.identifier, item_data["meta"])
        try:
            item.update(self._get_parsed_xml(meta_url))
        except Exception as e:
            self._result = TaskResult(TaskResult.ERROR,
                                      None,
                                      self.__class__.__name__,
                                      self._skip_doc_message(meta_url, e))
            return

        if item_data["marc"]:
            marc_url = self.file_url_pattern.format(self.identifier, item_data["marc"])
            try:
                item.update(self._get_parsed_xml(marc_url))
            except Exception as e:
                self._result = TaskResult(TaskResult.WARN,
                                          item,
                                          self.__class__.__name__,
                                          self._skip_doc_message(marc_url, e, "WARN"))
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
        try:
            self._enrich_func(*self._args_tuple)
        except Exception as e:
            self._result = TaskResult(TaskResult.ERROR,
                                      {"enriched": self._docs_num, "enrich_fails": self._docs_num},
                                      self.__class__.__name__,
                                      "%s: %s" % (e.__class__.__name__, str(e)))
        else:
            self._result = TaskResult(TaskResult.SUCCESS, {"enriched": self._docs_num}, self.__class__.__name__)

    def get_result(self):
        return self._result


# Taken from python 2.7 source, for python 2.6 compatibility:

########################################################################
###  Counter
########################################################################

class Counter(dict):
    '''Dict subclass for counting hashable items.  Sometimes called a bag
    or multiset.  Elements are stored as dictionary keys and their counts
    are stored as dictionary values.

    >>> c = Counter('abcdeabcdabcaba')  # count elements from a string

    >>> c.most_common(3)                # three most common elements
    [('a', 5), ('b', 4), ('c', 3)]
    >>> sorted(c)                       # list all unique elements
    ['a', 'b', 'c', 'd', 'e']
    >>> ''.join(sorted(c.elements()))   # list elements with repetitions
    'aaaaabbbbcccdde'
    >>> sum(c.values())                 # total of all counts
    15

    >>> c['a']                          # count of letter 'a'
    5
    >>> for elem in 'shazam':           # update counts from an iterable
    ...     c[elem] += 1                # by adding 1 to each element's count
    >>> c['a']                          # now there are seven 'a'
    7
    >>> del c['b']                      # remove all 'b'
    >>> c['b']                          # now there are zero 'b'
    0

    >>> d = Counter('simsalabim')       # make another counter
    >>> c.update(d)                     # add in the second counter
    >>> c['a']                          # now there are nine 'a'
    9

    >>> c.clear()                       # empty the counter
    >>> c
    Counter()

    Note:  If a count is set to zero or reduced to zero, it will remain
    in the counter until the entry is deleted or the counter is cleared:

    >>> c = Counter('aaabbc')
    >>> c['b'] -= 2                     # reduce the count of 'b' by two
    >>> c.most_common()                 # 'b' is still in, but its count is zero
    [('a', 3), ('c', 1), ('b', 0)]

    '''
    # References:
    #   http://en.wikipedia.org/wiki/Multiset
    #   http://www.gnu.org/software/smalltalk/manual-base/html_node/Bag.html
    #   http://www.demo2s.com/Tutorial/Cpp/0380__set-multiset/Catalog0380__set-multiset.htm
    #   http://code.activestate.com/recipes/259174/
    #   Knuth, TAOCP Vol. II section 4.6.3

    def __init__(self, iterable=None, **kwds):
        '''Create a new, empty Counter object.  And if given, count elements
        from an input iterable.  Or, initialize the count from another mapping
        of elements to their counts.

        >>> c = Counter()                           # a new, empty counter
        >>> c = Counter('gallahad')                 # a new counter from an iterable
        >>> c = Counter({'a': 4, 'b': 2})           # a new counter from a mapping
        >>> c = Counter(a=4, b=2)                   # a new counter from keyword args

        '''
        super(Counter, self).__init__()
        self.update(iterable, **kwds)

    def __missing__(self, key):
        'The count of elements not in the Counter is zero.'
        # Needed so that self[missing_item] does not raise KeyError
        return 0

    def most_common(self, n=None):
        '''List the n most common elements and their counts from the most
        common to the least.  If n is None, then list all element counts.

        >>> Counter('abcdeabcdabcaba').most_common(3)
        [('a', 5), ('b', 4), ('c', 3)]

        '''
        # Emulate Bag.sortedByCount from Smalltalk
        if n is None:
            return sorted(self.iteritems(), key=_itemgetter(1), reverse=True)
        return _heapq.nlargest(n, self.iteritems(), key=_itemgetter(1))

    def elements(self):
        '''Iterator over elements repeating each as many times as its count.

        >>> c = Counter('ABCABC')
        >>> sorted(c.elements())
        ['A', 'A', 'B', 'B', 'C', 'C']

        # Knuth's example for prime factors of 1836:  2**2 * 3**3 * 17**1
        >>> prime_factors = Counter({2: 2, 3: 3, 17: 1})
        >>> product = 1
        >>> for factor in prime_factors.elements():     # loop over factors
        ...     product *= factor                       # and multiply them
        >>> product
        1836

        Note, if an element's count has been set to zero or is a negative
        number, elements() will ignore it.

        '''
        # Emulate Bag.do from Smalltalk and Multiset.begin from C++.
        return _chain.from_iterable(_starmap(_repeat, self.iteritems()))

    # Override dict methods where necessary

    @classmethod
    def fromkeys(cls, iterable, v=None):
        # There is no equivalent method for counters because setting v=1
        # means that no element can have a count greater than one.
        raise NotImplementedError(
            'Counter.fromkeys() is undefined.  Use Counter(iterable) instead.')

    def update(self, iterable=None, **kwds):
        '''Like dict.update() but add counts instead of replacing them.

        Source can be an iterable, a dictionary, or another Counter instance.

        >>> c = Counter('which')
        >>> c.update('witch')           # add elements from another iterable
        >>> d = Counter('watch')
        >>> c.update(d)                 # add elements from another counter
        >>> c['h']                      # four 'h' in which, witch, and watch
        4

        '''
        # The regular dict.update() operation makes no sense here because the
        # replace behavior results in the some of original untouched counts
        # being mixed-in with all of the other counts for a mismash that
        # doesn't have a straight-forward interpretation in most counting
        # contexts.  Instead, we implement straight-addition.  Both the inputs
        # and outputs are allowed to contain zero and negative counts.

        if iterable is not None:
            if isinstance(iterable, Mapping):
                if self:
                    self_get = self.get
                    for elem, count in iterable.iteritems():
                        self[elem] = self_get(elem, 0) + count
                else:
                    super(Counter, self).update(iterable) # fast path when counter is empty
            else:
                self_get = self.get
                for elem in iterable:
                    self[elem] = self_get(elem, 0) + 1
        if kwds:
            self.update(kwds)

    def subtract(self, iterable=None, **kwds):
        '''Like dict.update() but subtracts counts instead of replacing them.
        Counts can be reduced below zero.  Both the inputs and outputs are
        allowed to contain zero and negative counts.

        Source can be an iterable, a dictionary, or another Counter instance.

        >>> c = Counter('which')
        >>> c.subtract('witch')             # subtract elements from another iterable
        >>> c.subtract(Counter('watch'))    # subtract elements from another counter
        >>> c['h']                          # 2 in which, minus 1 in witch, minus 1 in watch
        0
        >>> c['w']                          # 1 in which, minus 1 in witch, minus 1 in watch
        -1

        '''
        if iterable is not None:
            self_get = self.get
            if isinstance(iterable, Mapping):
                for elem, count in iterable.items():
                    self[elem] = self_get(elem, 0) - count
            else:
                for elem in iterable:
                    self[elem] = self_get(elem, 0) - 1
        if kwds:
            self.subtract(kwds)

    def copy(self):
        'Return a shallow copy.'
        return self.__class__(self)

    def __reduce__(self):
        return self.__class__, (dict(self),)

    def __delitem__(self, elem):
        'Like dict.__delitem__() but does not raise KeyError for missing values.'
        if elem in self:
            super(Counter, self).__delitem__(elem)

    def __repr__(self):
        if not self:
            return '%s()' % self.__class__.__name__
        items = ', '.join(map('%r: %r'.__mod__, self.most_common()))
        return '%s({%s})' % (self.__class__.__name__, items)

    # Multiset-style mathematical operations discussed in:
    #       Knuth TAOCP Volume II section 4.6.3 exercise 19
    #       and at http://en.wikipedia.org/wiki/Multiset
    #
    # Outputs guaranteed to only include positive counts.
    #
    # To strip negative and zero counts, add-in an empty counter:
    #       c += Counter()

    def __add__(self, other):
        '''Add counts from two counters.

        >>> Counter('abbb') + Counter('bcc')
        Counter({'b': 4, 'c': 2, 'a': 1})

        '''
        if not isinstance(other, Counter):
            return NotImplemented
        result = Counter()
        for elem, count in self.items():
            newcount = count + other[elem]
            if newcount > 0:
                result[elem] = newcount
        for elem, count in other.items():
            if elem not in self and count > 0:
                result[elem] = count
        return result

    def __sub__(self, other):
        ''' Subtract count, but keep only results with positive counts.

        >>> Counter('abbbc') - Counter('bccd')
        Counter({'b': 2, 'a': 1})

        '''
        if not isinstance(other, Counter):
            return NotImplemented
        result = Counter()
        for elem, count in self.items():
            newcount = count - other[elem]
            if newcount > 0:
                result[elem] = newcount
        for elem, count in other.items():
            if elem not in self and count < 0:
                result[elem] = 0 - count
        return result

    def __or__(self, other):
        '''Union is the maximum of value in either of the input counters.

        >>> Counter('abbb') | Counter('bcc')
        Counter({'b': 3, 'c': 2, 'a': 1})

        '''
        if not isinstance(other, Counter):
            return NotImplemented
        result = Counter()
        for elem, count in self.items():
            other_count = other[elem]
            newcount = other_count if count < other_count else count
            if newcount > 0:
                result[elem] = newcount
        for elem, count in other.items():
            if elem not in self and count > 0:
                result[elem] = count
        return result

    def __and__(self, other):
        ''' Intersection is the minimum of corresponding counts.

        >>> Counter('abbb') & Counter('bcc')
        Counter({'b': 1})

        '''
        if not isinstance(other, Counter):
            return NotImplemented
        result = Counter()
        for elem, count in self.items():
            other_count = other[elem]
            newcount = count if count < other_count else other_count
            if newcount > 0:
                result[elem] = newcount
        return result


