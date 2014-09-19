# Utility methods 
import os
import sys
import time
import tarfile
import re
from functools import wraps
from datetime import datetime

def iterify(iterable):
    """Treat iterating over a single item or an iterator seamlessly"""
    if (isinstance(iterable, basestring) or isinstance(iterable, dict)):
        iterable = [iterable]
    try:
        iter(iterable)
    except TypeError:
        iterable = [iterable]

    return iterable

def remove_key_prefix(data, prefix):
    """Removes the prefix from keys in a dictionary"""
    for key in data.keys():
        if not key == "originalRecord":
            new_key = key.replace(prefix, "")
            if new_key != key:
                data[new_key] = data[key]
                del data[key]
            for item in iterify(data[new_key]):
                if isinstance(item, dict):
                    remove_key_prefix(item, prefix)

    return data

def make_tarfile(source_dir):
    with tarfile.open(source_dir + ".tar.gz", "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def couch_id_builder(src, lname):
    return "--".join((src, lname))

def couch_rec_id_builder(src, record):
    lname = record.get("id", "no-id").strip().relace(" ", "__")
    return couch_id_builder(src, lname)

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
                    time.sleep(sleep_sec)

            for attempt in xrange(1, attempts_num + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    args_string = "" if not print_args_if_error else \
                                  ", with arguments: %s, %s" % (args, kwargs)
                    msg = ("Error [%s: %s] " % (e.__class__.__name__, str(e)) +
                           "occurred while trying to call \"%s\"%s. " %
                           (func.__name__, args_string) +
                           "Attempt #%d failed" % (attempt))
                    if attempt == attempts_num:
                        raise
                    else:
                        pause(attempt)

        return func_with_retries

    return apply_with_retries

def clean_date(d):
    """Return a given date string without certain characters and expressions"""
    regex = [("\s*to\s*|\s[-/]\s", "-"), ("[\?\(\)]|\s*ca\.?\s*|~|x", "")]
    if not "circa" in d and not "century" in d:
        regex.append(("\s*c\.?\s*", ""))
    for p, r in regex:
        d = re.sub(p, r, d)
    return d.strip()

def remove_single_brackets_and_strip(d):
    """
    Return a given date-range string without single, unmatched square brackets
    """
    bracket = ""
    if d.count("[") == 1 and d.count("]") == 0:
        bracket = "["
    elif d.count("]") == 1 and d.count("[") == 0:
        bracket = "]"
    return d.replace(bracket, "").strip(". ")

def remove_all_brackets_and_strip(d):
    """Return a given date-range string without square brackets"""
    return d.replace("[", "").replace("]", "").strip(". ")

def strip_unclosed_brackets(s):
    return re.sub(r'\[(?![^\]]*?\])', '', s)

def iso_utc_with_tz(dt=None):
    """Given a UTC datetime, return an ISO 8601-conformant string w/ timezone
    If None, use datetime.datetime.utcnow() as the default."""
    if not dt:
        dt = datetime.utcnow()
    return dt.isoformat() + "Z"