# Utility methods 
import os
import tarfile

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
