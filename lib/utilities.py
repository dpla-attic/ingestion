# Utility methods 

def iterify(iterable):
    """Treat iterating over a single item or an iterator seamlessly"""
    if (isinstance(iterable, basestring) or isinstance(iterable, dict)):
        iterable = [iterable]
    try:
        iter(iterable)
    except TypeError:
        iterable = [iterable]

    return iterable
