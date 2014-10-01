"""
Extract character data (text) nodes from xmltodict-style dictionaries
"""


__all__ = ['NoTextNodeError', 'textnode']


class NoTextNodeError(Exception):
    """Thrown when no character data node can be found"""
    pass


def textnode(d):
    try:
        if isinstance(d, basestring) and d:
            return d
        elif isinstance(d, dict) and d['#text']:
            return d['#text']
        else:
            raise NoTextNodeError()
    except KeyError:
        raise NoTextNodeError()
