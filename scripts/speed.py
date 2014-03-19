#!/usr/bin/env python

"""
Produce elapsed-time reports for logfiles written in DPLA Akara
combined log format (like Apache combined format, but with endpoint
microsecond elapsed field at end)

Takes input from stdin.

Usage:
    $ speed.py < access.log | sort -rn -k 2
"""

import re
import sys

def resource_and_elapsed(lines):
    """Yield the resource and elapsed-microseconds fields of each line"""
    for line in lines:
        request_uri = line.split(None, 7)[6]
        resource = request_uri.split('?')[0]
        elapsed = float(line.rsplit(None, 1)[1])
        yield (resource, elapsed)

def main():
    total = {}
    count = {}
    for resource, elapsed in resource_and_elapsed(sys.stdin):
        total.setdefault(resource, 0)
        count.setdefault(resource, 0)
        total[resource] += elapsed
        count[resource] += 1.0
    avg_elapsed = lambda tot, count: int(round(tot / count))
    for resource in total.keys():
        print resource, avg_elapsed(total[resource], count[resource])


if __name__ == '__main__':
    main()

