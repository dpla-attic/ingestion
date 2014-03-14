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

def main():
    total = {}
    count = {}
    p = re.compile(r'(\S+) HTTP.* (\S+)$')
    for line in sys.stdin:
        m = p.search(line)
        path, elapsed = m.group(1, 2)
        path = re.sub(r'\?.*$', '', path)
        total.setdefault(path, 0)
        total[path] += float(elapsed)
        count.setdefault(path, 0)
        count[path] += 1.0
    print "\n".join(["%s\t%d" % (path, int(round(total[path] / count[path])))
                     for path in total.keys()])


if __name__ == '__main__':
    main()

