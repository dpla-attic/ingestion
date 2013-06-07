#!/usr/bin/env python
"""
Script to add/update the database views.

Usage:
    $ python scripts/sync_couch_views.py
"""
import time
from dplaingestion.couch import Couch

def main():
    couch = Couch()
    couch._sync_views()

if __name__ == "__main__":
    main()
