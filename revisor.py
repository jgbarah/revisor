#! /usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2014 Bitergia
##
## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program; if not, write to the Free Software
## Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

## Data retriever for source code management systems.
## Currently is only intended to support Gerrit HTTP API.

import argparse
import urllib2
import json
import time

description = """
Simple script to retrieve data from Gerrit systems.

Information about Gerrit API:

http://gerrit-review.googlesource.com/Documentation/rest-api.html
http://gerrit-review.googlesource.com/Documentation/dev-rest-api.html

https://gerrit.wikimedia.org/r/Documentation/rest-api.html

"""

def parse_args ():
    """
    Parse command line arguments

    """

    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("url",
                        help = "Base url of the HTTP API for the " + \
                            "Gerrit system to mine"
                        )
    args = parser.parse_args()
    return args

def get_changes (status = "open", period = None):
    """Get all changes modified since period ago.

    GET /API/changes/?q=-age:$period

    Example of url to use for open (new) changes that changed during
    the last week:
    https://gerrit.wikimedia.org/r/changes/?q=-age:1week&n=500

    Example of url to use for getting 300 abandoned changes, starting from
    _sortkey 002f101600025
    https://gerrit.wikimedia.org/r/changes/?q=status:abandoned&N=002f1016000257d4&n=300
 
    Parameters
    ----------

    status: { "open", "merged", "abandoned" }
        Status of tickets to get
    period: int
        Get tickets that changed since this number of days ago

    """

    base = args.url + "/changes/?q=status:" + status
    if period is not None:
        base = base + "+-age:" + str(period) + "day"
    base = base + "&n=300"
    all_changes = {}
    more = True
    url = base
    while more:
        print "Getting..." + str (len(all_changes))
        res = urllib2.urlopen(url)
        first_line = res.readline()
        changes_json = res.read()
        changes = json.loads(changes_json)
        for change in changes:
            id = change["_number"]
            if id in all_changes:
                print "Repeated: " + id
            else:
                all_changes[id] = change
        if "_more_changes" in changes[-1]:
            more = changes[-1]["_more_changes"]
        else:
            more = False
        if more:
            next = changes[-1]["_sortkey"]
            url = base + "&N=" + next
            time.sleep(3)
    print "Done (" + status + "): " + str (len(all_changes))


def get_change (change):
    """Get detailed information for a given change.

    GET /API/changes/$change/detail?o=ALL_REVISIONS

    Example of url to use:
    https://gerrit.wikimedia.org/r/changes/117091/detail?o=ALL_REVISIONS
    
    """

    pass

def get_patchset (change, patchset):
    """Get information for a given patchset.

    GET /API/changes/$change/revisions/$patchset/review

    Example of url to use:
    https://gerrit.wikimedia.org/r/changes/117091/revisions/1/review
    
    """

    pass

if __name__ == "__main__":

    args = parse_args()
    print args.url
    for status in ["open", "abandoned", "merged"]:
        get_changes (status = status, period = 50)
