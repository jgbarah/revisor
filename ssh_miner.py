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

## Retrieve JSON files via ssh with detailed Gerrit output.

import argparse
from subprocess import check_output

description = """
Simple script to retrieve data from Gerrit systems via ssh.

Example of command used:

ssh -p 29418 gerrit.wikimedia.org gerrit query --format=JSON --files --comments --patch-sets --all-approvals --commit-message --submit-records > /tmp/open.json

"""

def parse_args ():
    """
    Parse command line arguments

    """

    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("server",
                        help = "Gerrit server to be accessed via ssh"
                        )
    parser.add_argument("port",
                        help = "Gerrit port to be accessed via ssh"
                        )
    parser.add_argument("file",
                        help = "File to write the resulting JSON document"
                        )
    args = parser.parse_args()
    return args

def find_last_str (string, key):
    """Find last key in JSON string, and return its value (string).

    """
    start = string.rfind('"' + key + '":"')
    end = string.find('","', start)
    value = string[start+len(key)+4:end]
    return value

def find_last_int (string, key):
    """Find last key in JSON string, and return its value (integer).

    """
    start = string.rfind('"' + key + '":')
    end = string.find(',"', start)
    value = string[start+len(key)+3:end]
    return int(value)

if __name__ == "__main__":

    args = parse_args()
    print "Writing to file: " + args.file
    file = open (args.file, "w")
    # JSON documents are collections of items, but lacks the list marker:
    # write it around the documents
    file.write ("[\n")
    base_command = ["ssh", "-p", args.port, args.server, "gerrit", "query",
                    "--format=JSON", "--files", "--comments", "--patch-sets",
                    "--all-approvals", "--commit-message", "--submit-records",
                    "limit:100"]
    complete = False
    records = 0

    while not complete:
        if records == 0:
            command = base_command
        else:
            command = base_command + ["resume_sortkey:" + sortkey,]
        output = check_output(command)
        rows = find_last_int (output, "rowCount")
        if rows > 0:
            records = records + rows
            print "Records read: " + str(records) + "."
            sortkey = find_last_str (output, "sortKey")
            # Find last newline (before the final newline) 
            last_nl = output.rfind("\n", 0, -1)
            # Write everything except for the last line
            file.write (output[0:last_nl+1])
        else:
            complete = True

    file.write ("]\n")
    file.close()
    print "Done."
