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
from subprocess import check_output, CalledProcessError
from time import sleep
import json

description = """
Simple script to retrieve data from Gerrit systems via ssh.

Example of command used:

ssh -p 29418 gerrit.wikimedia.org gerrit query --format=JSON --files --comments --patch-sets --all-approvals --commit-message --submit-records > /tmp/open.json

Example of execution:

ssh_miner.py --projectlist wikimedia_projects.json gerrit.wikimedia.org 29418 /tmp/changes.json

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
    parser.add_argument("--sortkey",
                        help = "Sortkey to resume retrieval " + \
                            "(usually needs --status too)."
                        )
    parser.add_argument("--status",
                        help = "Status of changes to retrieve, separated by commas, such as 'merged,abandoned,open'."
                        )
    parser.add_argument("--projectlist",
                        help = "List of strings, in JSON format, with projects to retrieve changes from."
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

def retrieve (file, base_command, records, sortkey = None):
    """Retrieve changes according to status_command, including retries.

    Parameters
    ----------

    file: file
       File to write retrieved records to.
    base_command: list of str
       Arguments of base command to retrieve Gerrit records.
    records: int
       Number of records retrieved so far.
    sortkey: str
       sortkey used by gerrit to resume a retrieval.

    Returns
    -------

    int: Number of retrieved records, counting from records up.

    """

    complete = False
    while not complete:
        if sortkey is None:
            command = base_command
        else:
            command = base_command + ["resume_sortkey:" + sortkey,]
        # Run up to three times if fails.
        for n in xrange(3):
            try:
                output = check_output(command)
                break
            except CalledProcessError:
                sleep (5 * (n+2))
                print "Retrying...."
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
    return records

def retrieve_projects (file, base_command, records, projects, size = 5):
    """Retrieve changes for several projects, in chuncks.

    Parameters
    ----------

    file: file
       File to write retrieved records to.
    base_command: list of str
       Arguments of base command to retrieve Gerrit records.
    records: int
       Number of records retrieved so far.
    projects: list of str
       Projects to be retrieved.
    size: int
       Size of chuncks (to split projects list).

    Returns
    -------

    int: Number of retrieved records, counting from records up.

    """

    project_chuncks = [projects[i:i + size]
                       for i in range(0, len(projects), size)]
    for chunck in project_chuncks:
        print "Projects: " + ", ".join(chunck) + "."
        query = ["project:" + item for item in chunck]
        or_query = []
        for project in query:
            or_query.extend([project, "OR"])
        or_query.pop()
        status_command = base_command + or_query
        records = retrieve (file, status_command, records)
    return records

if __name__ == "__main__":

    args = parse_args()
    if args.projectlist:
        with open (args.projectlist, "r") as listfile:
            projects_json = listfile.read()
            projects = json.loads(projects_json)
    with open (args.file, "w") as file:
        print "Writing to file: " + args.file
        base_command = ["ssh", "-p", args.port, args.server, "gerrit", 
                        "query", "--format=JSON", "--files",
                        "--comments", "--patch-sets", "--all-approvals",
                        "--commit-message", "--submit-records",
                    ]
#                    "limit:500"]
#                    "--dependencies"]
        records = 0
        if args.sortkey:
            sortkey = args.sortkey
        else:
            sortkey = None
        if args.status:
            statuses = args.status.split(",")
            for status in statuses:
                print "Status: " + status + "." 
                status_command = base_command + ["status:" + status]
                retrieve (file, status_command, records, sortkey)
        if args.projectlist:
            retrieve_projects (file, base_command, records, projects)
    print "Done."
