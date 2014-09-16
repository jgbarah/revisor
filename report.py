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
## Package to report about details from a Gerrit database retrieved via revisor
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from query_gerrit import DB, Query

from sqlalchemy import func, Column
from sqlalchemy.sql import label
from datetime import datetime

import argparse

description = """
Simple script to produce reports with information extracted from a
revisor Gerrit-based database.

Example:

report.py --summary mysql://jgb:XXX@localhost/ gerrit_changes

"""

def parse_args ():
    """
    Parse command line arguments

    """

    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("--summary",
                        help = "Summary of main stats in the database",
                        action = "store_true"
                        )
    parser.add_argument("database",
                        help = "SQLAlchemy url of the database " + \
                            "to write the data to (schema not included)."
                        )
    parser.add_argument("schema",
                        help = "Name of the schema " + \
                            "to write the data to."
                        )
    args = parser.parse_args()
    return args

def summary ():
    """Summary of main stats in the database.

    Number of changes, messages, revisions, approvals.

    """

    res = session.query(label ("changes",
                               func.count (DB.Change.id)))
    print "Changes: " + str(res.scalar())
    res = session.query(label ("messages",
                               func.count (DB.Message.uid)))
    print "Messages: " + str(res.scalar())
    res = session.query(label ("revisions",
                               func.count (DB.Revision.uid)))
    print "Revisions: " + str(res.scalar())
    res = session.query(label ("approvals",
                               func.count (DB.Approval.uid)))
    print "Approvals: " + str(res.scalar())

if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner

    stdout_utf8()
    args = parse_args()

    database = DB (url = args.database,
                   schema = args.schema,
                   schema_id = args.schema)
    session = database.build_session(Query, echo = False)

    if args.summary:
        summary()
