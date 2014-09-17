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

from sqlalchemy import func, Column, and_
from sqlalchemy.sql import label
from datetime import datetime, timedelta

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
    parser.add_argument("database",
                        help = "SQLAlchemy url of the database " + \
                            "to write the data to (schema not included)."
                        )
    parser.add_argument("schema",
                        help = "Name of the schema " + \
                            "to write the data to."
                        )
    parser.add_argument("--summary",
                        help = "Summary of main stats in the database",
                        action = "store_true"
                        )
    parser.add_argument("--change",
                        help = "Summary of a change, given change number"
                        )
    parser.add_argument("--check_upload",
                        help = "Check upload time of first revision with " + \
                            "created time for change. (time in mins.)"
                        )
    args = parser.parse_args()
    return args

def show_summary ():
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

    res = session.query(label("max",
                              func.max(DB.Change.updated)))
    last_date = res.one().max
    print last_date
    res = session.query(DB.Change).filter(DB.Change.updated == last_date)
    last_change = res.one()
    print "Last change: " + str(last_change.number)
    print "  Updated: " + str(last_change.updated)

def show_change_record (change):
    """Show a change record.

    Parameters
    ----------

    change: DB.Change
        Change record to show.

    """

    print "Change: " + str(change.number)
    print "Project: " + change.project + " (branch: " + \
        change.branch + ") " + change.url
    print "Status: " + change.status + " / Created: " + \
        str (change.created) + \
        " Updated: " + str (change.updated)
    print "Subject: " + change.subject
    
def show_revision_record (rev, approvals = True):
    """Show a change record.

    Parameters
    ----------

    rev: DB.Revision
        Revision record to show.
    approvals: bool
        Flag to show approvals (or not).

    """

    print "Revision (patchset) " + str(rev.number) + " (" + \
        rev.revision + ")"
    print "  Date: " + str(rev.date)
    if approvals:
        res = session.query(DB.Approval) \
            .filter(DB.Approval.revision_id == rev.uid) \
            .order_by(DB.Approval.date)
        for approval in res.all():
            show_approval_record (approval)

def show_approval_record (approval):
    """Show an approval record.

    Parameters
    ----------

    approval: DB.Approval
        Approval record to show.

    """

    print "  " + approval.type + ": " + str(approval.value)
    print "    Date: " + str(approval.date)

def show_message_record (message):
    """Show an message record.

    Parameters
    ----------

    message: DB.Message
        Message record to show.

    """

    print "Message: " + message.header
    print "  Date: " + str(message.date)

def show_change (change_no):
    """Summary of data for a change (including revisions, approvals, etc.)

    Parameters
    ----------

    change_no: str
        Change number.

    """

    res = session.query(DB.Change) \
        .filter (DB.Change.number == change_no)
    change = res.one()
    show_change_record (change)
    res = session.query(DB.Revision) \
        .filter (DB.Revision.change_id == change.uid) \
        .order_by (DB.Revision.number)
    for revision in res.all():
         show_revision_record (revision)
    res = session.query(DB.Message) \
        .filter(DB.Message.change_id == change.uid) \
        .order_by(DB.Message.date)
    for message in res.all():
        show_message_record (message)

def check_upload (diff):
    """Check upload time of first revision with created time for change.

    For each change, the upload time of the first revision (patchset) is
    matched against the created time for the change. Those changes with
    more than diff mins. of difference are shown.

    Parameters
    ----------

    diff: int
        Minutes of difference considered.

    """

    revs = session.query(label ("daterev",
                                func.min(DB.Revision.date)),
                         label ("change_id",
                                DB.Revision.change_id),
                         label ("number",
                                DB.Change.number)) \
          .filter (DB.Revision.change_id == DB.Change.uid) \
          .group_by("change_id") \
          .subquery()
    res = session.query(label ("number",
                               revs.c.number),
                        label ("created",
                               DB.Change.created),
                        label ("daterev",
                               revs.c.daterev),
                        label ("diff",
                               func.timediff(DB.Change.created,
                                             revs.c.daterev))) \
       .filter(and_(
                 func.abs(func.timediff(
                       DB.Change.created,
                       revs.c.daterev) > timedelta (minutes = diff)),
                 DB.Change.uid == revs.c.change_id)) \
       .order_by (func.timediff(DB.Change.created, revs.c.daterev))
    messages = res.all()
    for message in messages:
        print "Change " + str(message.number) + ": " + str(message.diff) + \
             " -- " + str(message.created) + " (created), " + \
             str(message.daterev) + " (first revision)"
    print "Total changes with discrepancy: " + str (len(messages))

if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner

    stdout_utf8()
    args = parse_args()

    database = DB (url = args.database,
                   schema = args.schema,
                   schema_id = args.schema)
    session = database.build_session(Query, echo = False)

    if args.summary:
        show_summary()
    if args.change:
        show_change(args.change)
    if args.check_upload:
        check_upload(int(args.check_upload))
