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

from sqlalchemy import func, Column, and_, desc, type_coerce, Float, or_
from sqlalchemy.sql import label
from datetime import datetime, timedelta

import argparse
import textwrap

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
                            "to read data from (schema not included)."
                        )
    parser.add_argument("schema",
                        help = "Name of the schema " + \
                            "to read data from."
                        )
    parser.add_argument("--summary",
                        help = "Summary of main stats in the database",
                        action = "store_true"
                        )
    parser.add_argument("--full_messages",
                        help = "Show full messages wherever messages " + \
                            "are to be shown.",
                        action = "store_true"
                        )
    parser.add_argument("--change",
                        help = "Summary of a change, given change number"
                        )
    parser.add_argument("--check_upload",
                        help = "Check upload time of first revision with " + \
                            "created time for change. (time in mins.)"
                        )
    parser.add_argument("--check_first_revision",
                        help = "Check that changes have a first revision."
                        )
    parser.add_argument("--check_status",
                        help = "Check status of changes"
                        )
    parser.add_argument("--check_abandon",
                        help = "Check that changes with an 'Abandoned' " \
                            + "message are abandoned."
                        )
    parser.add_argument("--check_subm",
                        help = "Check Check that changes with 'SUBM' " + \
                            "approval are closed"
                        )
    parser.add_argument("--check_newer_dates",
                        help = "Check that all dates related to a " + \
                            "are newer than the creation date " + \
                            "(print at most n issues found)."
                        )
    parser.add_argument("--show_drafts",
                        help = "Show revisins with isdraft == True, up to the number specified.",
                        )
    parser.add_argument("--calc_duration_changes",
                        help = "Calculate duration of changes.",
                        )
    parser.add_argument("--calc_duration_changes_approvals",
                        help = "Calculate duration of changes (using approvals).",
                        )
    parser.add_argument("--events_start_change",
                        help = "Produce a list with all change start events..",
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
    
def show_revision_record (rev, approvals = True, change = None):
    """Show a revision record.

    Parameters
    ----------

    rev: DB.Revision
        Revision record to show.
    approvals: bool
        Flag to show approvals (or not).
    change: int
        Change number (show if not None)
    """

    print "Revision (patchset) " + str(rev.number) + " (" + \
        rev.revision + ")",
    if change:
        print ", Change: " + str(change),
    if rev.isdraft:
        print " (rev is DRAFT)",
    print
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
    if args.full_messages:
        print "  Full text: \n" + \
            "".join (
            ["   " + line for line in message.message.splitlines(True)]
            )

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
    res = session.query(
        label ("number",
               revs.c.number),
        label ("created",
               DB.Change.created),
        label ("daterev",
               revs.c.daterev)
        ) \
        .filter(and_(
                func.abs(func.timediff(
                        DB.Change.created,
                        revs.c.daterev) > timedelta (minutes = diff)),
                 DB.Change.uid == revs.c.change_id)) \
       .order_by (func.datediff(DB.Change.created, revs.c.daterev),
                  func.timediff(DB.Change.created, revs.c.daterev))
    messages = res.all()
    for message in messages:
        print "Change " + str(message.number) + ": " + \
            str(message.created - message.daterev) + \
             " -- " + str(message.created) + " (created), " + \
             str(message.daterev) + " (first revision)"
    print "Total changes with discrepancy: " + str (len(messages))

def check_newer_dates(max):
    """Check that dates related to a change are newer than creation date.

    This will print sumary stats about dates that are not correct,
    and will show at most max cases.

    Parameters
    ----------

    max: int
        Max number of cases to show among those violating the check.

    """

    res = session.query(
        label ("number",
               DB.Change.number),
        label ("created",
               DB.Change.created),
        label ("updated",
               DB.Change.updated)
        ) \
        .filter (DB.Change.created > DB.Change.updated) \
        .order_by (desc (func.datediff(DB.Change.created,
                                       DB.Change.updated)))
    cases = res.limit(max).all()
    for case in cases:
        print str(case.number) + ": " + str(case.created) + \
            " (created), " + str(case.updated) + " (updated) Mismatch: " + \
            str(case.created - case.updated) + ")"
    print "Total number of mismatchs: " + str(res.count())

def check_first_revision(max):
    """Check that changes have a first revision.

    Parameters
    ----------

    max: int
        Max number of cases to show among those violating the check.

    """

    first = session.query(
        label ("change", DB.Revision.change_id),
        ) \
        .filter (DB.Revision.number == 1) \
        .subquery()
    q = session.query(
        label ("change", DB.Change.number),
        ) \
        .filter (~DB.Change.uid.in_(first))
    for change in q.limit(max).all():
        print change.change
    print "Total number of changes with no first revision: " + str(q.count())
    

def check_status(max):
    """Check status of changes.

    Check the status of each change, in combination with its "open" flag.

    Parameters
    ----------

    max: int
        Max number of cases to show among those violating the check.

    """

    q = session.query(
        label("num", func.count(DB.Change.uid)),
        label("open", DB.Change.open),
        label("status", DB.Change.status),
        ) \
        .group_by (DB.Change.open, DB.Change.status)
    for state in q.all():
        print "Open is " + str(state.open) + ", status is " \
            + state.status + ": " + str(state.num)

def check_abandon(max):
    """Check that changes with an "Abandoned" message are abandoned.

    Parameters
    ----------

    max: int
        Max number of cases to show among those violating the check.

    """

    q = session.query(
        label("num", DB.Change.number),
        ) \
        .filter (DB.Change.status == "ABANDONED")
    print q.count()
    q = session.query(
        label("num", DB.Change.number),
        ) \
        .filter (DB.Change.status == "ABANDONED") \
        .join (DB.Message) \
        .filter (DB.Message.header == "Abandoned")
    print q.count()

    q_abandoned = session.query(DB.Message) \
        .filter(DB.Change.uid == DB.Message.change_id,
                or_ (DB.Message.header == "Abandoned",
                     DB.Message.header.like ("Patch%Abandoned")))
    q = session.query(
        label("num", DB.Change.number),
        ) \
        .filter (DB.Change.status == "ABANDONED") \
        .filter(~q_abandoned.exists())
    print q.count()
    for change in q.limit(max).all():
        print str(change.num),
    print

    q = session.query(
        label("num", DB.Change.number),
        ) \
        .filter (DB.Change.status != "ABANDONED") \
        .filter(q_abandoned.exists())
    print q.count()
    

def check_subm(max):
    """Check that changes with "SUBM" approval are closed.

    Parameters
    ----------

    max: int
        Max number of cases to show among those violating the check.

    """

    # Subquery for changes with at least one SUBM approval
    q_subm = session.query(DB.Revision) \
        .join(DB.Approval) \
        .filter (DB.Change.uid == DB.Revision.change_id,
                 DB.Approval.type == "SUBM")
    # Query for list of changes with at least one SUBM approval
    q_changes_subm = session.query(
        label ("num", DB.Change.number),
        ) \
        .filter(q_subm.exists())
    total = q_changes_subm.count()
    print "Changes with at least a SUBM approval (" + str(total) + "):"
    # Query for cases of changes with at least one SUBM approval
    q_changes_subm_cases = session.query(
        label ("open", DB.Change.open),
        label ("num", func.count(DB.Change.uid)),
        ) \
        .filter(q_subm.exists()) \
        .group_by (DB.Change.open)
    cases = q_changes_subm_cases.all()
    for case in cases:
        print "  Open is " + str(case.open) + ": " + str(case.num)
        if case.open == 1:
            print "    Changes still open (list): ",
            cases = q_changes_subm.filter(DB.Change.open == 1).limit(max).all()
            for case in cases:
                print str(case.num) + " ",
            print

    # Query for list of changes with no SUBM approval
    q_changes_nosubm = session.query(
        label ("num", DB.Change.number),
        ) \
        .filter(~q_subm.exists())
    total = q_changes_nosubm.count()
    print "Changes with no SUBM approval (" + str(total) + "):"
    # Query for cases of changes with no SUBM approval
    q_changes_nosubm_cases = session.query(
        label ("open", DB.Change.open),
        label ("num", func.count(DB.Change.uid)),
        label ("status", DB.Change.status),
        ) \
        .filter(~q_subm.exists()) \
        .group_by (DB.Change.open)
    cases = q_changes_nosubm_cases.all()
    for case in cases:
        print "  Open is " + str(case.open) + ": " + str(case.num)
        if case.open == 0:
            # Closed changes, but no SUBM
            cases_status = q_changes_nosubm_cases \
                .filter(DB.Change.open == 0) \
                .group_by (DB.Change.status).all()
            for case_status in cases_status:
                print "    Status is " + case_status.status \
                    + ": " + str(case_status.num)
                if case_status.status == "MERGED":
                    # Closed & merged changes, but no SUBM 
                    pushed = q_changes_nosubm \
                        .join(DB.Message) \
                        .filter(DB.Change.status == "MERGED") \
                        .filter(DB.Change.open == 0) \
                        .filter(DB.Message.header.like(
                            "Change has been successfully pushed%"
                            ))
                    print "      Changes merged by being pushed: " \
                        + str(pushed.count())
                    # Other remaining changes
                    q_pushed = session.query(DB.Message) \
                        .filter(DB.Change.uid == DB.Message.change_id,
                                DB.Message.header.like(
                                    "Change has been successfully pushed%"
                                    ))
                    not_pushed = q_changes_nosubm \
                        .filter(DB.Change.status == "MERGED") \
                        .filter(DB.Change.open == 0) \
                        .filter(~q_pushed.exists())
                    not_pushed_no = not_pushed.count()
                    print "      Other changes (" + str(not_pushed_no) \
                        + ", list): ",
                    changes = not_pushed.limit(max).all()
                    for change in changes:
                        print str(change.num),
                    print

def show_drafts(max):
    """Find revisins with isdraft == True up to the number specified.

    Parameters
    ----------

    max: int
        Maximum number of isdraft revisions to print.

    """

    res = session.query(DB.Revision,
                        label("change",
                              DB.Change.number)) \
          .join(DB.Change) \
          .filter (DB.Revision.isdraft == True)
    for rev in res.limit(max).all():
        show_revision_record(rev = rev.Revision, change = rev.change)
    print "Total number of drafts: " + str(res.count())

def calc_duration_changes(max):
    """Calculate duration of changes (time from created to updated).

    This will print sumary stats about the duration of the
    changes in the review system, and will show some of them.

    Parameters
    ----------

    max: int
        Max number of changes to show.

    """

    res = session.query(
        label ("number",
               DB.Change.number),
        label ("start",
               DB.Change.created),
        label ("finish",
               DB.Change.updated),
        ) \
        .filter (DB.Change.created < DB.Change.updated) \
        .order_by (desc (func.datediff(DB.Change.updated,
                                       DB.Change.created)))
    cases = res.limit(max).all()
    for case in cases:
        print str(case.number) + ": " + str(case.start) + \
            " (start), " + str(case.finish) + " (finish) Duration: " + \
            str(case.finish - case.start)

def calc_duration_changes_approvals(max):
    """Calculate duration of changes using information about approvals.

    This will print sumary stats about the duration of the
    changes in the review system, and will show some of them.
    A change is defined to start when the first upload for it is
    found, and defined to end when the latest approval is found.

    Parameters
    ----------

    max: int
        Max number of changes to show.

    """

    starts = session.query(
        label ("number",
               DB.Change.number),
        label ("date",
               func.min (DB.Revision.date)),
        ) \
        .filter (DB.Change.uid == DB.Revision.change_id) \
        .group_by (DB.Change.uid) \
        .subquery()
    finishes = session.query(
        label ("number",
               DB.Change.number),
        label ("date",
               func.max (DB.Approval.date)),
        ) \
        .filter (DB.Change.uid == DB.Revision.change_id,
                 DB.Revision.uid == DB.Approval.revision_id) \
        .group_by (DB.Change.uid) \
        .subquery()
    query = session.query(
        label ("number", starts.c.number),
        label ("start", starts.c.date),
        label ("finish", finishes.c.date),
        ) \
        .filter (starts.c.number == finishes.c.number)
    cases = query.limit(max).all()
    for case in cases:
        print str(case.number) + ": " + str(case.start) + \
            " (start), " + str(case.finish) + " (finish) Duration: " + \
            str(case.finish - case.start)

def events_start_change(max):
    """Produce a list with all change start events.

    Uses the time of the first revision for each change.

    Parameters
    ----------

    max: int
        Max number of changes to consider (0 means "all").

    """

    import pandas
    import numpy as np

    q = session.query(
        label ("date", func.min(DB.Revision.date)),
        label ("change", DB.Change.number),
        ) \
        .join(DB.Change) \
        .group_by(DB.Change.uid)
    if max != 0:
        q = q.limit(max)
    events = np.array (q.all()).transpose()
    #print events
    df = pandas.DataFrame (events[1], index=events[0] )
    #print df
    for month, values in df.groupby(lambda x: x.month):
        print month,
        print len(values)
    bymonths = df.groupby(pandas.TimeGrouper(freq="M"))
    number = bymonths.aggregate(pandas.Series.nunique)
    print number

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
    if args.check_first_revision:
        check_first_revision(int(args.check_first_revision))
    if args.check_status:
        check_status(int(args.check_status))
    if args.check_abandon:
        check_abandon(int(args.check_abandon))
    if args.check_subm:
        check_subm(int(args.check_subm))
    if args.show_drafts:
        show_drafts(args.show_drafts)
    if args.check_newer_dates:
        check_newer_dates(args.check_newer_dates)
    if args.calc_duration_changes:
        calc_duration_changes(args.calc_duration_changes)
    if args.calc_duration_changes_approvals:
        calc_duration_changes_approvals(
            args.calc_duration_changes_approvals)
    if args.events_start_change:
        events_start_change(args.events_start_change)
