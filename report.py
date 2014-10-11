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

from ggplot import *
import pandas as pd
import numpy as np

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
    parser.add_argument("--projects",
                        help = "Name of projects to consider, " + \
                            "separated by comma."
                        ) 
    parser.add_argument("--max_results",
                        help = "Maximum number of results"
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
    parser.add_argument("--plot",
                        help = "Produce plots, when applicable.",
                        action = "store_true"
                        )
    parser.add_argument("--plot_file",
                        help = "File name to plot to, when applicable.",
                        )
    parser.add_argument("--period",
                        help = "Period length: day, week, month."
                        ) 
    parser.add_argument("--change",
                        help = "Summary of a change, given change number"
                        )
    parser.add_argument("--check_change_numbers",
                        help = "Check change numbers."
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
    parser.add_argument("--check_abandon_cont",
                        help = "Check changes with an 'Abandoned' " \
                            + "but continuing with activity."
                        )
    parser.add_argument("--check_subm",
                        help = "Check Check that changes with 'SUBM' " + \
                            "approval are closed"
                        )
    parser.add_argument("--check_events",
                        help = "Check that evolution of events matches " \
                            + "current situation.",
                        action = "store_true"
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
    parser.add_argument("--show_events",
                        help = "Produce a list with all " \
                            + "events of specified kind " \
                            + "('start', 'submit', 'push', " \
                            + "'abandon','restore','revert').",
                        )
    parser.add_argument("--show_events_byperiod",
                        help = "Produce a list with number of " \
                            "events by period..",
                        )
    args = parser.parse_args()
    return args

def show_summary ():
    """Summary of main stats in the database.

    Number of changes, messages, revisions, approvals.

    """

    q = session.query(label ("changes",
                             func.count (DB.Change.id)))
    print "Changes: " + str(q.scalar()) + " (",
    q = session.query(
        label ("status", DB.Change.status),
        label ("number", func.count(DB.Change.uid))
        ) \
        .group_by(DB.Change.status)
    for row in q.all():
        print row.status + ": " + str(row.number),
    print ")"
    q = session.query(label ("messages",
                             func.count (DB.Message.uid)))
    print "Messages: " + str(q.scalar())
    q = session.query(label ("revisions",
                             func.count (DB.Revision.uid)))
    print "Revisions: " + str(q.scalar())
    q = session.query(
        label ("change", DB.Revision.uid),
        ) \
        .join(DB.Change) \
        .group_by(DB.Change.uid)
    print "Changes with revisions: " + str(q.count())
    q = session.query(label ("approvals",
                             func.count (DB.Approval.uid)))
    print "Approvals: " + str(q.scalar())
    q = session.query(label("max",
                            func.max(DB.Change.updated)))
    last_date = q.one().max
    print last_date
    q = session.query(DB.Change).filter(DB.Change.updated == last_date)
    last_change = q.one()
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

def check_change_numbers(max):
    """Check change numbers.

    """

    numbers = session.query(
        label("number", DB.Change.number),
        label("rep", func.count(DB.Change.number)),
        ) \
        .group_by (DB.Change.number).subquery()
    q = session.query(
        label("number", numbers.c.number),
        ) \
        .filter(numbers.c.rep > 1)
    print "Repeated change numbers: " + str(q.count()) + " [",
    for number in q.limit(max).all():
        print number.number,
    print "]"

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

    q = session.query(
        label ("revision", DB.Revision.uid),
        ) \
        .join (DB.Change) \
        .filter (DB.Revision.number == 1) \
        .group_by (DB.Change.uid)
    print "Changes with first revision: " + str(q.count())
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
    print "Changes with no first revision: " + str(q.count())
    

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
    
def check_abandon_cont(max):
    """Check changes with an "Abandoned" but continuing with activity.

    Parameters
    ----------

    max: int
        Max number of cases to show among those violating the check.

    """

    q_abandons = session.query(
        label("id", DB.Change.uid),
        label("date", func.min(DB.Message.date)),
        label("num", DB.Change.number)
        ) \
        .select_from(DB.Change) \
        .join(DB.Message) \
        .filter (or_ (DB.Message.header == "Abandoned",
                      DB.Message.header.like ("Patch%Abandoned"))) \
        .group_by(DB.Change.uid) \
        .subquery()
    q = session.query(
        label("num", q_abandons.c.num)
        ) \
        .join(DB.Message,
              DB.Message.change_id == q_abandons.c.id) \
        .filter(DB.Message.date > q_abandons.c.date) \
        .group_by(q_abandons.c.id)
    changes = q.count()
    print "Changes abandoned, with activity after abandon (" \
        + str(changes) + "): ",
    for change in q.limit(max).all():
        print change.num
    print

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

def check_events (projects = None):
    """Check that evolution of events matches current situation.

    Parameters
    ----------

    projects: list of str
        List of projects to consider. Default: None

    """

    q = query_start (projects)
    started = q.count()
    print "Started: " + str(started)
    q = query_submit (projects)
    submitted = q.count()
    print "Submitted: " + str(submitted)
    q = query_in_header ("Pushed", "Change has been successfully pushed%",
                         projects)
    pushed = q.count()
    print "Pushed: " + str(pushed)
    q = query_in_header ("Abandoned", "Patch%Abandoned", projects)
    abandoned = q.count()
    print "Abandoned: " + str(abandoned)
    q = query_in_header ("Restored", "Patch%Restored", projects)
    restored = q.count()
    print "Restored: " + str(restored)
    q = query_in_header ("Reverted", "Patch%Reverted", projects)
    reverted = q.count()
    print "Reverted: " + str(reverted)
    res_merged = submitted + pushed
    print "Resulting merged: " + str(res_merged)
    res_abandoned = abandoned - restored
    print "Resulting abandoned: " + str(res_abandoned)
    print "Resulting new:" + str(
        started - res_merged - res_abandoned)

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


def plot_events_byperiod (byperiod, filename = None):
    """Plot a series of events.

    Parameters
    ----------

    byperiod: panda.timeseries
        Events to plot, as a timeseries dataframe with three columns:
        period (as starting date), event name, number of events.
    filename: str
        File name to plot to. (Default: None, means plot online).

    """

    chart = ggplot (aes(x='date', y='change', color='event'),
                  data=byperiod) \
                  + geom_line() \
                  + labs("Date", "Number of events")
    if filename is None:
        print chart
    else:
        ggsave (filename, chart)

def query_create (projects = None):
    """Produce a query for selecting change cretae events.

    The query will select "date" as the date for the event, and
    "change" for the change number. The date is the "created"
    field for the change.

    Parameters
    ----------

    projects: list of str
        List of projects to consider. Default: None.

    Returns
    -------

    query_gerrit.query: produced query

    """

    q = session.query(
        label ("date", DB.Change.created),
        label ("change", DB.Change.number),
        )
    if projects is not None:
        q = q.filter (DB.Change.project.in_(projects))
    return q

def query_start (projects = None):
    """Produce a query for selecting chnage start events.

    The query will select "date" as the date for the event, and
    "change" for the change number. The date is calculated as
    the date of the first revision.

    Parameters
    ----------

    projects: list of str
        List of projects to consider. Default: None.

    Returns
    -------

    query_gerrit.query: produced query

    """

    q = session.query(
        label ("date", func.min(DB.Revision.date)),
        label ("change", DB.Change.number),
        ) \
        .join(DB.Change)
    if projects is not None:
        q = q.filter (DB.Change.project.in_(projects))
    q = q.group_by(DB.Change.uid)
    return q

def query_submit (projects = None):
    """Produce a query for selecting submit (ready to merge) events.

    The query will select "date" as the date for the event, and
    "change" for the change number.

    Parameters
    ----------

    projects: list of str
        List of projects to consider. Default: None.

    Returns
    -------

    query_gerrit.query: produced query

    """

    q = session.query(
        label ("date", func.max(DB.Approval.date)),
        label ("change", DB.Change.uid),
        ) \
        .select_from(DB.Change) \
        .join(DB.Revision) \
        .join(DB.Approval) \
        .filter (DB.Approval.type == "SUBM")
    if projects is not None:
        q = q.filter (DB.Change.project.in_(projects))
    q = q.group_by(DB.Change.uid)
    return q

def query_in_header (header, like_header,
                     projects = None, unique = False):
    """Produce a query for selecting events by finding header in messages.

    The query will select "date" as the date for the event, and
    "change" for the change number.

    Parameters
    ----------

    header: str
        String to find (exactly) in header of messages.
    like_header: str
        String to find (using like) in header of messages.
    projects: list of str
        List of projects to consider. Default: None.
    unique: bool
        Consider only unique changes (count as one if a change has
        several abandoned.

    Returns
    -------

    query_gerrit.query: produced query

    """

    if unique:
        q = session.query(
            label ("date", func.min(DB.Message.date)),
            label ("change", DB.Change.number),
            )
    else:
        q = session.query(
            label ("date", DB.Message.date),
            label ("change", DB.Change.number),
            )
    q = q.select_from(DB.Change) \
        .join(DB.Message) \
        .filter (or_ (DB.Message.header == header,
                      DB.Message.header.like (like_header)))
    if projects is not None:
        q = q.filter (DB.Change.project.in_(projects))
    if unique:
        q = q.group_by(DB.Change.uid)
    return q

def query_revisions (projects = None):
    """Produce a query for selecting new revision events.

    The query will select "date" in revision record as the date
    for the event, and "change" for the change number.

    Parameters
    ----------

    projects: list of str
        List of projects to consider. Default: None.

    """

    q = session.query(
        label ("date", DB.Revision.date),
        label ("change", DB.Change.number),
        )
    q = q.select_from(DB.Revision) \
        .join(DB.Change)
    if projects is not None:
        q = q.filter (DB.Change.project.in_(projects))
    return q

def get_events (kinds, max, projects = None):
    """Get a dataframe with avents of kind kinds.

    Parameters
    ----------

    kinds: list of {"start", "submit", "push", "abandon", "restore", "revert"}
        Kinds of events to be produced.
    max: int
        Max number of changes to consider (0 means "all").
    projects: list of str
        List of projects to consider. Default: None

    Returns
    -------

    pandas.dataframe: Events
        Dataframe with columns "date" (datetime), "change"
        (change number), "event" (str, kind of event).

    """

    queries = {}
    if "create" in kinds:
        queries["create"] = query_create (projects)
    if "start" in kinds:
        queries["start"] = query_start (projects)
    if "submit" in kinds:
        queries["submit"] = query_submit (projects)
    if "push" in kinds:
        queries["push"] = query_in_header (
            "Pushed",
            "Change has been successfully pushed%",
            projects)
    if "abandon" in kinds:
        queries["abandon"] = query_in_header ("Abandoned", 
                                              "Patch%Abandoned", projects)
    if "restore" in kinds:
        queries["restore"] = query_in_header ("Restored",
                                              "Patch%Restored", projects)
    if "revert" in kinds:
        queries["revert"] = query_in_header ("Reverted",
                                             "Patch%Reverted", projects)
    if "revision" in kinds:
        queries["revision"] = query_revisions (projects)
    event_list = []
    for kind in queries:
        # Add limit to query, query, add kind column
        if max != 0:
            queries[kind] = queries[kind].limit(max)
        for date, change in queries[kind]:
            event_list.append( [date, change, kind] )
    events_df = pd.DataFrame.from_records (
        event_list,
        columns = ["date", "change", "event"]
        )
    return events_df

def get_events_byperiod (events_df, period = "month"):
    """Get a pandas timeseries with avents per period.

    Parameters
    ----------

    events_df: pandas.dataframe
        Events to group by period. It is a dataframe with the
        following columns: "date" (datetime), "change"
        (change number), "event" (str, kind of event).
    period: { "day", "week", "month" }
        Length of period (Default: "month").

    Returns
    -------

    pandas.timeseries: Number of events per period
        Pandas grouped object.

    """

    if period == "month":
        freq = 'M'
    elif period == "day":
        freq = 'D'
    elif period == "week":
        freq = 'W'
    ts = events_df.set_index(['date'])
    byperiod = ts.groupby([pd.TimeGrouper(freq=freq), "event"],
                          as_index=False)
    byperiod_agg = byperiod.aggregate(len)
    return byperiod_agg

def show_events (kinds, max, projects = None,
                 plot = False, plot_file = False):
    """Produce a list with avents of kind kinds.

    Parameters
    ----------

    kinds: list of {"start", "submit", "push", "abandon", "restore", "revert"}
        Kinds of events to be produced.
    max: int
        Max number of changes to consider (0 means "all").
    projects: list of str
        List of projects to consider. Default: None
    plot_file: str
        file name to plot to (Default: None, means plot online)

    """

    events_df = get_events (kinds, max, projects)
    print events_df
    if plot:
        plot_events_all(events_df, plot_file)
    #grouped = event_df.groupby("event")
    # for name, group in grouped:
    #     plot_events(group)

def show_events_byperiod (kinds, max, projects = None,
                          plot = False, plot_file = None,
                          period = "month"):
    """Produce a list with number of events by period.

    Parameters
    ----------

    kinds: list of {"start", "submit", "push", "abandon", "restore", "revert"}
        Kinds of events to be produced.
    max: int
        Max number of changes to consider (0 means "all").
    projects: list of str
        List of projects to consider. Default: None.
    plot_file: str
        File name to plot to (Default: None, means plot online).
    period: { "day", "week", "month" }
        Length of period (Default: "month").

    """

    events_df = get_events (kinds, max, projects)
    byperiod = get_events_byperiod (events_df, period)
    print byperiod
    print "Total number of changes: " + str(byperiod.sum()["change"])
    if plot:
        plot_events_byperiod(byperiod, plot_file)



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
    if args.projects:
        projects = args.projects.split (",")
    else:
        projects = None
    if args.max_results:
        max_results = args.max_results
    else:
        max_results = 0
    if args.plot:
        plot = True
    else:
        plot = False
    if args.plot_file:
        plot_file = args.plot_file
    else:
        plot_file = None
    if args.period:
        period = args.period
    else:
        period = "month"
    if args.check_change_numbers:
        check_change_numbers(int(args.check_change_numbers))
    if args.check_upload:
        check_upload(int(args.check_upload))
    if args.check_first_revision:
        check_first_revision(int(args.check_first_revision))
    if args.check_status:
        check_status(int(args.check_status))
    if args.check_abandon:
        check_abandon(int(args.check_abandon))
    if args.check_abandon_cont:
        check_abandon_cont(int(args.check_abandon_cont))
    if args.check_subm:
        check_subm(int(args.check_subm))
    if args.check_events:
        check_events(projects)
    if args.show_drafts:
        show_drafts(args.show_drafts)
    if args.check_newer_dates:
        check_newer_dates(args.check_newer_dates)
    if args.calc_duration_changes:
        calc_duration_changes(args.calc_duration_changes)
    if args.calc_duration_changes_approvals:
        calc_duration_changes_approvals(
            args.calc_duration_changes_approvals)
    if args.show_events:
        show_events(args.show_events,
                    max_results, projects, plot, plot_file)
    if args.show_events_byperiod:
        show_events_byperiod(args.show_events_byperiod,
                             max_results, projects, plot, plot_file,
                             period)

