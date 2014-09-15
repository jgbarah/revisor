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
## This version assumes that data produced by running the gerrit command
## (usually throguh ssh) is available as a JSON file.

import argparse
import urllib2
import json
import time
import re
from datetime import tzinfo, timedelta, datetime

from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

description = """
Simple script to retrieve data from Gerrit systems.

Information about Gerrit API:

http://gerrit-review.googlesource.com/Documentation/rest-api.html
http://gerrit-review.googlesource.com/Documentation/dev-rest-api.html

https://gerrit.wikimedia.org/r/Documentation/rest-api.html

"""


class UTC(tzinfo):
    """UTC Class, for datetime timezones

    """

    def utcoffset(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return timedelta(0)
utc = UTC()

Base = declarative_base()

class Change(Base):
    """Table for changes.

    Fields are those found for a change in the corresponding JSON document.

    """

    __tablename__ = "changes"

    uid = Column(Integer, primary_key=True)
    id = Column(String(300))
    project = Column(String(100))
    branch = Column(String(50))
    number = Column(Integer)
    subject = Column(String(200))
    topic = Column(String(50))
    url = Column(String(100))
    open = Column(Boolean)
    status = Column(String(20))
    created = Column(DateTime)
    updated = Column(DateTime)
    sortkey = Column(String(20))
    
    def __repr__(self):
        return "<Change(project='%s', id='%s', " + \
            "status='%s', number='%s'" % (
            self.project, self.id, self.status, self.number)


class Message(Base):
    """Table for messages.

    Fields are those found for a message in the corresponding JSON document.

    """

    __tablename__ = "messages"

    uid = Column(Integer, primary_key=True)
    change_id = Column(Integer, ForeignKey('changes.uid'))
    date = Column(DateTime)
    header = Column(String(100))
    message = Column(String(5000))

    change = relationship ("Change", backref=backref("messages",
                                                     order_by = uid))

class Revision(Base):
    """Table for revisions.

    Fields are those found for a message in the corresponding JSON document.

    """

    __tablename__ = "revisions"

    uid = Column(Integer, primary_key=True)
    change_id = Column(Integer, ForeignKey('changes.uid'))
    number = Column(Integer)
    revision = Column(String(50))
    date = Column(DateTime)
    isdraft = Column(Boolean)

    change = relationship ("Change", backref=backref("revisions",
                                                     order_by = uid))

class Approval(Base):
    """Table for approvals.

    Fields are those found for an approval in the corresponding
    JSON document.

    """

    __tablename__ = "approvals"

    uid = Column(Integer, primary_key=True)
    revision_id = Column(Integer, ForeignKey('revisions.uid'))
    type = Column(String(20))
    description = Column(String(20))
    value = Column(Integer)
    date = Column(DateTime)

    change = relationship ("Revision", backref=backref("approvals",
                                                       order_by = uid))


def parse_args ():
    """
    Parse command line arguments

    """

    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("file",
                        help = "JSON file with the change records, " \
                            + "as produced by the gerrit command."
                        )
    args = parser.parse_args()
    return args

def analyze_header (header):

    if header == "Abandoned":
        return ("Abandoned", None)
    if header == "Change has been successfully merged into the git repository.":
        return ("Merged", None)
    match = re.match (r'Uploaded patch set (\d*).$', header)
    if match:
        return ("Upload", int(match.group(1)))
    match = re.match (r'Patch Set (\d*): .*Code-Review(.\d).*$', header)
    if match:
        return ("Review", int(match.group(2)))
    match = re.match (r'Patch Set (\d*): .*-Code-Review.*$', header)
    if match:
        return ("Review", 0)
    match = re.match (r"Patch Set (\d*): Do not submit.*$", header)
    if match:
        return ("Review", -2)
    match = re.match (r"Patch Set (\d*): There's a problem with.*$", header)
    if match:
        return ("Review", -1)
    match = re.match (r"Patch Set (\d*): Looks good to me, but.*$", header)
    if match:
        return ("Review", 1)
    match = re.match (r"Patch Set (\d*): Looks good to me, approved.*$", header)
    if match:
        return ("Review", 2)
    match = re.match (r'Patch Set (\d*): Verified(..)$', header)
    if match:
        return ("Verify", int(match.group(2)))
    match = re.match (r'Patch Set (\d*): -Verified$', header)
    if match:
        return ("Verify", 0)
    match = re.match (r'Patch Set (\d*): Checked$', header)
    if match:
        return ("Check", None)
    match = re.match (r'Patch Set (\d*): Patch Set (\d*) was rebased$', header)
    if match:
        return ("Rebase", match.group(2))
    match = re.match (r'Patch Set (\d*): Commit message was updated$', header)
    if match:
        return ("Update", None)
    match = re.match (r'Patch Set (\d*): Cherry Picked.*$', header)
    if match:
        return ("Cherry", None)
    match = re.match (r'Patch Set (\d*): Restored.*$', header)
    if match:
        return ("Restore", None)
    match = re.match (r'Patch Set (\d*): Reverted.*$', header)
    if match:
        return ("Revert", None)
    match = re.match (r'Topic(.*)$', header)
    if match:
        return ("Topic", None)
    match = re.match (r'Change could not be merged(.*)$', header)
    if match:
        return ("Not merged", None)
    match = re.match (r'Change cannot be merged(.*)$', header)
    if match:
        return ("Not merged", None)
    match = re.match (r'Patch Set (\d*).$', header)
    if match:
        return ("Comment", None)
    return ("Unknown", None)

def db_messages (message_list):
    """Produce list of message records out of list of messages in Gerrit JSON.

    Parameters
    ----------

    message_list: list
        Message list as obtained form the Gerrit JSON document.

    Returns
    -------

    list: List of Message records
    
    """

    message_records = []
    for message in message_list:
        header = message["message"].split('\n', 1)[0]
        message_record = Message (
            date = datetime.fromtimestamp (int(message["timestamp"])),
            header = header,
            message = message["message"],
            )
        message_records.append (message_record)
    return (message_records)

def db_revisions (revision_list):
    """Produce list of revision records out of list of revisions in Gerrit JSON.

    Parameters
    ----------

    revision_list: list
        Revision list as obtained form the Gerrit JSON document.

    Returns
    -------

    list: List of Revision records
    
    """

    records = []
    for revision in revision_list:
        record = Revision (
            number = revision["number"],
            revision = revision["revision"],
            date = datetime.fromtimestamp (int(revision["createdOn"])),
            isdraft = revision["isDraft"],
            )
        records.append (record)
    return (records)

def db_approvals (approval_list):
    """Produce list of approval records out of list of approvals in Gerrit JSON.

    Parameters
    ----------

    approval_list: list
        Approval list as obtained form the Gerrit JSON document.

    Returns
    -------

    list: List of Approval records
    
    """

    records = []
    for approval in approval_list:
        if "description" in approval:
            description = approval["description"]
        else:
            description = None
        record = Approval (
            type = approval["type"],
            description = description,
            value = approval["value"],
            date = datetime.fromtimestamp (int(approval["grantedOn"])),
            )
        records.append (record)
    return records
    
def db_change (change):
    """Produce change records (and related information).

    Parameters
    ----------

    change: str
        Change dictionary as obtained form the Gerrit JSON document.

    """
    
    if "topic" in change:
        topic = change["topic"]
    else:
        topic = None
    change_record = Change (
        id = change["id"],
        project = change["project"],
        branch = change["branch"],
        number = change["number"],
        subject = change["subject"],
        topic = topic,
        url = change["url"],
        open = change["open"],
        status = change["status"],
        created = datetime.fromtimestamp (int(change["createdOn"]), utc),
        updated = datetime.fromtimestamp (int(change["lastUpdated"]), utc),
        sortkey = change["sortKey"]
        )
    return change_record
            
def get_changes (status = "open", period = None):
    """Get all changes modified since period ago.

    GET /API/changes/?q=-age:$period

    Example of url to use for open (new) changes that changed during
    the last week:
    https://gerrit.wikimedia.org/r/changes/?q=-age:1week&n=500

    Example of url to use for getting 300 abandoned changes, starting from
    _sortkey 002f101600025
    https://gerrit.wikimedia.org/r/changes/?q=status:abandoned&N=002f1016000257d4&n=300

    Example of other useful options:
    o=DETAILED_LABELS&o=ALL_REVISIONS&o=ALL_COMMITS&o=DETAILED_ACCOUNTS&o=MESSAGES

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
    base = base + "&o=MESSAGES"
    base = base + "&o=ALL_REVISIONS"
    all_changes = {}
    session = Session()

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
                change_record = db_change (change, status)
                session.add(change_record)
        if "_more_changes" in changes[-1]:
            more = changes[-1]["_more_changes"]
        else:
            more = False
        if more:
            next = changes[-1]["_sortkey"]
            url = base + "&N=" + next
            retrieving_record = Retrieving (id = status,
                                            sortkey = next,
                                            date = datetime.now())
        else:
            retrieving_record = Retrieving (id = status,
                                            sortkey = None,
                                            date = datetime.now())
        session.merge (retrieving_record)
        session.commit()
    print "Done (" + status + "): " + str (len(all_changes))


if __name__ == "__main__":

    args = parse_args()

    from sqlalchemy import create_engine
    database = 'mysql://jgb:XXX@localhost/revisorj_test'
    trailer = "?charset=utf8&use_unicode=0"
    database = database + trailer

    engine = create_engine(database, echo=False)
    
    Base.metadata.drop_all(engine) 
    Base.metadata.create_all(engine) 

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()

    count = 0
    for line in open (args.file, "r"):
        count = count + 1
        change = json.loads(line)
        print count, change["number"], change["url"]
        change_record = db_change (change)
        if "comments" in change:
            change_record.messages = db_messages (change["comments"])
            print "Messages added: " + str (len (change_record.messages))
        if "patchSets" in change:
            change_record.revisions = db_revisions (change["patchSets"])
            print "Revisions added: " + str (len (change_record.revisions))
            for rev, revision in enumerate (change["patchSets"]):
                if "approvals" in revision:
                    change_record.revisions[rev].approvals = \
                        db_approvals(revision["approvals"])
                    print "Approvals added: " + \
                        str (len (change_record.revisions[rev].approvals))
                    
        session.add(change_record)

        session.commit()

