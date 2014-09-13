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
import re

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

Base = declarative_base()

class Change(Base):
    """Table for changes.

    Fields are those found for a change in the corresponding JSON document.

    """

    __tablename__ = "changes"

    kind = Column(String(50))
    id = Column(String(200), primary_key=True)
    project = Column(String(100))
    branch = Column(String(50))
    topic = Column(String(50))
    change_id = Column(String(50))
    subject = Column(String(200))
    status = Column(String(20))
    created = Column(DateTime)
    updated = Column(DateTime)
    mergeable = Column(Boolean)
    _sortkey = Column(String(20))
    _number = Column(Integer)

    def __repr__(self):
        return "<Change(project='%s', change_id='%s', " + \
            "status='%s', _number='%s'" % (
            self.project, self.change_id, self.status, self._number)


class Message(Base):
    """Table for messages.

    Fields are those found for a message in the corresponding JSON document.

    """

    __tablename__ = "messages"

    id = Column(String(20), primary_key=True)
    date = Column(DateTime)
    change_id = Column(String(200), ForeignKey('changes.id'))
    _revision_number = Column(Integer)
    action = Column(String(10))
    value = Column(Integer)
    message = Column(String(1000))

    change = relationship ("Change", backref=backref("messages",
                                                     order_by = id))

class Revision(Base):
    """Table for revisions.

    Fields are those found for a message in the corresponding JSON document.

    """

    __tablename__ = "revisions"

    id = Column(String(50), primary_key=True)
    _number = Column(Integer)
    change_id = Column(String(200), ForeignKey('changes.id'))

    change = relationship ("Change", backref=backref("revisions",
                                                     order_by = id))

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

def analyze_header (header):

    if header == "Abandoned":
        return ("Abandoned", None)
    if header == "Change has been successfully merged into the git repository.":
        return ("Merged", None)
    match = re.match (r'Uploaded patch set (\d*).$', header)
    if match:
        return ("Upload", int(match.group(1)))
    match = re.match (r'Patch Set (\d*): .*Code-Review(..).*$', header)
    if match:
        return ("Review", int(match.group(2)))
    match = re.match (r'Patch Set (\d*): .*-Code-Review.*$', header)
    if match:
        return ("Review", 0)
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
        (action, value) = analyze_header (header)
        message_record = Message (
            id = message["id"],
            date = message["date"],
            _revision_number = message["_revision_number"],
            message = message["message"],
            action = action,
            value = value
            )
        if action == "Unknown":
            print "Unknown header: " + header + "."
        message_records.append (message_record)
    return (message_records)

def db_revisions (revision_dict):
    """Produce list of revision records out of dict of revisions in Gerrit JSON.

    Parameters
    ----------

    revision_dict: list
        Revision dictionary as obtained form the Gerrit JSON document.

    Returns
    -------

    list: List of Revision records
    
    """

    revision_records = []
    for revision in revision_dict:
        revision_record = Revision (
            id = revision,
            _number = message["_number"],
            )
        revision_records.append (revision_record)
    return (revision_records)
    
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
    if "mergeable" in change:
        mergeable = change["mergeable"]
    else:
        mergeable = None
    change_record = Change (
        kind = change["kind"],
        id = change["id"],
        project = change["project"],
        branch = change["branch"],
        topic = topic,
        change_id = change["change_id"],
        subject = change["subject"],
        status = change["status"],
        created = change["created"],
        updated = change["updated"],
        mergeable = mergeable,
        _sortkey = change["_sortkey"],
        _number = change["_number"]
        )
    print change["_number"]
    if "messages" in change:
        change_record.messages = db_messages (change["messages"])
        print "Messages added: " + str (len (change_record.messages))
    if "revisions" in change:
        change_record.revisions = db_revisions (change["revisions"])
        print "Revisions added: " + str (len (change_record.revisions))
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
                change_record = db_change (change)
                session.add(change_record)
                session.commit()
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

    from sqlalchemy import create_engine
    database = 'mysql://jgb:XXX@localhost/revisor_test'
    trailer = "?charset=utf8&use_unicode=0"
    database = database + trailer

    engine = create_engine(database, echo=False)
    
    Base.metadata.drop_all(engine) 
    Base.metadata.create_all(engine) 

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)

    for status in ["open", "abandoned", "merged"]:
        get_changes (status = status, period = 1)
