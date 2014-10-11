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

# To properly support utf8mb4 in SQLAlchemy
# http://blog.bbdouglas.com/unicode-surrogate-characters-and-lamp/
# Important: Create database with (being `revisor_test` the schema name):
# CREATE SCHEMA `revisor_test` DEFAULT CHARACTER SET utf8mb4 ;

import encodings
encodings._aliases["utf8mb4"] = "utf_8"

description = """
Simple script to store in a database the contents of a JSON document
obtained via the gerrit command (usually through an ssh connection)
of a Gerrit system.

Example of use:

revisor_json.py changes.json mysql://jgb:XXX@localhost/gerrit_changes

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
    project = Column(String(100), index=True)
    branch = Column(String(50))
    number = Column(Integer, index=True)
    subject = Column(String(200))
    topic = Column(String(50))
    url = Column(String(100))
    open = Column(Boolean)
    status = Column(String(20))
    created = Column(DateTime)
    updated = Column(DateTime)
    sortkey = Column(String(20))
    owner_id = Column(Integer, ForeignKey('people.uid'))

    owner = relationship ("People")
    
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
    header = Column(String(100), index=True)
    message = Column(String(5000))
    author_id = Column(Integer, ForeignKey('people.uid'))

    author = relationship ("People")

    change = relationship (
        "Change",
        backref=backref("messages",
                        order_by = uid,
                        cascade="all, delete-orphan")
        )

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
    author_id = Column(Integer, ForeignKey('people.uid'))

    author = relationship ("People")

    change = relationship (
        "Change",
        backref=backref("revisions",
                        order_by = uid,
                        cascade="all, delete-orphan")
        )

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
    author_id = Column(Integer, ForeignKey('people.uid'))

    author = relationship ("People")

    change = relationship (
        "Revision",
        backref=backref("approvals",
                        order_by = uid,
                        cascade="all, delete-orphan")
        )

class People(Base):
    """Table for people.

    Fields describing people that are found at varios places
    in the corresponding JSON document.

    """

    __tablename__ = "people"

    uid = Column(Integer, primary_key=True)
    name = Column(String(100), index=True)
    email = Column(String(100), index=True)
    username = Column(String(50), index=True)

#    __table_args__ = { mysql_charset='utf8mb4' }

def parse_args ():
    """
    Parse command line arguments

    """

    parser = argparse.ArgumentParser(description = description)
    parser.add_argument("file",
                        help = "JSON file with the change records, " \
                            + "as produced by the gerrit command."
                        )
    parser.add_argument("database",
                        help = "SQLAlchemy url of the database " + \
                            "to write the data to."
                        )
    parser.add_argument("--createdb",
                        help = "Create database if it does not exist.",
                        action = "store_true"
                        )
    args = parser.parse_args()
    return args

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
            author = db_people(message["reviewer"])
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
            author = db_people(revision["uploader"]),
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
            author = db_people(approval["by"]),
            )
        records.append (record)
    return records
    
def db_people (person):
    """Produce or link person (people) records.

    Parameters
    ----------

    person: str
        Person dictionary as obtained form the Gerrit JSON document.

    """

#    print person
    if "name" in person:
        name = person["name"]
    else:
        name = None
    if "email" in person:
        email = person["email"]
    else:
        email = None
    if "username" in person:
        username = person["username"]
    else:
        username = None
    q = session.query(People) \
        .filter(People.name == name,
                People.email == email,
                People.username == username)
    if q.count() > 0:
#        for person in q.all():
#            print "Found person: ", person.username
        record = q.one()
    else:
#        print person
        record = People (
            name = name,
            email = email,
            username = username
            )
        session.add (record)
#        print "Adding person: ", record.username
    return record

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
        sortkey = change["sortKey"],
        owner = db_people(change["owner"])
        )
    return change_record
            

if __name__ == "__main__":

    args = parse_args()

    from sqlalchemy import create_engine

    if args.createdb:
        # To create database, we need a SQLAlchemy url for the database
        # without schema name, since we have to connect to it to create
        # the schema.
        (db, schema) = args.database.rsplit ('/', 1)
        dbengine = create_engine(db, echo=False, encoding='utf8mb4')
        dbengine.execute ("CREATE DATABASE IF NOT EXISTS " + schema \
                            + " DEFAULT CHARACTER SET utf8mb4")

    database = args.database
#    trailer = "?charset=utf8&use_unicode=0"
    trailer = "?charset=utf8mb4&use_unicode=0"
    database = database + trailer

#    engine = create_engine(database, echo=False, encoding='utf8')
    engine = create_engine(database, echo=False, encoding='utf8mb4')

    Base.metadata.drop_all(engine) 
    Base.metadata.create_all(engine) 

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()

    count = 0
    for line in open (args.file, "r"):
        # Each line includes all info related to a change
        count = count + 1
        change = json.loads(line)
        print str(count) + ": " + str(change["number"]),
        change_record = db_change (change)
        if "comments" in change:
            change_record.messages = db_messages (change["comments"])
            #print "Messages added: " + str (len (change_record.messages))
        if "patchSets" in change:
            change_record.revisions = db_revisions (change["patchSets"])
            #print "Revisions added: " + str (len (change_record.revisions))
            for rev, revision in enumerate (change["patchSets"]):
                if "approvals" in revision:
                    change_record.revisions[rev].approvals = \
                        db_approvals(revision["approvals"])
                    #print "Approvals added: " + \
                    #    str (len (change_record.revisions[rev].approvals))
                    
        q = session.query(Change) \
            .filter(Change.number == change["number"])
        if q.count() > 0:
            print " REPEATED, deleting old records."
            session.delete(q.one())
        else:
            print
        session.add(change_record)
        if count % 1000 == 0:
            print "Comitting..."
            session.commit()
    print "Comitting..."
    session.commit()

