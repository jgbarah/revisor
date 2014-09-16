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
## Package to deal with queries for Gerrit, retrieved via revisor
##
## Authors:
##   Jesus M. Gonzalez-Barahona <jgb@bitergia.com>
##

from grimoirelib_alch.type.timeseries import TimeSeries
from grimoirelib_alch.type.activity import ActivityList
from grimoirelib_alch.query.common import GrimoireDatabase, GrimoireQuery

from sqlalchemy import func, Column, Integer, ForeignKey, or_
from sqlalchemy.sql import label
from datetime import datetime

class DB (GrimoireDatabase):
    """Class for dealing with SCM (CVSAnalY) databases.

    """
 
    def _query_cls(self):
        """Return que defauld Query class for this database

        Returns
        -------

        GrimoireQuery: default Query class.

        """

        return Query

    def _create_tables(self):
        """Create all SQLAlchemy tables.

        Builds a SQLAlchemy class per SQL table, by using _table().
        It assumes self.Base, self.schema and self.schema_id are already
        set (see super.__init__() code).

        """

        DB.Change = GrimoireDatabase._table (
            bases = (self.Base,), name = 'Change',
            tablename = 'changes',
            schemaname = self.schema)

        DB.Message = GrimoireDatabase._table (
            bases = (self.Base,), name = 'Message',
            tablename = 'messages',
            schemaname = self.schema,
            columns = dict (
                change_id = Column(
                    Integer,
                    ForeignKey(self.schema + '.' + 'changes.uid')
                    ),
                ))

        DB.Revision = GrimoireDatabase._table (
            bases = (self.Base,), name = 'Revision',
            tablename = 'revisions',
            schemaname = self.schema,
            columns = dict (
                change_id = Column(
                    Integer,
                    ForeignKey(self.schema + '.' + 'changes.uid')
                    ),
                ))

        DB.Approval = GrimoireDatabase._table (
            bases = (self.Base,), name = 'Approval',
            tablename = 'approvals',
            schemaname = self.schema,
            columns = dict (
                change_id = Column(
                    Integer,
                    ForeignKey(self.schema + '.' + 'changes.uid')
                    ),
                ))

class Query (GrimoireQuery):
    """Class for dealing with Gerrit-related queries"""

if __name__ == "__main__":

    from grimoirelib_alch.aux.standalone import stdout_utf8, print_banner

    stdout_utf8()

    database = DB (url = 'mysql://jgb:XXX@localhost/',
                   schema = 'revisorj_test',
                   schema_id = 'revisorj_test')
    session = database.build_session(Query, echo = False)
    
    #---------------------------------
    print_banner ("Number of changes, messages, revisions, approvals")
    res = session.query(label ("changes",
                               func.count (DB.Change.id)))
    print res.scalar()
    res = session.query(label ("messages",
                               func.count (DB.Message.uid)))
    print res.scalar()
    res = session.query(label ("revisions",
                               func.count (DB.Revision.uid)))
    print res.scalar()
    res = session.query(label ("approvals",
                               func.count (DB.Approval.uid)))
    print res.scalar()
