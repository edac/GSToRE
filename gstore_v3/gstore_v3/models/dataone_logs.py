from gstore_v3.models import DataoneBase, DataoneSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref

from sqlalchemy import desc, asc, func

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

import os

'''
This is a separate postgres database just for the dataone logging. 
It is tied to a different session, etc. FYI.
'''

class DataoneLog(DataoneBase):
    __table__ = Table('logs', DataoneBase.metadata,
        Column('id', Integer, primary_key=True),
        Column('identifier', String(500)),
        Column('ip_address', String(20)),
        Column('useragent', String(50)),
        Column('subject', String(200)),
        Column('logged', TIMESTAMP, FetchedValue()),  #, default='now()'
        Column('event', String(20)),
        Column('node', String(100))
    )

    def __init__(self, identifier, ip_address, subject, event, node, useragent='public'):
        self.identifier = identifier
        self.ip_address = ip_address
        self.useragent = useragent
        self.subject = subject
        self.event = event
        self.node = node

    def __repr__(self):
        return '<Log (%s, %s, %s, %s)>' % (self.id, self.identifier, self. event, self.logged)

    #TODO: maybe just go back to the template? meh. six of one, probably.
    def get_log_entry(self):
        '''
        <logEntry>
            <entryId>${d['id']}</entryId>
            <identifier>${d['identifier']}</identifier>
            <ipAddress>${d['ip']}</ipAddress>
            <userAgent>${d['useragent']}</userAgent>
            <subject>${d['subject']}</subject>
            <event>${d['event']}</event>
            <dateLogged>${d['dateLogged']}</dateLogged>
            <nodeIdentifier>${d['node']}</nodeIdentifier>
        </logEntry>
        '''
        fmt = '%Y-%m-%dT%H:%M:%S+00:00'
        entry = """<logEntry>
                    <entryId>%(id)s</entryId>
                    <identifier>%(identifier)s</identifier>
                    <ipAddress>%(ip)s</ipAddress>
                    <userAgent>%(useragent)s</userAgent>
                    <subject>%(subject)s</subject>
                    <event>%(event)s</event>
                    <dateLogged>%(logged)s</dateLogged>
                    <nodeIdentifier>%(node)s</nodeIdentifier>
                   </logEntry>""" % {'id':self.id, 'identifier':self.identifier, 'ip':self.ip_address, 'useragent':self.useragent, 'subject':self.subject, 'event':self.event, 'logged':self.logged.strftime(fmt), 'node':self.node}
        return entry

    #if we do go back to the template
    def get_json(self):
        fmt = '%Y-%m-%dT%H:%M:%S+00:00'
        return {'id': self.id, 'identifier': self.identifier, 'ip': self.ip_address, 'useragent': self.useragent, 'subject': self.subject, 'event': self.event, 'dateLogged': self.logged.strftime(fmt), 'node': self.node}

    #TODO: deal with session and user agent (not as string)


class DataoneError(DataoneBase):
    __table__ = Table('errors', DataoneBase.metadata,
        Column('id', Integer, primary_key=True),
        Column('message', String(500)),
        Column('received', TIMESTAMP, FetchedValue())
    )

    def __init__(self, message):
        self.message = message
    def __repr__(self):
        return '<DataONE Error: %s, %s>' % (self.id, self.received)
    
    
