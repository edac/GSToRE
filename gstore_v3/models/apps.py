from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey, Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

'''
apps model
'''

class GstoreApp(Base):
    __table__ = Table('apps', Base.metadata,
        Column('id', Integer, primary_key = True),
        Column('name', String(25)),
        Column('full_name', String(250)),
        Column('url', String(500)),
        Column('route_key', String(15)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

    '''
    just the metadata requirements right now (liability, etc)
    '''

    def __repr__(self):
        return '<App (%s, %s)>' % (self.id, self.name)

    def __init__(self, name, route_key):
        self.name = name
        self.route_key = route_key



