from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref

from pyramid.threadlocal import get_current_registry

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

'''
metadata
(not called metadata to avoid any confusion/conflict with the sqlalchemy object)
'''


#intermediate table
class DatasetMetadata(Base):
    __table__ = Table('original_metadata', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('original_xml', String),
        Column('original_text', String),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        schema='gstoredata'
    )

    def __repr__(self):
        return '<Original Metadata (%s)>' % (self.id)


#TODO: implement the full metadata schema
