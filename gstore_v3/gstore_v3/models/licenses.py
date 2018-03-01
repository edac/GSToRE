from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey, Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

'''
license model
'''

class License(Base):
    __table__ = Table('licenses', Base.metadata,
        Column('id', Integer, primary_key = True),
        Column('uuid', UUID, FetchedValue()),
        Column('name', String(250)),
        Column('alias', String(50)),
        Column('legal_code_uri', String(500)),
        Column('image_url', String(500)),
        schema='gstoredata'
    )

    '''
    dataset-level license assignment
    '''

    #relate to datasets
    licenses = relationship('Dataset', backref='licenses')

    def __repr__(self):
        return '<License (%s, %s, %s)>' % (self.id, self.name, self.alias)

    def __init__(self, name, alias, legal_code_uri):
        self.name = name
        self.alias = alias
        self.legal_code_uri = legal_code_uri



