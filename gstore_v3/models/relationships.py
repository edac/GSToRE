from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey, Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY


'''
gstoredata.relationships
'''

class DatasetRelationship(Base):
    __table__ = Table('relationships', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('base_dataset', Integer),
        Column('related_dataset', Integer),
        Column('relationship', String(100)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

    def __init__(self, base_dataset, related_dataset, relationship):
        self.base_dataset = base_dataset
        self.related_dataset = related_dataset
        self.relationship = relationship

    def __repr__(self):
        return '<Relationship (%s, %s, %s, %s)>' % (self.id, self.base_dataset, self.related_dataset, self.relationship)


