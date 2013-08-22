from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey, Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY


'''
gstoredata.citations
'''
datasets_citations = Table('datasets_citations', Base.metadata,
    Column('citation_id', Integer, ForeignKey('gstoredata.citations.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)

class Citation(Base):
    __table__ = Table('citations', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('full_citation', String(1000)),
        Column('date_added', TIMESTAMP,FetchedValue()),
        schema = 'gstoredata'
    )

    def __inti__(self, citation):
        self.citation = citation

    def __repr__(self):
        return '<Citation %s, %s>' % (self.id, self.uuid)

