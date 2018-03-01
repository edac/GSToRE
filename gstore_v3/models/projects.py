from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey, Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY


'''
gstoredata.projects
'''
projects_datasets = Table('projects_datasets', Base.metadata,
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    Column('project_id', Integer, ForeignKey('gstoredata.projects.id')),
    schema='gstoredata'    
)

class Project(Base):
    __table__ = Table('projects', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(200)),
        Column('description', String(1000)),
        Column('acknowledgments', String(500)),
        Column('funder', String(200)),
        schema='gstoredata'
    )

    def __init__(self, name, description, funder):
        self.name = name
        self.description = description
        self.funder = funder

    def __repr__(self):
        return '<Project (%s, %s, %s)>' % (self.id, self.name, self.funder)

