from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, Numeric, FetchedValue
from sqlalchemy.orm import relationship, backref, deferred

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID

'''
features
except really just the postgres component for the model
'''

#TODO: have a think about the time field in shapes, except it kinda won't matter - 
#      mongo will have the observed times
class Feature(Base):
    __table__ = Table('shapes', Base.metadata,
        Column('fid', Integer, primary_key=True), #this should really be the primary key
        Column('gid', Integer),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        Column('geom', String),
        Column('uuid', UUID, FetchedValue()),
        schema = 'gstoredata'
    )

    def __init__(self, gid, geom, dataset_id):
        self.geom = geom
        self.gid = gid
        self.dataset_id = dataset_id
