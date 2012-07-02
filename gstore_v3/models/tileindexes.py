from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY


'''
tile indexes
'''

tileindexes = Table('tileindexes', Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('dateadded', TIMESTAMP, default='now()'),
    Column('bbox', ARRAY(Numeric)),
    Column('epsg', Integer),
    Column('uuid', UUID, FetchedValue()),
    schema='gstoredata'
)

tileindexes_datasets = Table('tileindexes_datasets', Base.metadata,
    Column('tile_id', Integer, ForeignKey('gstoredata.tileindexes.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)

##the view
#tileindex_view = Table('get_tileindexes', Base.metadata,
#    Column('tile_id', Integer),
#    Column('tile_name', String),
#    Column('dataset_id', Integer),
#    Column('dataset_description', String),
#    Column('location', String),
#    Column('geom', String),
#    Column('fid', Integer),
#    Column('srctype', String),
#    Column('orig_epsg', Integer),
#    Column('valid_start', TIMESTAMP),
#    Column('valid_end', TIMESTAMP),
#    schema='gstoredata'
#)
