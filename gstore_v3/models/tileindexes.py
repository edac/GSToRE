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

class TileIndex(Base):
    __table__ = Table('tileindexes', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('dateadded', TIMESTAMP, default='now()'),
        Column('bbox', ARRAY(Numeric)),
        Column('epsgs', ARRAY(Integer)),
        Column('uuid', UUID, FetchedValue()),
        Column('basename', String(25)),
        Column('taxonomy', String(25)),
        schema='gstoredata'
    )
    '''
    bbox is the extent of the union of all extents in the tile index dataset collection
    epsgs is a list of all epsgs in the tile index collection
    '''

    #relate to datasets
    #although it's not necessary from the tileindex ogc service point of view
    datasets = relationship('Dataset', secondary='gstoredata.tileindexes_datasets')

    def __repr__(self):
        return '<TileIndex (%s, %s)>' % (self.uuid, self.name)


    #generate the extent from the union of all bboxes for the datasets in the collection
    def get_index_extent(self):
        '''
        --you don't need the cte really, certainly not for sqla
        with tile_geom as (
	        select st_union(d.geom) as bbox
	        from gstoredata.tileindexes t, gstoredata.tileindexes_datasets td, gstoredata.datasets d
	        where t.id = td.tile_id and t.id = 3 and td.dataset_id = d.id
        )
        --to get the wkb geometry of the extent
        --select encode(st_asbinary(st_extent(bbox)), 'hex') from tile_geom;
        --or to get the wkt extent
        select st_extent(bbox) from tile_geom;
        '''
        pass

#dataset - tileindex join table
tileindexes_datasets = Table('tileindexes_datasets', Base.metadata,
    Column('tile_id', Integer, ForeignKey('gstoredata.tileindexes.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)

#TODO: change to valid start and valid end 
class TileIndexView(Base):
    __table__ = Table('get_tileindexes', Base.metadata,
        Column('tile_id', Integer, primary_key=True),
        Column('tile_uuid', UUID),
        Column('tile_name', String),
        Column('gid', Integer, primary_key=True), #the dataset id
        Column('dataset_uuid', UUID),
        Column('description', String), #dataset description
        Column('location', String), #source file for the tif as the tile raster
        Column('geom', String), #dataset bbox as the geometry of each tile
        Column('orig_epsg', Integer),
        Column('begin_datetime', TIMESTAMP),
        Column('end_datetime', TIMESTAMP),
        schema='gstoredata'
    )

    def __repr__(self):
        return '<TileIndexView (%s, %s)>' % (self.tile_uuid, self.dataset_uuid)


