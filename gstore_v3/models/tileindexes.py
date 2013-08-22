from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql.expression import and_

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from ..lib.spatial import *
from osgeo import ogr, osr

import os 
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
        Column('is_active', Boolean),
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

    def generate_index_shapefiles(self, out_location):
        '''
        build the shapefile with the dataset bboxes and locations

        BUT because a tile index can consist of datasets with different spatial refs, we
        need to build a shapefile for each epsg
        '''

        #append the tile uuid to the out_location to keep everything together and safe
        if not os.path.isdir(os.path.join(out_location, self.uuid)):
            os.mkdir(os.path.join(out_location, self.uuid))

        out_location = os.path.join(out_location, self.uuid)

        epsgs = self.epsgs

        spatialref = epsg_to_sr(4326)
        
        for epsg in epsgs:
            #get the right datasets, from the tileindex (to avoid the instrumented list deal)
            epsg_set = DBSession.query(TileIndexView).filter(and_(TileIndexView.tile_id==self.id, TileIndexView.orig_epsg==epsg))


            #set up the shapefile
            drv = ogr.GetDriverByName('ESRI Shapefile')
            shpfile = drv.CreateDataSource(os.path.join(out_location, 'tile_%s.shp' % (epsg)))

            lyr = shpfile.CreateLayer('tile_%s' % (epsg), None, ogr.wkbPolygon)

            locfld = ogr.FieldDefn('location', ogr.OFTString)
            namefld = ogr.FieldDefn('name', ogr.OFTString)

            #make the field bigger - truncates long paths (default = 80)
            locfld.SetWidth(250)

            lyr.CreateField(locfld)
            lyr.CreateField(namefld)

            timefld = ogr.FieldDefn('time', ogr.OFTString)
            lyr.CreateField(timefld)

            outref = epsg_to_sr(epsg)

            for d in epsg_set:
                wkb = d.geom
                geom = wkb_to_geom(wkb, epsg)

                feature = ogr.Feature(lyr.GetLayerDefn())
                feature.SetField('location', str(d.location))
                feature.SetField('name', str(d.description))
                feature.SetField('time', d.begin_datetime.strftime('%Y-%m-%dT%H:%M:%S') if d.begin_datetime else None)

                feature.SetGeometry(geom)
                lyr.CreateFeature(feature)
                feature.Destroy()

            prjfile = open('%s/tile_%s.prj' % (out_location, epsg), 'w')
            outref.MorphToESRI()
            prjfile.write(outref.ExportToWkt())
            prjfile.close()


        '''
>>> from gstore_v3.models import *
>>> base_path = request.registry.settings['BASE_DATA_PATH'] + '/tileindexes'
>>> base_path = '/home/sscott/tileindexes'
>>> ti = DBSession.query(tileindexes.TileIndex).filter(tileindexes.TileIndex.id==1).first()
>>> ti.generate_index_shapefiles(base_path)
        '''

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


