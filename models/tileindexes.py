from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql.expression import and_
from sqlalchemy import func

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from ..lib.spatial import *
from osgeo import ogr, osr

import os 
import subprocess
from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY


'''
tile indexes
'''
#TODO: test mrcog 2010 index with external pyramids:
#gdaladdo --config COMPRESS_OVERVIEW JPEG --config PHOTOMETRIC_OVERVIEW YCBCR --config INTERLEAVE_OVERVIEW PIXEL -ro A1_NE.tif 2 4 8 16
#for each tif in a directory
#for f in *.tif; do gdaladdo --config COMPRESS_OVERVIEW JPEG --config INTERLEAVE_OVERVIEW PIXEL -ro $f 2 4 6 8 16; done

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


    #TODO: this didn't work through pshell but the function DOES work from psql
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

        #execute the generate_tileindex_bbox(id) stored procedure to update the bbox
        DBSession.query(func.gstoredata.generate_tileindex_bbox(self.id))

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


        SRID = 4326
        spatialref = epsg_to_sr(SRID)
        
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

                reproject_geom(geom, SRID, epsg)

                feature = ogr.Feature(lyr.GetLayerDefn())
                feature.SetField('location', str(d.location))
                feature.SetField('name', str(d.description))
                feature.SetField('time', d.begin_datetime.strftime('%Y-%m-%dT%H:%M:%S') if d.begin_datetime else None)

                feature.SetGeometry(geom)
                lyr.CreateFeature(feature)
                feature.Destroy()

            prjfile = open('%s/tile_%s.prj' % (out_location, epsg), 'w')
            #outref.MorphToESRI()
            prjfile.write(outref.ExportToWkt())
            prjfile.close()

            #self.generate_spatial_index('tile_%s' % epsg, out_location)

            
    def generate_spatial_index(self, tile, out_location):
        '''
        add a spatial index (qix)
        this may or may not really improve performance much, most of the tile issues are network-bound reads 
        
        ogrinfo -sql "CREATE SPATIAL INDEX on tile_2258" /home/sscott/tileindexes/e305d3ed-9db2-4895-8a35-bde79150e272/tile_2258.shp
        '''
        cmd = 'ogrinfo -sql "CREATE SPATIAL INDEX on %s" %s.shp' % (tile, os.path.join(out_location, tile))
        s = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
        output = s.communicate()[0]
        ret = s.wait()         

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
'''
to build the view:

-- View: gstoredata.get_tileindexes

-- DROP VIEW gstoredata.get_tileindexes;

CREATE OR REPLACE VIEW gstoredata.get_tileindexes AS 
 SELECT t.id AS tile_id, t.uuid AS tile_uuid, t.name AS tile_name, d.id AS gid, d.uuid AS dataset_uuid, d.description, f.location, d.geom, d.orig_epsg, d.begin_datetime, d.end_datetime
   FROM gstoredata.tileindexes t
   LEFT JOIN gstoredata.tileindexes_datasets ts ON ts.tile_id = t.id
   LEFT JOIN gstoredata.datasets d ON ts.dataset_id = d.id
   LEFT JOIN gstoredata.sources s ON d.id = s.dataset_id
   LEFT JOIN gstoredata.source_files f ON s.id = f.source_id
  WHERE s.set::text = 'original'::text AND s.extension::text = 'tif'::text AND f.location::text ~~ '%.tif'::text;

ALTER TABLE gstoredata.get_tileindexes
  OWNER TO gstore;
GRANT ALL ON TABLE gstoredata.get_tileindexes TO gstore;
GRANT SELECT ON TABLE gstoredata.get_tileindexes TO gstoreread;
'''

'''
for mrcog 2012

http://129.24.63.115/apps/rgis/tileindexes/c9035d03-f6ff-4317-a959-456a6df5b56a/services/ogc/wms?REQUEST=GetMap&SERVICE=WMS&VERSION=1.1.1&FORMAT=image/png&LAYERS=test_mrcog_2012_2903&width=1000&height=1000&style=&SRS=epsg:2903&bbox=1454715,1413770,1459965,1418945

http://129.24.63.115/apps/rgis/tileindexes/c9035d03-f6ff-4317-a959-456a6df5b56a/services/ogc/wms?REQUEST=GetMap&SERVICE=WMS&VERSION=1.1.1&FORMAT=image/png&LAYERS=test_mrcog_2012_2903&width=1000&height=1000&style=&SRS=epsg:2903&bbox=1454790,1411071,1473263,1429519
'''

