import meta
from pylons import config

from sqlalchemy import *

from gstore.model import Dataset, spatial_ref_sys

import os
import osgeo.osr as osr
from osgeo import gdal as gdal
from osgeo import ogr as ogr

from osgeo.gdalconst import *

import zipfile, tempfile

from geoutils import transform_bbox, bbox_to_polygon, reproject_geom

__all__ = ['TileIndexDataset', 'VectorTileIndexDataset', 'RasterTileIndexDataset']

# Read README at the end of this file.

class TileIndexDataset(object):
    def __init__(self, dataset, config):
        if dataset.taxonomy not in ['rtindex','vtindex']:
            raise Exception('Dataset is not tile index compatible')
        self.dataset = dataset
        self.bundle_id = dataset.bundle_id
        self.SRID = int(config['SRID'])
        if self.bundle_id is None:
            raise Exception('Dataset has no bundle associated of Tile index type')
        
        self.shapefile = None
        if dataset.sources_ref:
            for src in dataset.sources_ref:
                if src.extension == 'shp':
                    self.shapefile = src.location

    def get_index_from_shapefile(self):
        pass

    def write_shapefile_from_index(self, index, destination_path, overwrite = True):
        """
        index: list(list(r.location, r.geom, r.dataset_id))
        destination_path: string target file name for the shapefile
        overwrite: bool Ok to overwrite in case the destination path already exists.

        Usage: 
        >>> d = meta.Session.query(Dataset).get(100157)
        >>> r =  RasterTileIndexDataset(d)
        >>> r.write_shapefile_from_index(r.get_index_from_bundle(), '/tmp/naip2009rtindex.shp')
        """
        if os.path.isfile(destination_path) and overwrite == False:
            raise Exception('Shapefile already exists. Set parameter overwrite = True to ovewrite')

        # Geometries are always stored in Geographic projection and the original projection of the 
        # datasets in the mosaics may be different.
        sr = osr.SpatialReference()
        sr.SetWellKnownGeogCS('WGS84')
        
        orig_sr = osr.SpatialReference()
        orig_sr.ImportFromEPSG(self.dataset.orig_epsg)

        drv = ogr.GetDriverByName('ESRI Shapefile')
        ds = drv.CreateDataSource(destination_path)
    
        lyr = ds.CreateLayer(str(self.dataset.basename), None, ogr.wkbPolygon)

        # Location to the source file for MapServer access
        field_defn1 = ogr.FieldDefn("location", ogr.OFTString)
        lyr.CreateField(field_defn1)

        # Additional field to provide users with direct download links to 
        # first available format
        field_defn2 = ogr.FieldDefn("download", ogr.OFTString)
        lyr.CreateField(field_defn2)
        
        # Label for the index layer if any
        field_defn3 = ogr.FieldDefn("label", ogr.OFTString)
        lyr.CreateField(field_defn3)

        for row in index:
            feat = ogr.Feature(lyr.GetLayerDefn())
            feat.SetField('location', str(row.location))
            feat.SetField(
                'download', 
                str('http://gstore.unm.edu/apps/%(app_id)s/datasets/%(id)s.%(format)s') % { 
                    'id': row.dataset_id, 
                    'format': 'ecw',
                    'app_id': self.dataset.apps_cache[0]
            })
            feat.SetField('label', str(os.path.basename(row.location).split('.')[0]))

            # Set the MBR (minimum bounding rectangle) geometry
            mbr = ogr.CreateGeometryFromWkb(row.geom.decode('hex'))
            if self.dataset.orig_epsg != self.SRID:
                if reproject_geom(mbr, sr, orig_sr):
                    raise Exception('Can not reproject geometry from %s to WGS84' % self.dataset.orig_epsg)
        
            feat.SetGeometry(mbr)
            lyr.CreateFeature(feat)
            feat.Destroy()

        # Flush to disk
        ds = None  
            
        # Write prj file

        prj_file = open('%s.prj' % destination_path , 'w')
        orig_sr.MorphToESRI()
        prj_file.write(orig_sr.ExportToWkt())
        prj_file.close()
            
         
    def _get_index_from_bundle(self, file_format):
        """ 
        Query the source file locations directly by joining related tables in the database.
        The desired sql should like: (assume self.bundle.id = 3):

        gstore# SELECT source.location as location
                  FROM source 
                  JOIN datasets_sources 
                    ON source.id = datasets_sources.source_id 
                   AND source.extension = 'tif' 
                   AND datasets_sources.dataset_id  IN ( 
                       SELECT dataset_id 
                         FROM datasets_bundles 
                        WHERE bundle_id = 3);
        """
                
        # ToDO: Use SQLAlchemy ORM!
        res = meta.Session.execute("""
            SELECT source.location AS location,
                   datasets.geom AS geom,
                   datasets.id AS dataset_id
              FROM source, datasets,  datasets_sources 
             WHERE source.id = datasets_sources.source_id
               AND datasets.id = datasets_sources.dataset_id  
               AND source.extension = '%s' 
               AND datasets_sources.dataset_id  IN ( 
                   SELECT dataset_id 
                     FROM datasets_bundles 
                    WHERE bundle_id = %s);
                """ % (file_format, self.bundle_id)
            ).fetchall()
        meta.Session.close()

        return res
        
       
class VectorTileIndexDataset(TileIndexDataset):
    pass 

class RasterTileIndexDataset(TileIndexDataset):
    """
    @ivar dataset: c(Dataset) instance.
    @ivar geoimage: c(GDALDataset) instance.
    Make sure geoimage is closed by setting it to None once your are done with it.
    """
    is_mappable = True

    def __init__(self, dataset, config):
        """
        @param dataset: c(Dataset) instance.
        """  
        if dataset.taxonomy != 'rtindex':
            raise Exception('Dataset is not raster tile index compatible.')

        super(RasterTileIndexDataset, self).__init__(dataset, config)

    def get_index_from_bundle(self):
        return self._get_index_from_bundle('tif') 
         
    def get_srid(self):
        return self.SRID
    
    def set_srid(self, srid):
        pass


"""

gstore=# begin;
BEGIN
gstore=# insert into bundles (description, long_description, type) values ('MRCOG 2010 Raster Virtual Mosaic - Entire Area', 'Mid-Region Council of Governments (MRCOG) 2010 - Raster Virtual Mosaic', 'rtindex');


-- bundle_id will be 7

-- This is the subselect that lists all MRCOG 2010 datasets
gstore=# select id, description from datasets where subtheme = '2010 (Color RGB)' and groupname = 'MRCOG (6-in)';


-- Create the cross product:
gstore=# insert into datasets_bundles (dataset_id, bundle_id) select id, 7 from datasets where subtheme = '2010 (Color RGB)' and groupname = 'MRCOG (6-in)';
INSERT 0 11221

-- Create a mapfile_template entry (formally for now, it will be required in the near future once we implement custom mapfiles per dataset):

gstore=# insert into mapfile_template (description, taxonomy, xml) values ('MRCOG 2010 Virtual Mosaic', 'rtindex', '<Map/>');
INSERT 0 1

-- mapfile_template_id will be 5

-- Now that we have a bundle for MRCOG10 create a dataset of tipe "rtindex" (Raster Index)
gstore=# insert into datasets (description, taxonomy, basename, theme, subtheme, groupname, inactive, formats_cache, bundle_id, mapfile_template_id, orig_epsg, apps_cache) values ('MRCOG 2010 Raster Virtual Mosaic - Entire Area', 'rtindex', 'mrcog10rtindex', 'Digital Orthophotography', '2010 (Color RGB)', 'MRCOG (6-in)', false, 'zip', 7, 5, 2903, ARRAY['rgis','epscor']);

-- id will be 118650.

-- Find the bounding box of this bundle/virtual dataset and define it as its geom column field.

gstore=# select st_extent(geom) from datasets where subtheme = '2010 (Color RGB)' and groupname = 'MRCOG (6-in)';
                           st_extent                            
----------------------------------------------------------------
 BOX(-107.204350446 34.2502819175,-105.976487013 35.6395726939)
(1 row)

-- Box is an array that caches this bounding box, useful for javascript side of things

gstore=# update datasets set box = array[-107.204350446, 34.2502819175,-105.976487013, 35.6395726939] where id = 118650;
UPDATE 1

-- Finally set the geom column of the rtindex in the datasets table
gstore=# update datasets set geom = (select setsrid(st_extent(geom), 4326) from datasets where subtheme = '2010 (Color RGB)' and groupname = 'MRCOG (6-in)') where id = 118650;
UPDATE 1

gstore=# commit;
COMMIT


-- Now that we have a dataset for the rtindex drop an index shapefile. ( Note the basename define the shapefiles filename.)
For this look at the class "TileIndexDataset" under /var/gstore/gstore/model/tindices.py

Open a paster shell and execute this client code:

renzo@app-dev:/var/gstore$ paster shell
Python 2.6.5 (r265:79063, Apr 16 2010, 13:57:41) 
Type "copyright", "credits" or "license" for more information.

IPython 0.10.1 -- An enhanced Interactive Python.
?         -> Introduction and overview of IPython's features.
%quickref -> Quick reference.
help      -> Python's own help system.
object?   -> Details about 'object'. ?object also works, ?? prints more.


  All objects from gstore.lib.base are available
  Additional Objects:
  mapper     -  Routes mapper object
  wsgiapp    -  This project's WSGI App instance
  app        -  paste.fixture wrapped around wsgiapp


In [1]: from gstore.model import *
In [5]: d = meta.Session.query(Dataset).get(118650)
15:00:22,716 INFO  [sqlalchemy.engine.base.Engine.0x...ac50] [MainThread] select version()
15:00:22,716 INFO  [sqlalchemy.engine.base.Engine.0x...ac50] [MainThread] {}
15:00:22,718 INFO  [sqlalchemy.engine.base.Engine.0x...ac50] [MainThread] select current_schema()
15:00:22,718 INFO  [sqlalchemy.engine.base.Engine.0x...ac50] [MainThread] {}
15:00:22,720 INFO  [sqlalchemy.engine.base.Engine.0x...ac50] [MainThread] BEGIN (implicit)
15:00:22,723 INFO  [sqlalchemy.engine.base.Engine.0x...ac50] [MainThread] SELECT datasets.has_metadata_cache AS datasets_has_metadata_cache, datasets.formats_cache AS datasets_formats_cache, datasets.id AS datasets_id, datasets.description AS datasets_description, datasets.taxonomy AS datasets_taxonomy, datasets.feature_count AS datasets_feature_count, datasets.abstract AS datasets_abstract, datasets.dateadded AS datasets_dateadded, datasets.basename AS datasets_basename, datasets.theme AS datasets_theme, datasets.subtheme AS datasets_subtheme, datasets.groupname AS datasets_groupname, datasets.old_idnum AS datasets_old_idnum, datasets.box AS datasets_box, datasets.orig_epsg AS datasets_orig_epsg, datasets.geom AS datasets_geom, datasets.geomtype AS datasets_geomtype, datasets.inactive AS datasets_inactive, datasets.metadata_xml AS datasets_metadata_xml, datasets.mapfile_template_id AS datasets_mapfile_template_id, datasets.apps_cache AS datasets_apps_cache, datasets.bundle_id AS datasets_bundle_id, features_attributes_1.id AS features_attributes_1_id, features_attributes_1.dataset_id AS features_attributes_1_dataset_id, features_attributes_1.name AS features_attributes_1_name, features_attributes_1.array_id AS features_attributes_1_array_id, features_attributes_1.orig_name AS features_attributes_1_orig_name, features_attributes_1.description AS features_attributes_1_description, features_attributes_1.attribute_type AS features_attributes_1_attribute_type, features_attributes_1.ogr_type AS features_attributes_1_ogr_type, features_attributes_1.ogr_justify AS features_attributes_1_ogr_justify, features_attributes_1.ogr_width AS features_attributes_1_ogr_width, features_attributes_1.ogr_precision AS features_attributes_1_ogr_precision, source_1.location AS source_1_location, source_1.id AS source_1_id, source_1.type AS source_1_type, source_1.is_external AS source_1_is_external, source_1.extension AS source_1_extension, source_1.zipgroup AS source_1_zipgroup, source_1.orig_epsg AS source_1_orig_epsg, source_1.active AS source_1_active 
FROM datasets LEFT OUTER JOIN features_attributes AS features_attributes_1 ON datasets.id = features_attributes_1.dataset_id LEFT OUTER JOIN datasets_sources AS datasets_sources_1 ON datasets.id = datasets_sources_1.dataset_id LEFT OUTER JOIN source AS source_1 ON source_1.id = datasets_sources_1.source_id 
WHERE datasets.id = %(param_1)s
15:00:22,723 INFO  [sqlalchemy.engine.base.Engine.0x...ac50] [MainThread] {'param_1': 118650}

In [6]: d.geom
Out[6]: '0103000020E610000001000000050000008E9FE41314CD5AC0A938E53C092041408E9FE41314CD5AC089DB9D84DDD14140707362C37E7E5AC089DB9D84DDD14140707362C37E7E5AC0A938E53C092041408E9FE41314CD5AC0A938E53C09204140'

In [7]: d.description
Out[7]: u'MRCOG 2010 Raster Virtual Mosaic - Entire Area'

In [12]: d.basename
Out[12]: u'mrcog10rtindex'

-- Ok, it is our dataset, let's create the index shapefile:

In [9]: from gstore.model.tindices import RasterTileIndexDataset
In [11]: r =  RasterTileIndexDataset(d, config)

In [16]: r.write_shapefile_from_index(r.get_index_from_bundle(), '/tmp/mrcog10rtindex.shp')
15:03:13,417 INFO  [sqlalchemy.engine.base.Engine.0x...ac50] [MainThread] 
            SELECT source.location AS location,
                   datasets.geom AS geom,
                   datasets.id AS dataset_id
              FROM source, datasets,  datasets_sources 
             WHERE source.id = datasets_sources.source_id
               AND datasets.id = datasets_sources.dataset_id  
               AND source.extension = 'tif' 
               AND datasets_sources.dataset_id  IN ( 
                   SELECT dataset_id 
                     FROM datasets_bundles 
                    WHERE bundle_id = 7);
                
15:03:13,417 INFO  [sqlalchemy.engine.base.Engine.0x...ac50] [MainThread] {}


In [17]: ls -lt /tmp/mrcog10rtindex.*
-rw-r--r-- 1 renzo renzo     452 2011-07-28 15:03 /tmp/mrcog10rtindex.shp.prj
-rw-r--r-- 1 renzo renzo 1806517 2011-07-28 15:03 /tmp/mrcog10rtindex.dbf
-rw-r--r-- 1 renzo renzo 1526020 2011-07-28 15:03 /tmp/mrcog10rtindex.shp
-rw-r--r-- 1 renzo renzo   89860 2011-07-28 15:03 /tmp/mrcog10rtindex.shx


-- Zip up this shapefile and add it as a source to the main dataset


In [19]: mkdir /clusterdata/gstore/formats/118650

In [20]: zip /tmp/mrcog10rtindex mrcog10rtindex.*
updating: mrcog10rtindex.dbf (deflated 97%)
updating: mrcog10rtindex.shp (deflated 51%)
updating: mrcog10rtindex.shp.prj (deflated 32%)
updating: mrcog10rtindex.shx (deflated 72%)

In [21]: cp /tmp/mrcog10rtindex.* /clusterdata/gstore/formats/118650/
In [22]: ls -lt /clusterdata/gstore/formats/118650/
total 4184
-rw-r--r-- 1 renzo renzo  834259 2011-07-28 16:28 mrcog10rtindex.zip
-rw-r--r-- 1 renzo renzo 1806517 2011-07-28 16:26 mrcog10rtindex.dbf
-rw-r--r-- 1 renzo renzo 1526020 2011-07-28 16:26 mrcog10rtindex.shp
-rw-r--r-- 1 renzo renzo     452 2011-07-28 16:26 mrcog10rtindex.shp.prj
-rw-r--r-- 1 renzo renzo   89860 2011-07-28 16:26 mrcog10rtindex.shx

-- Add this source to the dataset

In [3]: s = Resource('/clusterdata/gstore/formats/118650/mrcog10rtindex.zip')

In [4]: d.sources_ref.append(s)

In [5]: meta.Session.commit()
15:32:49,918 INFO  [sqlalchemy.engine.base.Engine.0x...3c50] [MainThread] INSERT INTO source (location, type, is_external, extension, zipgroup, orig_epsg, active) VALUES (%(location)s, %(type)s, %(is_external)s, %(extension)s, %(zipgroup)s, %(orig_epsg)s, %(active)s) RETURNING source.id
15:32:49,918 INFO  [sqlalchemy.engine.base.Engine.0x...3c50] [MainThread] {'orig_epsg': None, 'is_external': False, 'extension': 'zip', 'zipgroup': None, 'location': '/clusterdata/gstore/formats/118650/mrcog10rtindex.zip', 'active': None, 'type': 'file'}
15:32:49,929 INFO  [sqlalchemy.engine.base.Engine.0x...3c50] [MainThread] INSERT INTO datasets_sources (source_id, dataset_id) VALUES (%(source_id)s, %(dataset_id)s)
15:32:49,929 INFO  [sqlalchemy.engine.base.Engine.0x...3c50] [MainThread] {'source_id': 344570, 'dataset_id': 118650}
15:32:49,932 INFO  [sqlalchemy.engine.base.Engine.0x...3c50] [MainThread] COMMIT


-- Lovingly-handcraft a map file pointing to this shapefile. Since NAIP09 is very similar, copy it over and edit it.

In [23]: cp /clusterdata/gstore/maps/naip2009rtindex.map /clusterdata/gstore/maps/mrcog10rtindex.map

-- Ok, I think it's time to get out of paster shell (ipython)

vim /clusterdata/gstore/maps/mrcog10rtindex.map

Make sure we define the following mapserver tags correctly:

Set the EXTENT in its native project 2903

EXTENT 1352028.95258608 1182567.67631635 1723066.12575741 1689448.3457696

We can calculate this back in postgis as follows:
gstore=# select st_extent(st_transform(geom, 2903)) from datasets where id = 118650;
                                st_extent                                
-------------------------------------------------------------------------
 BOX(1352028.95258608 1182567.67631635,1723066.12575741 1689448.3457696)
(1 row)


SHAPEPATH "/clusterdata/gstore/formats/118650"

"wcs_label" "imagery_wcs_mrcog10" #*#

    LAYER
        NAME "mrcog10rtindex_idx" #
        DATA "/clusterdata/gstore/formats/118650/mrcog10rtindex.shp"

... etc, replace all ocurrences of NAIP09 with MRCOG10 and the new dataset id 118650




"""
