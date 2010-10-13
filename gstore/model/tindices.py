import meta
from pylons import config

from sqlalchemy import *

from geobase import Dataset, spatial_ref_sys

import os
import osgeo.osr as osr
from osgeo import gdal as gdal
from osgeo import ogr as ogr

from osgeo.gdalconst import *

import zipfile, tempfile

from shapes_util import transform_bbox, bbox_to_polygon, reproject_geom

__all__ = ['RasterDataset']

SRID = int(config['SRID'])

class TileIndexDataset(object):
    def __init__(self, dataset):
        if dataset.taxonomy not in ['rtindex','vtindex']:
            raise Exception('Dataset is not tile index compatible')
        self.dataset = dataset
        self.bundle_id = dataset.bundle_id
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

            # Set the MBR (minimum bounding rectangle) geometry
            mbr = ogr.CreateGeometryFromWkb(row.geom.decode('hex'))
            if self.dataset.orig_epsg != SRID:
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

    def __init__(self, dataset):
        """
        @param dataset: c(Dataset) instance.
        """  
        if dataset.taxonomy != 'rtindex':
            raise Exception('Dataset is not raster tile index compatible.')

        super(RasterTileIndexDataset, self).__init__(dataset)

    def get_index_from_bundle(self):
        return self._get_index_from_bundle('tif') 
         
    def get_srid(self):
        srid = None
        return srid
    
    def set_srid(self, srid):
        pass



