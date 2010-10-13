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

from shapes_util import transform_bbox, bbox_to_polygon

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
        index: list(list(r.location, r.geom))
        destination_path: string target file name for the shapefile
        overwrite: bool Ok to overwrite in case the destination path already exists.
        """
        if os.path.isfile(destination_path) and overwrite == False:
            raise Exception('Shapefile already exists. Set parameter overwrite = True to ovewrite')
        drv = ogr.GetDriverByName('ESRI Shapefile')
        ds = drv.CreateDataSource(destination_path)
    
        lyr = ds.CreateLayer(str(self.dataset.basename), None, ogr.wkbPolygon)

        field_defn = ogr.FieldDefn("location", ogr.OFTString)
        lyr.CreateField(field_defn)
        #field_defn = ogr.FieldDefn("download_url", ogr.OFTString)
        #lyr.CreateField(field_defn)
        for row in index:
            feat = ogr.Feature(lyr.GetLayerDefn())
            feat.SetField('location', str(row.location))
            #feat.SetField('download_url', 'http://gstore.unm.edu/apps/%(app_id)s/datasets/%(id)s.%(format)s')

            # set the MBR (minimum bounding rectangle)
            mbr = ogr.CreateGeometryFromWkb(row.geom.decode('hex'))
            feat.SetGeometry(mbr)
            lyr.CreateFeature(feat)
            feat.Destroy()

        # Flush to disk
        ds = None  
            
         
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
                   datasets.geom AS geom
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



