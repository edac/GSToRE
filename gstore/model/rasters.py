import meta
from pylons import config

from gstore.model import Dataset, spatial_ref_sys

import osgeo.osr as osr
from osgeo import gdal as gdal
from osgeo.gdalconst import *

import zipfile, tempfile

from geoutils import transform_bbox, bbox_to_polygon

__all__ = ['RasterDataset']

SRID = int(config['SRID'])

class RasterDataset(object):
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
        if dataset.taxonomy != 'geoimage':
            raise Exception('Dataset not raster compatible.')

        self.dataset = dataset
        self.geoimage = None
        # only vector datasets and derivatives have attributes
        self.attributes_ref = None 

        if dataset.sources_ref:
            for src in dataset.sources_ref:
                if src.extension == 'tif':
                    self.geoimage = gdal.Open(str(src.location), GA_ReadOnly)
                    self.filename = src.location

    def get_srid(self):
        srid = None
        wkt = self.geoimage.GetProjectionRef()
        if wkt:
            s = osr.SpatialReference()
            s.SetFromUserInput(wkt)
            auth_srid = meta.Session.query(spatial_ref_sys.c.auth_srid).filter(\
                    spatial_ref_sys.c.proj4text == s.ExportToProj4()).first()
            if auth_srid:
                srid = auth_srid.auth_srid
            meta.Session.close()

        return srid
    
    def set_srid(self, srid):
        if self.dataset.sources_ref:
            for src in dataset.sources_ref:
                src.orig_epsg = srid

    def get_raster_envelope(self):
        """ 
        http://www.gdal.org/gdal_tutorial.html
        GeoTransform[0] /* top left x */
        GeoTransform[1] /* w-e pixel resolution */
        GeoTransform[2] /* rotation, 0 if image is "north up" */
        GeoTransform[3] /* top left y */
        GeoTransform[4] /* rotation, 0 if image is "north up" */
        GeoTransform[5] /* n-s pixel resolution */
        
        http://trac.osgeo.org/gdal/browser/trunk/gdal/apps/gdalinfo.c#L680
        
        return a Polygon geometry to mimic DatasetFootprint. See set_envelope
        """
        (minX, we, r1, maxY, r2, ns) = self.geoimage.GetGeoTransform()
        xsize = self.geoimage.RasterXSize
        ysize = self.geoimage.RasterYSize
        # Lower right corner
        maxX = minX + we*xsize + r1*ysize
        minY = maxY + r2*xsize + ns*ysize 

        bbox = (minX, minY, maxX, maxY) 
    
        srid = get_srid()   
        if srid is not None and srid != SRID:
            return bbox_to_polygon(transform_bbox(bbox, srid, SRID))
        else:
            return bbox_to_polygon(bbox)


    def set_dataset_envelope(self, envelope):
        """
        envelope: Polygon geometry instance, typically an OGRGeometry.GetExtent()
        """
        from_srid = self.get_srid()
        if from_srid != SRID:
            self.dataset.geom = bbox_to_polygon(transform_bbox(envelope.GetExtent(), from_srid, SRID))
        else:
            self.dataset.geom = envelope

    def set_dataset_sources(self, filenames):
        """
        Not gonna use it!
        sources: list of full paths to companion files for the dataset of the form
                ((pathname1, zipgroup), (pathname2, zipgroup), ... ))
                or
                (pathname1, pathname2, ..)
                when zipgroups are implied to be equal to the file extension.
        """
        sources = []
        srid = self.get_srid()
        for filename in filenames:  
            zipgroup = None
            if type(filename) == list:
                filename, zipgroup = filename
                
            if not os.path.isfile(filename):
                raise Exception('Path %s does not exist.' % filename)

            src = FileSource(filename)
            if not zipgroup:
                src.zipgroup = src.extension

            sources.append(src)

        self.dataset.sources_ref = sources



