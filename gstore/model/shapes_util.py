# Geometry related helper functions

from osgeo import osr as osr
from osgeo import ogr as ogr
from osgeo import gdal as gdal

def reproject_geom(geom, from_sr, to_sr):
    """
    geom: ogr.Geometry instance in write mode
    from_sr: osr.SpatialReference instance
    to_sr: osr.SpatialReference instance
    """
    geom.AssignSpatialReference(from_sr)
    if not geom.TransformTo(to_sr):
        return 0 
    else:
        return (1, 'Could not reproject geometry reference')

def transform_to(geom, from_epsg, to_epsg):
    sf = osr.SpatialReference()
    sf.ImportFromEPSG(from_epsg)
    st = osr.SpatialReference()
    st.ImportFromEPSG(to_epsg)
    reproject_geom(geom, sf, st)

def bbox_to_polygon(env): 
    """ 
    # bbox The geometry's (minX, minY, maxX, maxY) bounding box. 
    """ 

    wkt = """POLYGON((%(minx)s %(miny)s,%(minx)s %(maxy)s,%(maxx)s %(maxy)s,%(maxx)s %(miny)s,%(minx)s %(miny)s))""" % { 'minx': env[0], 'miny': env[1], 'maxx': env[2], 'maxy': env[3]}

    return ogr.CreateGeometryFromWkt(wkt)

# Adapted from Ribot's gdaltools.py 
def transform_bbox(inbbox, s_srs, t_srs): 
    """returns a bbox (array of 4 doubles) transformed from the source srs (s_srs) 
       to the target srs (t_srs). 
    """ 
    bbox = [0.0, 0.0, 0.0, 0.0] 
 
    if inbbox is None or s_srs is None or t_srs is None : 
        raise Exception, "Invalid input parameters" 
 
    osrError = 0 
    sourceSRS = osr.SpatialReference() 
    osrError = sourceSRS.ImportFromEPSG(s_srs) 
 
    if osrError > 0 : 
        raise Exception, gdal.GetLastErrorMsg() 
    targetSRS = osr.SpatialReference() 
 
    osrError = targetSRS.ImportFromEPSG(t_srs) 
 
    if osrError > 0 : 
        raise Exception, gdal.GetLastErrorMsg() 
 
    coordTrans = osr.CoordinateTransformation(sourceSRS, targetSRS) 
 
    (bbox[0], bbox[1], z) =  coordTrans.TransformPoint(inbbox[0], inbbox[1]) # min x min y 
    (bbox[2], bbox[3], z) =  coordTrans.TransformPoint(inbbox[2], inbbox[3]) # max x max y 
 
    return bbox 
