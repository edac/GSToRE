# Geometry related helper functions
import re

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

# shamelessly copied from geoalchemy which shamelessly copied from feature server 
def to_geojson(geom, id = None, properties = {}, as_feature = True):
    """Converts from ANY WKT geom to a GeoJSON-like geometry.
    If geom is not given we proceed with a lazy load of the instance properties.
    We drop dependency with geojson module.
    """
    wkt_linestring_match = re.compile(r'\(([^()]+)\)')
    re_space             = re.compile(r"\s+")

    coords = []
    wkt = geom 

    for line in wkt_linestring_match.findall(wkt):
        rings = [[]]
        for pair in line.split(","):

            if not pair.strip():
                rings.append([])
                continue
            rings[-1].append(map(float, re.split(re_space, pair.strip())))

        coords.append(rings[0])

    if wkt.startswith("MULTIPOINT"):
        geomtype = "MultiPoint"
        coords = coords[0]
    elif wkt.startswith("POINT"):
        geomtype = "Point"
        if wkt != 'POINT EMPTY':
            coords = coords[0][0]
        else:
            coords = 'EMPTY'

    elif wkt.startswith("MULTILINESTRING"):
        geomtype = "MultiLineString"
    elif wkt.startswith("LINESTRING"):
        geomtype = "LineString"
        coords = coords[0]

    elif wkt.startswith("MULTIPOLYGON"):
        geomtype = "MultiPolygon"
    elif wkt.startswith("POLYGON"):
        geomtype = "Polygon"
    else:
        geomtype = wkt[:wkt.index["("]]
        raise Exception("Unsupported geometry type %s" % geomtype)

    if as_feature:
        ret = {'type' : 'Feature', 'geometry': {"type": geomtype, "coordinates": coords} }
    else:
        ret =  {"type": geomtype, "coordinates": coords}
    if properties:
        ret.update({'properties': properties})
    if id:
        ret.update({'id' : id})

    return ret

