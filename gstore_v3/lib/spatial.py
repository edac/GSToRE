from osgeo import ogr, osr
from datetime import datetime

'''
the ogr field constants
'''

#just to have the integer values explicitly defined somewhere
_FIELD_TYPES = [
    (ogr.OFTInteger, 'integer', 0),
    (ogr.OFTIntegerList, 'integer list', 1),
    (ogr.OFTReal, 'double precision', 2),
    (ogr.OFTRealList, 'double precision list', 3),
    (ogr.OFTString, 'varchar', 4),
    (ogr.OFTStringList, 'varchar list', 5),
    (ogr.OFTWideString, 'text', 6),
    (ogr.OFTWideStringList, 'text list', 7),
    (ogr.OFTBinary, 'bytea', 8),
    (ogr.OFTDate, 'date', 9),
    (ogr.OFTTime, 'time', 10),
    (ogr.OFTDateTime, 'datetime', 11)
]

_GEOM_TYPES = [
    ('POINT', ogr.wkbPoint),
    ('LINESTRING', ogr.wkbLineString),
    ('POLYGON', ogr.wkbPolygon),
    ('MULTIPOINT', ogr.wkbMultiPoint),
    ('MULTILINESTRING', ogr.wkbMultiLineString),
    ('MULTIPOLYGON', ogr.wkbMultiPolygon),
    ('GEOMETRYCOLLECTION', ogr.wkbGeometryCollection),
    ('3D LINESTRING', ogr.wkbLineString25D),
    ('3D MULTILINESTRING', ogr.wkbMultiLineString25D),
    ('3D MULTIPOINT', ogr.wkbMultiPoint25D),
    ('3D MULTIPOLYGON', ogr.wkbMultiPolygon25D),
    ('3D POINT', ogr.wkbPoint25D),
    ('3D POLYGON', ogr.wkbPolygon25D)
]

_FILE_TYPES = [
    ('kml', 'KML', 'Keyhole Markup Language'),
    ('csv', 'CSV', 'Comma Separated Value'),
    ('gml', 'GML', 'Geographic Markup Language'),
    ('shp', 'ESRI Shapefile', 'ESRI Shapefile'),
    ('json', 'GeoJSON', 'Geographic Javascript Object Notation'),
    ('sqlite', 'SQLite', 'SQLite/SpatiaLite'),
    ('georss', 'GeoRSS', 'GeoRSS')
]

#get the database type from the ogr type
def ogr_to_psql(ogr_type):
    t = [g[1] for g in _FIELD_TYPES if g[0] == ogr_type]
    t = t[0] if t else 'varchar'
    return t

#get the ogr type from the database type
def psql_to_ogr(psql):
    t = [g[0] for g in _FIELD_TYPES if g[1] == psql]
    t = t[0] if t else ogr.OFTString
    return t

#get the ogr-defined file type
def format_to_filetype(format):
    f = [f[1] for f in _FILE_TYPES if f[0] == format]
    f = f[0] if f else None
    return f

#return geometry type
def postgis_to_ogr(postgis):
    t = [g[1] for g in _GEOM_TYPES if g[0] == postgis]
    t = t[0] if t else ogr.wkbUnknown
    return t

def ogr_to_postgis(ogrgeom):
    pass



#convert to python by ogr_type
#probably want to make sure it's not null and not nodata (as defined by the attribute)
def convert_by_ogrtype(value, ogr_type, datefmt=''):
    if not value:
        #do nothing
        return value
    if ogr_type == ogr.OFTInteger:
        try :
            return int(value)
        except:
            return value
    if ogr_type == ogr.OFTReal:
        try:
            return float(value)
        except:
            return value
    if ogr_type == ogr.OFTDateTime:
        #no expected format, no datetime
        if not datefmt:
            return value
        try:
            #try to parse
            return datetime.strptime(value, datefmt)
        except:
            return value
    return value

'''
transformations & reprojections
'''
#epsg to spatial reference
def epsg_to_sr(epsg):
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(epsg)
    return sr

#bbox to wkt
def bbox_to_wkt(bbox):
    return """POLYGON((%(minx)s %(miny)s,%(minx)s %(maxy)s,%(maxx)s %(maxy)s,%(maxx)s %(miny)s,%(minx)s %(miny)s))""" % { 'minx': bbox[0], 'miny': bbox[1], 'maxx': bbox[2], 'maxy': bbox[3]}

'''
0103000000010000000500000098CD95A187FB5AC0D7A37287D47B414098CD95A187FB5AC04499A34D494443407BE60D79A93F5AC04499A34D494443407BE60D79A93F5AC0D7A37287D47B414098CD95A187FB5AC0D7A37287D47B4140
{-107.930153271,34.9674233732,-104.994718803,38.5334870385}

tests:
bbox = [-107.930153271,34.9674233732,-104.994718803,38.5334870385]
>>> spatial.bbox_to_wkb(bbox, 4326)
'00000000030000000100000005c05afb87a195cd9840417bd48772a3d7c05afb87a195cd98404344494da39944c05a3fa9790de67b404344494da39944c05a3fa9790de67b40417bd48772a3d7c05afb87a195cd9840417bd48772a3d7'
>>> spatial.wkb_to_bbox('0103000000010000000500000098CD95A187FB5AC0D7A37287D47B414098CD95A187FB5AC04499A34D494443407BE60D79A93F5AC04499A34D494443407BE60D79A93F5AC0D7A37287D47B414098CD95A187FB5AC0D7A37287D47B4140', 4326)
[-107.930153271, 34.9674233732, -104.994718803, 38.5334870385]
>>> spatial.wkb_to_bbox('00000000030000000100000005c05afb87a195cd9840417bd48772a3d7c05afb87a195cd98404344494da39944c05a3fa9790de67b404344494da39944c05a3fa9790de67b40417bd48772a3d7c05afb87a195cd9840417bd48772a3d7', 4326)
[-107.930153271, 34.9674233732, -104.994718803, 38.5334870385]

'''

#box to wkb
#bbox as float[] so transform first
#and epsg of the input
def bbox_to_wkb(bbox, epsg):
    #convert to wkt
    wkt = bbox_to_wkt(bbox)

    #get the spatial reference
    sr = epsg_to_sr(epsg)
    
    #create geometry
    geom = ogr.CreateGeometryFromWkt(wkt, sr)

    #export to hex-encoded wkb
    return geom.ExportToWkb().encode('hex')

#bbox to geometry
def bbox_to_geom(bbox, epsg):
    #convert to wkt
    wkt = bbox_to_wkt(bbox)

    #get the spatial reference
    sr = epsg_to_sr(epsg)
    
    #create geometry
    return ogr.CreateGeometryFromWkt(wkt, sr)

#wkb to box with epsg of the input geom
def wkb_to_bbox(wkb, epsg):
    #convert back to minx, miny, maxx, maxy

    #get the spatial reference
    sr = epsg_to_sr(epsg)

    #get the geometry
    geom = ogr.CreateGeometryFromWkb(wkb.decode('hex'), sr)

    #get the bbox from the extent
    env = geom.GetEnvelope()

    #reorder the pieces to match our bbox and return
    return [env[0], env[2], env[1], env[3]]

    
#wkb to ogr.geometry
def wkb_to_geom(wkb, epsg):
   #get the spatial reference
    sr = epsg_to_sr(epsg)

    #get the geometry
    return ogr.CreateGeometryFromWkb(wkb.decode('hex'), sr)

#reproject geometry from srs a to srs b
#and it doesn't matter if it's a bbox or not
def reproject_geom(geom, in_epsg, out_epsg):
    #get the spatial references
    in_sr = epsg_to_sr(in_epsg)
    out_sr = epsg_to_sr(out_epsg)

    #make sure the geom has the in_sr
    geom.AssignSpatialReference(in_sr)

    #reproject
    try:
        geom.TransformTo(out_sr)
    except OGRError as err:
        return None


'''
any other bbox stuff
'''

