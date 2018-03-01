from osgeo import ogr, osr
from datetime import datetime

from pyramid.wsgi import wsgiapp

from xml.sax.saxutils import escape, unescape

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
    ('geojson', 'GeoJSON', 'Geographic Javascript Object Notation'),
    ('sqlite', 'SQLite', 'SQLite/SpatiaLite'),
    ('georss', 'GeoRSS', 'GeoRSS')
]

#for metadata reference
_FORMATS = {
    "tif": "Tagged Image File Format (TIFF)",
    "sid": "Multi-resolution Seamless Image Database (MrSID)",
    "ecw": "ERDAS Compressed Wavelets (ecw)",
    "img": "ERDAS Imagine (img)",
    "zip": "ZIP",
    "shp": "ESRI Shapefile (shp)",
    "kml": "KML",
    "gml": "GML",
    "geojson": "GeoJSON",
    "json": "JSON",
    "csv": "Comma Separated Values (csv)",
    "xls": "MS Excel format (xls)",
    "xlsx": "MS Office Open XML Spreadsheet (xslx)",
    "pdf": "PDF",
    "doc": "MS Word format (doc)",
    "docx": "MS Office Open XML Document (docx)",
    "html": "HTML",
    "txt": "Plain Text",
    "dem": "USGS ASCII DEM (dem)"
}

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

#get the metadata file format
def format_to_definition(format):
    return _FORMATS[format] if format in _FORMATS else 'Unknown'

def ogr_to_postgis(ogrgeom):
    pass


def ogr_to_kml_fieldtype(ogr_type):
    if ogr_type in [ogr.OFTInteger]:
        return 'int'
    elif ogr_type in [ogr.OFTReal]:
        return 'double'
    else:
        return 'string'
    pass

def encode_as_ascii(s):
    return ('%s' % s).encode('ascii', 'xmlcharrefreplace')

#convert to python by ogr_type
#probably want to make sure it's not null and not nodata (as defined by the attribute)
#and convert to str before encoding in case it is nodata
def convert_by_ogrtype(value, ogr_type, fmt='', datefmt=''):
    '''
    
    '''
    if not value:
        #do nothing
        return ''
    if ogr_type == ogr.OFTInteger:
        try :
            return int(value)
        except:
            #return value
            pass
    if ogr_type == ogr.OFTReal:
        try:
            return float(value)
        except:
            #return value
            pass
    if ogr_type == ogr.OFTDateTime:
        #no expected format, no datetime
#        if not datefmt:
#            return value
        if datefmt:
            try:
                #try to parse
                return datetime.strptime(value, datefmt)
            except:
                #return value
                pass
    #it's just a string
    #value = '%s' % value
    value = encode_as_ascii(value) if fmt in ['kml', 'gml', 'csv'] else value.encode('utf-8')
    
    #and do one last check for kml, gml & ampersands
    value = escape(unescape(value)) if fmt in ['kml', 'gml'] else value
    
    #wrap the string in double-quotes if it's a csv 
    #TODO: change the csv handling to something else
    value = '"%s"' % value if fmt in ['csv'] and ',' in value else value
    return value

'''
transformations & reprojections
'''
#epsg to spatial reference
def epsg_to_sr(epsg):
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(epsg)
    return sr

#extent as string to float array
#ASSUME: box = minx,miny,maxx,maxy
def string_to_bbox(box):
    try:
        if isinstance(box, basestring):
            bbox = map(float, box.split(','))
        else:
            #try as a list of strings
            bbox = map(float, box)
    except:
        bbox = []
    return bbox

#bbox to wkt
def bbox_to_wkt(bbox):
    return """POLYGON((%(minx)s %(miny)s,%(minx)s %(maxy)s,%(maxx)s %(maxy)s,%(maxx)s %(miny)s,%(minx)s %(miny)s))""" % { 'minx': bbox[0], 'miny': bbox[1], 'maxx': bbox[2], 'maxy': bbox[3]}

#geom to wkt
def geom_to_wkt(geom, srid=''):
    wkt = geom.ExportToWkt()
    wkt = 'SRID=%s;%s' % (srid, wkt) if srid else wkt
    return wkt

#calculate the geom's wkb size as mb (mongoimport limited to 4mb for our install)
def check_wkb_size(wkb):
    return len(wkb.encode('utf-8')) *  0.0000009536743

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

#wkt to geom 
def wkt_to_geom(wkt, epsg):
    #get the spatial reference
    sr = epsg_to_sr(epsg)
    #create geometry
    geom = ogr.CreateGeometryFromWkt(wkt, sr)

    return geom

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

#geom to bbox
def geom_to_bbox(geom):
    #should already be in the desired projection
    env = geom.GetEnvelope()
    #reorder the pieces to match our bbox and return
    return [env[0], env[2], env[1], env[3]]
    
#geom to wkb
def geom_to_wkb(geom):
    return geom.ExportToWkb().encode('hex')

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

#convert the wkb string to an ogr string (kml, gml, geojson)
def wkb_to_output(wkb, epsg, output_type='kml'):
    #convert wkb to geom
    geom = wkb_to_geom(wkb, epsg)
    
    #convert it to the right output type 
    #geojson, kml, or gml
    if output_type == 'kml':
        return geom.ExportToKML()
    elif output_type == 'gml':
        return geom.ExportToGML()
    elif output_type == 'geojson':
        return geom.ExportToJson()
    else:
        return ''

'''
any other bbox stuff
'''

'''
extent methods
'''

def check_for_valid_extent(bbox):
    '''
    check the dataset bounding box to see if the extent
    has no area, i.e. it is the extent of a single point
    where minx, miny == maxx, maxy

    this is not considered a valid extent in mapscript/mapserver
    '''
    return 0.0 < ((bbox[2] - bbox[0]) * (bbox[3] - bbox[1]))

def buffer_point_extent(bbox, radius):
    '''
    build a buffer for the point extent situation
    where the point is the same for minx, miny or maxx, maxy
    and return in the same order as a standard gstore bbox (minx, miny, maxx, maxy)

    bbox should be in wgs84 (gstore default)
    '''
    point = ogr.CreateGeometryFromWkt('POINT (%s %s)' % (bbox[0], bbox[1]))
    buf = point.Buffer(radius, 30)
    env = buf.GetEnvelope()
    return env[0], env[2], env[1], env[3]


