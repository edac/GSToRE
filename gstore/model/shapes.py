import meta

from pylons import config

from sqlalchemy import *
from sqlalchemy.sql import func, text
from hstore import HStore, HStoreColumn

import os
import re
import StringIO
import commands
import codecs
import xlwt, csv
from decimal import Decimal
import datetime

from osgeo import osr
from osgeo import ogr

import zlib
import zipfile, tempfile

from geoutils import *

SRID = int(config.get('SRID', 4326))
YIELD_PER_ROWS = int(config.get('YIELD_PER_ROWS', 100))

__ALL__ = ['VectorDataset', 'ShapesAttribute', 'ShapesVector',
           'CreateVectorDataset', 'spatial_ref_sys', 'shapes_table', 'shapes_attributes']

def decode_field(str, decoder):
    try: 
        value = decoder(str)[0]
    except UnicodeDecodeError:
        raise Exception('Can not convert DBF data into Unicode \
                        from the specified encoding')
    return value

def value_to_string(v):
    if type(v) == int:
        return str(v)
    if type(v) == float:
        return str(Decimal(str(float(v))))
    if isinstance(v, basestring):
        st = v 
        try:
            st = v.decode('latin1').encode('utf8')
        except: 
            pass
        return st

    return None


def unzip(file_input_path, output_path = 'tmp'):
	"""
	Unzip a file.
	
	USAGE:
	>>> unzip('my.zip','/tmp')
	"""
	return not os.system('unzip -j -o %s -d %s' % (file_input_path, output_path))

def get_decoder(encoding):
    if encoding:
        decoder = codecs.getdecoder(encoding)
    else:
        # Fallback into DOS encoding
        decoder = codecs.getdecoder('cp437')
    return decoder

# OGR Dictionaries / constant conversions
def pythontype_to_ogrtype(value):
    if type(value) == int:
        return ogr.OFTInteger
    elif type(value) == float:
        return ogr.OFTReal
    elif isinstance(value, basestring):
        return ogr.OFTString
    elif isinstance(value, datetime.datetime):
        return ogr.OFTDateTime
    else:
        return ogr.OFTString

def pgtype_to_ogrtype(pgtype):
    types = {
        'integer' : ogr.OFTInteger,
        'integer list' : ogr.OFTIntegerList,
        'double precision' : ogr.OFTReal,
        'double precision list' : ogr.OFTRealList,
        'varchar' : ogr.OFTString,
        'varchar list' : ogr.OFTStringList,
        'text' : ogr.OFTWideString,
        'text list' : ogr.OFTWideStringList,
        'bytea' : ogr.OFTBinary,
        'date' : ogr.OFTDate,
        'time' : ogr.OFTTime,
        'datetime' : ogr.OFTDateTime
    }
    return types.get(pgtype, ogr.OFTString)

def ogrtype_to_pgtype(ogrtype):
    types = {   
        ogr.OFTInteger: 'integer',
        ogr.OFTIntegerList: 'integer list',
        ogr.OFTReal: 'double precision',
        ogr.OFTRealList: 'double precision list',
        ogr.OFTString: 'varchar',
        ogr.OFTStringList: 'varchar list',
        ogr.OFTWideString: 'text',
        ogr.OFTWideStringList: 'text list',
        ogr.OFTBinary: 'bytea',
        ogr.OFTDate: 'date',
        ogr.OFTTime: 'time',
        ogr.OFTDateTime: 'datetime'
    }
    return types.get(ogrtype, 'varchar')
        
def check_ogrtype(ogr_type):
    ogr_types = [ ogr.OFTInteger,ogr.OFTIntegerList, ogr.OFTReal,
        ogr.OFTRealList, ogr.OFTString, ogr.OFTWideString, ogr.OFTWideStringList,
        ogr.OFTBinary, ogr.OFTDate, ogr.OFTTime, ogr.OFTDateTime]
    if ogr_type in ogr_types:
        return True
    else: 
        return False

def check_ogrtype_value(ogr_type, value):
    if ogr_type != pythontype_to_ogrtype(value):
        return False
    else:
        return True
        
def postgistype_to_ogrwkbformat(postgistype):
    """
    Mapping of Postgis geometry types identifiers 
     to OGR format types
    """
    types = { 
        'POINT': ogr.wkbPoint,
        'LINESTRING': ogr.wkbLineString,
        'POLYGON': ogr.wkbPolygon,
        'MULTIPOINT': ogr.wkbMultiPoint,
        'MULTILINESTRING': ogr.wkbMultiLineString,
        'MULTIPOLYGON': ogr.wkbMultiPolygon,
        'GEOMETRYCOLLECTION': ogr.wkbGeometryCollection,
        '3D LINESTRING': ogr.wkbLineString25D,
        '3D MULTILINESTRING': ogr.wkbMultiLineString25D,
        '3D MULTIPOINT': ogr.wkbMultiPoint25D,
        '3D MULTIPOLYGON': ogr.wkbMultiPolygon25D,
        '3D POINT': ogr.wkbPoint25D,
        '3D POLYGON': ogr.wkbPolygon25D
    }
    return types.get(postgistype, ogr.wkbUnknown)

def ogrwkbformat_to_postgistype(ogrformat):
    types = {   
        ogr.wkbPoint : 'POINT',
        ogr.wkbLineString: 'LINESTRING',
        ogr.wkbPolygon: 'POLYGON',
        ogr.wkbMultiPoint: 'MULTIPOINT',
        ogr.wkbMultiLineString: 'MULTILINESTRING',
        ogr.wkbMultiPolygon: 'MULTIPOLYGON',
        ogr.wkbGeometryCollection: 'GEOMETRYCOLLECTION',
        ogr.wkbLineString25D: '3D LINESTRING',
        ogr.wkbMultiLineString25D: '3D MULTILINESTRING',
        ogr.wkbMultiPoint25D: '3D MULTIPOINT',
        ogr.wkbMultiPolygon25D: '3D MULTIPOLYGON',
        ogr.wkbPoint25D: '3D POINT',
        ogr.wkbPolygon25D: '3D POLYGON'
    }
    return types.get(ogrformat, 'UNKKOWN')
    

# Available vector formats
vector_formats = {
    'kml' : ['KML', 'Keyhole Markup Language'],
    'csv' : ['CSV', 'Comma Separated Value'],
    'gml' : ['GML', 'Geographic Markup Language'],
    'shp' : ['ESRI Shapefile', 'ESRI Shapefile'],
    'json' : ['GeoJSON', 'Geographic Javascript Object Notation'],
    'sqlite' : ['SQLite', 'SQLite/SpatiaLite'],
    'georss': ['GeoRSS', 'GeoRSS']
}

# SQLAlchemy Tables 
spatial_ref_sys = Table(
    'spatial_ref_sys', 
    meta.Base.metadata,
    Column('srid', Integer, primary_key=True),
    Column('auth_name', String),    
    Column('auth_srid', Integer),   
    Column('srtext', String),
    Column('proj4text', String)
) 

shapes_table = Table(
    'shapes',
    meta.Base.metadata,
    Column('gid', Integer, primary_key=True),
    Column('dataset_id', Integer, ForeignKey('datasets.id'), primary_key=True),
    HStoreColumn('values', HStore()),
    Column('geom', String),
    Column('time', TIMESTAMP)
)

shapes_attributes = Table(
    'features_attributes', 
    meta.Base.metadata,
    Column('id', Integer, primary_key = True),
    Column('dataset_id', Integer, ForeignKey('datasets.id')),
    Column('name', String),
    Column('array_id', Integer),
    Column('orig_name', String),
    Column('description', String),
    Column('attribute_type', Integer),
    Column('ogr_type', Integer),
    Column('ogr_justify', Integer), 
    Column('ogr_width', Integer),
    Column('ogr_precision', Integer)
)


class ShapesAttribute(object):
    def get_ogr_oft(self):
        return pgtype_to_ogrtype(self.attribute_type)  
    def get_ref(self, baseclass):
        if baseclass == ogr.FieldDefn:
            fd = ogr.FieldDefn()
            fd.SetName(self.name)
            fd.SetType(self.ogr_type)
            fd.SetWidth(self.ogr_width)
            fd.SetPrecision(self.ogr_precision)
            fd.SetJustify(self.ogr_justify)
            return fd
            
class ShapesVector(object):
   
    def get_definitions(self, classname = 'FeatureDefinitions'):
        if classname == 'FeatureDefinitions':
            definitions = FeatureDefinitions([])
            for attribute in self.properties:
                # skip geom attribute case
                if attribute.array_id != -1:
                    c = FeatureDefinition()
                    c.reset_from(attribute)
                    definitions.add(c)
                else:
                    continue
            return definitions

        elif classname == 'ogr.FeatureDefn':
            return self.get_definitions().get_ref(classname = 'ogr.FeatureDefn')
        elif classname == 'ogr.LayerDefn':
            return self.get_definitions(classname = 'ogr.FeatureDefn')

             

class FeatureDefinition(object):
    """
    Metadata rules for feature a attribute. Common interface switch among 
    different classes that share the same intrinsec pattern:
    
    - ShapesAttribute
    - ogr.FieldDefn
    
    """
    def __init__(self, **kw):
        
        self.name = kw.get('name')
        if kw.get('ogr_type'):
            if not check_ogrtype(kw.get('ogr_type')):
                raise Exception('ogr_type invalid OGR type')
        self.array_id = kw.get('array_id')
        if not kw.get('orig_name'):
            self.orig_name = kw.get('name')
        else:
            self.orig_name = kw.get('orig_name')
        self.description = kw.get('description')
        self.attribute_type = kw.get('attribute_type')
        self.ogr_type = kw.get('ogr_type') 
        self.ogr_justify = kw.get('ogr_justify')
        self.ogr_width = kw.get('ogr_width')
        self.ogr_precision = kw.get('ogr_precision')
        self.dataset_id = kw.get('dataset_id')

    def get(self, key):
        return self.__getattribute__(key)

    def reset_from(self, obj):
        if isinstance(obj, ogr.FieldDefn):
            self.name = obj.GetName()
            self.ogr_precision = obj.GetPrecision()
            self.ogr_justify = obj.GetJustify()
            self.ogr_width = obj.GetWidth()
            self.ogr_type = obj.GetType()
            self.attribute_type = ogrtype_to_pgtype(obj.GetType())
            return 0

        elif isinstance(obj, ShapesAttribute):
            self.ogr_type = obj.ogr_type
            self.attribute_type = ogrtype_to_pgtype(obj.ogr_type) 
            self.ogr_precision = obj.ogr_precision
            self.ogr_justify = obj.ogr_justify
            self.ogr_width = obj.ogr_width
            self.description = obj.description
            self.orig_name = obj.orig_name
            self.name = obj.name
            self.array_id = obj.array_id
            self.dataset_id = obj.dataset_id
            return 0
        else:
            raise Exception('obj not a valid class instance')

    def reset_for(self, obj):
        """
        Provide creation of instances of the above base class given for 
        interoperability.
        """
        if isinstance(obj , ogr.FieldDefn):
            obj.SetName(self.name)
            obj.SetPrecision(self.ogr_precision)
            obj.SetWidth(self.ogr_width)
            obj.SetJustify(self.ogr_justify)
            obj.SetType(self.ogr_type)   
            return 0
    
        elif isinstance(obj, ShapesAttribute):
            obj.name = self.name
            obj.orig_name = self.orig_name
            obj.description = self.description  
            obj.attribute_type = self.attribute_type 
            obj.ogr_type = self.ogr_type
            obj.ogr_precision = self.ogr_precision
            obj.ogr_justify = self.ogr_justify
            obj.ogr_width = self.ogr_width
            obj.dataset_id = self.dataset_id
            obj.array_id = self.array_id
            return 0       
 
    def as_dict(self):
        return self.__dict__
   

class FeatureDefinitions(object):
    """
    This is a mere list containing FeatureDefinition objects.
    """
    def __init__(self, container):
        if not getattr(container, '__iter__', False):
            raise Exception("Container is not iterable") 
        self.container = container

    def add(self,fd):
        if not isinstance(fd, FeatureDefinition):
            raise Exception('f not an instance of FeatureDefinition')
        
        fd.array_id = len(self.container) + 1 
        self.container.append(fd)
    
    def remove(self, f):
        if not isinstance(f, FeatureDefinition):
            raise Exception('f not an instance of FeatureDefinition')
        try:
            self.container.remove(f)
            # update the array_id's 
            for i in range(len(self.container)):
                self.container[i].array_id = i + 1
        except ValueError:
            raise Exception('f instance is not in container')

        return 0 
   
    def reset_from(self, obj):
        self.container = []
 
        if isinstance(obj, ogr.FeatureDefn):
            for i in range(obj.GetFieldCount()):
                fd = FeatureDefinition()
                fd.reset_from(obj.GetFieldDefn(i))
                self.add(fd) 
        else:   
            for att in obj: 
                if isinstance(att, ShapesAttribute):
                    fd = FeatureDefinition()
                    fd.reset_from(att)
                    self.add(fd)            
        return 0
                
    def as_dict(self):
        return zip([(f.name, f) for f in self.container])

    def __str__(self):
        return str(self.container)

    def __repr__(self):
        return str(self.container)    
                      
    def __getitem__(self, key):
        return self.container[key]

    def __len__(self):
        return len(self.container)    


def shapes_to_ogrfeature(shape_vector, set_geom = True):
    """
    shape_vector: ShapesVector instance
    feature_definition: ogr.FeatureDefn instance
    """
    if isinstance(shape_vector, ShapesVector):
        feature_definition = shapes_vector.get_definitions(classname = 'ogr.FeatureDefn')
        new_feature = ogr.Feature(feature_definition)
        if set_geom:
            geom = ogr.CreateGeometryFromWkb(shape.geom.wkb)
            new_feature.SetGeometry(geom)
            geom.Destroy()
            new_feature.SetFID(shape.gid)
        for field in fields:
            new_feature.SetField(field.GetName(), shape.values[i])

        return new_feature

    else:
        raise Exception('Argument not a ShapeVector instance')

            
def ogrfeature_to_shapes(ogrfeature, decoder, properties, attach_properties = True):
    """
    ogrfeature: instance of ogr.Feature
    decoder: instance method of codecs.getdecoder(encoding)
    properties: instance of ShapesAttribute
    attach_properties: bool 

    DESCRIPTION:
        We do fast string extraction with GetFieldAsString as unicode string.
        We could have used the following but it is slow plus we want to 
        recover the exact same string back into its original OGR type, from the string 
        that OGR originally produced.
    NOTES:
        - Admin client code should provide the right encoding of the DBF file 
        otherwise the data will be saved with garbled or non printable characters.
        - Make sure the client code sets the dataset_id value later if attach_properties 
        is true.
    """
    s = ShapesVector()
    s.geom = ogrfeature.GetGeometryRef().ExportToWkb().encode('hex')
    s.gid = ogrfeature.GetFID()

    s.values = {}
 
    for att in properties:
        str = ogrfeature.GetFieldAsString(att.array_id - 1)
        #s.values[att.name] = decode_field(str, decoder)
        s.values[att.name] = str 

    if attach_properties:
        s.properties.extend(properties)

    return s


def PromoteVectorDatasetFromShapefile(sourcepath, dataset, session, load_data = True, **kw):
    # Open source as read only
    filename = str(sourcepath) # filename may be unicode string
    datasource = ogr.Open(filename)
    if not datasource:
        return (1, 'Not a valid OGR data source')

    L = datasource.GetLayer(0)

    #session.begin(subtransactions = True, nested = True)

    # The rule here is to set basename to lowercase
    if not kw.get('basename'):
        basename = L.GetName().lower()
    else:
        basename = kw.get('basename')

    if kw.has_key('basename'):
        dataset.basename = kw['basename']
    else:
        dataset.basename = basename

    # Proj file
    if kw.has_key('orig_epsg'):
        dataset.orig_epsg = kw.get('orig_epsg')
    else:
        # get it from the prj file that must be there
        prjfile = None
        if os.path.isdir(filename):
            (o, prjfile) = commands.getstatusoutput("find %s -name '*.prj'" % filename)
        else:
            # Try basename.shp -> basename.prj
            prjfile = filename.split('.shp')[0] + '.prj'
             
        if prjfile and not os.path.isfile(prjfile):
            return (2, 'No prj file present')

        dataset.orig_epsg = kw.get('orig_epsg') 

        if dataset.orig_epsg == -1:
            return (3, 'Projection not present')
 
    decoder = get_decoder(kw.get('encoding'))    

    # Check database constraints here, if you add dataset to the 
    # session without respecting these it will rollback.
    dataset.taxonomy = 'vector'
    dataset.theme = kw.get('theme')
    dataset.subtheme = kw.get('subtheme')
    dataset.groupname = kw.get('groupname')
    formats = kw.get('formats').split(',')
    if kw.has_key('formats'):
        dataset.formats = kw.get('formats')
    else:
        for format in vector_formats.keys(): 
            if format not in formats:
                formats.append(format)
        dataset.formats = ','.join(formats)
    dataset.metadata_xml = kw.get('metadata_xml')

    dataset.mapfile_template_id = kw.get('mapfile_template_id')

    # Map field definitions
    dataset.feature_count = L.GetFeatureCount()

    if dataset.feature_count == 0:
        return (4, 'No features in shapefile')

    # Assemble the dataset.shapes.properties or, the same, dataset.attributes_ref.
    FD = FeatureDefinitions([])
    FD.reset_from(L.GetLayerDefn())

    properties = []
    
    for fd in FD:
        sa = ShapesAttribute()
        fd.reset_for(sa)
        properties.append(sa)
    if len(properties) == 0:
        return (2, 'No valid attributes')

    # See if we need to reproject. We always store in geographic projection
    sr = osr.SpatialReference()
    sr.SetWellKnownGeogCS('WGS84')

    if dataset.orig_epsg != SRID:
        orig_sr = osr.SpatialReference()
        orig_sr.ImportFromEPSG(dataset.orig_epsg)
    else:
        orig_sr = None

    # Update footprint
    # MinX, MaxX, MinY, MaxY  OGREnvelope
    env = L.GetExtent()
    # (minX, minY, maxX, maxY) = bbox
    bbox = (env[0], env[2], env[1], env[3])
    if orig_sr is not None:
        dataset.geom = bbox_to_polygon(transform_bbox(bbox, dataset.orig_epsg, SRID)).ExportToWkb().encode('hex')
    else:
        dataset.geom = bbox_to_polygon(bbox).ExportToWkb().encode('hex')
    
    # Map features
    shapes = []
    geomtype = L.GetLayerDefn().GetGeomType()
    if load_data:
        for i in range(dataset.feature_count):
            of = L.GetFeature(i)
            if orig_sr is not None:
                nf = of.Clone()
                geom = nf.GetGeometryRef()
                if reproject_geom(geom, orig_sr, sr):
                    raise Exception('Can not reproject geometry to WGS84')
                shapes.append(ogrfeature_to_shapes(nf, decoder, properties))
                nf.Destroy()
            else:
                geom = of.GetGeometryRef()
                shapes.append(ogrfeature_to_shapes(of, decoder, properties))
            
            of.Destroy()
    
    # Update important dataset attributes
    dataset.geomtype = ogrwkbformat_to_postgistype(geomtype)
   
    for shp in shapes:
        shp.dataset_id = dataset.id
        session.add(shp)
    for prp in properties:
        prp.dataset_id = dataset.id
        session.add(prp)
 
    dataset.shapes = shapes
    dataset.attributes_ref = properties

    datasource.Destroy()

    session.commit()
    session.expunge(dataset)
    for shp in shapes:
        session.expunge(shp)
    for prp in properties:
        session.expunge(prp)
    session.remove()

    return (0, '')
    
def PromoteVectorDatasetFromKml(sourcepath, dataset, session, load_data = True, **kw):
    # Open source as read only
    filename = str(sourcepath) # filename may be unicode string
    datasource = ogr.Open(filename)
    if not datasource:
        return (1, 'Not a valid OGR data source')

    L = datasource.GetLayer(0)

    session.begin(subtransactions = True, nested = True)

    # The rule here is to set basename to lowercase
    if not kw.get('basename'):
        basename = L.GetName().lower()
    else:
        basename = kw.get('basename')

    if kw.has_key('basename'):
        dataset.basename = kw['basename']
    else:
        dataset.basename = basename

    # Proj file
    if kw.has_key('orig_epsg'):
        dataset.orig_epsg = kw.get('orig_epsg')
    else:
        # get it from the prj file that must be there
        prjfile = None
        if os.path.isdir(filename):
            (o, prjfile) = commands.getstatusoutput("find %s -name '*.prj'" % filename)
        else:
            # Try basename.shp -> basename.prj
            prjfile = filename.split('.shp')[0] + '.prj'
             
        if prjfile and not os.path.isfile(prjfile):
            return (2, 'No prj file present')

        dataset.orig_epsg = kw.get('orig_epsg') 

        if dataset.orig_epsg == -1:
            return (3, 'Projection not present')
 
    decoder = get_decoder(kw.get('encoding'))    

    # Check database constraints here, if you add dataset to the 
    # session without respecting these it will rollback.
    dataset.taxonomy = 'vector'
    dataset.theme = kw.get('theme')
    dataset.subtheme = kw.get('subtheme')
    dataset.groupname = kw.get('groupname')
    formats = kw.get('formats').split(',')
    if kw.has_key('formats'):
        dataset.formats = kw.get('formats')
    else:
        for format in vector_formats.keys(): 
            if format not in formats:
                formats.append(format)
        dataset.formats = ','.join(formats)
    dataset.metadata_xml = kw.get('metadata_xml')

    dataset.mapfile_template_id = kw.get('mapfile_template_id')

    # Map field definitions
    dataset.feature_count = L.GetFeatureCount()

    if dataset.feature_count == 0:
        return (4, 'No features in shapefile')

    if kw.has_key('inactive'):
        dataset.inactive = kw.get('inactive')

    # Assemble the dataset.shapes.properties or, the same, dataset.attributes_ref.
    FD = FeatureDefinitions([])
    FD.reset_from(L.GetLayerDefn())

    properties = []
    
    for fd in FD:
        sa = ShapesAttribute()
        fd.reset_for(sa)
        properties.append(sa)
    if len(properties) == 0:
        return (2, 'No valid attributes')

    # See if we need to reproject. We always store in geographic projection
    sr = osr.SpatialReference()
    sr.SetWellKnownGeogCS('WGS84')

    if dataset.orig_epsg != SRID:
        orig_sr = osr.SpatialReference()
        orig_sr.ImportFromEPSG(dataset.orig_epsg)
    else:
        orig_sr = None

    # Update footprint
    # MinX, MaxX, MinY, MaxY  OGREnvelope
    env = L.GetExtent()
    # (minX, minY, maxX, maxY) = bbox
    bbox = (env[0], env[2], env[1], env[3])
    if orig_sr is not None:
        dataset.geom = bbox_to_polygon(transform_bbox(bbox, dataset.orig_epsg, SRID)).ExportToWkb().encode('hex')
    else:
        dataset.geom = bbox_to_polygon(bbox).ExportToWkb().encode('hex')
    
    # Map features
    shapes = []
    geomtype = L.GetLayerDefn().GetGeomType()
    if load_data:
        for i in range(dataset.feature_count):
            of = L.GetFeature(i)
            if orig_sr is not None:
                nf = of.Clone()
                geom = nf.GetGeometryRef()
                if reproject_geom(geom, orig_sr, sr):
                    raise Exception('Can not reproject geometry to WGS84')
                shapes.append(ogrfeature_to_shapes(nf, decoder, properties))
                nf.Destroy()
            else:
                geom = of.GetGeometryRef()
                shapes.append(ogrfeature_to_shapes(of, decoder, properties))
            
            of.Destroy()
    
    # Update important dataset attributes
    dataset.geomtype = ogrwkbformat_to_postgistype(geomtype)
   
    for shp in shapes:
        shp.dataset_id = dataset.id
        session.add(shp)
    for prp in properties:
        prp.dataset_id = dataset.id
        session.add(prp)
 
    dataset.shapes = shapes
    dataset.attributes_ref = properties

    datasource.Destroy()

    session.commit()
    session.expunge(dataset)
    for shp in shapes:
        session.expunge(shp)
    for prp in properties:
        session.expunge(prp)
    session.remove()

    return (0, '')
 
class VectorDataset(object):
    is_mappable = True

    def __init__(self, dataset, session, config):
        if dataset.taxonomy != 'vector':
            raise Exception('Dataset not vector compatible')
        self.Session = session
        self.config = config
        self.dataset = dataset
        self.projection = osr.SpatialReference()
        self.projection.SetWellKnownGeogCS('WGS84')
        self.geomtype = dataset.geomtype
 
        self.basename = str(dataset.basename)
        self.shapefile = "%s/%s/shp/%s.shp" % (config.get('FORMATS_PATH'), dataset.id, dataset.basename)

        if self.dataset.orig_epsg:
            self.orig_projection = osr.SpatialReference()
            if not self.orig_projection.ImportFromEPSG(dataset.orig_epsg):
                self.orig_projection = None
         
        self.properties_ref = self.dataset.attributes_ref

    def get_extent(self):
        return self.dataset.geom.wkt
    
    def get_ogr_wkb_format(self):
        return postgistype_to_ogrwkbformat(self.dataset.geomtype)

    def get_ogr_vector_driver(self, format):
        """
        Map web downloadable extension (formats) to OGR vector layer driver formats.
        See http://www.gdal.org/ogr/ogr_formats.html
        Notice 'xls' : ['XLS', 'Microsoft Excel Spreadsheet'] is not a supported format.
        """        
        if format in vector_formats.keys():
            return vector_formats[format]
        else:
            return [None, None]       

    def get_index(self):
        return """CREATE INDEX shapes_%(id)s_idx ON shapes(gid, dataset_id) WHERE dataset_id = %(id)s""" %{
            'id' : self.dataset.id,
        }


    def get_fields(self, format):
        """Create vector file fields iterating over Dataset.attributes_ref """
        fields = range(len(self.properties_ref))
        for att in self.properties_ref:
            if att.array_id != -1:
                fields[att.array_id - 1] = ogr.FieldDefn(str(att.name), att.ogr_type)
                if format not in ['csv', 'kml', 'gml']:
                    if att.ogr_justify:
                        fields[att.array_id - 1].SetJustify(att.ogr_justify)
                    if att.ogr_width:
                        fields[att.array_id - 1].SetWidth(att.ogr_width)
                    if att.ogr_precision:
                        fields[att.array_id - 1].SetPrecision(att.ogr_precision)
        return fields

    def shp2sql(self, destination_filename, source = 'source', dump = True, encoding = 'utf-8'):
        """
        Dump contents of vector dataset from a shapefile to a sql dump file.
        source : The source shapefile that can be: 
                - source, for the original shapefile stored in the Resource object
                - cached, for the shapefile transformed into geographic saved in the cache
                - database, for iterating over the Shapes table keyed by self.dataset.id
        """

        # Copied from gstore/model/hstore.py
        def serialize_hstore(val):
            """
            Serialize a dictionary into an hstore literal. Keys and values must both be
            strings.
            """
            def esc(s, position):
                try:
                    return s.encode('string_escape').replace('"', r'\"')
                except AttributeError:
                    raise ValueError("%r in %s position is not a string." %
                            (s, position))
            return ', '.join( '"%s"=>"%s"' % (esc(k, 'key'), esc(v, 'value'))
                    for k, v in val.iteritems() )
        # end copy
    
        dest = open(destination_filename, 'w')
        dest.write('BEGIN;\n')
        dest.write('COPY shapes (gid, dataset_id, geom, values) FROM stdin; \n')

        if source == 'database':
            for shape in self.dataset.shapes.yield_per(YIELD_PER_ROWS):
                values = serialize_hstore(shape.values)
                dest.write('\t'.join([ str(shape.gid), str(self.dataset.id), shape.geom, values]) + '\n') 

        elif source == 'cached':    
            pass
        elif source == 'source':

            # The source is always a zipped shapefile and is the first entry in the sources_ref list
            temp_basepath =  tempfile.mkdtemp()
            zipped_shapefile = self.dataset.sources_ref[0].location
            unzip(zipped_shapefile, temp_basepath)       
            decoder = get_decoder(encoding)    
    
            D = ogr.Open(temp_basepath) 
            L = D.GetLayerByIndex(0)
            S = L.GetSpatialRef()

            for i in range(0, L.GetFeatureCount()):
                ftr = L.GetNextFeature()
                geom = ftr.GetGeometryRef().Clone()
                if self.dataset.orig_epsg != SRID:    
                    reproject_geom(geom, S, self.projection) 
                values = {}
                for att in self.properties_ref:
                    value =  ftr.GetFieldAsString(att.array_id - 1).decode(encoding).encode('utf8')
                    if not value:
                        value = 'NULL'
                        # actually skip this key=>value
                        continue
                    values[str(att.name)] = value

                dest.write('\t'.join([ str(ftr.GetFID()), str(self.dataset.id), geom.ExportToWkb().encode('hex'), serialize_hstore(values)]) + '\n') 
            D.Destroy()

        dest.write('\.\n\nCOMMIT;')
        dest.close()

        return 0                

    def write_vector_format(self, format, basepath):
        if os.path.abspath(basepath) != basepath:
            return (1, 'Please provide a valid absolute path.')
        if not os.path.isdir(basepath):
            return (1, '%s is not a valid path.' % basepath)
        if format == 'xls':
            basepath = os.path.join(basepath, str(self.dataset.id))
            if not os.path.isdir(basepath):
                os.mkdir(basepath)
            basepath = os.path.join(basepath, 'xls')
            if not os.path.isdir(basepath):
                os.mkdir(basepath)
            return self.write_xls_format(basepath)
        elif not self.get_ogr_vector_driver(format)[0]:
            return (1, '%s is not a valid vector format.' % format)

        driver = ogr.GetDriverByName(self.get_ogr_vector_driver(format)[0])
        if driver is None:
            return (1, 'No OGR driver available for the given format')

        temp_basepath =  tempfile.mkdtemp()
        if format != 'shp':
            temp_filename = os.path.join(temp_basepath, '%s.%s' % (self.basename, format))
        else:
            temp_filename = temp_basepath

        vectorfile = driver.CreateDataSource(str(temp_filename))
        prj4 = self.Session.query(spatial_ref_sys.c.proj4text).filter(spatial_ref_sys.c.srid == SRID).first()
        spatialReference = osr.SpatialReference()
        spatialReference.SetFromUserInput(str(prj4.proj4text))

        L = vectorfile.CreateLayer(self.basename, spatialReference, self.get_ogr_wkb_format())

        fields = self.get_fields(format)
        for field in fields:
            L.CreateField(field)
        
        for shape in self.dataset.shapes: 
            new_feature = ogr.Feature(L.GetLayerDefn())
            if format not in ['csv']:
                geom = ogr.CreateGeometryFromWkb(shape.geom.decode('hex'))
                new_feature.SetGeometry(geom)
                geom.Destroy()
            new_feature.SetFID(shape.gid)
            for field in fields:
                new_feature.SetField(field.GetName(), shape.values.get(field.GetName()))
            L.CreateFeature(new_feature)
            new_feature.Destroy()

        self.Session.expunge_all()
        # Since cached shapefiles are MapServer data sources we create a spatial index, (.qix) file
        if format == 'shp':
            vectorfile.ExecuteSQL('CREATE SPATIAL INDEX ON %s '% self.basename)

        # flush and write to disk
        vectorfile.Destroy()
        L = None

        # add proj and metadata files!
        if format == 'shp':
            prj_file = open('%s.prj' %  os.path.join(temp_basepath, self.basename), 'w')
            spatialReference.MorphToESRI()
            prj_file.write(spatialReference.ExportToWkt())
            prj_file.close()

            if self.dataset.metadata_xml:
                mtd = str(self.dataset.metadata_xml.encode('utf8'))
                mtd_file = open('%s.shp.xml' % os.path.join(temp_basepath, self.basename), 'w')
                mtd_file.write(mtd)
                mtd_file.close()
    
        # append drop temp files, basepath should always have a final directory of name self.basename
        if os.path.split(basepath)[1] != str(self.dataset.id):
            basepath  = os.path.join(basepath, str(self.dataset.id))
            if not os.path.isdir(basepath):
                os.mkdir(basepath)  
        basepath = os.path.join(basepath, format)
        if not os.path.isdir(basepath):
            os.mkdir(basepath)

        # cache in a zip file (kmz if format is kml) 
        filename = os.path.join(basepath, self.dataset.get_filename(format, compressed = True))

        def dropzip(args, dirname, fnames):
            filename, basepath = args
            zipped = zipfile.ZipFile(filename, 'w')
            for file in fnames:
                if os.path.isdir(os.path.join(dirname,file)):
                    continue
                tmpfile = open(os.path.join(dirname,file), 'r')
                dropfilename = os.path.join(basepath, file)
                dropfile = open(dropfilename, 'w')
                dropfile.write(tmpfile.read())
                tmpfile.close()
                os.unlink(os.path.join(dirname, file))
                dropfile.close()
                if not file.endswith('.qix'): # don't include mapserver index file for shapefiles
                    zipped.write(dropfilename, '%s/%s' % (str(self.dataset.id), file), zipfile.ZIP_DEFLATED)

            zipped.close()

        os.path.walk(temp_basepath, dropzip, [filename, basepath])

        # Check for CRC errors
        zipped = zipfile.ZipFile(filename, 'r')
        failed = zipped.testzip()
        zipped.close()
        if failed:
            return (1, 'CRC check failure in file %s' % failed)
 
        # OGR's csv output creates an extra subdirectory, delete it
        if format == 'csv': 
            os.rmdir(os.path.join(temp_basepath, '%s.csv' % self.basename))
        os.rmdir(temp_basepath)

 
        return (0, 'Success')
    
    def write_xls_format(self, basepath):
        style = xlwt.easyxf('font: name Times New Roman, color-index black, bold on', num_format_str='#,##0.00')
        wb = xlwt.Workbook()
        sheetname = self.basename[0:29] # xlwt will see it as an invalid sheetname

        ws = wb.add_sheet(sheetname)
        y = 0
        x = 0 
        # Write header
        for att in self.properties_ref:
            if att.array_id != -1:
                ws.write(0, x, att.name, style = style)
                x += 1

        y = 1
        for row in self.dataset.shapes.values(ShapesVector.values):   
            x = 0
            for key in row.values:
                value = row.values.get(key)
                att = self.properties_ref[x] 
                try: 
                    if att.ogr_type ==  ogr.OFTReal:
                        if value:
                            value = Decimal(value.strip())
                        else:
                            value = None
                    elif att.ogr_type == ogr.OFTInteger:
                        if value:
                            value = int(float(value))
                        else:
                            value = None
                except:
                    pass
 
                ws.write(y,x, value)
                x += 1
            y += 1
            if y > 65535:
                break

        filename = self.dataset.get_filename('xls')
        wb.save(os.path.join(basepath, filename))

        self.Session.expunge_all()

        return (0, 'Success')

