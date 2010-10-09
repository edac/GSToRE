import meta

from pylons import config

from sqlalchemy import *
from sqlalchemy.orm import mapper, relation, column_property, synonym
from sqlalchemy.sql import func

from sqlalchemy.dialects.postgresql import ARRAY as ARRAY

from sqlalchemy.ext.declarative import declarative_base

import shapely
from postgis import GISColumn, Geometry, Spatial
from geobase import Dataset, spatial_ref_sys

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

from shapely.wkb import loads

from shapes_util import *

#Base = declarative_base()
Base = meta.Base

SRID = int(config.get('SRID', 4326))
FORMATS_PATH = config.get('FORMATS_PATH', '/tmp')

__ALL__ = ['VectorDataset', 'ShapesAttribute', 'CreateVectorDataset']

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

def clean_string(s):
    """Strip non printable characters from string."""
    return re.compile('[%s]' % re.escape(''.join(map(unichr, range(0,32) + range(127,160))))).sub('',s)

def extractall(zipped_file_path, destination_path):
    """
    zipped: ZipFile instance
    destination_path: Distination path in file system. It should exist.
    """
    src = open(zipped_file_path, 'rb')
    zipped = zipfile.ZipFile(src)
    # extract_all function new in Python 2.6 
    if hasattr(zipped, 'extract_all'):
        zipped.extractall(destination_path)
    else:
        # Adapted from http://stackoverflow.com/questions/339053/how-do-you-unzip-very-large-files-in-python/339506#339506 
        for m in zipped.infolist():
            # Examine the header
            # print m.filename, m.header_offset, m.compress_size, repr(m.extra), repr(m.comment)
            src.seek(m.header_offset)
            src.read(30) # Good to use struct to unpack this.
            nm = src.read(len(m.filename))
            if len(m.extra) > 0:
                ex = src.read(len(m.extra))
            if len(m.comment) > 0: 
                cm = src.read(len(m.comment)) 

            # Build a decompression object
            decomp = zlib.decompressobj(-15)

            # This can be done with a loop reading blocks
            out = open( os.path.join(destination_path, m.filename), "wb")
            result = decomp.decompress(src.read(m.compress_size))
            out.write(result)
            result = decomp.flush()
            out.write(result)
            # end of the loop
            out.close()

    zipped.close()
    src.close()

    return 0        

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
        
def dbgeomtype_to_ogrwkbformat(dbgeomtype):
    """
    Map database geometry string types to OGR format types
    select distinct(geomtype) from datasets;
      geomtype  
    ------------
     LINESTRING
     POINT
     POLYGON

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
    return types.get(dbgeomtype, ogr.wkbUnknown)

def ogrwkbformat_to_dbgeomtype(ogrformat):
    types = {   
        ogr.wkbPoint : 'POINT',
        ogr.wkbLineString: 'LINESTRING',
        ogr.wkbPolygon: 'POLYGON',
        ogr.wkbMultiPoint: 'MULTIPOINT',
        ogr.wkbMultiLineString: 'MULTILINESTRING',
        ogr.wkbMultiPolygon: 'MULTIPOLYGON',
        ogr.wkbGeometryCollection: 'GEOMETRYCOLLECTION',
        # Collapsing 2D to 3D for now
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

shapes_table = Table(
    'shapes',
    Base.metadata,
    Column('gid', Integer, primary_key=True),
    Column('dataset_id', Integer, ForeignKey(Dataset.id)),
    Column('values', String),
    Column('geom', String)
)

shapes_attributes = Table(
    'features_attributes', 
    Base.metadata,
    Column('id', Integer, primary_key = True),
    Column('dataset_id', Integer, ForeignKey(Dataset.id)),
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
            
mapper(ShapesAttribute, shapes_attributes,
    properties = {
        'dataset' : 
            relation(
                Dataset,
                primaryjoin = shapes_attributes.c.dataset_id == Dataset.id,
#                foreign_keys = [shapes_attributes.c.dataset_id],
                backref = 'attributes_ref',
                lazy = True
            )
    }
)


class ShapesVector(Spatial):
   
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

         
    def get_json_properties(self):
        properties = []
        if self.values:
            for c in self.get_definitions():
                properties.append({c.name: self.values[c.array_id-1]})
    
        return properties
             

mapper(
    ShapesVector, 
    shapes_table, 
    properties = {
        #'geom': GISColumn('geom', Geometry(2)),
        'geom': GISColumn('geom', Geometry()),
        'dataset': 
            relation(
                Dataset,
                primaryjoin = shapes_table.c.dataset_id == Dataset.id,
                foreign_keys = [shapes_table.c.dataset_id],
                backref = 'shapes',
                lazy = True
            )
        ,
        'properties': 
            relation(
                ShapesAttribute, 
                primaryjoin = shapes_table.c.dataset_id == shapes_attributes.c.dataset_id,
                foreign_keys = [shapes_attributes.c.dataset_id],
                lazy = True
            )
    }
)


class Feature(object):   
    """ Shapely interface"""
    def __init__(self, gid, geom={}, properties = None , dataset_id = None):
        """The geom *must* provide the geometry geo interface."""
        self.gid = gid
        self.geom = geom
        if dataset_id:
            self.dataset_id = dataset_id
        if properties:
            self.properties = properties
    
    @property
    def __geo_interface__(self):
        return {'type': 'Feature', 'gid': self.gid, 'properties': self.properties, 'geometry': self.geom}


class FeatureCollection(list):
    @property
    def __geo_interface__(self):    
       return {'type': 'FeatureCollection', 'features': self} 


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


def shapes_to_feature(shape_vector):
    if isinstance(shape_vector, ShapesVector):
        return Feature(shape_vector.gid,shape_vector.geom.__geo_interface__, shape_vector.get_json_properties())
    else:
        raise Exception('Argument not a ShapeVector instance')

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

    else:
        raise Exception('Argument not a ShapeVector instance')

    
    
def feature_to_shapes(feature, feature_definitions, attach_properties = False):
    """
    feature: Feature instance. Must contain id. Dataset_id may not be included but 
            must be set later. 
    feature_definitions: instance of FeatureDefinitions
    """
    if not isinstance(feature, Feature):
        raise Exception('feature argument not a Feature instance')
    else:
        array_values = [ None for i in feature_definitions]
        s = ShapesVector()
        for pdict in feature.properties:
            key, value = pdict.items()[0]
            if check_ogrtype_value(feature_definitions[key].get('ogr_type'), value):
                colmeta = feature_definitions.get(key)
                array_values[colmeta.get('array_id')-1] = value_to_string(value)

                if attach_attributes:
                    s.properties = feature_definitions
            else:
                raise Exception('Value incompatible with OGR type.')
        s.values = array_values
        s.geom = shapely.geometry.asShape(feature.geom)
        if attach_properties:
            for f in feature_definitions:
                s.properties.append(f.get_ref(ShapesAttributes))
        
        
        return s
            
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
        >>> values.append(value_to_string(ogrfeature.GetField(att.array_id - 1)))
    NOTES:
        - Admin client code should provide the right encoding of the DBF file 
        otherwise the data will be saved with garbled or non printable characters.
        - Make sure the client code sets the dataset_id value later if attach_properties 
        is true.
    """
    s = ShapesVector()
    #s.geom = loads(ogrfeature.GetGeometryRef().ExportToWkb())
    ogr_geom = ogrfeature.GetGeometryRef()
    #s.geom = ogr_geom.ExportToWkb().encode('hex')
    s.geom = ogr_geom
    s.gid = ogrfeature.GetFID()


    s.values = []
 
    for att in properties:
        str = ogrfeature.GetFieldAsString(att.array_id - 1)
        s.values.append(decode_field(str, decoder))

    if attach_properties:
        s.properties.extend(properties)

    return s

def ogrfeature_to_feature(ogrfeature):
    gid = ogrfeature.GetFID()
    geom = ogrfeature.GetGeometryRef().ExportToJson()
    properties = []
    for i in range(ogrfeature.GetFieldCount()):
        properties.append({ogrfeature.GetFieldDefnRef(i).GetName(): ogrfeature.GetField(i)})
    return Feature(gid, geom, properties)

class ShapesVectorCollection():
    def __init__(self, features=[]):
        """The features *must* provide the geometry geo interface."""
        self.features = features
    @property
    def __geo_interface__(self):
        return {'type': 'FeatureCollection', 'features': self.features}

    def __init__(self, contexts):
        for d in contexts:
            try:
                self.contexts.add(shapely.geometry.asShape(d))
            except ValueError:
                raise Exception('Context does not provide geo interface')
    
        
class ShapesVectorGroup(object):
    def __init__(self, vectors):
        for vector in vectors:
            if not isinstance(vector, ShapesVector):
                raise Exception('Found not a ShapesVector type in collection.')


    
def PromoteVectorDataset(sourcepath, dataset, session, load_data = True, **kw):
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

            
        pf = open(prjfile, 'r')
        prj = pf.read()
        pf.close()
        dataset.orig_epsg = get_code_from_wkt(prj)
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
        dataset.geom = bbox_to_polygon(transform_bbox(bbox, dataset.orig_epsg, SRID))
    else:
        dataset.geom = bbox_to_polygon(bbox)
    
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
    
    print dataset.feature_count     
    # Update important dataset attributes
    dataset.geomtype = ogrwkbformat_to_dbgeomtype(geomtype)
   
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
    #session.expire(dataset)

    return (0, '')
 
class VectorDataset(object):
    is_mappable = True

    def __init__(self, dataset):
        if dataset.taxonomy != 'vector':
            raise Exception('Dataset not vector compatible')
    
        self.dataset = dataset
        self.projection = osr.SpatialReference()
        self.projection.SetWellKnownGeogCS('WGS84')
        self.geomtype = dataset.geomtype
 
        self.basename = str(dataset.basename)
        self.shapefile = "%s/%s/shp/%s.shp" % (FORMATS_PATH, dataset.id, dataset.basename)

        if self.dataset.orig_epsg:
            self.orig_projection = osr.SpatialReference()
            if not self.orig_projection.ImportFromEPSG(dataset.orig_epsg):
                self.orig_projection = None
         
        self.properties_ref = self.dataset.attributes_ref

    def get_extent(self):
        return self.dataset.geom.wkt
    
    def get_ogr_wkb_format(self):
        return dbgeomtype_to_ogrwkbformat(self.dataset.geomtype)

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

    def get_geomtype_from_shapes(self):
        s = meta.Session.query(shapes_table).filter(shapes_table.c.dataset_id == self.dataset.id).limit(1).first()

        return s.geom.geom_type.upper()

    def get_extent_from_shapes(self):
        return "SELECT ST_SetSRID(ST_Extent(geom), %s) from shapes where dataset_id = %s" % (SRID, self.dataset.id)

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
        dest = open(destination_filename, 'w')

        if source == 'database':
            pass
        elif source == 'cached':    
            pass
        elif source == 'source':
            # The source is always a zipped shapefile and is the first entry in the sources_ref list
            temp_basepath =  tempfile.mkdtemp()
            zipped_shapefile = self.dataset.sources_ref[0].location
            #extractall(zipped_shapefile, temp_basepath)
            unzip(zipped_shapefile, temp_basepath)       
            decoder = get_decoder(encoding)    
    
            D = ogr.Open(temp_basepath) 
            L = D.GetLayerByIndex(0)
            S = L.GetSpatialRef()
            dataset_fields = self.get_fields('sql')

            
            dest.write('BEGIN;\n')
            dest.write('COPY shapes (gid, dataset_id, geom, values) FROM stdin; \n')

            for i in range(0, L.GetFeatureCount()):
                ftr = L.GetNextFeature()
                geom = ftr.GetGeometryRef().Clone()
                if self.dataset.orig_epsg != SRID:    
                    reproject_geom(geom, S, self.projection) 
                values = []
                for att in self.properties_ref:
                    value = '"' + ftr.GetFieldAsString(att.array_id - 1) + '"'
                    value = decode_field(value, decoder).encode('utf8')
                    if not value:
                        value = 'NULL'
                    values.append(value)
                values = "{" + ",".join(values) + "}"
                dest.write('\t'.join([ str(ftr.GetFID()), str(self.dataset.id), geom.ExportToWkb().encode('hex'), values]) + '\n') 
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
        prj4 = meta.Session.query(spatial_ref_sys.c.proj4text).filter(spatial_ref_sys.c.srid == SRID).first()
        spatialReference = osr.SpatialReference()
        spatialReference.SetFromUserInput(str(prj4.proj4text))

        L = vectorfile.CreateLayer(self.basename, spatialReference, self.get_ogr_wkb_format())

        fields = self.get_fields(format)
        for field in fields:
            L.CreateField(field)
        
        for shape in meta.Session.query(ShapesVector).filter(ShapesVector.dataset_id == self.dataset.id).all():   
            new_feature = ogr.Feature(L.GetLayerDefn())
            if format not in ['csv']:
                geom = shape.geom.ogr.Clone()
                new_feature.SetGeometry(geom)
                geom.Destroy()
            new_feature.SetFID(shape.gid)
            i = 0
            for field in fields:
                if i < len(shape.values):
                    new_feature.SetField(field.GetName(), shape.values[i])
                i += 1
            L.CreateFeature(new_feature)
            new_feature.Destroy()

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

        meta.Session.close()
 
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
        for row in meta.Session.query(shapes_table.c.values).filter(shapes_table.c.dataset_id == self.dataset.id).all():   
            x = 0
            for value in row.values:
                att = self.properties_ref[x]  
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
                
                ws.write(y,x, value)
                x += 1
            y += 1
            if y > 65535:
                break

        filename = self.dataset.get_filename('xls')
        wb.save(os.path.join(basepath, filename))

        return (0, 'Success')

