import os
import meta

from sqlalchemy import *
from sqlalchemy.orm import mapper, relation, backref, column_property
from sqlalchemy.sql import or_
from sqlalchemy.ext.declarative import declarative_base

from pylons import config, cache

from BeautifulSoup import BeautifulStoneSoup

from postgis import GISColumn, Geometry, Spatial
from files import Resource, source_table

from osgeo import ogr

Base = declarative_base()

__all__ = ['Dataset', 'Bundle', 'DatasetFootprint', 'spatial_ref_sys']


SRID = int(config.get('SRID', 4326))

FORMATS_PATH = config.get('FORMATS_PATH', '/tmp')

spatial_ref_sys = Table('spatial_ref_sys', Base.metadata,
    Column('srid', Integer, primary_key=True),
    Column('auth_name', String),    
    Column('auth_srid', Integer),   
    Column('srtext', String),
    Column('proj4text', String)
) 
    
mapfile_template = Table('mapfile_template', Base.metadata, 
    Column('id', Integer, primary_key=True),
    Column('description', String),
    Column('taxonomy', String),
    Column('xml', String)
)

class Dataset(Base):
    """
        Column     |            Type             |                       Modifiers                       
    ---------------+-----------------------------+-------------------------------------------------------
     id            | integer                     | not null default nextval('datasets_id_seq'::regclass)
     extent        | character varying(75)       | 
     description   | character varying(200)      | 
     taxonomy      | character varying(50)       | not null default 'vector'::character varying
     feature_count | integer                     | 
     abstract      | text                        | 
     dateadded     | timestamp without time zone | not null default now()
     metadata_xml  | text                     | 
     basename      | character varying(100)      | 
     old_idnum     | integer                     | 
     geom          | geometry                    | 
    """
    __tablename__ = 'datasets'
    id = Column(Integer, primary_key=True)
    description = Column(String)
    extent = Column(String)
    taxonomy = Column(String)
    feature_count = Column(Integer)
    abstract = Column(String)
    dateadded = Column(TIMESTAMP, default="now()")
    basename = Column(String)
    theme = Column(String)
    subtheme = Column(String)
    groupname = Column(String)
    old_idnum = Column(Integer)
    #geom = Column('geom', Geometry(config['SRID']))
    geom = GISColumn(Geometry(2))
    orig_epsg = Column(Integer)
    geomtype = Column(String)
    box = Column(String)
    formats = Column('formats_cache',String)
    inactive = Column(Boolean, default=False)
    metadata_xml = Column('metadata_xml', Text)
    mapfile_template_id = Column(Integer, ForeignKey('mapfile_template.id'))
    bundles_ref = relation('Bundle', secondary = Table('datasets_bundles', Base.metadata, 
                                Column('dataset_id', Integer, ForeignKey('datasets.id')),
                                Column('bundle_id', Integer, ForeignKey('bundles.id'))
                                ), lazy = True, backref = backref('datasets', order_by=id))
    sources_ref = relation(Resource, secondary = Table('datasets_sources', Base.metadata,
                                Column('source_id', Integer, ForeignKey(Resource.id)),
                                Column('dataset_id', Integer, ForeignKey('datasets.id'))
                                ), lazy = False, backref = backref('datasets'))
    has_metadata_cache = Column(Boolean)
    apps_cache = Column(String)
    bundle_id = Column(Integer, ForeignKey('bundles.id')) 

    def __init__(self, basename, metadata_xml = None):
        self.basename = basename
        if metadata_xml:
            self.metadata_xml = metadata_xml

    def __repr__(self):
        return "<Dataset(%s, %s)>" % (self.taxonomy, self.basename)

    def get_extent(self, from_epsg = None, to_epsg = None, format='array'):
        return self.geom.ogr.GetEnvelope()

    def get_box(self, from_epsg = None, to_epsg = None, format = 'array'):
        # self.box is an array of Decimal coming from a PGArray numeric[]
        return [float(l) for l in self.box]
            
    def get_attributes(self):
        # From a JSON serializable list
        attrs = []
        grid_columns = []
        for a in self.attributes_ref:
            if a.name == 'geom':
                continue 
            # Translate PG attribute types into EXTJS/Javascript types
            if a.attribute_type == 'double precision':
                att_type = 'float'
            elif a.attribute_type == 'integer':
                #att_type = 'int'
                att_type = 'int'
            elif a.attribute_type == 'varchar':
                att_type = 'string'
            else:
                att_type = 'string'         

            attrs.append( 
                { 'name' : a.name, 'type' : att_type }
            )
            grid_columns.append(
                { 'header' : a.name, 'dataIndex' : a.name , 'tooltip': a.description, 'sortable' : True }
            )
        return (attrs, grid_columns)

    def get_filename(self, format, compressed = False):
        if compressed:
            if format == 'kml':
                format = 'kmz'
            else:
                format = '%s.zip' % format

        return '%s_%s.%s' % (self.basename, self.dateadded.strftime('%m-%d-%y'), format)

    def clip_zip(self, format):
        # filelist: [(path, filename, content = None)]
        filelist = []
        xml_basename = self.basename
        for source in self.sources_ref:
            Res = Resource(source.location)
            if source.zipgroup == format:
                filelist.append((Res.dirname, str(Res.name), None))
                if source.extension == format:
                    xml_basename = Res.name
        if self.metadata_xml:
            filelist.append(('', '%s.xml' % str(xml_basename), self.metadata_xml.encode('utf8')))

        return filelist


    def fetchall_attributes(self):
        if self.taxonomy != 'vector':
            return None
        else:
            attrs = self.get_attributes()[0]
            cols = ','.join([ c['name'] for c in attrs if c['name'] !='geom'])
            rows = meta.Session.execute("select %s from %s" % (cols, self.basename)).fetchall()
            return rows



    def dump_format(self, format, use_cache = True, compress = False):
        if format not in self.formats:
            return None
 
        content = None
        if self.taxonomy != 'vector':
            return None
        else:
            basepath = os.path.join(FORMATS_PATH, str(self.id))
            if not os.path.isdir(basepath):
                os.mkdir(basepath)
            # Create cache dir for each format. Shapefile has many files itself
            # this can happen for other rich formats so use directories for all.
            formatpath = os.path.join(basepath, format)
            if not os.path.isdir(formatpath):
                os.mkdir(formatpath)
            
            filename = os.path.join(formatpath, self.get_filename(format))
            # Exceptions for shp and kml
            if format == 'shp':
                filename = '%s.zip' % filename
            if format == 'kml':
                filename = '%s.kmz' % filename

            if use_cache and os.path.isfile(filename):
                f = open(filename,'r')
                content = f.read()
                f.close()
            else:
                failed = False
                if format == 'shp':
                    failed = self.write_shp(formatpath, DBSession)
                else:
                    rows = self.fetchall_attributes(DBSession)
                    if rows:
                        if format == 'csv':
                            failed = util.write_csv(rows, self.basename, filename, compress)
                        elif format == 'xls':
                            failed = util.write_xls(rows, self.basename, filename, compress)
                        elif format == 'kml':   
                            shapespath = os.path.join(basepath,'shp')
                            if not os.path.isdir(shapespath):
                                os.mkdir(shapespath)
                                failed = self.write_shp(shapespath, DBSession)
                            if not failed:
                                failed = util.write_kml(rows, self.basename, basepath, self.get_filename('kml'), compress)
                if not failed:
                    f = open(filename,'r')
                    content = f.read()
                    f.close()
        return content

    def update_attributes_from_metadata(self, xml = None):
        if xml is None:
            if self.metadata_xml:
                xml = self.metadata_xml
        if xml:
            soup = BeautifulStoneSoup(xml)
            attrs = soup.findAll('attr')
            met_attrs = {}
            for att in attrs:
                label = att.find('attrlabl')
                description = att.find('attrdef')
                if label and description:
                    label = label.contents
                    description = description.contents
                    if label and description:
                        description = description[0].replace('\r\n','')
                        met_attrs[label[0]] = description
            if self.attributes_ref:
                for attr in self.attributes_ref:
                    if attr.orig_name in met_attrs.keys():
                        attr.description = met_attrs[attr.orig_name]   
 
    def get_formats(self):
        formats = {}
        for source in self.sources_ref:
            source.get_file_info(source.location)
            # put here extended format name
            size = 0
            if formats.has_key(source.zipgroup):
                if source.size:
                    size = source.size
        
                formats[source.zipgroup] += size
            else:
                formats[source.zipgroup] = size
        # Do not advertise direct download of file groups (formats) over 20MB
        return [ f for f in formats.keys() if formats[f] < 20000000]
            
    def set_formats(self):
        self.formats = ','.join(self.get_formats())

    @staticmethod
    def get_services(cls):
        services = []
        if cls.taxonomy == 'vector':
            services.append('wms')
            services.append('wfs')
        elif cls.taxonomy == 'geoimage':
            services.append('wms')
            services.append('wcs')
            pass
        return services

    @staticmethod
    def get_metadata(cls):
        metadata_xml = []
        if cls.has_metadata:
            metadata_xml.append('xml')
            metadata_xml.append('txt')
            metadata_xml.append('html')
        return metadata_xml

    @staticmethod   
    def get_tools(cls):
        """
        0 : Download
        1 : Map
        2 : Metadata
        3 : Services
        4 : Print
        5 : Cart
        """
        tools = [ 0 for i in range(6)] 
        if cls.formats:
            tools[0] = 1
        if cls.taxonomy in ['vector','geoimage']:
            tools[1] = 1 # all mappable for now
            tools[2] = 1 # all metadata for now
            tools[3] = 1 # all have at least one service
        else:
            tools[1] = 0
            tools[2] = 0
            tools[3] = 0
        tools[4] = 0 # no print
        tools[5] = 0 # no cart
        if cls.has_metadata:
            tools[2] = 1

        return tools
                

#- end of Dataset(Base)

class Bundle(Base):
    __tablename__ = 'bundles'
    id = Column(Integer, primary_key=True)
    description = Column(String)
    type = Column(String)
    long_description = Column(Text)
    parent_id = Column(Integer)
#    datasets = relation('Dataset', secondary = Table('datasets_bundles', Base.metadata,        
#                                Column('dataset_id', Integer, ForeignKey('datasets.id')),
#                                Column('bundle_id', Integer, ForeignKey('bundles.id'))
#                                ) , backref = backref('bundles_ref', order_by=id), lazy= False)
        
    def __init__(self, description, type, long_description, parent_id):
        self.description = description
        self.type = type
        self.long_description = long_description
        self.parent_id = parent_id

    def __repr__(self):
        return "<Bundle('%s','%s')>" % (self.description, self.type)


class GeoLookup(Base):
    """
    geolookups;
          Column      |        Type         |                        Modifiers                         
    ------------------+---------------------+----------------------------------------------------------
     gid              | integer             | not null default nextval('geolookups_gid_seq'::regclass)
     description      | character varying   | not null
     box              | character varying   | 
     geom             | geometry            | 
     box_geom         | geometry            | 
     extra_attributes | character varying   | 
     what             | character varying   | 
     app_ids          | character varying[] | 
    """
    __tablename__ = 'geolookups'

    id = Column(Integer, primary_key=True)
    description = Column(String)
    box = Column(String)
    geom = GISColumn(Geometry(2))
    box_geom = GISColumn(Geometry(2))
    extra_attributes = Column(String)
    what = Column(String)
    app_ids = Column(String)
