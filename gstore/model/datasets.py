import meta

from sqlalchemy import *

from pylons import config

from geoutils import transform_bbox 


__all__ = ['datasets_table', 'Dataset', 'DatasetFootprint',\
           'bundles_table', 'Bundle', 'geolookups_table', 'GeoLookup', \
           'mapfile_templates_table', 'SpatialReference', 'MapfileTemplate']


SRID = int(config.get('SRID', 4326))

FORMATS_PATH = config.get('FORMATS_PATH', '/tmp')

    
mapfile_templates_table = Table('mapfile_template', meta.Base.metadata, 
    Column('id', Integer, primary_key=True),
    Column('description', String),
    Column('taxonomy', String),
    Column('xml', String)
)

datasets_table = Table('datasets', meta.Base.metadata, 
    Column('id', Integer, primary_key=True),
    Column('description', String),
    Column('taxonomy', String),
    Column('feature_count', Integer),
    Column('abstract', String),
    Column('dateadded', TIMESTAMP, default="now()"),
    Column('basename', String),
    Column('theme', String),
    Column('subtheme', String),
    Column('groupname', String),
    Column('old_idnum', Integer),
    Column('box', String),
    Column('orig_epsg', String),
    Column('geom', String),
    Column('geomtype', String),
    Column('formats_cache', String),
    Column('inactive', Boolean, default=False),
    Column('metadata_xml', Text),
    Column('mapfile_template_id', Integer, ForeignKey('mapfile_template.id')),
    Column('has_metadata_cache', Boolean),
    Column('apps_cache', String),
    Column('bundle_id', Integer, ForeignKey('bundles.id'))
)

class SpatialReference(object):
    pass
 
class MapfileTemplate(object):
    pass

class Dataset(object):
    """
    """

    def __init__(self, basename, metadata_xml = None):
        self.basename = basename
        if metadata_xml:
            self.metadata_xml = metadata_xml

    def __unicode__(self):
        return self.__repr__()

    def __repr__(self):
        return "<Dataset(%s, %s)>" % (self.taxonomy, self.basename)

    def get_box(self, from_epsg, to_epsg, format = 'array'):
        # self.box is an array of Decimal coming from a PGArray numeric[] and it is always in geographic projection
        inbbox = [float(l) for l in self.box]
        if to_epsg == SRID:
            return inbbox
        else:
            return transform_bbox(inbbox, from_epsg, to_epsg)            

    def get_light_attributes(self, format = 'kml'):
        """
        Translate PG attribute types into KML or Javascript types and 
        provide column ordering.
        """
        attrs = []
        for a in self.attributes_ref:
            if a.name == 'geom':
                continue 
            if a.attribute_type == 'double precision':
                if format == 'kml':
                    att_type = 'double'
                else:
                    att_type = 'float'
            elif a.attribute_type == 'integer':
                att_type = 'int'
            elif a.attribute_type == 'varchar':
                att_type = 'string'
            else:
                att_type = 'string'         

            attrs.append((a.array_id , (a.name, att_type)))
        
        attrs.sort()

        return attrs 

    # TODO: implement a full attribute description including
    # geometry types, field precision, justification, etc.
    def get_full_attributes(self):
        pass

    def get_schema(self, format = 'kml'):
        if format == 'kml': 
            return """<?xml version="1.0" encoding="UTF-8"?>\n<Schema name="%(name)s" id="%(name)sID">
            <SimpleField type="string" name="gstore_time"><displayName>GSTORE Time</displayName></SimpleField>
            <SimpleField type="integer" name="gstore_dataset_id"><displayName>GSTORE Dataset Id</displayName></SimpleField>
            %(simplefields)s
        </Schema>""" % {
        'name': 'gstore_dataset_%s' % self.id,
        'simplefields': '\n'.join([
            """<SimpleField type="%(type)s" name="%(name)s"><displayName>%(name)s</displayName></SimpleField>""" %
                dict(type = att[1][1], name = att[1][0], id = self.id) for att in self.get_light_attributes('kml')
            ])
        }
 

    def get_filename(self, format, compressed = False):

        if format == 'zip':
            return self.sources_ref[0].location

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
        elif cls.taxonomy in ['geoimage', 'rtindex', 'vtindex']:
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
                

#- end of Dataset(object)
bundles_table = Table('bundles', meta.Base.metadata,  
    Column('id', Integer, primary_key=True),
    Column('description', String),
    Column('type', String),
    Column('long_description', Text),
    Column('parent_id', Integer)
)

    
class DatasetFootprint(object):
    """
    A class that maps a subset of columns of Dataset for efficiency.
    """


class Bundle(object):
    """
          Column      |          Type          |                      Modifiers                       
    ------------------+------------------------+------------------------------------------------------
     id               | integer                | not null default nextval('bundles_id_seq'::regclass)
     description      | character varying(200) | 
     long_description | text                   | 
     type             | character varying(50)  | not null default 'other'::character varying
     parent_id        | integer                | 
    Indexes:
        "bundles_pkey" PRIMARY KEY, btree (id)
        "bundle_description_type" UNIQUE, btree (description, type)
    Check constraints:
        "valid_bundle_type" CHECK (type::text = ANY (ARRAY['app'::character varying, 'edac_defined'::character varying, 'user_defined'::character varying, 'cart'::character varying, 'vtindex'::character varying, 'rtindex'::character varying]::text[]))

    """        
    def __init__(self, description, type, long_description, parent_id):
        self.description = description
        self.type = type
        self.long_description = long_description
        self.parent_id = parent_id

    def __repr__(self):
        return "<Bundle('%s','%s')>" % (self.description, self.type)

geolookups_table = Table('geolookups', meta.Base.metadata, 
    Column('id', Integer, primary_key = True),
    Column('description', String),
    Column('box', String),
    Column('geom', String),
    Column('box_geom', String),
    Column('extra_attributes', String),
    Column('what', String),
    Column('app_ids', String)
) 

class GeoLookup(object):
    """
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
    pass
