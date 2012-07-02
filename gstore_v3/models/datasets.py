from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref

from pyramid.threadlocal import get_current_registry

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

from osgeo import ogr, osr

from ..lib.utils import getFormats, createZip
from ..lib.spatial import *
from ..lib.mongo import gMongo

import os, tempfile


#__all__ = ['Dataset', 'categories_datasets', 'Category']

'''
gstoredata.datasets
'''
#TODO: set up the schema for the engine (or metadata, somewhere) but 
#      check first - not required for 10, for example

#could autoload but there's a lot of old junk in there that we don't really care about
class Dataset(Base):
    __table__ = Table('datasets', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('description', String(200)),
        Column('taxonomy', String(50)),
        Column('feature_count', Integer),
        Column('record_count', Integer),
        Column('dateadded', TIMESTAMP, default="now()"),
        Column('basename', String(100)),
        Column('geom', String),
        Column('geomtype', String),
        Column('formats_cache', String),
        Column('orig_epsg', Integer),
        Column('inactive', Boolean, default=False), #shuts it off completely
        Column('box', ARRAY(Numeric)),
        Column('apps_cache', ARRAY(String)),
        Column('begin_datetime', TIMESTAMP),
        Column('end_datetime', TIMESTAMP),
        Column('is_available', Boolean, default=True), #leaves the text data visible but the downloads (data) unavailable
        Column('has_metadata_cache', Boolean, default=True), 
        Column('excluded_formats', ARRAY(String)),
        Column('uuid', UUID),
        schema='gstoredata' #THIS IS KEY
    ) 

    #category join
    #note: the table name should be the metadata tablename which i guess includes the schema
    categories = relationship('Category', 
                    secondary='gstoredata.categories_datasets',
                    backref='datasets')

    #TODO: add a join condition where sources.active only
    #relate to sources
    sources = relationship('Source', backref='datasets')

    #relate to attributes
    attributes = relationship('Attribute', backref='dataset')

    #relate to relationships (meta)

    #relate to shapes
    shapes = relationship('Feature')

    #relate to collections
    collections = relationship('Collection', 
                    secondary='gstoredata.collections_datasets',
                    backref='datasets')

    #TODO: update this when the new schema is in place. this is an intermediate step
    #relate to the metadata
    dataset_metadata = relationship('DatasetMetadata')
      
    def __init__(self, description):
        self.description = description

        #TODO: generate the uuid for this on create (but overwrite for the tristate sharing datasets, so remember that)
        #that way it at least has a uuid no matter what
        
    def __repr__(self):
        return '<Dataset (%s, %s)>' % (self.description, self.uuid)

    #get the available formats for the dataset
    #use the excluded_formats & the taxonomy_list defaults to get the actual supported formats
    def get_formats(self):
        lst = getFormats(self.taxonomy)
        exc_lst = self.excluded_formats

        #get all from one not in the other
        fmts = [i for i in exc_lst if i not in lst]
        return fmts


    #return a source by set & extension for this dataset
    #default to active sources only
    def get_source(self, set, extension, active=True):
        pass

    #get the source location (path) for the mapfile
    #return the source id (for the mapfile) and the data filepath
    def get_mapsource(self):
        '''
        if file:
            return null
            
        if geoimage:
            get tif if tif
            get sid if sid
            get ecw if ecw

        if vector:
            if shp in src, get shp
            if shp in formats, get shp
            build shp

        '''

        if self.taxonomy == 'file':
            return None
        elif self.taxonomy == 'geoimage':
            srcs = self.sources
            src = None
            ext = ''
            for r in ['tif', 'sid', 'ecw']:
                src = [s for s in srcs if s.extension == r and s.active == True]
                if src:
                    ext = r
                    break
            if not src:
                #no match for valid raster type
                return None, None

            #make sure the chosen source is unzipped
            src = src[0]
            files = src.src_files
            #get the location that ends with .{ext}
            f = [f for f in files if f.location.split('.')[-1] == ext]

            if not f:
                #maybe it's the right type but already packed as a zip
                return None, None

            #this should be the data file we're looking for
            return src, f[0].location
        elif self.taxonomy == 'vector':
            srcs = self.sources
            if srcs:
                #we found something

                return None, None

            #nope, check for the cached file
            fmtpath = get_current_registry().settings['FORMATS_PATH']

            #nope, go build the vector
            return None, None

        return None, None
            
    def build_vector(self, format, basepath):
        '''
        pull the data from mongo
        get the attribute data
        create an ogr dataset
        populate
        '''
        connstr = get_current_registry().settings['mongo_uri']
        collection = get_current_registry().settings['mongo_collection']
        gm = gMongo(connstr, collection)
        vectors = gm.query({'d.id': self.id})

        #check the base location
        if os.path.abspath(basepath) != basepath or not os.path.isdir(basepath):
            return 1 #do something for a reasonable error

        if format == 'xls':
            #do something else

            return (1, 'excel')

        #for everything else, we can use the ogr file formats
        driver = ogr.GetDriverByName(format_to_filetype(format))
        if driver is None:
            return (1, 'bad driver %s' % format)

        #set up a temp location
        tmp_path = tempfile.mkdtemp()
#        if format == 'shp':
#            tmp_file = tmp_path
#        else:
        tmp_file = os.path.join(tmp_path, '%s.%s' % (self.basename, format))

        datasource = driver.CreateDataSource(str(tmp_file))

        #get the default projection
        epsg = get_current_registry().settings['SRID']
        epsg = int(epsg)
        sr = epsg_to_sr(epsg)
        
        #set up the layer
        lyrtype = postgis_to_ogr(self.geomtype)
        layer = datasource.CreateLayer(str(self.basename), sr, lyrtype)

        #deal with the fields
        #FID AND SHAPE are always set except for csv
        flds = self.attributes
        for fld in flds:
            layer.CreateField(fld.att_to_fielddefn(format))

        #TODO: add the observed field

        lyrdef = layer.GetLayerDefn()

        #now add data
        for v in vectors:
            feature = ogr.Feature(lyrdef)

            if format not in ['csv']:
                #add the geometry
                wkb = v['geom']['g']
                geom = wkb_to_geom(wkb, epsg)
                feature.SetGeometry(geom)
                geom.Destroy()
            #TODO: think about GIDs but not very hard since we decided, what with the snotel, that they were not so meaningful

            #get the attribute data from the mongo doc
            atts = v['atts']
            
            #add the attribute data to the feature
            for fld in flds:
                att = [a for a in atts if a['name'] == fld.name]
                if att:
                    value = str(att[0]['val'])
                    #convert it to the right type based on the field with string as default
                    value = convert_by_ogrtype(value, fld.ogr_type)
                    feature.SetField(str(fld.name), value)

            #TODO: add the observed value   
                

            #add the feature to the layer
            layer.CreateFeature(feature)
            feature.Destroy()

        #create a spatial index for the shapefile
        if format == 'shp':
            datasource.ExecuteSQL('CREATE SPATIAL INDEX ON %s' % (str(self.basename)))

        datasource.Destroy()
        layer = None

        #now the metadata and the projection file
        #just for shapes? or add metadata to all in the zip
        if format == 'shp':
            #write out the .prj file
            #TODO: add the basepath
            prjfile = open('%s.prj' % (os.path.join(tmp_path, self.basename)), 'w')
            sr.MorphToESRI()
            prjfile.write(sr.ExportToWkt())
            prjfile.close()

            #and the metadata
            #TODO: NEED SOME METADATA BUILDER THAT IS NOT RESPONSE TEMPLATE FOR THIS
            if self.has_metadata_cache:
                mt = str(self.dataset_metadata[0].original_xml.encode('utf8'))
                mtfile = open('%s.shp.xml' % (os.path.join(tmp_path, self.basename)), 'w')
                mtfile.write(mt)
                mtfile.close()
                
        #set up the formats directory WITHOUT BASENAMES
        #NO - move this to the view so that we can build vector formats wherever we want

        filename = os.path.join(basepath, '%s.%s.zip' % (self.uuid, format))
        #get the files
        #files = [f.location for f in files]
        files = []
        if format=='shp':
            exts = ['shp', 'shx', 'dbf', 'prj', 'shp.xml', 'sbn', 'sbx']
            for e in exts:
                if os.path.isfile(os.path.join(tmp_path, '%s.%s' % (self.basename, e))):
                    files.append(os.path.join(tmp_path, '%s.%s' % (self.basename, e)))
        else:
            files.append(os.path.join(tmp_path, '%s.%s' % (self.basename, format)))
        output = createZip(filename, files)
        
        
        return (0, 'success')
    

'''
gstoredata.categories and the join table
'''
categories_datasets = Table('categories_datasets', Base.metadata,
    Column('category_id', Integer, ForeignKey('gstoredata.categories.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)

class Category(Base):
    __table__ = Table('categories', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('theme', String(150)),
        Column('subtheme', String(150)),
        Column('groupname', String(150)),
        Column('apps', ARRAY(String)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )
    #relate to datasets handled in the datasets backref

    def __repr__(self):
        return '<Category (%s, %s, %s, %s)>' % (self.id, self.theme, self.subtheme, self.groupname)

'''
gstoredata.collections and join table
'''

collections_datasets = Table('collections_datasets', Base.metadata,
    Column('collection_id', Integer, ForeignKey('gstoredata.collections.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)

class Collection(Base):
    __table__ = Table('collections', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('description', String(200)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

    #relate with datasets (see Dataset)

    def __repr__(self):
        return '<Collection (%s, %s, %s)>' % (self.id, self.name, self.uuid)



'''
gstoredata.relationships
'''
