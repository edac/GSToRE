from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, Numeric, FetchedValue
from sqlalchemy.orm import relationship, backref

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID

import math
from ..lib.utils import *
from datasets import *
from hstore import HStore, HStoreColumn

'''
models related to the source file data
used in the dataset.get requests, ogc requests (if tif or shp stored here)

DO NOT CONFUSE THIS WITH THE ORIGINAL SOURCE TABLE
this is not that at all

also, DO NOT DELETE THE ORIGINAL SOURCE TABLE. just in case something slipped through the migration cracks.
'''

'''
the source collection models
'''
#TODO: set up the schema for the engine (or metadata, somewhere) but 
#      check first - not required for 10, for example

#TODO: add application/gzip (tar.gz) 
class Source(Base):
    __table__ = Table('sources', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('extension', String(25)),
        Column('set', String(25)),
        Column('is_external', Boolean),
        Column('active', Boolean),
        Column('file_mimetype', String(100)),
        Column('uuid', UUID, FetchedValue()),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')), #foreign key to datasets
        schema='gstoredata' #THIS IS KEY
    )   

    #to the source files
    src_files = relationship('SourceFile', backref='source') 

    #to the mapfile settings
    map_settings = relationship('MapfileSetting')
    
    def __init__(self, set, extension):
        self.set = set
        self.extension = extension
        
    def __repr__(self):
        return '<Source (%s, %s, %s)>' % (self.extension, self.set, self.uuid)

    def pack_source(self, outpath, outname, xslt_path, metadata_info):
        #pack up the zip (if it's not a zip) with all files in the set
        #and add the metadata based on the src.dataset ref
        files = [f.location for f in self.src_files]
        if not files:
            #we may have a problem with an nfs mount that can lead to empty zips
            #which will be cached. that's not great
            return ''

        #TODO: the basename in the database DOES NOT MATCH the filename of the raster on disk 
        #      so this is not necessarily going to match the dataset. 
        parts = outname.replace('.zip', '').split('_') 
        metaname = '.'.join(['_'.join(parts[:-1]), parts[-1], 'xml'])
        metadata_file = os.path.join(outpath, metaname)
        
        #let's get the metadata obj
        #out_standard = 'FGDC-STD-012-2002' if self.datasets.taxonomy == 'geoimage' else 'FGDC-STD-001-1998'
        out_standard = metadata_info['standard']
        out_format = 'xml'
        
        written = self.datasets.write_metadata(metadata_file, out_standard, out_format, metadata_info)
        if written:
            files.append(metadata_file)    
        
        output = create_zip(os.path.join(outpath, outname), files)

        #and delete the new xml
        if os.path.isfile(metadata_file):
            os.remove(metadata_file)
        
        return output

    #TODO: check if this will work with service locations, i.e. links, that don't have a format
    #      since we assume that a service source will only have one source_file. fyi.
    def get_location(self, format=None):
        #get a specific file from the src_files set for this source obj
        if len(self.src_files) > 1 and format is not None:
            f = [g for g in self.src_files if g.location.split('.')[-1] == format]
            f = f[0] if f else None
        else:
            f = self.src_files[0]
        return f.location if f is not None else ''

    def get_filesize_mb(self):
        '''
        this is not, NOT!, the size of the final zipfile (unless the source is actually a zip)
        this is pretty much the size of the raster file.
        this does not include the estimate for the schrodinger's vectors, btw.
        '''
        
        #don't know. don't care. we don't host it. it's a cruel, cruel world.
        if self.is_external:
            return -99
        
        files = self.src_files
        #sort out the right file which assumes that the extension matches one of the actual file extensions. 
        #TODO: this might not be a good assumption
        test_file = [f.location for f in files if f.location[-len(self.extension):] == self.extension]
        if not test_file:
            return -99
        
        filesize = os.path.getsize(test_file[0]) / (1024 * 1024.0)
        return math.ceil(filesize)

'''
and the actual file paths on the server (geodata, etc)
'''
class SourceFile(Base):
    __table__ = Table('source_files', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('location', String(500)),
        Column('source_id', Integer, ForeignKey('gstoredata.sources.id')), #foreign key to sources
        schema='gstoredata'
    )
    def __init__(self, location):
        self.location = location
    def __repr__(self):
        return '<SourceFile (%s, %s)>' % (self.id, self.location)
    

'''

map-related models

'''
#TODO: think about adding a set for layers (if we need to do something for the hdf4/5 bands or something)
class MapfileSetting(Base):
    __table__ = Table('mapfile_settings', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('source_id', Integer, ForeignKey('gstoredata.sources.id')),
        Column('uuid', UUID, FetchedValue()),
        HStoreColumn('settings', HStore()),
        schema='gstoredata'
    )

    classes = relationship('MapfileClass', secondary='gstoredata.mapfile_settings_classes')
    styles = relationship('MapfileStyle', secondary='gstoredata.mapfile_settings_styles')

#    def __init__(self, source_id, values):
#        self.source_id = source_id
#        #value = {'key': 'value', 'key': 'value'}
#        self.settings = values

    def __init__(self, values):
        #value = {'key': 'value', 'key': 'value'}
        self.settings = values

    def __repr__(self):
        return '<MapfileSetting (%s, %s)>' % (self.id, self.source_id)

    def get_processing(self):
        #return the mapfile processing directives from the hstore
        #the keys list may need to be updated based on future needs. this list is for RASTER data
        keys = ['LUT', 'BANDS', 'COLOR_MATCH_THRESHOLD', 'DITHER', 'LOAD_FULL_RES_IMAGE', 'LOAD_WHOLE_IMAGE', 'OVERSAMPLE_RATIO', 'RESAMPLE', 'SCALE']
        
        if not self.settings:
            return []

        directives = []    
        for key in keys:
            if key in self.settings:
                directives.append('%s=%s' % (key, self.settings[key]))
        return directives

class MapfileClass(Base):
    __table__ = Table('mapfile_classes', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        HStoreColumn('settings', HStore()),
        Column('name', String(50)),
        schema='gstoredata'
    )

    def __repr__(self):
        return '<MapfileClass (%s, %s)>' % (self.id, self.name)

class MapfileStyle(Base):
    __table__ = Table('mapfile_styles', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        HStoreColumn('settings', HStore()),
        Column('name', String(25)),
        schema='gstoredata'
    )

    def __repr__(self):
        return '<MapfileStyle (%s, %s)>' % (self.id, self.name)

#join tables

#for the list of styles available for the map
mapfile_settings_styles = Table('mapfile_settings_styles', Base.metadata,
    Column('settings_id', Integer, ForeignKey('gstoredata.mapfile_settings.id')),
    Column('style_id', Integer, ForeignKey('gstoredata.mapfile_styles.id')),
    schema='gstoredata'
)

#for the classes for a map (where the default class style is one of the settings)
mapfile_settings_classes = Table('mapfile_settings_classes', Base.metadata,
    Column('class_id', Integer, ForeignKey('gstoredata.mapfile_classes.id')),
    Column('settings_id', Integer, ForeignKey('gstoredata.mapfile_settings.id')),
    schema='gstoredata'
)








