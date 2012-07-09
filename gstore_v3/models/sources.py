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

from datasets import *
from hstore import HStore, HStoreColumn

'''
models related to the source file data
used in the dataset.get requests, ogc requests (if tif or shp stored here)

DO NOT CONFUSE THIS WITH THE ORIGINAL SOURCE TABLE
this is not that at all
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
        Column('file_filesize_mb', Numeric),
        Column('file_hash', String(150)),
        Column('file_hash_type', String(20)),
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
        self.description = description
        
    def __repr__(self):
        return '<Source (%s, %s, %s)>' % (self.extension, self.set, self.uuid)

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

    def __repr__(self):
        return '<MapfileSetting (%s, %s)>' % (self.id, self.source_id)

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








