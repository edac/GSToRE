from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref

from sqlalchemy import desc, asc, func

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

import os

from ..lib.utils import getHash


'''
dataone models
'''

class DataoneCore(Base):
    __table__ = Table('dataone_core', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('dataone_uuid', UUID, FetchedValue()),
        Column('object_uuid', UUID),
        Column('object_type', String(25)),
        Column('date_added', TIMESTAMP, default="now()"),
        Column('format_id', Integer, ForeignKey('gstoredata.dataone_formatids.id')),
        schema='gstoredata'
    )
    '''
    dataone_uuid = new immutable dataone uuid
    object_uuid = dataset source uuid (from sources) or metadata uuid (from metadata) or package uuid (from datapackages) or vector uuid (from vectors)
    object_type = source | vector | metadata | package (this is not great)

    each data object in d1 has to have its own identifier so we'll make one set for any dataone thing
    these uuids dont' change - if something is updated, see _obsoletes
    '''

    obsoletes = relationship('DataoneObsolete', backref='core', order_by='DataoneObsolete.date_changed')

    format = relationship('DataoneFormat')

    def __init(self, object_uuid, object_type, format_id):
        self.object_uuid = object_uuid
        self.object_type = object_type
        self.format_id = format_id

        #TODO: on insert, also insert new dataone_obsoletes object for this uuid so that everything starts there

    def __repr__(self):
        return '<DateONE Object (%s, %s, %s)>' % (self.dataone_uuid, self.object_uuid, self.object_type)


    def get_current(self):
        #return the obsolete_uuid for the set of uuids 
        #where current object is the uuid with the most recent date-modified value in dataone_obsoletes
        #THIS IS THE OBSOLETEDBY VALUE IN THE SYSTEM METADATA
        if self.obsoletes:
            return self.obsoletes[-1].obsolete_uuid
        else:
            return None

    def get_obsoletes(self, test_uuid):
        #return a list of uuids for any obsolete_uuid in the set of obsolete_uuids for this object
        #THESE ARE THE OBSOLETES VALUES IN THE SYSTEM METADATA
        obs = [o.obsolete_uuid for o in self.obsoletes]
        prev = obs[:obs.index(test_uuid)]
        return prev

    def get_object(self, path):
        '''
        get the object by type for the uuid

        but we store things by dataone uuid and all other uuids are just used to track back to gstore in case of disaster, etc
        cleaner that way in terms of delivering the d1 obj - don't need to pack things, look through five tables, etc

        return the file path to the obj (so cache the rdf package)
        '''
        #TODO: change this to the right uuid (dataone uuid won't exist when the dataset is promoted to dataone)

        #need to get objects by their object id 
        if self.object_type in ['source', 'vector']:
            #in cache/datasets
            obj = os.path.join(path, 'datasets', str(self.object_uuid) + '.zip')
        elif self.object_type == 'metadata':
            #in cache/metadata
            obj = os.path.join(path, 'metadata', str(self.object_uuid) + '.xml')
        elif self.object_type == 'package':
            package_loc = os.path.join(path, 'packages', str(self.object_uuid) + '.xml')
            if not os.path.isfile(package_loc):
                #make it after getting the package object
                pkg = DBSession.query(DataonePackage).filter(DataonePackage.package_uuid==self.object_uuid).first()
                if not pkg:
                    return None
                pkg.build_rdf(package_loc)
                
            obj = package_loc
        else:
            return None

        if not os.path.isfile(obj):
            return None
        
        return obj

    def get_hash(self, algo, path):
        '''
        only supports md5 and sha-1 right now (because that's all dataone supports)
        '''
    
        #let's get the path (that is the object)
        f = self.get_object(path)

        if not f:
            return ''

        #and then get the hash
        return getHash(f, algo)

    def get_size(self, path):
        f = self.get_object(path)

        if not f:
            return ''
            
        return os.path.getsize(f)
        
class DataonePackage(Base):
    __table__ = Table('dataone_datapackages', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('package_uuid', UUID, FetchedValue()),
        Column('dataset_object', UUID),
        Column('metadata_object', UUID),
        schema='gstoredata'
    )
    '''
    set up a package object (returns that rdf chunk) where a package here is a dataset and a metadata file. 
    this is not going to handle the collections if we decide to do that.

    dataset_object = the dataone_uuid for a record in dataone_core where dataone_uuid == dataset_object and object_type == source
    metadata_object = the dataone_uuid for a record in dataone_core where dataone_uuid == metadata_object and object_type == metadata
    

    not great
    '''

    def __init__(self, dataset_object, metadata_object):
        self.dataset_object = dataset_object
        self.metadata_object = metadata_object

    def __repr__(self):
        return '<DataONE Package (%s, %s, %s)>' % (self.package_uuid, self.dataset_object, self.metadata_object)

    def build_rdf(self, location):
        return ''

class DataoneVector(Base):
    __table__ = Table('dataone_vectors', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('vector_uuid', UUID, FetchedValue()),
        Column('dataset_uuid', UUID),
        Column('format', String(20)),
        Column('date_added', TIMESTAMP, default="now()"),
        schema='gstoredata'
    )
    '''
    container for uuids to represent the vector datasets
    which, i guess, could just be the dataone_uuid but that
    doesn't get us to the dataset+format so 
    so we'll just do this to explicitly define what gets to be a 
    dataone object
    '''

    def __init__(self, dataset_uuid, format):
        self.dataset_uuid = dataset_uuid
        self.format = format

    def __repr__(self):
        return '<DataONE Vector (%s, %s, %s)>' % (self.vector_uuid, self.format, self.dataset_uuid)

    def add_vector(self, format, path):
        '''
        get the vector zip (if it's cached just copy the zip, otherwise build it and copy the zip)
        for the dataset id and format requested (if format in supported list)
        where the file name is the new vector uuid 
        '''
        pass

class DataoneObsolete(Base):
    __table__ = Table('dataone_obsoletes', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('dataone_uuid', UUID, ForeignKey('gstoredata.dataone_core.dataone_uuid')),
        Column('obsolete_uuid', UUID, FetchedValue()),
        Column('date_changed', TIMESTAMP, default="now()"),
        schema='gstoredata'
    )    
    '''
    dataone_uuid = foriegn key to dataone_core
    obsolete_uuid = new uuid if a dataone object has been modified
    date_changed = timestamp for when object changed and so we can return the most current uuid for an object

    just for kicks, a dataone_core obj is added to obsoletes as soon as it's made
    so that we have one path to the object instead of many. 

    so we have a d1 object in core (id = u456 as an example)
    we update u456 so that becomes a new version of the data object for dataone 
    we have to create a new identifier for it by adding a record here
    pointing to the original dataone uuid so that we can always return
    the object (we aren't keeping the versions)
    and so that we can add the obsoletes/obsoleted by references in the system metadata rdf

    the first search from the view method comes here and checks for obsolete_uuid
    if it's not there, assume it is unmodified and go to dataone_core to check the dataone_uuid
    '''

    def __init__(self, dataone_uuid):
        self.dataone_uuid = dataone_uuid
    
    def __repr__(self):
        return '<DataONE Obsolete (%s, %s)>' % (self.obsolete_uuid, self.dataone_uuid)

    def current_object(self):
        #figure out if this is the active uuid for the object
        #so if this obs uuid == uuid of [all obs uuids for d1 uuid][1] (i.e. if uiid == uuid of the first list item)
        #if not, we need to get the current uuid for obsoleted_by
        obsoleteds = DBSession.query(DataoneObsolete).filter(DataoneObsolete.dataone_uuid==self.dataone_uuid).order_by(desc(DataoneObsolete.date_changed))        
        current_obsoleted = obsoleteds[0]

        #if the uuids don't match, there's a newer version
        if current_obsoleted.obsolete_uuid != self.obsolete_uuid:
            return False, current_obsoleted.obsolete_uuid

        #it's the current uuid
        return True, None

class DataoneFormat(Base):
    __table__ = Table('dataone_formatids', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('format', String(200)),
        Column('name', String(200)),
        Column('type', String(20)),
        schema = 'gstoredata'
    )  
    '''
    list the formatIds from the CN list 
    but only what we are likely to use in gstore
    '''


class DataoneSearch(Base):
    __table__ = Table('search_dataone', Base.metadata,
        Column('the_uuid', UUID, primary_key=True),
        Column('the_date', TIMESTAMP),
        Column('format', String(200)),
        schema = 'gstoredata'
    )
    '''
    this is just to make the listObjects (search) a little nicer
    since the format is part of the core object and the dates are part of the obsolete objects
    and the max function to get those returns a tuple that is not chainable with sqla (that i'm aware of)

    anyway, this handles the search by date and format with limit/offset in one run

    NOTE: the_uuid is the dataone_uuid NOT the obsolete_uuid so this won't work for PID-specific searches
    '''
    
