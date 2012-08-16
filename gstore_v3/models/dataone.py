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

    def __init(self, object_uuid, object_type):
        self.object_uuid = object_uuid
        self.object_type = object_type

        #TODO: on insert, also insert new dataone_obsoletes object for this uuid so that everything starts there

    def __repr__(self):
        return '<DateONE Object (%s, %s, %s)>' % (self.dataone_uuid, self.object_uuid, self.object_type)


    def get_current_object(self):
        #return the obsolete_uuid for the set of uuids 
        #where current object is the uuid with the most recent date-modified value in dataone_obsoletes
        #THIS IS THE OBSOLETED_BY VALUE IN THE SYSTEM METADATA


        

        pass
        
    

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

    def build_rdf(self):
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

    so we get have a d1 object in core (id = u456 as an example)
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

        if current_obsoleted.obsolete_uuid != self.obsolete_uuid:
            return False, current_obsoleted.obsolete_uuid

        #it's the current uuid
        return True, None

    

    
