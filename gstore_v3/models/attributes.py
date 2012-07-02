from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP
from sqlalchemy.orm import relationship, backref

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

from osgeo import ogr

'''
features_attributes model

see also vocabs models for the parameter cv, etc
'''

class Attribute(Base):
    __table__ = Table('features_attributes', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('description', String),
        Column('orig_name', String),
        Column('ogr_type', Integer), 
        Column('ogr_justify', Integer),
        Column('ogr_width', Integer),
        Column('ogr_precision', Integer),
        Column('uuid', UUID, FetchedValue()),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        schema='gstoredata'
    )

    #relate to parameters with parameters as the parent

    def __init__(self, name, ogr_type):
        self.name = name
        self.ogr_type = ogr_type

    def __repr__(self):
        return '<Attribute (%s)>' % (self.name)

    #TODO: convert to ogr.fielddefn
    def att_to_fielddefn(self, format='shp'):
        fld = ogr.FieldDefn(str(self.name), self.ogr_type)
        #TODO: add the other flags
        if format not in ['csv', 'km;', 'gml']:
            pass
        
        return fld
    

