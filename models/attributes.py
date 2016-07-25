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
        Column('nodata', String(10)),
        Column('uuid', UUID, FetchedValue()),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        Column('parameter_id', Integer, ForeignKey('gstoredata.parameters.id')),
        schema='gstoredata'
    )

    #relate to parameters with parameters as the parent

    #relate to the representations
#    representations = relationship('gstoredata.feature_attribute_relationships')

    def __init__(self, name, ogr_type):
        self.name = name
        self.ogr_type = ogr_type

    def __repr__(self):
        return '<Attribute (%s)>' % (self.name)

    #convert our field/attribute/parameter to an ogr fielddefn
    def att_to_fielddefn(self, format='shp'):
        fld = ogr.FieldDefn(str(self.name), self.ogr_type)
        if format not in ['csv', 'kml', 'gml']:
            if self.ogr_width:
                fld.SetWidth(self.ogr_width)
            if self.ogr_precision:
                fld.SetPrecision(self.ogr_precision)     
        return fld


##listing for the representations (i.e. this attribute is an odm parameter, etc)
#feature_attribute_representations = Table('feature_attribute_representations', Base.metadata,
#    Column('id', Integer, primary_key=True),
#    Column('attribute_id', Integer, ForeignKey('gstoredata.features_attributes.id')),
#    Column('representation', String(20)),
#    schema='gstoredata'
#)
    
