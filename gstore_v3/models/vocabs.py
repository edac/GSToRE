from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, FetchedValue, Numeric
from sqlalchemy.orm import relationship, backref

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY
from hstore import HStore, HStoreColumn

'''
models for all of the controlled vocabularies
'''

'''
gstore parameter cvs
'''
class Parameter(Base):
    __table__ = Table('parameters', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(20)),
        Column('description', String(250)),
        Column('nodata', String(20)),
        #Column('source_type', String(25)),
        Column('time_frequency', String(50)),
        Column('unit_id', Integer, ForeignKey('gstoredata.cv_units.id')),
        Column('timeunit_id', Integer, ForeignKey('gstoredata.cv_timeunits.id')),
        Column('original_name', String(150)),
        Column('original_description', String(1000)),
        Column('zvalue', Numeric),
        Column('zvalue_units_id', Integer, ForeignKey('gstoredata.cv_units.id')),
        Column('datatype_id', Integer, ForeignKey('gstoredata.cv_datatypes.id')),
        Column('parametersource_id', Integer, ForeignKey('gstoredata.cv_parametersources.id')),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )
    lookups = relationship('ParameterLUT', secondary='gstoredata.parameters_parameterluts', backref='params')

    def __repr__(self):
        return '<Parameter (%s, %s, %s)>' % (self.id, self.name, self.uuid) 

class ParameterLUT(Base):
    __table__ = Table('lut_parameters', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('term', String(25)),
        Column('description', String(500)),
        Column('source', String),
        HStoreColumn('properties', HStore()),
        schema='gstoredata'
    )
    
    def __repr__(self):
        return '<ParameterLUT (%s, %s, %s)>' % (self.id, self.term, self.uuid)

parameters_luts_jointable = Table('parameters_parameterluts', Base.metadata,
    Column('parameter_id', Integer, ForeignKey('gstoredata.parameters.id')),
    Column('lut_id', Integer, ForeignKey('gstoredata.lut_parameters.id')),
    schema='gstoredata'
)    

class Units(Base):
    __table__ = Table('cv_units', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('type', String),
        Column('abbreviation', String),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    ) 
    params = relationship('Parameter', backref='units', primaryjoin='Parameter.unit_id==Units.id')
    zunits = relationship('Parameter', backref='zunits', primaryjoin='Parameter.zvalue_units_id==Units.id')


class TimeUnits(Base):
    __table__ = Table('cv_timeunits', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('type', String),
        Column('abbreviation', String),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )   
    params = relationship('Parameter', backref='timeunits', primaryjoin='Parameter.timeunit_id==TimeUnits.id')



'''
geolookups (it's like a cv for search)
'''
geolookups = Table('geolookups', Base.metadata,
    Column('gid', Integer, primary_key=True),
    Column('description', String),
    Column('geom', String),
    Column('box_geom', String),
    Column('what', String),
    Column('box', ARRAY(Numeric)),
    Column('app_ids', ARRAY(String)),
    schema='gstoredata'
)   

'''
odm cvs
'''

class cvCensorCode(Base):
    __table__ = Table('cv_censorcodes', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('term', String(50)),
        Column('description', String(200)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

class cvDataType(Base):
    __table__ = Table('cv_datatypes', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('term', String(50)),
        Column('description', String(200)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )
    param_datatypes = relationship('Parameter', backref='datatypes')

class cvGeneralCategory(Base):
    __table__ = Table('cv_generalcategories', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('term', String(50)),
        Column('description', String(200)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

class cvSampleMedium(Base):
    __table__ = Table('cv_samplemediums', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('term', String(50)),
        Column('description', String(200)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

class cvSampleType(Base):
    __table__ = Table('cv_sampletypes', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('term', String(50)),
        Column('description', String(200)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

class cvVariableName(Base):
    __table__ = Table('cv_variablenames', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('term', String(50)),
        Column('description', String(200)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

class cvValueType(Base):
    __table__ = Table('cv_valuetypes', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('term', String(50)),
        Column('description', String(200)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

class cvSpeciation(Base):
    __table__ = Table('cv_speciation', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('term', String(50)),
        Column('description', String(200)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

class cvQualityControlLevel(Base):
    __table__ = Table('cv_qualitycontrollevels', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('code', String(50)),
        Column('definition', String(250)),
        Column('explanation', String(500)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

class cvParameterSource(Base):
    __table__ = Table('cv_parametersources', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100)),
        Column('description', String(500)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )
    param_sources = relationship('Parameter', backref='sources')


