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
#from datasets import *


class OdmNetworks(Base):
    __table__ = Table('odm_networks', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('name', String(100)),
        Column('code', String(20)),
        schema='gstoredata'
    )

    def __repr__(self):
        return '<OdmNetwork (%s, %s)>' % (self.id, self.name)

class OdmSites(Base):
    __table__ = Table('odm_sites', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('name', String(50)),
        Column('code', String(20)),
        Column('odm_id', Integer),
        Column('geom', String),
        Column('orig_epsg', Integer),
        Column('elevation_m', Numeric),
        Column('vertical_datum', String(50)),
        Column('state', String(50)),
        Column('county', String(100)),
        #Column('dateadded', TIMESTAMP),
        schema='gstoredata'
    )

    def __repr__(self):
        return '<OdmSite (%s, %s)>' % (self.id, self.name)



#for the list of styles available for the map
odm_sites_datasets = Table('odm_sites_datasets', Base.metadata,
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    Column('site_id', Integer, ForeignKey('gstoredata.odm_sites.id')),
    schema='gstoredata'
)




        
