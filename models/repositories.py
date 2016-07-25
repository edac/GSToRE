from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey, Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import relationship, backref

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy import func

from sqlalchemy.dialects.postgresql import UUID, ARRAY

from ..lib.spatial import *
from ..lib.utils import *

'''
basic repository definition

see also: datasets.excluded_repositories

note: while rgis is presented as a repo in documentation, for database purposes it is 
not a repository. so if rgis is selected as a repo, add to apps/apps_cache as appropriate and
not as a repo
'''
class Repository(Base):
    __table__ = Table('repositories', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(250)),
        Column('description', String(500)),
        Column('uuid', UUID, FetchedValue()),
        Column('url', String(750)),
        Column('excluded_standards', ARRAY(String)),
        schema='gstoredata'
    )

    def __init__(self, name, url):  
        self.name = name
        self.url = url

    def __repr__(self):
        return '<Repository (%s, %s, %s)>' % (self.id, self.name, self.uuid)

    def get_standards(self, req=None):
        if not req:
            lst = get_current_registry().settings['DEFAULT_STANDARDS']
        else:
            lst = req.registry.settings['DEFAULT_STANDARDS']

        if not lst:
            return None
        lst = lst.split(',')

        exc_lst = self.excluded_standards + ['GSTORE']
        return [i for i in lst if i not in exc_lst]


#the join table for REPO + APP + DATASET
repository_sets = Table('repositories_apps_datasets', Base.metadata,
    Column('repository_id', Integer, ForeignKey('gstoredata.repositories.id')),
    Column('app_id', Integer, ForeignKey('gstoredata.apps.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)        
