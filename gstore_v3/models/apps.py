from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey, Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import relationship, backref

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

'''
apps model
'''

class GstoreApp(Base):
    __table__ = Table('apps', Base.metadata,
        Column('id', Integer, primary_key = True),
        Column('name', String(25)),
        Column('full_name', String(250)),
        Column('url', String(500)),
        Column('route_key', String(15)),
        Column('uuid', UUID, FetchedValue()),
        Column('preferred_metadata_standards', ARRAY(String)),
        schema='gstoredata'
    )

    #relate to repositories
    repositories = relationship('Repository', secondary='gstoredata.repositories_apps_datasets', backref='apps')

    prov_ontologies = relationship('ProvOntology', backref='prov_apps')

    '''
    just the metadata requirements right now (liability, etc)
    '''

    def __repr__(self):
        return '<App (%s, %s)>' % (self.id, self.name)

    def __init__(self, name, route_key):
        self.name = name
        self.route_key = route_key

    def get_repositories(self, req=None):
        '''
        get the list of repos with the supported metadata standards

        and a count of things?

        {
            {repo}: {
                "description": 
                "url":
                "standards": []                
            }
        }
        '''

        repos = self.repositories

        output = {}

        for repo in repos:
            output[repo.name] = {
                "description": repo.description,
                "url": repo.url,
                "standards": repo.get_standards(req)
            }

        return output
