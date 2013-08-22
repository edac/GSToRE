from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey, Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

'''
gstoredata.collections and join table
'''
collections_datasets = Table('collections_datasets', Base.metadata,
    Column('collection_id', Integer, ForeignKey('gstoredata.collections.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)

class Collection(Base):
    __table__ = Table('collections', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('description', String(200)),
        Column('apps', ARRAY(String)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

    #relate with datasets (see Dataset)

    #relate with categories with the backref
    categories = relationship('Category',
                    secondary='gstoredata.categories_collections',
                    backref='collections')

    def __init__(self, name, apps):  
        self.name = name
        self.apps = apps

    def __repr__(self):
        return '<Collection (%s, %s, %s)>' % (self.id, self.name, self.uuid)

#and the collection-to-category join
collections_categories = Table('categories_collections', Base.metadata,
    Column('collection_id', Integer, ForeignKey('gstoredata.collections.id')),
    Column('category_id', Integer, ForeignKey('gstoredata.categories.id')),
    schema='gstoredata'
)


