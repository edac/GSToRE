from sqlalchemy import Column
from sqlalchemy import Unicode
from sqlalchemy import Integer, String

from gstore_v3.models import Base

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

#these are in the init script so ignore them
#DBSession = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))
#Base = declarative_base()

#__all__ = ['MyModel']


# class MyModel(Base):
#     # __tablename__ = 'models'
# #     id = Column(Integer, primary_key=True)
# #     name = Column(Unicode(255), unique=True)
# #     value = Column(Integer)
# # 
# #     def __init__(self, name, value):
# #         self.name = name
# #         self.value = value
#     __tablename__ = 'gstoredata.app'
#     id = Column(Integer, primary_key=True)
#     name = Column(String(255))
# #    domain = Column(String(250))
#     long_name = Column(String(250))
#     api_key = Column(String(250))
#     
#     def __init__(self, name):
#         self.name = name
# #         self.domain = domain
# #         self.long_name = long_name
# #         self.api_key = api_key


#ORIGINAL MODELS CODE
# from sqlalchemy import (
#     Column,
#     Integer,
#     Text,
#     )
# 
# from sqlalchemy.ext.declarative import declarative_base
# 
# from sqlalchemy.orm import (
#     scoped_session,
#     sessionmaker,
#     )
# 
# from zope.sqlalchemy import ZopeTransactionExtension
# 
# DBSession = scoped_session(sessionmaker(extension=ZopeTransactionExtension()))
# Base = declarative_base()
# 
# class MyModel(Base):
#     __tablename__ = 'models'
#     id = Column(Integer, primary_key=True)
#     name = Column(Text, unique=True)
#     value = Column(Integer)
# 
#     def __init__(self, name, value):
#         self.name = name
#         self.value = value

