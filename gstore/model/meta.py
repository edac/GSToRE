"""SQLAlchemy Metadata and Session object"""
from sqlalchemy import MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

__all__ = ['Base', 'engine', 'Session', 'metadata']

# SQLAlchemy session manager. Updated by model.init_model()
Session = scoped_session(sessionmaker())
engine = None

# The declarative Base
Base = declarative_base()
metadata = Base.metadata
