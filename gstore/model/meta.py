"""SQLAlchemy Metadata and Session object"""
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import caching_query

from pylons import cache

__all__ = ['Base', 'Session']

# SQLAlchemy session manager. Updated by model.init_model()
Session = scoped_session(
    sessionmaker(
        query_cls = caching_query.query_callable(cache)        
    )
)

# The declarative Base
Base = declarative_base()
