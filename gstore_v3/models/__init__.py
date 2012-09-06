from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

DBSession = scoped_session(sessionmaker())
#DBMetadata = MetaData()
Base = declarative_base()

DataoneSession = scoped_session(sessionmaker())
#DateoneMetadata = MetaData()
DataoneBase = declarative_base()


def initialize_sql(engines):
    DBSession.configure(bind=engines[0])
    Base.metadata.bind = engines[0]
    Base.metadata.create_all(engines[0])

    DataoneSession.configure(bind=engines[1])
    DataoneBase.metadata.bind = engines[1]
    DataoneBase.metadata.create_all(engines[1])

    
    
    
#if the models have been modified - try running this to reset (maybe)
#../bin/initialize_tutorial_db development.ini
    
