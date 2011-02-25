import os
import new
import meta
from pylons import config

from sqlalchemy import *

import zipfile, tempfile

from urlparse import urlparse


__all__ = ['Resource', 'sources_table', 'datasets_sources_table'] 

sources_table = Table('source', meta.Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('location', String),
    Column('type', String, default = 'file'),
    Column('is_external', Boolean, default = False),
    Column('extension', String(3)),
    Column('zipgroup', String),
    Column('orig_epsg', Integer),
    Column('active', Boolean)
)

datasets_sources_table = Table('datasets_sources', meta.Base.metadata,
    Column('source_id', Integer, ForeignKey('source.id')),
    Column('dataset_id', Integer, ForeignKey('datasets.id'))
)


class FileSource(object):
    def __init__(self, location, zipgroup = None):
        self.location = location
        if zipgroup:
            self.zipgroup = zipgroup

    def set_url_stats(self):
        url = urlparse(self._url)
        if url.scheme == 'file' or os.path.isfile(url.path):
            # All files are locally accesible
            self._url = url.path
            filename = self._url 
            (_basename, extension) = os.path.splitext(filename)
            name = os.path.basename(filename)
            self.name = name
            self.basename = name.split('.')[0]
            self.extension = extension.strip('.')
            self.size = os.path.getsize(filename)
            self.dirname = os.path.dirname(filename)
        else:
            self.is_external = True
            (_basename, extension) = os.path.splitext(url.path)
            name = os.path.basename(url.path)
            self.basename = name.split('.')[0]
            self.extension = extension.strip('.') 
             
    def _set_url_info(self, location):
        self._url = location
        self.set_url_stats()

    def _get_url_location(self):
        return self._url

    location = property(_get_url_location, _set_url_info)

    def check_valid_url(self, url):
        pass

    def __repr__(self):
        return "<Resource(%s, %s)>" % (self.location, self.zipgroup)

Resource = new.classobj('Resource', (FileSource,), {})    
    


