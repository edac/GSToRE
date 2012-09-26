from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref

import os
from lxml import etree

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

'''
metadata
(not called metadata to avoid any confusion/conflict with the sqlalchemy object)
'''


#intermediate table
class OriginalMetadata(Base):
    __table__ = Table('original_metadata', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('original_xml', String),
        Column('original_text', String),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        schema='gstoredata'
    )

    migrated_metadata = relationship('DatasetMetadata')

    def __repr__(self):
        return '<Original Metadata (%s)>' % (self.id)

    #generate the metadata for standard + format
    #except it's onyl fgdc today
    #TODO: fix the to text transform (empty? response)
    def transform(self, standard, format, xslt_path):
        #TODO: update this for iso, fgdc, dc, etc
        #TODO: xml-to-text transform iffy? or esri metadata in database so not valid fgdc
        #original xslt - 'fgdc_classic_rgis.xsl'
        xslt = 'fgdc-details_update.xslt' if format == 'html' else 'xml-to-text.xsl'

        xml = self.original_xml

        if format == 'xml':
            return xml.encode('utf8'), 'text/xml; charset=UTF-8'

        #or transform it
        xslt_path = os.path.join(xslt_path, xslt)
        xsltfile = open(xslt_path, 'r')

        try:
            content_type = 'text/html; charset=UTF-8' if format == 'html' else 'text; charset=UTF-8'
            xslt = etree.parse(xsltfile)
            transform = etree.XSLT(xslt)
            xml_enc = etree.XML(xml.encode('utf8'))
            output = transform(xml_enc)
            output = etree.tostring(output, encoding='utf8')

            #if the transform failed without error
            if not output:
                return xml.encode('utf8'), 'text/xml; charset=UTF-8'
                
            return output, content_type
        except:
            return xml.encode('utf8'), 'text/xml; charset=UTF-8'

    #export the xml to a file
    def write_xml_to_disk(self, filename):
        xml = self.original_xml.encode('utf-8')
        with open(filename, 'w') as f:
            f.write(xml)
        


#TODO: implement the full metadata schema
#this is not that schema. it's just an intermediate widget for dataone uuids
class DatasetMetadata(Base):
    __table__ = Table('metadata', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('original_id', Integer, ForeignKey('gstoredata.original_metadata.id')),
        schema = 'gstoredata'
    )
