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
class DatasetMetadata(Base):
    __table__ = Table('original_metadata', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('original_xml', String),
        Column('original_text', String),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        schema='gstoredata'
    )

    def __repr__(self):
        return '<Original Metadata (%s)>' % (self.id)

    #generate the metadata for standard + format
    #except it's onyl fgdc today
    #TODO: fix the to text transform (empty? response)
    def transform(self, standard, format, xslt_path):
        #TODO: update this for iso, fgdc, dc, etc
        #TODO: xml-to-text transform iffy? or esri metadata in database so not valid fgdc
        xslt = 'fgdc_classic_rgis.xsl' if format == 'html' else 'xml-to-text.xsl'

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

            return output, content_type
        except:
            return xml.encode('utf8'), 'text/xml; charset=UTF-8'


#TODO: implement the full metadata schema
