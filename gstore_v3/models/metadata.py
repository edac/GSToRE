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

#this is bad
from pyramid.threadlocal import get_current_registry

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

    def append_onlink(self, base_url):
        '''
        add all of the download and service options for the dataset as onlink (online linkages)
        to the citation (citeinfo) element
        '''
        xml = self.original_xml
        if not xml:
            return None

        #parse the xml and insert all of the links
        mod_xml = etree.fromstring(xml.encode('utf-8'))

        citation = mod_xml.find('idinfo/citation/citeinfo')
        if citation is None:
            return None

        #remove any exsting onlinks if they are not valid urls (or at least they don't start with http)
        existing_onlinks = citation.findall('onlink')
        if existing_onlinks:
            for existing_onlink in existing_onlinks:
                if existing_onlink.text[0:4] != 'http':
                    citation.remove(existing_onlink)

        #get all of the links
        #TODO: maybe not this but for now, let's just roll with it
        #go get the dictionary for the dataset
        dct = self.datasets.get_full_service_dict(base_url, None)

        services = dct['services'][0].values() if dct['services'] else []
        downloads = dct['downloads'][0].values() if dct['downloads'] else []
        
#        #TODO: add metadata links to the online linkages. that's a little meta.
#        metadatas = dct['metadata'] if 'metadata' in dct else []
#        #flatten the dictionary of links by standard
#        metadatas =  sum([e.values() for e in a.values() for a in metadatas if a], [])      

#        onlinks = services + downloads + metadatas

        onlinks = services + downloads

        for onlink in onlinks:
            link = etree.SubElement(citation, 'onlink')
            link.text = onlink
        
        return etree.tostring(mod_xml)

    #generate the metadata for standard + format
    #except it's onyl fgdc today
    def transform(self, standard, format, xslt_path, base_url=''):
        #TODO: update this for iso, fgdc, dc, etc
        #TODO: xml-to-text transform iffy? or esri metadata in database so not valid fgdc
        #NOTE: dropped the text transform as not particularly necessary
        #original xslt - 'fgdc_classic_rgis.xsl' 
        xslt = 'fgdc-details_update.xslt' if format == 'html' else 'xml-to-text.xsl'

        xml = self.original_xml

        if standard == 'fgdc' and base_url:
            xml = self.append_onlink(base_url)

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
    def write_xml_to_disk(self, filename, include_onlinks=False, base_url=''):
        xml = self.original_xml

        if not xml:
            return False

        if include_onlinks and base_url:
            xml = self.append_onlink(base_url)
            
        with open(filename, 'w') as f:
            f.write(xml.encode('utf-8'))

        return True


#TODO: implement the full metadata schema
#this is not that schema. it's just an intermediate widget for dataone uuids
class DatasetMetadata(Base):
    __table__ = Table('metadata', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('original_id', Integer, ForeignKey('gstoredata.original_metadata.id')),
        schema = 'gstoredata'
    )
