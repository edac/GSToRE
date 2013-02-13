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

from ..models.apps import GstoreApp
from ..models.standards import FGDC

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
        Column('original_xml_standard', String(15)),
        Column('date_modified', TIMESTAMP),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        schema='gstoredata'
    )

    migrated_metadata = relationship('DatasetMetadata')

    def __repr__(self):
        return '<Original Metadata (%s)>' % (self.id)

    
    def get_as_xml(self):
        '''
        convert the text blob to xml
        '''
        text = self.original_xml
        if not text:
            return None
        
        try:
            return etree.fromstring(text.encode('utf-8'))
        except:
            return None

    def generate_download_links(self, dataset, downloads):
        '''
        rebuild the list of downloads to include the transfer size, etc, required for the metadata
        '''
        new_downloads = {}
        for k, v in downloads.iteritems():
            #let's go find the source so it will be a filesize value instead of a not very good estimate
            if k != 'html':
                name = v.split('/')[-1]
                aset = 'original' if '.original.%s' % k in name else 'derived'
                source = dataset.get_source(aset, k, True)
            else:
                source = None    
        
        
            if not source and dataset.taxonomy == 'vector':
                #this is not very accurate
                #get an estimate based on number of records and number of attributes (without knowing the avg attr size or vector size)
                recs = dataset.record_count
                geoms = dataset.feature_count
                attrs = len(dataset.attributes)
                
                #the very wrong, no good estimates
                if dataset.geomtype.lower() == 'polygon':
                    multiplier = 0.0005
                elif dataset.geomtype.lower() == 'multipolygon':
                    multiplier = 0.0005
                elif 'linestring' in dataset.geomtype.lower():
                    multiplier = 0.0007
                else:
                    multiplier = 0.0001
                
                if '3d' in dataset.geomtype.lower():
                    multiplier += 0.002
                
                multiplier += attrs * 0.00001
                
                transize = multiplier * recs if recs == geoms else multiplier * recs * geoms
                
                #bin it because they are very bad estimates
                if transize < 5:
                    transize = 5
                elif transize >= 5 and transize < 25:
                    transize = 25
                elif transize >= 25 and transize < 50:
                    transize = 50
                elif transize >= 50 and transize < 100:
                    transize = 100
                    
            elif not source:
                transize = -99
            
            else:
                #hooray, we don't have to guess
                #for those playing along, the performance hit for generating the gstore download is 
                #too big for just updating an element in the metadata (are seven)
                transize = source.get_filesize_mb()
            
            new_downloads.update({k: {'transize': transize, 'link': v}})
            
        #our new dictionary of links + formats + file sizes/estimates    
        return new_downloads
        
    def update_fgdc(self, app, base_url, dataone={}):
        '''
        revise the existing fgdc xml to include the gstore onlinks, the correct distinfo info, the correct 
        metadata contact, the dataset description as title and the dataset id if raster
        '''
        dataset = self.datasets
        
        #get the json response so we can grab the onlink urls and the download urls
        if not dataone:
            dct = dataset.get_full_service_dict(base_url, None)
#            services = dct['services'][0].values() if dct['services'] else []
#            metadatas = dct['metadata'] if dct['metadata'] else {}
#            downloads = dct['downloads'][0].values() if dct['downloads'] else []

            #get the flattened list
            #TODO: ESCAPE THE SERVICE LINKS (version, etc with AMP)
            services = dct['services'] if dct['services'] else []
            services = [link for values in services for link in values.itervalues()]
            
            downloads = dct['downloads'][0].values() if dct['downloads'] else []

            #flatten the list for the metadata links (std: {})
            #metadatas = sum([e.values() for e in metadatas['fgdc'].values()], []) if 'fgdc' in metadatas and metadatas else []

            metadatas = dct['metadata'] if dct['metadata'] else []
            metadatas = [m for m in metadatas if m['fgdc']]
            metadatas = metadatas[0]['fgdc'].values() if metadatas else []
            
            #tada: online linkages
            onlinks = services + downloads + metadatas
        
            #modify the downloads part for transize, etc
            downloads = self.generate_download_links(dataset, dct['downloads'][0]) if dct['downloads'][0] else {}
            
            taxonomy = dataset.taxonomy
        else:
            #use the dataone elements that were just passed in (do not want to figure out what the source url is meant to be here)
            onlinks = dataone['onlinks']
            downloads = dataone['downloads']
            #TODO: change this if we can ever serve anything other than zips. 
            taxonomy = 'file'
        
        #need to get the info about the app (url, name, etc)
        gstoreapp = DBSession.query(GstoreApp).filter(GstoreApp.route_key==app).first()
        if not gstoreapp:
            #default to rgis and fingers crossed i guess
            gstoreapp = DBSession.query(GstoreApp).filter(GstoreApp.route_key=='rgis').first()

        #set up the fgdc xml obj
        xml = self.get_as_xml()
        fgdc = FGDC(xml, gstoreapp)

        return fgdc.update(dataset.uuid, dataset.description, taxonomy, onlinks, downloads)
        

    #generate the metadata for standard + format
    #except it's onyl fgdc today
    def transform(self, format, xslt_path, metadata_info):
        #TODO: update this for iso, fgdc, dc, etc
        #TODO: xml-to-text transform iffy? or esri metadata in database so not valid fgdc
        #NOTE: dropped the text transform as not particularly necessary
        
        '''
        metadata info keys:
            base_url: string
            app: string
            standard: string 
            dataone: {'onlink': string, 'distlink': string, 'distsize': int, 'distfmt': string}
            
        NOTE: dataone is really just for the xml to disk option, to see the metadata for a science metadata obj, use the d1 api

        NOTE: xml is the TEXT representation and not an actual etree obj 
        '''        
        standard = metadata_info['standard'] if 'standard' in metadata_info else 'fgdc'
        if standard == 'fgdc':
            xslt = 'fgdc-details_update.xslt' if format == 'html' else 'xml-to-text.xsl'
        else:
            return None, None

        #update all the fgdc bits we know need updating for the original_xml blob o text 
        if standard == 'fgdc' and metadata_info['base_url']:
            xml = self.update_fgdc(metadata_info['app'], metadata_info['base_url'], metadata_info['dataone'] if 'dataone' in metadata_info else {})
        else:
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
            
        #borked    
        return None, None

    #export the xml to a file
    def write_xml_to_disk(self, filename, metadata_info):
        '''
        
        '''
        if self.original_xml_standard == 'fgdc' and metadata_info:
            xml = self.update_fgdc(metadata_info['app'], metadata_info['base_url'])
        else:
            xml = self.original_xml

        if not xml:
            return False

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
