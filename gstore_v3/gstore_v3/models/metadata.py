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
from ..models.standards import GstoreMetadata
from ..lib.utils import *

#this is bad
from pyramid.threadlocal import get_current_registry


<<<<<<< HEAD
=======

>>>>>>> gstore/master
'''
temp definition of xslt filenames (path from the config and must be passed in)
'''
TO_GSTORE_XSLTS = {
    "FGDC-STD-001-1998": "fgdc_to_gstore.xsl",
    "FGDC-STD-012-2002": "fgdc_to_gstore.xsl",
    "ISO-19115:2003": "iso_to_gstore-ns.xsl"
}
FROM_GSTORE_XSLTS = {
    "FGDC-STD-001-1998|XML": "gstore_to_fgdc.xsl",
    "FGDC-STD-012-2002|XML": "gstore_to_fgdc.xsl",
    "ISO-19115:2003|XML": "gstore_to_iso.xsl",
    "ISO-19115:2003|HTML": "gstore_to_iso_html.xsl",
    "ISO-19110|XML": "gstore_to_19110.xsl",
    "ISO-19119|XML": "gstore_to_19119.xsl",
    "FGDC-STD-001-1998|HTML": "gstore_to_fgdc_html.xsl",
    "FGDC-STD-012-2002|HTML": "gstore_to_fgdc_html.xsl",
<<<<<<< HEAD
    "ISO-19115:DS|XML": "gstore_to_ds.xsl"
=======
    "ISO-19115:DS|XML": "gstore_to_ds.xsl",
    "ISO-19115:MD|XML": "gstore_to_md.xsl",
    "apollo|XML": "gstore_to_apollo.xsl",
    "apollo_FGDC|XML": "gstore_to_apollo_FGDC.xsl"
>>>>>>> gstore/master
}


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

    '''
    the xml metadata as received from wherever. if possible, it is transformed to the gstore schema and 
    saved in gstoredata.metadata. if not possible (wonky iso, unsupported standard, etc), it stays here
    and is returned as xml, unmodified, when requested. 

    there are no transformations and no modifications to this xml.
    '''

    migrated_metadata = relationship('DatasetMetadata', primaryjoin='DatasetMetadata.dataset_id==OriginalMetadata.dataset_id', foreign_keys='[DatasetMetadata.dataset_id]', backref="original_metadata")

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


    def convert_to_gstore_metadata(self, xslt_path, commit=True):
        '''
        take the original xml and convert it to the gstore schema
        if valid, store in the datasetmetadata table
        
        '''
        xslt_fn = TO_GSTORE_XSLTS[self.original_xml_standard] if self.original_xml_standard in TO_GSTORE_XSLTS else ''
        if not xslt_fn:
            return None

        try:
            #open the xml and set the encoding just in case
            xml = self.get_as_xml()

            gstore_xml = transform_xml(etree.tostring(xml, encoding=unicode), os.path.join(xslt_path, xslt_fn), {})

            if commit:
                #create a new metadata obj if one doesn't exist for the dataset_id
                if self.migrated_metadata:
                    self.migrated_metadata[0].gstore_xml = gstore_xml
                else:
                    #create a new one
                    gm = DatasetMetadata(dataset_id=self.dataset_id, gstore_xml=gstore_xml)
                    DBSession.add(gm)
                DBSession.commit()
            else:
                return gstore_xml
        except Exception as e:
            raise e

        #this is stupid
        return 'success'

        '''
from gstore_v3.models import *
xslt_path = request.registry.settings['XSLT_PATH'] + '/xslts'
om = DBSession.query(metadata.OriginalMetadata).filter(metadata.OriginalMetadata.id==58336).first()
om.convert_to_gstore_metadata(xslt_path)



        '''

#TODO: implement the full metadata schema
#this is not that schema. it's just an intermediate widget for dataone uuids
class DatasetMetadata(Base):
    __table__ = Table('metadata', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('gstore_xml', String),
        #Column('original_id', Integer, ForeignKey('gstoredata.original_metadata.id')),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        Column('date_modified', TIMESTAMP, FetchedValue()),
        schema = 'gstoredata'
    )

    '''
    the gstore metadata (gstore schema). from this, we can transform into whatever standard we
    support (and have xslts for).

    '''

    def get_as_xml(self):
        '''
        convert the text blob to xml
        '''
        text = self.gstore_xml
        if not text:
            return None
        
        try:
            return etree.fromstring(text.encode('utf-8'))
        except:
            return None

    def transform(self, out_standard, out_format, xslt_path, metadata_info, validate=True):
        '''
        - transform the gstore_xml to whatever standard specified
        - strip in all of the extra bits
        - validate
        - return complete xml

        metadata_info:
            app
            base_url
        '''

        xslt_fn = FROM_GSTORE_XSLTS['%s|%s' % (out_standard, out_format.upper())] if '%s|%s' % (out_standard, out_format.upper()) in FROM_GSTORE_XSLTS and out_standard != 'GSTORE' else ''
        if not xslt_fn and out_standard != 'GSTORE':
            return 'No matching stylesheet'

        #for the fgdc only
        ref_path = ''
        if ('fgdc' in out_standard.lower() or 'gstore' in out_standard.lower()) and xslt_path:
            ref_path = os.path.join(xslt_path, 'spatialrefs.xml')
            ref_path = '' if not os.path.isfile(ref_path) else ref_path

        xslt_path = os.path.join(xslt_path, xslt_fn)

        d = self.datasets

        #strip in all of the extra bits: onlinks, distribution links, distributor, metadata pubdate, metadata contact, spref info (if fgdc), publication citations

        onlinks = metadata_info['onlinks'] if 'onlinks' in metadata_info else d.get_onlinks(metadata_info['base_url'], metadata_info['request'], metadata_info['app'])
        distribution_links = metadata_info['distribution_links'] if 'distribution_links' in metadata_info else d.get_distribution_links(metadata_info['base_url'], metadata_info['request'], metadata_info['app'])
            

        #get the rest of the distribution info (liability, etc)
        #TODO: revise this and the method (ordering v instructions - something is whacky)
        distribution_info = {
            "links": distribution_links,
            "liability": "The material on this site is made available as a public service. Maps and data are to be used for reference purposes only and the Earth Data Analysis Center (EDAC), %s and The University of New Mexico are not responsible for any inaccuracies herein contained. No responsibility is assumed for damages or other liabilities due to the accuracy, availability, use or misuse of the information herein provided. Unless otherwise indicated in the documentation (metadata) for individual data sets, information on this site is public domain and may be copied without permission; citation of the source is appreciated." % metadata_info['app-name'],
            "ordering": "Contact Earth Data Analysis Center at clearinghouse@edac.unm.edu",
            "instructions": "Contact Earth Data Analysis Center at clearinghouse@edac.unm.edu",
            "fees": "None. The files are available to download from %s (%s)." % (metadata_info['app-name'], metadata_info['app-url']),
            "access": "Download from %s at %s." % (metadata_info['app-name'], metadata_info['app-url']),
            "prereqs": "Adequate computer capability is the only technical prerequisite for viewing data in digital form.",
            "description": "Downloadable Data"
        }

        identifier = metadata_info['identifier'] if 'identifier' in metadata_info else str(d.uuid)

        #don't know why you would pass in anything not from the dataset, but dataone?
        bbox = metadata_info['bbox'] if 'bbox' in metadata_info else d.box
        pubdate = metadata_info['date_added'] if 'date_added' in metadata_info else d.dateadded
        timeperiod = metadata_info['timeperiod'] if 'timeperiod' in metadata_info else {}
        if not timeperiod:
            #try building it
            valid_start = d.begin_datetime
            valid_end = d.end_datetime
            timeperiod = {}
            if valid_start and not valid_end:
                timeperiod = {"single": valid_start}
            elif not valid_start and valid_end:
                #because a single date might be weirder?
                timeperiod = {"start": "Unknown", "end": valid_end}
            elif valid_start and valid_end:
                timeperiod = {"start": valid_start, "end": valid_end}
        
        elements_to_update = {"identifier": identifier, "title": d.description, "onlinks": onlinks, "base_url": metadata_info['base_url'], "distribution": distribution_info} 

        if d.citations:
            elements_to_update.update({"publications": d.citations})
        if bbox:
            elements_to_update.update({"bbox": bbox})
        if pubdate:
            elements_to_update.update({"pubdate": pubdate})    
        if timeperiod:
            elements_to_update.update({"timeperiod": timeperiod})
        
        xml = self.get_as_xml()
        
        gm = GstoreMetadata(xml)
        gm.update_xml(elements_to_update, out_standard, ref_path)    
        
        updated_xml = gm.get_as_text()

        if out_standard == 'GSTORE':
            #TODO: put the schema in first and then just punt
            #http://129.24.63.115/xslts/gstore_schema.xsd
            return updated_xml

        params = {} 
        if '19119' in out_standard:
            svc = metadata_info['service'] if 'service' in metadata_info else 'wms'
            vsn = '1.1.1' if svc == 'wms' else '1.0.0' if svc == 'wfs' else '1.1.2'
            params.update({"service-type": svc})
            params.update({"service-version": vsn})
            #params.update({"service-base-url": '%s%s/services/ogc' % (metadata_info['base_url'], d.uuid) if 'service' in metadata_info else ''})

            #strip off the query params for this request 
            #TODO: change the builder to not include query params maybe
            params.update({"service-base-url": (metadata_info['base_url'] + build_ogc_url(metadata_info['app'], 'datasets', d.uuid, svc, vsn)).split('?')[0] if 'service' in metadata_info else ''})

        if '19115' in out_standard and d.taxonomy in ['vector', 'table']:
            #add the param for the fc link
            #params.update({"fc-url": '%s%s/metadata/ISO-19110.xml' % (metadata_info['base_url'], d.uuid) if 'base_url' in metadata_info else ''})
            params.update({"fc-url": metadata_info['base_url'] + build_metadata_url(metadata_info['app'], 'datasets', d.uuid, 'ISO-19110', 'xml') if 'base_url' in metadata_info else ''})

        #we need to make sure we include the app string to generate unique fileIdentifiers for ISO (app::uuid::standard)
        if out_standard in ['ISO-19115:2003','ISO-19119']:
            params.update({"app": metadata_info['app'] if 'app' in metadata_info else 'rgis'})

        if out_format == 'html':
            params = {}

        output = transform_xml(updated_xml, xslt_path, params)

        if validate:
            valid = validate_xml(output)
            if 'ERROR' in valid:
                return None

        return output

    #TODO: these are silly, do something else to get the elements
    def get_abstract(self):
        xml = self.get_as_xml()
        gm = GstoreMetadata(xml)
        return gm.get_abstract()

    def get_isotopic(self):
        xml = self.get_as_xml()
        gm = GstoreMetadata(xml)
        return gm.get_isotopic()

    def get_keywords(self):
        xml = self.get_as_xml()
        gm = GstoreMetadata(xml)
        return gm.get_keywords()
        
    def write_to_disk(self, output_location, out_standard, out_format, xslt_path, metadata_info, validate=True):
        '''
        write the metadata to a file on disk
        after transforming to whatever flavor it needs to be
        '''

        output = self.transform(out_standard, out_format, xslt_path, metadata_info, validate)

        if output:
            with open(output_location, 'w') as f:
                f.write(output)
            return True

        return False

'''
gstore metadata for the collection objects (a little different, more on the output side than the input/storage side)
'''
class CollectionMetadata(Base):
    __table__ = Table('collection_metadata', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('gstore_xml', String),
        Column('collection_id', Integer, ForeignKey('gstoredata.collections.id')),
        Column('date_modified', TIMESTAMP),
        schema='gstoredata'
    )

    def get_as_xml(self):
        '''
        convert the text blob to xml
        '''
        text = self.gstore_xml
        if not text:
            return None
        
        try:
            return etree.fromstring(text.encode('utf-8'))
        except:
            return None

    def transform(self, out_standard, out_format, xslt_path, metadata_info, validate=True):
        '''
        - transform the gstore_xml to whatever standard specified
        - strip in all of the extra bits (THIS DOESN'T INCLUDE DOWNLOADY BITS)
        - validate
        - return complete xml

        metadata_info:
            app
            base_url
        '''

        xslt_fn = FROM_GSTORE_XSLTS['%s|%s' % (out_standard, out_format.upper())] if '%s|%s' % (out_standard, out_format.upper()) in FROM_GSTORE_XSLTS and out_standard != 'GSTORE' else ''
        if not xslt_fn and out_standard != 'GSTORE':
            return 'No matching stylesheet'

        #NOTE: we are not including the spatial reference info in the collection-level fgdc, just a bbox

        xslt_path = os.path.join(xslt_path, xslt_fn)

        c = self.collections

        onlinks = metadata_info['onlinks'] if 'onlinks' in metadata_info else c.get_onlinks(metadata_info['base_url'], metadata_info['request'], metadata_info['app'])

        dataset_links = c.get_dataset_links(metadata_info['base_url'], metadata_info['request'], metadata_info['app']) if 'FGDC' not in out_standard else []
       
        identifier = metadata_info['identifier'] if 'identifier' in metadata_info else str(c.uuid)
        
        elements_to_update = {"identifier": identifier, "title": c.name, "onlinks": onlinks, "base_url": metadata_info['base_url'], "dataset_links": dataset_links} 
        xml = self.get_as_xml()
        
        gm = GstoreMetadata(xml)
        gm.update_xml(elements_to_update, out_standard, '')    
        
        updated_xml = gm.get_as_text()

        if out_standard == 'GSTORE':
            #TODO: put the schema in first and then just punt
            #http://129.24.63.115/xslts/gstore_schema.xsd
            return updated_xml

        params = {} 
        #TODO: add the service link for the tile index if there is one
#        if '19119' in out_standard:
#            svc = metadata_info['service'] if 'service' in metadata_info else 'wms'
#            vsn = '1.1.1' if svc == 'wms' else '1.0.0' if svc == 'wfs' else '1.1.2'
#            params.update({"service-type": svc})
#            params.update({"service-version": vsn})
#            #params.update({"service-base-url": '%s%s/services/ogc' % (metadata_info['base_url'], d.uuid) if 'service' in metadata_info else ''})
#            params.update({"service-base-url": metadata_info['base_url'] + build_ogc_url(metadata_info['request'], metadata_info['app'], 'datasets', c.uuid, svc, vsn) if 'service' in metadata_info else ''})

        #we need to make sure we include the app string to generate unique fileIdentifiers for ISO (app::uuid::standard)
        if out_standard in ['ISO-19115:2003','ISO-19119']:
            params.update({"app": metadata_info['app'] if 'app' in metadata_info else 'rgis'})

        if out_format == 'html':
            params = {}

        output = transform_xml(updated_xml, xslt_path, params)

        if validate:
            valid = validate_xml(output)
            if 'ERROR' in valid:
                return None

        return output


    #TODO: these are silly, do something else to get the elements
    def get_abstract(self):
        xml = self.get_as_xml()
        gm = GstoreMetadata(xml)
        return gm.get_abstract()

    def get_isotopic(self):
        xml = self.get_as_xml()
        gm = GstoreMetadata(xml)
        return gm.get_isotopic()

    def get_keywords(self):
        xml = self.get_as_xml()
        gm = GstoreMetadata(xml)
        return gm.get_keywords()

    def write_to_disk(self, output_location, out_standard, out_format, xslt_path, metadata_info, validate=True):
        '''
        write the metadata to a file on disk
        after transforming to whatever flavor it needs to be
        '''

        output = self.transform(out_standard, out_format, xslt_path, metadata_info, validate)

        if output:
            with open(output_location, 'w') as f:
                f.write(output)
            return True

        return False



class MetadataStandards(Base):
    __table__ = Table('metadata_standards', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(100)),
        Column('alias', String(50)),
        Column('description', String(500)),
        Column('supported_formats', ARRAY(String)),
        schema = 'gstoredata'
    )
    '''
    the list of metadata standards currently supported


    >>> from gstore_v3.models import *
    >>> fmt = "'xml'=ANY(supported_formats)"
    >>> from sqlalchemy.sql.expression import and_
    >>> ms = DBSession.query(metadata.MetadataStandards).filter(and_(metadata.MetadataStandards.alias=='FGDC-STD-001-1998',fmt)).first()

    '''

    scimeta_objects = relationship('DataoneScienceMetadataObject', backref='standards')

    def __repr__(self):
        return '<MetadataStandard %s (%s)>' % (self.alias, ','.join(self.supported_formats))

    def get_urls(self, app, doctype, baseurl, identifier, services=[]):
        '''
        return the set of urls for the standard + supported formats

        pass if it's 19119 and there's no service array
        '''

        md_fmts = {}
        if self.alias == 'ISO-19119' and services:
            for service in services:
                md_fmts['%s:%s' % (self.alias, service.upper())] = self.supported_formats
        elif self.alias != 'ISO-19119':
            md_fmts[self.alias] = self.supported_formats

        return [{s: {e: baseurl + build_metadata_url(app, doctype, identifier, s, e) for e in md_fmts[s]} for s in md_fmts}]  
        
  
