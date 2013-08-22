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
from ..lib.utils import transform_xml, validate_xml

#this is bad
from pyramid.threadlocal import get_current_registry


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
    "FGDC-STD-012-2002|HTML": "gstore_to_fgdc_html.xsl"
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


    def convert_to_gstore_metadata(self, xslt_path):
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

            #create a new metadata obj if one doesn't exist for the dataset_id
            if self.migrated_metadata:
                self.migrated_metadata[0].gstore_xml = gstore_xml
            else:
                #create a new one
                gm = DatasetMetadata(dataset_id=self.dataset_id, gstore_xml=gstore_xml)
                DBSession.add(gm)
            DBSession.commit()
        except Exception as e:
            raise e

        #this is stupid
        return 'success'

        '''
from gstore_v3.models import *
xslt_path = request.registry.settings['XSLT_PATH'] + '/xslts'
om = DBSession.query(metadata.OriginalMetadata).filter(metadata.OriginalMetadata.id==58336).first()
om.convert_to_gstore_metadata(xslt_path)



http://129.24.63.115/apps/rgis/datasets/6d7801b8-68e9-4d92-8ba7-dc590157b093/metadata/fgdc.xml
http://129.24.63.115/apps/rgis/datasets/0cd9878b-dd39-40fb-aef0-0dd41862983a/metadata/fgdc.xml
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
        Column('date_modified', TIMESTAMP),
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

        onlinks = d.get_onlinks(metadata_info['base_url'])
        distribution_links = d.get_distribution_links(metadata_info['base_url'])

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
        
        elements_to_update = {"identifier": str(d.uuid), "title": d.description, "onlinks": onlinks, "base_url": metadata_info['base_url'], "distribution": distribution_info, "publications": d.citations} 
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
            params.update({"service-type": svc})
            params.update({"service-version": '1.1.1' if svc == 'wms' else '1.0.0' if svc == 'wfs' else '1.1.2'})
            params.update({"service-base-url": '%s%s/services/ogc' % (metadata_info['base_url'], d.uuid) if 'service' in metadata_info else ''})

        if '19115' in out_standard and d.taxonomy == 'vector':
            #add the param for the fc link
            params.update({"fc-url": '%s%s/metadata/ISO-19110.xml' % (metadata_info['base_url'], d.uuid) if 'base_url' in metadata_info else ''})

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

    def __repr__(self):
        return '<MetadataStandard %s (%s)>' % (self.alias, ','.join(self.supported_formats))

  
    
