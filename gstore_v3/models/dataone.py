from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref

from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

from mako.template import Template

import os, shutil, tempfile
from ..lib.utils import generate_hash
from ..models.datasets import Dataset
from ..models.metadata import DatasetMetadata, MetadataStandards


'''
dataone models

three basic models for the gstore to d1 registration:
    dataset to data object (dataset uuid, format)
    dataset.metadata to science metadata object
    data object + science metadata to data package

    these generally each have a 'register dataset' and a generate() something method 


dataone obsoletes is the versioning component for any core objects. basically core is the go-between 
for gstore<->dataone. 

        


'''

class DataoneDataObject(Base):
    __table__ = Table('dataone_dataobjects', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        Column('dataset_format', String(20)),
        Column('format_id', Integer, ForeignKey('gstoredata.dataone_formatids.id')),
        Column('date_added', TIMESTAMP, FetchedValue()),
        schema='gstoredata'
    )

    formats = relationship('DataoneFormat', backref='dataobjects')

    def __init__(self, dataset_id, dataset_format, format_id):
        self.dataset_id = dataset_id
        self.dataset_format = dataset_format
        self.format_id = format_id

    def __repr__(self):
        return '<DataObject (%s, %s, %s, %s)>' % (self.uuid, self.dataset_id, self.dataset_format, self.format_id)

    def get_current_obsolete(self, use_all=False):
        '''
        return the most current obsolete uuid 
        where active only for use_all = False or most recent period for use_all = True
        '''

        core = DBSession.query(DataoneCore).filter(DataoneCore.object_uuid==self.uuid).first()
        if not core:
            return None

        filters= [DataoneObsolete.core_id==core.id]
        if not use_all:
            filters.append(DataoneObsolete.active==True)
        obsolete = DBSession.query(DataoneObsolete).filter(and_(*filters)).order_by(DataoneObsolete.date_changed.desc()).first()
        if not obsolete:
            return None
            
        return obsolete

    def register_object(self):
        '''
        push to core
        push core to obsolete
        '''

        #push to core
        core = DataoneCore(self.uuid, 'data object')
        try:
            DBSession.add(core)
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(core)
        except Exception as ex:
            DBSession.rollback()
            raise ex

        #push to obsolete
        obsolete = DataoneObsolete(core.id)
        try:
            DBSession.add(obsolete)
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(obsolete)
        except Exception as ex:
            DBSession.rollback()
            raise ex

<<<<<<< HEAD
        sysmeta = DataoneSystemMetadata(obsolete.id)
        #TODO: not hardcode this. i hate dataone   
        sysmeta.replication_policy = False
        sysmeta.access_policies = '<allow><subject>public</subject><permission>read</permission></allow>'
        try:
            DBSession.add(sysmeta)
            DBSession.commit()
        except:
            DBSession.rollback()
            raise

=======
>>>>>>> gstore/master
        return core.object_uuid, obsolete.uuid

    def register_dirty_object(self):
        '''
        get the core and add a new obsolete uuid
        '''
        core = DBSession.query(DataoneCore).filter(DataoneCore.object_uuid==self.uuid).first()
        if not core:
            raise Exception('no core')

        #push to obsolete
        obsolete = DataoneObsolete(core.id)
        try:
            DBSession.add(obsolete)
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(obsolete)
        except Exception as ex:
            DBSession.rollback()
            raise ex
         
    def activate_object(self):
        '''
        activate core
        activate obsolete
        '''
        core = DBSession.query(DataoneCore).filter(DataoneCore.object_uuid==self.uuid).first()
        if not core:
            raise Exception('no core')

        obsolete = self.get_current_obsolete(True)

        core.active = True
        obsolete.active = True

        try:
            DBSession.commit()       
        except:
            DBSession.rollback()
            raise Exception('activate data object error: %s' % self.uuid)   

    def generate_object(self, output_path, obsolete_uuid, epsg=None, mongo_uri=None, metadata_info={}):  
        '''
        build the object, based on the format type (zip vs csv, for example)
        store it in the dataone data object cache
        with the obsolete uuid

        note: the obsolete uuid has to be NEW and inactive while we're generating everything


        the metadata_info dict needs to have the d1 distribution links and onlinks
        '''

        '''

        from gstore_v3.models import *
        from gstore_v3.lib import mongo
        o = DBSession.query(dataone.DataoneDataObject).first()
        mconn = request.registry.settings['mongo_uri']
        mcoll = request.registry.settings['mongo_collection']
        mongo_uri = mongo.gMongoUri(mconn, mcoll)
        epsg = request.registry.settings['SRID']
        n = o.get_current_obsolete(True)
        xslt = request.registry.settings['XSLT_PATH'] + '/xslts'
        op = '/home/sscott/metadata/d1/datasets'

        metadata_info = {"app": "dataone", "distribution_links": [{"link": '%s/dataone/v1/object/%s' % ('http://129.24.63.115', n.uuid), "type": "zip", "size": 0}], "onlinks": ['%s/dataone/v1/object/%s' % ('http://129.24.63.115', n.uuid)], "base_url": 'http://129.24.63.115', "xslt_path": xslt, "identifier": n.uuid, "standard": "FGDC-STD-001-1998"}

        o.generate_object(op, n.uuid, epsg, mongo_uri, metadata_info)



        metadata_info = {"app": "dataone", "distribution_links": [{"link": '%s/dataone/v1/object/%s' % ('http://129.24.63.115', n.uuid), "type": "zip", "size": 0}], "onlinks": ['%s/dataone/v1/object/%s' % ('http://129.24.63.115', n.uuid)], "base_url": 'http://129.24.63.115', "xslt_path": xslt, "identifier": n.uuid, "standard": "FGDC-STD-012-2002"}
        '''


        d = self.datasets

        #try the source table first (meh)
        src = d.get_source('original', self.dataset_format)
        src = d.get_source('derived', self.dataset_format) if not src else src
        if not src and d.taxonomy in ['geoimage', 'file']:
            #this is a problem
            raise Exception('how do i not have the source file for this dataset?')

        if not os.path.isdir(os.path.join(output_path, self.dataset_format.lower())):       
            os.mkdir(os.path.join(output_path, self.dataset_format.lower()))

        #if taxonomy==vector and no source, go build it
        if d.taxonomy == 'vector' and self.formats.format == 'text/csv':
            #no zip
            tmp_csv_file = d.stream_text('csv', '', mongo_uri, epsg)
            #move it to the cache with the obsolete uuid as the new name
            shutil.move(tmp_csv_file, os.path.join(output_path, 'csv', '%s.csv' % obsolete_uuid))
        elif d.taxonomy == 'vector' and not src:
            #build the data and metadata to a tmp file (do not want to ever use the metadata for gstore proper)
            tmp_path = tempfile.mkdtemp()
            status, msg = d.stream_vector(self.dataset_format, tmp_path, mongo_uri, epsg, metadata_info)
            if status:
                raise Exception('failed vector build')

            shutil.move(os.path.join(tmp_path, '%s_%s.zip' % (d.basename, self.dataset_format)), os.path.join(output_path, self.dataset_format.lower(), '%s.zip' % obsolete_uuid))
        else:
            #zip
            if not src:
                raise Exception('no source files')

            #build with the basename first and then copy (we'll need the basename for the metadata anyway)
            tmp_path = tempfile.mkdtemp()
            success = src.pack_source(tmp_path, '%s_%s.zip' % (d.basename, self.dataset_format), metadata_info['xslt_path'], metadata_info)
            if not success:
                raise Exception('no cached source')
        
            shutil.move(os.path.join(tmp_path, '%s_%s.zip' % (d.basename, self.dataset_format)), os.path.join(output_path, self.dataset_format.lower(), '%s.zip' % obsolete_uuid))
        

class DataoneScienceMetadataObject(Base):
    __table__ = Table('dataone_sciencemetadata', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
        Column('standard_id', Integer, ForeignKey('gstoredata.metadata_standards.id')),
        Column('format_id', Integer, ForeignKey('gstoredata.dataone_formatids.id')),
        Column('date_added', TIMESTAMP, FetchedValue()),
        schema="gstoredata"
    )

    formats = relationship('DataoneFormat', backref='scimetaobjects')

    def __init__(self, dataset_id, standard_id, format_id):
        self.dataset_id = dataset_id
        self.standard_id = standard_id
        self.format_id = format_id

    def __repr__(self):
        return '<ScienceMetadataObject (%s, %s, %s, %s)>' % (self.uuid, self.dataset_id, self.standard_id, self.format_id)

    def get_current_obsolete(self, use_all=False):
        '''
        return the most current obsolete uuid 
        where active only for use_all = False or most recent period for use_all = True
        '''
        core = DBSession.query(DataoneCore).filter(DataoneCore.object_uuid==self.uuid).first()
        if not core:
            return None

        filters= [DataoneObsolete.core_id==core.id]
        if not use_all:
            filters.append(DataoneObsolete.active==True)
        obsolete = DBSession.query(DataoneObsolete).filter(and_(*filters)).order_by(DataoneObsolete.date_changed.desc()).first()
        if not obsolete:
            return None
            
        return obsolete

    def register_object(self):
        '''
        push to core
        push core to obsolete
        '''

        #push to core
        core = DataoneCore(self.uuid, 'science metadata object')
        try:
            DBSession.add(core)
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(core)
        except:
            DBSession.rollback()
            raise

        #push to obsolete
        obsolete = DataoneObsolete(core.id)
        try:
            DBSession.add(obsolete)
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(obsolete)
        except:
            DBSession.rollback()
            raise

<<<<<<< HEAD
        sysmeta = DataoneSystemMetadata(obsolete.id)
        #TODO: not hardcode this. i hate dataone   
        sysmeta.replication_policy = False
        sysmeta.access_policies = '<allow><subject>public</subject><permission>read</permission></allow>'
        try:
            DBSession.add(sysmeta)
            DBSession.commit()
        except:
            DBSession.rollback()
            raise

=======
>>>>>>> gstore/master
        return core.object_uuid, obsolete.uuid

    def register_dirty_object(self):
        '''
        get the core and add a new obsolete uuid
        '''
        core = DBSession.query(DataoneCore).filter(DataoneCore.object_uuid==self.uuid).first()
        if not core:
            raise Exception('no core')

        #push to obsolete
        obsolete = DataoneObsolete(core.id)
        try:
            DBSession.add(obsolete)
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(obsolete)
        except Exception as ex:
            DBSession.rollback()
            raise ex
            
    def activate_object(self):
        '''
        activate core
        activate obsolete
        '''

        core = DBSession.query(DataoneCore).filter(DataoneCore.object_uuid==self.uuid).first()
        if not core:
            raise Exception('no core')

        obsolete = self.get_current_obsolete(True)

        core.active = True
        obsolete.active = True

        try:
            DBSession.commit()       
        except:
            DBSession.rollback()
            raise Exception('activate science metadata object error: %s' % self.uuid) 


    def generate_object(self, output_path, obsolete_uuid, xslt_path, base_url):  
        '''
        build the object, based on the format type, xml
        store it in the dataone science metadata object cache
        with the obsolete uuid

        note: the obsolete uuid has to be NEW and inactive while we're generating everything

        the output standard is part of the obj, the output format is always xml,
        we need the xslt path and the base url
        '''

        #so get the dataset
        #get the data object for the metadata (what if there's more than one?)
        #put that in the distribution/onlink

        dataset = self.datasets
        if not dataset:
            raise Exception('no data!')

        if not dataset.gstore_metadata:
            raise Exception('no gstore metadata')

        standard = self.standards

        #get a dataone object for the dataset
        #TODO: the relate isn't returning objects (the select WHICH IS EXACTLY THE SAME does though)
        #dataobjects = dataset.data_objects
        dataobject = DBSession.query(DataoneDataObject).filter(DataoneDataObject.dataset_id==self.dataset_id).first()
        if not dataobject:
            raise Exception('no data objects')

        #get the current obsolete uuid for this data obj
        dataobj_obsolete = dataobject.get_current_obsolete()
        if not dataobj_obsolete:
            raise Exception('no obsolete data object')

        #TODO: change this to not be stupid later and also for the fgdc types
        obj_type = 'CSV' if 'csv' in dataobject.formats.format else 'ZIP'

        try:
            obj_size = dataobj_obsolete.get_size()
        except:
            obj_size = 0    

        '''
        >>> from gstore_v3.models import *
        >>> scimeta = DBSession.query(dataone.DataoneScienceMetadataObject).first()
        >>> xslt = request.registry.settings['XSLT_PATH'] + '/xslts'
        >>> op = '/home/sscott/metadata/d1/metadata'
        >>> ud = 'a9d5ea9b-adb8-4e09-ac98-2eaf7b251c22'
        >>> bu = 'http://129.24.63.115'
        >>> scimeta.generate_object(op, ud, xslt, bu) 
        '''
       
        metadata_info = {"base_url": base_url, "onlinks": ['%s/dataone/v1/object/%s' % (base_url, dataobj_obsolete.uuid)], "distribution_links": [{"link":'%s/dataone/v1/object/%s' % (base_url, dataobj_obsolete.uuid), "type": obj_type, "size": obj_size}], "identifer": obsolete_uuid, "app-name": "DataONE", "app-url": "http://www.dataone.org"}

        gm = dataset.gstore_metadata[0]
        #TODO: change the validation part post-shutdown
        xml = gm.transform(standard.alias, 'xml', xslt_path, metadata_info, validate=False)
        if not xml:
            raise Exception('metadata failure')

        with open(os.path.join(output_path, '%s.xml' % obsolete_uuid), 'w') as f:
            f.write(xml)
        


class DataoneDataPackage(Base):
    __table__ = Table('dataone_datapackages', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('dataobj_uuid', UUID, ForeignKey('gstoredata.dataone_dataobjects.uuid')),
        Column('scimetadataobj_uuid', UUID, ForeignKey('gstoredata.dataone_sciencemetadata.uuid')),
        Column('format_id', Integer, ForeignKey('gstoredata.dataone_formatids.id')),
        Column('date_added', TIMESTAMP, FetchedValue()),
        schema="gstoredata"
    )

    dataobjects = relationship('DataoneDataObject', backref='packages')
    scimetaobjects = relationship('DataoneScienceMetadataObject', backref='packages')

    formats = relationship('DataoneFormat', backref='packages')

    _datefmt = '%Y-%m-%dT%H:%M:%SZ'

    _template = Template("""<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
   xmlns:cito="http://purl.org/spar/cito/"
   xmlns:dc="http://purl.org/dc/elements/1.1/"
   xmlns:dcterms="http://purl.org/dc/terms/"
   xmlns:foaf="http://xmlns.com/foaf/0.1/"
   xmlns:ore="http://www.openarchives.org/ore/terms/"
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   xmlns:rdfs1="http://www.w3.org/2001/01/rdf-schema#"
>
  <rdf:Description rdf:about="http://www.openarchives.org/ore/terms/Aggregation">
    <rdfs1:label>Aggregation</rdfs1:label>
    <rdfs1:isDefinedBy rdf:resource="http://www.openarchives.org/ore/terms/"/>
  </rdf:Description>
  <rdf:Description rdf:about="https://cn.dataone.org/cn/v1/resolve/${package_obsolete_uuid}">
    <rdf:type rdf:resource="http://www.openarchives.org/ore/terms/ResourceMap"/>
    <dcterms:modified>${package_modified}</dcterms:modified>
    <dcterms:creator rdf:resource="http://foresite-toolkit.googlecode.com/#pythonAgent"/>
    <dc:format>http://www.opernarchives.org/ore/terms</dc:format>
    <ore:describes rdf:resource="https://cn.dataone.org/cn/v1/resolve/${package_obsolete_uuid}#aggregation"/>
    <dcterms:created>${package_created}</dcterms:created>
    <dcterms:identifier>${package_obsolete_uuid}</dcterms:identifier>
  </rdf:Description>
  <rdf:Description rdf:about="https://cn.dataone.org/cn/v1/resolve/${data_obsolete_uuid}">
    <cito:isDocumentedBy rdf:resource="https://cn.dataone.org/cn/v1/resolve/${metadata_obsolete_uuid}"></cito:isDocumentedBy>
    <dcterms:description>Data object ("${data_obsolete_uuid}")</dcterms:description>
    <dcterms:identifier>${data_obsolete_uuid}</dcterms:identifier>
    <dc:title>Dataset: "${data_obsolete_uuid}"</dc:title>
  </rdf:Description>
  <rdf:Description rdf:about="https://cn.dataone.org/cn/v1/resolve/${metadata_obsolete_uuid}">
    <cito:documents rdf:resource="https://cn.dataone.org/cn/v1/resolve/${data_obsolete_uuid}"></cito:documents>
    <dcterms:description>Science metadata object (${metadata_obsolete_uuid}) for Data object ("${data_obsolete_uuid}")</dcterms:description>
    <dcterms:identifier>${metadata_obsolete_uuid}</dcterms:identifier>
    <dc:title>Metadata: ${metadata_obsolete_uuid}</dc:title>
  </rdf:Description>
  <rdf:Description rdf:about="http://www.openarchives.org/ore/terms/ResourceMap">
    <rdfs1:label>ResourceMap</rdfs1:label>
    <rdfs1:isDefinedBy rdf:resource="http://www.openarchives.org/ore/terms/"/>
  </rdf:Description>
  <rdf:Description rdf:about="https://cn.dataone.org/cn/v1/resolve/${package_obsolete_uuid}#aggregation">
    <rdf:type rdf:resource="http://www.openarchives.org/ore/terms/Aggregation"/>
    <ore:aggregates rdf:resource="https://cn.dataone.org/cn/v1/resolve/${metadata_obsolete_uuid}"/>
    <ore:aggregates rdf:resource="https://cn.dataone.org/cn/v1/resolve/${data_obsolete_uuid}"/>
    <ore:isDescribedBy rdf:resource="https://cn.dataone.org/cn/v1/resolve/${package_obsolete_uuid}"/>
  </rdf:Description>
  <rdf:Description rdf:about="http://foresite-toolkit.googlecode.com/#pythonAgent">
    <foaf:name>Foresite Toolkit (Python)</foaf:name>
    <foaf:mbox>foresite@googlegroups.com</foaf:mbox>
  </rdf:Description>
</rdf:RDF>""")

    def __init__(self, dataobj_uuid, scimetadataobj_uuid, format_id):
        self.dataobj_uuid = dataobj_uuid
        self.scimetadataobj_uuid = scimetadataobj_uuid
        self.format_id = format_id

    def __repr__(self):
        return '<DataPackage (%s, %s, %s)>' % (self.uuid, self.dataobj_uuid, self.scimetadataobj_uuid)

    def get_current_obsolete(self, use_all=False):
        '''
        return the most current obsolete uuid 
        where active only for use_all = False or most recent period for use_all = True
        '''
        core = DBSession.query(DataoneCore).filter(DataoneCore.object_uuid==self.uuid).first()
        if not core:
            return None

        filters= [DataoneObsolete.core_id==core.id]
        if not use_all:
            filters.append(DataoneObsolete.active==True)
        obsolete = DBSession.query(DataoneObsolete).filter(and_(*filters)).order_by(DataoneObsolete.date_changed.desc()).first()
        if not obsolete:
            return None
            
        return obsolete

    def register_object(self):
        '''
        push to core
        push core to obsolete
        '''

        #push to core
        core = DataoneCore(self.uuid, 'data package')
        try:
            DBSession.add(core)
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(core)
        except:
            DBSession.rollback()
            raise

        #push to obsolete
        obsolete = DataoneObsolete(core.id)
        try:
            DBSession.add(obsolete)
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(obsolete)
        except:
            DBSession.rollback()
            raise

<<<<<<< HEAD
        sysmeta = DataoneSystemMetadata(obsolete.id)
        #TODO: not hardcode this. i hate dataone   
        sysmeta.replication_policy = False
        sysmeta.access_policies = '<allow><subject>public</subject><permission>read</permission></allow>'
        try:
            DBSession.add(sysmeta)
            DBSession.commit()
        except:
            DBSession.rollback()
            raise

=======
>>>>>>> gstore/master
        return core.object_uuid, obsolete.uuid

    def register_dirty_object(self):
        '''
        get the core and add a new obsolete uuid
        '''
        core = DBSession.query(DataoneCore).filter(DataoneCore.object_uuid==self.uuid).first()
        if not core:
            raise Exception('no core')

        #push to obsolete
        obsolete = DataoneObsolete(core.id)
        try:
            DBSession.add(obsolete)
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(obsolete)
        except Exception as ex:
            DBSession.rollback()
            raise ex

    def activate_object(self):
        '''
        activate core
        activate obsolete
        '''
        core = DBSession.query(DataoneCore).filter(DataoneCore.object_uuid==self.uuid).first()
        if not core:
            raise Exception('no core')

        obsolete = self.get_current_obsolete(True)

        core.active = True
        obsolete.active = True

        try:
            DBSession.commit()       
        except:
            DBSession.rollback()
            raise Exception('activate science metadata object error: %s' % self.uuid) 
        
    def generate_object(self, output_path, obsolete_uuid, use_active=True):  
        '''
        build the object, based on the format type (rdf from the template below)
        store it in the dataone data packages object cache
        with the obsolete uuid (this is the new, inactive, package obsolete uuid)

        note: the obsolete uuid has to be NEW and inactive while we're generating everything
        note: must be the right obsolete uuids for the data obj and the metadata obj


        use_active = if true, it will use only the active objects (not the package one, that's still prob inactive)
        '''
        
        #should just be one, but also possible to have collections for data
        dataobj = self.dataobjects
        scimetaobj = self.scimetaobjects

        dataobj_obsolete = dataobj.get_current_obsolete(use_active)

        scimetaobj_obsolete = scimetaobj.get_current_obsolete(use_active)
        
        pkg_obsolete = self.get_current_obsolete(True)

        #generate the rdf
        result = self._template.render(package_obsolete_uuid=obsolete_uuid, package_created=self.date_added.strftime(self._datefmt), package_modified=pkg_obsolete.date_changed.strftime(self._datefmt), metadata_obsolete_uuid=scimetaobj_obsolete.uuid, data_obsolete_uuid=dataobj_obsolete.uuid)
        
        with open(os.path.join(output_path, obsolete_uuid + '.xml'), 'w') as f:
            f.write(result)


    def generate_object_specific(self, output_path, obsolete_uuid, dataobj_obs_uuid, scimeta_obs_uuid):
        '''
        rebuild a data package with known objects

        this may make no sense but it's dataone.
        '''
        pkg_obsolete = self.get_current_obsolete(True)

        #generate the rdf
        result = self._template.render(package_obsolete_uuid=obsolete_uuid, package_created=self.date_added.strftime(self._datefmt), package_modified=pkg_obsolete.date_modified.strftime(self._datefmt), metadata_obsolete_uuid=scimetaobj_obs_uuid, data_obsolete_uuid=dataobj_obs_uuid)
        
        with open(os.path.join(output_path, obsolete_uuid + '.xml'), 'w') as f:
            f.write(result)

        
class DataoneCore(Base):
    __table__ = Table('dataone_core', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('object_uuid', UUID),
        Column('active', Boolean),
        Column('object_type', String(50)),
        schema='gstoredata'
    )

    obsoletes = relationship('DataoneObsolete', backref='cores')

    def __init__(self, object_uuid, object_type, active=False):
        self.object_uuid = object_uuid
        self.object_type = object_type
        self.active = active

    def __repr__(self):
        return '<CoreObject (%s, %s, %s, %s)>' % (self.id, self.object_uuid, self.object_type, self.active)

    def get_object(self):
        if self.object_type=='data object':
            obj = DBSession.query(DataoneDataObject).filter(DataoneDataObject.uuid==self.object_uuid).first()
        elif self.object_type=='science metadata object':
            obj = DBSession.query(DataoneScienceMetadataObject).filter(DataoneScienceMetadataObject.uuid==self.object_uuid).first()
        elif self.object_type=='data package':
            obj = DBSession.query(DataoneDataPackage).filter(DataoneDataPackage.uuid==self.object_uuid).first()
        else:
            return None

        return obj

class DataoneObsolete(Base): 
    __table__ = Table('dataone_obsoletes', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('uuid', UUID, FetchedValue()),
        Column('core_id', Integer, ForeignKey('gstoredata.dataone_core.id')),
        Column('active', Boolean),
        Column('date_changed', TIMESTAMP, FetchedValue()),
        schema='gstoredata'
    )

    system_metadatas = relationship('DataoneSystemMetadata', backref='obsoletes')

    def __init__(self, core_id, active=False):
        self.core_id = core_id
        self.active = active

    def __repr__(self):
        return '<Obsolete (%s, %s, %s, %s)>' % (self.core_id, self.uuid, self.date_changed, self.active)

    def get_hash(self, algo, cache_path):
        return generate_hash(cache_path, algo)

    def get_size(self, cache_path):
        return os.path.getsize(cache_path)

    def get_obsoleted_by(self):
        #figure out if this obsolete object had a previous version for the d1 object
        filters = [self.__table__.c.core_id==self.core_id, self.__table__.c.date_changed > self.date_changed, self.__table__.c.active==True]
        obsoleted_by = DBSession.query(self.__table__).filter(and_(*filters)).order_by(self.__table__.c.date_changed.asc()).first()
        return obsoleted_by

    def get_obsoletes(self):
        # get the previous obsolete if there is one
        filters = [self.__table__.c.core_id==self.core_id, self.__table__.c.date_changed < self.date_changed, self.__table__.c.active==True]
        obsoletes = DBSession.query(self.__table__).filter(and_(*filters)).order_by(self.__table__.c.date_changed.desc()).first()
        return obsoletes

class DataoneFormat(Base):
    __table__ = Table('dataone_formatids', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('format', String(200)),
        Column('name', String(200)),
        Column('type', String(20)),
        Column('mimetype', String(100)),
        schema = 'gstoredata'
    )  
    '''
    list the formatIds from the CN list 
    but only what we are likely to use in gstore
    '''

    def __repr__(self):
        return '<Dataone Format (%s, %s, %s, %s)>' % (self.id, self.format, self.name, self.type)

class DataoneSystemMetadata(Base):
    __table__ = Table('dataone_systemmetadata', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('obsolete_id', Integer, ForeignKey('gstoredata.dataone_obsoletes.id')),
        Column('replication_policy', Boolean),
        Column('access_policies', String),
<<<<<<< HEAD
        Column('date_changed', TIMESTAMP, FetchedValue()),
=======
        Column('date_changed', TIMESTAMP),
>>>>>>> gstore/master
        schema='gstoredata'
    )

    '''
    basically just a placeholder for the date changed when an object is obsoleted. but we are 
    trying to have it less wrong for when there may actually be a case for the other bits to change
    independently of obsolescence. deep.
    '''

    def __init__(self, obsolete_id):
        self.obsolete_id= obsolete_id

    def __repr__(self):
        return '<DataoneSystemMetadata (%s, %s, %s)>' % (self.id, self.obsolete_id, self.date_changed.strftime('%Y-%m-%dT%H:%M:%S.%f'))
    
    
class DataoneSearch(Base):
    __table__ = Table('search_dataone', Base.metadata,
        Column('object_uuid', UUID, primary_key=True),
        Column('object_type', String(50)),
        Column('object_format', String(50)),
        Column('object_ext', String(20)),
        Column('object_added', TIMESTAMP),
        Column('object_changed', TIMESTAMP),
        Column('core_id', Integer),
        Column('obsolete_uuid', UUID),
        schema='gstoredata'
    )
    #note: object_added and object_changed are basically the same. we try very hard for dataone.
    '''
    view to help with the object search view (by date or format)
    '''




















    
        
