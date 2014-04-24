from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy import func
from sqlalchemy.orm import relationship, backref

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    )

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy.dialects.postgresql import UUID, ARRAY

from osgeo import ogr, osr
import xlwt
import json
import pytz
import math

from ..lib.utils import *
from ..lib.spatial import *
from ..lib.mongo import gMongo
from ..lib.spatial_streamer import *

from ..models.categories import Category
from ..models.collections import Collection
from ..models.relationships import DatasetRelationship
from ..models.projects import Project
from ..models.citations import Citation
from ..models.features import Feature
from ..models.metadata import MetadataStandards
from ..models.apps import GstoreApp
from ..models.licenses import License
from ..models.provenance import *

import os, tempfile, shutil
import subprocess, re
from xml.sax.saxutils import escape

#this is bad and is just for the metadata right now
from pyramid.threadlocal import get_current_registry

class Dataset(Base):
    """

    Note:
        could autoload but there's a lot of old junk in there that we don't really care about

    Attributes:
        
    """

    __table__ = Table('datasets', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('description', String(200)),
        Column('taxonomy', String(50)),
        Column('feature_count', Integer),
        Column('record_count', Integer),
        Column('dateadded', TIMESTAMP, FetchedValue()),
        Column('basename', String(100)),
        Column('geom', String),
        Column('geomtype', String),
        Column('orig_epsg', Integer),
        Column('inactive', Boolean, default=False), #shuts it off completely
        Column('box', ARRAY(Numeric)),
        Column('apps_cache', ARRAY(String)),
        Column('begin_datetime', TIMESTAMP),
        Column('end_datetime', TIMESTAMP),
        Column('is_available', Boolean, default=True), #leaves the text data visible but the downloads (data) unavailable
        Column('has_metadata_cache', Boolean, default=True), 
        Column('excluded_formats', ARRAY(String)),
        Column('excluded_services', ARRAY(String)),
        Column('excluded_standards', ARRAY(String)),
        Column('date_acquired', TIMESTAMP),
        Column('is_embargoed', Boolean, default=False),
        Column('embargo_release_date', TIMESTAMP),
        Column('is_cacheable', Boolean, default=True),
        Column('aliases', ARRAY(String)),
        Column('license_id', Integer, ForeignKey('gstoredata.licenses.id')),
        Column('date_published', TIMESTAMP),
        Column('uuid', UUID), # we aren't setting this in postgres anymore
        schema='gstoredata'
    ) 

    #categories
    categories = relationship('Category', 
                    secondary='gstoredata.categories_datasets',
                    backref='datasets')

    #TODO: add a join condition where sources.active only (, primaryjoin='and_(Dataset.id==Source.dataset_id, Source.active==True)') except for now, they're all basically active
    #relate to sources
    sources = relationship('Source', backref='datasets')

    #relate to attributes
    attributes = relationship('Attribute', backref='dataset')

    #TODO: relate to relationships (meta)

    #relate to shapes
    shapes = relationship('Feature', backref='dataset')

    #relate to collections
    collections = relationship('Collection', 
                    secondary='gstoredata.collections_datasets',
                    backref='datasets')

    #TODO: update this when the new schema is in place. this is an intermediate step
    #relate to the metadata
    original_metadata = relationship('OriginalMetadata', backref='datasets')

    #to the gstore (and main) metadata
    gstore_metadata = relationship('DatasetMetadata', backref='datasets')

    #relate to projects
    projects = relationship('Project', secondary='gstoredata.projects_datasets', backref='datasets')

    #relate to citations
    citations = relationship('Citation', secondary='gstoredata.datasets_citations', backref='datasets')

    #relate to repositories
    repositories = relationship('Repository', secondary='gstoredata.repositories_apps_datasets', backref='datasets')

    #to the prov traces
    prov_bases = relationship('ProvBase', backref='datasets')

    #relates to the d1 stuff
    scimeta_objects = relationship('DataoneScienceMetadataObject', backref='datasets')
    data_objects = relationship('DataoneDataObject', backref='datasets')
      
    def __init__(self, description):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        self.description = description

        #TODO: generate the uuid for this on create (but overwrite for the tristate sharing datasets, so remember that)
        #that way it at least has a uuid no matter what
        
    def __repr__(self):
        return '<Dataset (%s, %s)>' % (self.description, self.uuid)

        
    def get_formats(self, req=None):
        """

        get the available formats for the dataset
        use the excluded_formats & the taxonomy_list defaults to get the actual supported formats

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        if not self.is_available:
            return []

        if not req:
            #try to get the list from the current registry
            lst = get_current_registry().settings['DEFAULT_FORMATS']
        else:
            lst = req.registry.settings['DEFAULT_FORMATS']

        if not lst:
            return None
        lst = lst.split(',')
        
        exc_lst = self.excluded_formats

        #get all from one not in the other
        fmts = [i for i in lst if i not in exc_lst]
        return fmts

    def get_services(self, req=None):
        """get the supported web services for the dataset

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        if not self.is_available:
            return []

        if not req:
            lst = get_current_registry().settings['DEFAULT_SERVICES']
        else:
            lst = req.registry.settings['DEFAULT_SERVICES']

        if not lst:
            return None    
        lst = lst.split(',')

        exc_lst = self.excluded_services

        #get all from one not in the other
        svcs = [i for i in lst if i not in exc_lst]
        return svcs

    def get_standards(self, req=None):
        """get the list of supported metadata standards for the dataset

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        if not self.is_available:
            return []

        if not req:
            lst = get_current_registry().settings['DEFAULT_STANDARDS']
        else:
            lst = req.registry.settings['DEFAULT_STANDARDS']

        if not lst:
            return None
        lst = lst.split(',')

        exc_lst = self.excluded_standards
        return [i for i in lst if i not in exc_lst]

    def get_repositories(self):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """

    
        '''
        get repos by app - 

        {   
            app: [repos],
            app: [repos]
        }
        '''
        repos = {}

        for app in self.apps_cache:
            rs = [r.name for r in self.repositories if app in [a.route_key for a in r.apps]]
            if rs:
                repos[app] = rs
        
        return repos


    def get_source(self, aset, extension, active=True):
        """
        return a source by set & extension for this dataset
        default to active sources only

        
        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        src = [s for s in self.sources if s.extension == extension and s.set == aset and s.active == active]
        return src[0] if src else None

    def get_mapsource(self, fmtpath='', mongo_uri=None, srid=4326, metadata_info=None):
        """

        get the source location (path) for the mapfile
        return the source id (for the mapfile) and the data filepat

        if file:
            return null
            
        if geoimage:
            get tif if tif
            get sid if sid
            get ecw if ecw

        if vector:
            if shp in src, get shp
            if shp in formats, get shp
            build shp

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        if self.taxonomy in ['file', 'table', 'service']:
            return None, None
        elif self.taxonomy == 'geoimage':
            srcs = self.sources
            src = None
            ext = ''
            #current supported formats
            for r in ['tif', 'img', 'sid', 'ecw']:
                src = [s for s in srcs if s.extension == r and s.active == True]
                if src:
                    ext = r
                    break
            if not src:
                #no match for valid raster type
                return None, None

            #make sure the chosen source is unzipped
            src = src[0]
            files = src.src_files
            #get the location that ends with .{ext}
            f = [f for f in files if f.location.split('.')[-1] == ext]

            if not f:
                #maybe it's the right type but already packed as a zip
                return None, None

            #this should be the data file we're looking for
            return src, f[0].location
        elif self.taxonomy == 'vector':
            #assuming that we are only mapping with shapefiles
            srcs = self.sources
            if srcs:
                #we found something, but only really care about shapefiles
                src = [s for s in srcs if s.extension == 'shp' and s.active == True]
                if src:
                    src = src[0]
                    files = src.src_files
                    #get the location that ends with .{ext}
                    f = [f for f in files if f.location.split('.')[-1] == 'shp']
                    return src, f[0].location

            #nope, check for the cached file
            if not fmtpath:
                return None, None
                
            fmtfile = os.path.join(fmtpath, str(self.uuid), 'shp', '%s.%s' % (self.basename, 'shp'))
            if os.path.isfile(fmtfile):
                #TODO: fix this - no set source but need src info
                return None, fmtfile

            #nope, go build the vector
            if not os.path.isdir(os.path.join(fmtpath, str(self.uuid), 'shp')):
                #make the directory
                if not os.path.isdir(os.path.join(fmtpath, str(self.uuid))):
                    os.mkdir(os.path.join(fmtpath, str(self.uuid)))
                os.mkdir(os.path.join(fmtpath, str(self.uuid), 'shp'))
            success, message = self.stream_vector('shp', os.path.join(fmtpath, str(self.uuid), 'shp'), mongo_uri, srid, metadata_info)
            if success != 0: 
                return None, None
                
            return None, fmtfile

        #and there's nothing left
        return None, None

    
    def get_full_service_dict(self, base_url, req, app):
        """
        build the dict for the full service description
        see the service view, search v3 results

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
    
        results = {'type': 'dataset', 'id': self.id, 'uuid': self.uuid, 'description': self.description, 
                'lastupdate': self.dateadded.strftime('%Y%m%d'), 'name': self.basename, 'taxonomy': self.taxonomy,
                'categories': [{'theme': t.theme, 'subtheme': t.subtheme, 'groupname': t.groupname} for t in self.categories]}
        if self.box:
            results.update({'spatial': {'bbox': string_to_bbox(self.box), 'epsg': self.orig_epsg}})
            

        if self.begin_datetime and self.end_datetime:
            if self.begin_datetime.year >= 1900 and self.end_datetime.year >= 1900:
                results.update({"valid_dates": {"start": self.begin_datetime.strftime('%Y%m%d'), "end": self.end_datetime.strftime('%Y%m%d')}})
            else:
                results.update({"valid_dates": {"start": '%s%02d%02d' % (self.begin_datetime.year, self.begin_datetime.month, self.begin_datetime.day), "end": '%s%02d%02d' % (self.end_datetime.year, self.end_datetime.month, self.end_datetime.day)}})

        if self.is_available:
            dlds = []
            links = []
            fmts = self.get_formats(req)
            svcs = self.get_services(req)            
            #TODO: change the relate to only include active sources
            srcs = [s for s in self.sources if s.active]

            if self.taxonomy == 'geoimage':
                #add the downloads by source
                #TODO: maybe compare to the formats list?
                dlds = [(s.set, s.extension) for s in srcs] # if not s.is_external]

                #links = [{s.extension: s.get_location()} for s in srcs if s.is_external]
            elif self.taxonomy in ['vector', 'table']:
                #get the formats
                #check for a source
                #if none, derived + fmt
                #if one, set + fmt
                #TODO: what if a vector dataset has external links?
                for f in fmts:
                    sf = [s for s in srcs if s.extension == f]
                    st = sf[0].set if sf else 'derived'
                    dlds.append((st, f))
            elif self.taxonomy == 'file':
                #just the formats
                for f in fmts:
                    sf = [s for s in srcs if s.extension == f]
                    if not sf:
                        #if it's not in there, that's a whole other problem (i.e. why is it listed in the first place?)
                        continue
                    sf = sf[0]
                    dlds.append((sf.set, f))
            elif self.taxonomy == 'service':
                #TODO: figure out what to put here
                pass

            #combine the links (don't change url) with downloads (build url)
            qp = '' if self.is_cacheable else '?ignore_cache=True'
            dlds = [{s[1]: base_url + build_dataset_url(app, self.uuid, self.basename, s[0], s[1]) + qp for s in dlds}] if dlds else [] 
            dlds = links + dlds

            #update the wxs services to have the more complete url (version, request, service)
            svc_list = []
            for s in svcs:
                if s in ['rest']:
#                    #TODO: resolve what this needs to be
                    continue

                if s in ['wms', 'wfs']:
                    vsn = '1.1.1' if s == 'wms'  else '1.0.0'
                else:
                    vsn = '1.1.2'
                    
                url = base_url + build_ogc_url(app, 'datasets', self.uuid, s, vsn)

                svc_list.append({s: url})

            results.update({'services': svc_list, 'downloads': dlds})

            #add the link to the mapper 
            #TODO: when the mapper moves, get rid of this
            if self.taxonomy in ['geoimage', 'vector']:
                results.update({'preview': base_url + build_mapper_url(app, self.uuid)})

        else:
            results.update({'services': [], 'downloads': [], 'preview': '', 'availability': False})

        if self.gstore_metadata:
            supported_standards = self.get_standards(req)

            #get the supported formats per standard
            mt = []
            for supported_standard in supported_standards:
                std = DBSession.query(MetadataStandards).filter(MetadataStandards.alias==supported_standard).first()
                if not std:
                    continue
                services = self.get_services(req) if '19119' in supported_standard else []    

                mt += std.get_urls(app, 'datasets', base_url, self.uuid, services)

            #TODO: add the date modified back in although it doesn't matter per standard (it's all gstore, so it's all the same date)
            #     MAYBE ADD SOME KEY? all: date? to go with fgdc: date, iso: date if not gstore-ified?
            utc = pytz.utc
            if self.gstore_metadata[0].date_modified:
                md = {"all": self.gstore_metadata[0].date_modified.astimezone(utc).strftime('%Y-%m-%dT%H:%M:%SZ')}
            else:
                md = {}
            
            
        elif not self.gstore_metadata and self.original_metadata:
            #TODO: change this to get the standard of the original_metadata if om & xml exists
            #      where the format is ONLY xml
        
            '''
            as {fgdc: {ext: url}}

            + metadata_modified element:
            metadata_modified: {'fgdc': 'yyyyMMdd', 'iso': 'yyyyMMd'} AS UTC!

            this is not one of the keys under metadata['fgdc'] to avoid borking rgis/epscor
            '''

            #get any xml for the dataset (ignore those that only have some unknown text blob)
            om = [o for o in self.original_metadata if o.original_xml]
            md = {}
            mt = []
            for o in om:
                mt.append({o.original_xml_standard: {"xml": base_url + build_metadata_url(app, 'datasets', self.uuid, o.original_xml_standard, 'xml')}})

#            #get the date-modified by standard
            utc = pytz.utc
            md = {}
            om = [o for o in self.original_metadata]
            for o in om:
                #get the standard and the date (2013-01-30 20:23:49.694577-07)
                if o.date_modified and o.original_xml_standard:
                    md.update({o.original_xml_standard: o.date_modified.astimezone(utc).strftime('%Y-%m-%dT%H:%M:%SZ')})
#           
#            mt = [{s: {e: '%s/metadata/%s.%s' % (base_url, s, e) for e in exts} for s in standards}]
        else:
            md = {}
            mt = []
        
        
        results.update({'metadata': mt})
        
        if md:
            results.update({'metadata-modified': md})


        #TODO: maybe not this? but maybe add the source links (maybe not though)
        #check for provenance bases for this app and dataset
        app_bases = DBSession.query(ProvBase).join(ProvOntology).join(GstoreApp).filter(and_(GstoreApp.route_key==app, ProvBase.dataset_id==self.id)).all()

        if app_bases:
            '''
            add the links for the prov OUTPUTS and maybe the original DS?
            for the app 

            "prov": {
                "ontology": {
                    "traces": {
                        "format": url
                    }
                }
            }

            /apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/prov/{ontology}.{ext}
            '''
            ontologies = {}

            for ab in app_bases:
                ont = ab.ontologies
                traces = ontologies[ont.ontology_key] if ont.ontology_key in ontologies else {}

                mappings = DBSession.query(ProvMapping).filter(and_(ProvMapping.ontology_id==ab.ontology_id, ProvMapping.inputstandards_id==ab.inputstandards_id)).all()

                for mapping in mappings:
                    traces.update({str(mapping.output_format): base_url + build_prov_trace_url(app, self.uuid, ont.ontology_key, str(mapping.output_format))})

                ontologies[ont.ontology_key] = traces

            results.update({"prov": ontologies})

        #TODO: add the html card view also maybe

        #TODO: add related datasets

        #TODO: add link to collections it's in?

        #TODO: add project

        return results


    def get_index_doc(self, req=None):
        """

        return the json for the elasticsearch index

        NOTE: THIS IS NOT THE METHOD FOR REGISTERING THE DATASET IN THE ES INSTANCE
            AND ISN'T UP TO DATE

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        fmts = self.get_formats(req)
        svcs = self.get_services(req)

        bbox = string_to_bbox(self.box)

        isotopic = ''
        abstract = ''
        if self.gstore_metadata:
            #get the abstract and other bits
            isotopic = self.gstore_metadata[0].get_isotopic()
            abstract = self.gstore_metadata[0].get_abstract()

        geom = wkb_to_geom(self.geom, 4326)
        area = geom.GetArea()
        

#        quads = DBSession.query(func.gstoredata.intersect_geolookup(self.id, 'nm_quads')).first()
#        quads = quads[0] if quads else []

#        counties = DBSession.query(func.gstoredata.intersect_geolookup(self.id, 'nm_counties')).first()
#        counties = counties[0] if counties else []

        categories = self.categories

        facets = []
        for cat in categories:
            facets.append({"theme": str(cat.theme), "subtheme": str(cat.subtheme), "groupname": str(cat.groupname), "apps": self.apps_cache})

        #if bbox[0] == bbox[2] and bbox[1] == bbox[3]:
        if area == 0:
            loc = {"type": "Point", "coordinates": [bbox[0], bbox[1]]}
        else:
            loc = {
                    "type": "Polygon",
                    "coordinates": [[[bbox[0], bbox[1]], [bbox[2], bbox[1]], [bbox[2], bbox[3]], [bbox[0], bbox[3]], [bbox[0], bbox[1]]]]
                }


        es = {
            "taxonomy": self.taxonomy,
            "title": str(self.description),
            "formats": fmts,
            "services": svcs,
            "date_added": self.dateadded.strftime('%Y-%m-%d'),
            "applications": self.apps_cache,
            "location": {
                "bbox": loc
            },
            "isotopic": isotopic,
            "abstract": abstract,
            "aliases": self.aliases if self.aliases else [],
            "active": not self.inactive,
            "embargo": self.is_embargoed,
            "available": self.is_available,
            "category": {"theme": categories[0].theme, "subtheme": categories[0].subtheme, "groupname": categories[0].groupname, "apps": self.apps_cache},
            "category_facets": facets, 
            "area": area
        }

        if self.taxonomy == 'vector':
            es.update({"geomtype": self.geomtype.lower()})

        return es
        

    def get_onlinks(self, base_url, req, app):
        """

        return a flattened list of online linkages

        NOTE: not for dataone

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        dct = self.get_full_service_dict(base_url, req, app)
        
        services = dct['services'] if dct['services'] else []
        services = [link for values in services for link in values.itervalues()]
        
        downloads = dct['downloads'][0].values() if dct['downloads'] else []
        downloads = [str(s) for s in downloads]

        metadatas = dct['metadata'] if dct['metadata'] else []
        metadatas = [link for keys in metadatas for key in keys.iterkeys() for link in keys[key].itervalues()]

        #tada: online linkages
        return services + downloads + metadatas

    def get_distribution_links(self, base_url, req, app):
        """

        return a dictionary of download links with file types and sizes
        ordered by our preferred formats by taxonomy 

        NOTE: not for dataone


        - get the downloads for the dataset
        - reorder the list based on the preferred list, where the canonical (i.e. first)
          format is the first match in the preferred list found in the supported formats
          so we have canonical and everything else
        - pack the links up with type and size

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        #TODO: maybe cache this? or something
        dct = self.get_full_service_dict(base_url, req, app)
        #as {fmt: link}
        downloads = dct['downloads'][0] if dct['downloads'] else {}

        preferred = ['zip', 'pdf', 'doc', 'docx', 'xls', 'xlsx', 'html', 'txt']
        if self.taxonomy == 'geoimage':
            preferred = ['tif', 'img', 'sid', 'ecw', 'dem', 'zip']
        elif self.taxonomy == 'vector':
            preferred = ['zip', 'shp', 'kml', 'gml', 'geojson', 'json', 'csv', 'xls']
        elif self.taxonomy == 'table':
            preferred = ['zip', 'csv', 'xls', 'json']

        canonical_format = ''
        for p in preferred:
            if p in downloads:
                canonical_format = p
                break

        reordered_downloads = []
        if canonical_format: 
            reordered_downloads.append((canonical_format, str(downloads[canonical_format])))
        for k, v in downloads.iteritems():
            if k == canonical_format:
                continue
            reordered_downloads.append((k, str(v)))

        # pack it up as a list of dicts (type:, size:, link:)
        distribution_links = []
        for item in reordered_downloads:
            fmt = format_to_definition(item[0])

            #estimate the file size where it's the filesize on disk if there's a source
            #or use the VERY BAD estimate if it's a schrodinger's vector
            if item[0] != 'html':
                name = item[1].split('/')[-1]
                aset = 'original' if 'original.%s' % item[0] in name else 'derived'
                source = self.get_source(aset, item[0], True)
            else:
                source = None

            if not source and self.taxonomy == 'vector':
                size = self.estimate_filesize(item[0])
            elif not source:
                size = -99
            else:
                #there's a source, get the actual filesize on disk and, in this case, we want an integer
                size = int(source.get_filesize_mb())
            
            distribution_links.append({"type": fmt, "size": size, "link": item[1]})

        return distribution_links
        
        
    def estimate_filesize(self, format):
        """

        VECTOR ONLY 
        estimate the filesize based on geomtype, record/feature counts and number of attributes

        THIS IS NOT REMOTELY ACCURATE - we'd need something like the average size of the geometry WKB and the length of the fields (i.e. text field that is 50chars 
        vs 1000chars) to start to come close, but that might be very expensive. also those estimates will change based on the format where the kml could be 600mb
        and the geojson 300mb for the same dataset.

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        if self.taxonomy != 'vector':
            return -99

        recs = self.record_count
        geoms = self.feature_count
        atts = len(self.attributes)

        geomtype = self.geomtype.lower()

        #seriously, these are not accurate estimates
        if 'polygon' in geomtype:
            multiplier = 0.0005
        elif 'linestring' in geomtype:
            multiplier = 0.0007
        else:
            multiplier = 0.0001
        
        if '3d' in geomtype:
            multiplier += 0.002

        multiplier += atts * 0.00001

        #handle one-to-one geometry-to-record or one-to-many geometry-to-records
        size = multiplier * recs if recs == geoms else multiplier * recs * geoms

        #bin the size because they are horrible estimates
        if size < 5:
            size = 5
        elif size >= 5 and size < 25:
            size = 25
        elif size >= 25 and size < 50:
            size = 50
        elif size >= 50 and size < 100:
            size = 100
        else:
            #round to the nearest hundredth
            size = int(math.ceil(size / 100)) * 100    

        return size


    def write_metadata(self, output_location, out_standard, out_format, metadata_info={}):
        """

        park the metadata (based on the standard and format) on disk
        if the standard isn't in the supported formats, or not original_metadata and no gstore format, bail

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """

        supported_standards = self.get_standards()
        if out_standard not in supported_standards:
            return False

        if self.gstore_metadata:
            #need to transform to the requested standard & format
            std = DBSession.query(MetadataStandards).filter(MetadataStandards.alias==out_standard).first()
            if not std:
                return False

            supported_formats = std.supported_formats
            if out_format.lower() not in supported_formats:
                return False

            #do the transformation
            gm = self.gstore_metadata[0]

            if 'app' in metadata_info:
                app = metadata_info['app'].lower()
            else:
                app = 'rgis'
            gstoreapp = DBSession.query(GstoreApp).filter(GstoreApp.route_key==app).first()
    
            metadata_info.update({"app-name": gstoreapp.full_name, "app-url": gstoreapp.url})

            #TODO: figure out when we want to run the validation
            validate = metadata_info['validate'] if 'validate' in metadata_info else False
            text = gm.transform(out_standard, out_format, metadata_info['xslt_path'], metadata_info, validate)
            if not text:
                return False
            
        elif not self.gstore_metadata and self.original_metadata and out_format == 'xml':
            #just get the xml and put it in the file (if the standard matches)
            om = [o for o in self.original_metadata if o.original_xml_standard == out_standard and o.original_xml]
            if not om:
                return False

            text = om[0].original_xml            
        else:
            return False

        with open(output_location, 'w') as f:
            f.write(text)

        #done
        return True

        
    #TODO: add a vector cache directory check + mkdir method


    #build an excel file for the data
    def build_excel(self, basepath, mongo_uri, metadata_info):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        style = xlwt.easyxf('font: name Times New Roman, color-index black, bold on', num_format_str='#,##0.00')
        workbook = xlwt.Workbook()
        sheetname = self.basename[0:29]

        worksheet = workbook.add_sheet(sheetname)
        x = 0
        y = 0

        #get the data
        gm = gMongo(mongo_uri)

        #only request as many as excel can handle
        vectors = gm.query({'d.id': self.id}, {}, {}, 65534, 0)

        #build the header
        atts = self.attributes
        for att in atts:
            worksheet.write(y, x, att.name, style=style)
            x += 1
            
        #the observed timestamp tacked on to the end
        worksheet.write(y, x, 'observed', style=style)

        #add the data
        y = 1
        for v in vectors:
            x = 0;
            vatts = v['atts']
            for att in atts:
                va = [a for a in vatts if a['name'] == att.name]
                if not va:
                    value = ''
                else:
                    value = unicode(va[0]['val']).encode('ascii', 'xmlcharrefreplace')
                 
                #convert it to the right type based on the field with string as default
                value = convert_by_ogrtype(value, att.ogr_type, 'xls')

                worksheet.write(y, x, value)
                x += 1
                
            #TODO: deal with the utc format and make sure it's what we want
            obs = v['obs'] if 'obs' in v else ''
            if obs:
                worksheet.write(y, x, obs.strftime('%Y-%m-%dT%H:%M:%S+00'))
            
            y += 1

        #write the file
        filename = os.path.join(basepath, '%s.xls' % (self.basename))
        workbook.save(filename)
        files = [filename]

        out_standard = metadata_info['standard']
        out_format = 'xml'
        metadata_file = '%s.xls.xml' % (os.path.join(basepath, self.basename))
        
        written = self.write_metadata(metadata_file, out_standard, out_format, metadata_info)
        if written:
            files.append(metadata_file)
                    
        output = create_zip(os.path.join(basepath, '%s_xls.zip' % (self.basename)), files)

        #still stupid
        return (0, 'success')



    #TODO: refactor for the two streams (stream_vector & stream_text)
    def stream_vector(self, format, basepath, mongo_uri, epsg, metadata_info):
        """

        THIS IS THE COMPRESSED FILE GENERATOR - output is a zip file with data file(s) and metadata file
        
        optimization to speed up the file generation, especially for large vector datasets  

        if the request is for gml, json, csv, or kml -> stream those
        if the request is for xls -> run the original excel builder (nothing we can do about that one)
        if the request is for a shapefile -> stream as gml and convert with ogr2ogr 

        then grab the metadata and pack everything as a zip for delivery
        and copy all of the files to the formats cache for mapserver, etc

        Notes:
            in some cases, this triggers a memory leak in ogr2ogr or at least
            something that ramps up the memory untilt he app becomes unresponsive

        Args:
            
        Returns:
        
        Raises:
        """
        #check the base location
        if os.path.abspath(basepath) != basepath or not os.path.isdir(basepath):
            return (1, 'invalid base path')

        #send the excel off for processing
        if format == 'xls':
            return self.build_excel(basepath, mongo_uri, metadata_info)    

        
        #convert from the geojson to avoid the encoding issues (ignored chars, etc)
        fmt = 'geojson' if format == 'shp' else format

        
        #build the text format
        tmp_file = self.stream_text(fmt, metadata_info['base_url'], mongo_uri, epsg)
        if not tmp_file or not os.path.isfile(tmp_file):
            raise Exception()

        tmp_path, tmp_name = os.path.split(tmp_file)
                      
        #pack up the results, with metadata, as a zip
        filename = os.path.join(basepath, '%s_%s.zip' % (self.basename, format))
        files = []
        if format == 'shp':
            #TODO: if we use this, change it back to the tmp folder and copy? 
            #convert it first          
            s = subprocess.Popen(['ogr2ogr', '-f', 'ESRI Shapefile', os.path.join(basepath, '%s.shp' % (self.basename)), tmp_file, '-lco', 'ENCODING=UTF-8'], shell=False) 
               
            status = s.wait()
            #note: the prj file should be generated already
            #TODO: add a spatial index
        
            exts = ['shp', 'shx', 'dbf', 'prj', 'shp.xml', 'sbn', 'sbx']
            for e in exts:
                if os.path.isfile(os.path.join(basepath, '%s.%s' % (self.basename, e))):
                    files.append(os.path.join(basepath, '%s.%s' % (self.basename, e)))

        else:
            files.append(os.path.join(tmp_path, '%s.%s' % (self.basename, format)))
            
        #add a metadata file if there's any metadata to add
        out_standard = metadata_info['standard']
        out_format = 'xml'
        metadata_file = '%s.%s.xml' % (os.path.join(tmp_path, self.basename), format)
        
        written = self.write_metadata(metadata_file, out_standard, out_format, metadata_info)
        if written:
            files.append(metadata_file)    
        
        #create the zip file
        output = create_zip(filename, files)

        #copy to the formats cache
        if format != 'shp':
            for f in files:
                outfile = f.replace(tmp_path, basepath)
                shutil.copyfile(f, outfile)        

        return (0, 'success')

    def stream_text(self, fmt, base_url, mongo_uri, epsg):
        """'
        
        THIS IS THE PLAIN, UNCOMPRESSED, UNDOCUMENTED TEXT DATA STREAM
        csv, json, geojson, kml, gml

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        #get the vectors
        gm = gMongo(mongo_uri)
        vectors = gm.query({'d.id': self.id})

        is_spatial = False if fmt in ['json', 'csv'] else True
        records = [self.convert_doc_to_record(vector, epsg, fmt, is_spatial) for vector in vectors]

        fields = [{"name": f.name, "type": f.ogr_type, "len": f.ogr_width} for f in self.attributes]

        if [a for a in records[0]['datavalues'] if a[0] == 'observed']:
            #this is not a good plan but we don't have the flag for "dataset contains observation timestamp" today
            #and it's as a string for now
            fields.append({"name": u"observed", "type": 4, "len": 20})

        if fmt == 'kml':
            streamer = KmlStreamer(fields)
            streamer.update_description(self.description)
        elif fmt =='gml':
            streamer = GmlStreamer(fields)
            streamer.update_description(self.description)
            streamer.update_namespace(self.basename)
        elif fmt == 'geojson':
            streamer = GeoJsonStreamer(fields)
        elif fmt == 'json':
            streamer = JsonStreamer(fields)
        elif fmt == 'csv':
            streamer = CsvStreamer(fields)
        else:
            return ''

        tmp_path = tempfile.mkdtemp()
        tmp_file = os.path.join(tmp_path, '%s.%s' % (self.basename, fmt))
        for g in streamer.yield_set(records):
            with open(tmp_file, 'a') as f:
                f.write(g)
        return tmp_file

    def convert_doc_to_record(self, doc, epsg, geometry_format, is_spatial=True):
        """take the mongo doc and convert to a record for streaming

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """    
        #deal with the fid v rid (must be one or the other from the doc)
        record_id = doc['f']['id'] if 'f' in doc else doc['r']['id'] if 'r' in doc else ''
        if not record_id:
            return {}

        #get the observed timestamp
        observed = doc['obs'].strftime('%Y-%m-%dT%H:%M:%S+00') if 'obs' in doc else ''
        
        #deal with the geometry
        geom_repr = ''
        if is_spatial:
            wkb = doc['geom']['g'] if 'geom' in doc else ''
            if not wkb and 'f' in doc:
                #just to doublecheck
                feat = DBSession.query(Feature).filter(Feature.fid==record_id).first()
                wkb = feat.geom if feat else ''

            if wkb:
                geom_repr = wkb_to_output(wkb, epsg, geometry_format)              
        
        #rebuild the data values as [(field, value), (field, value), ...]
        atts = doc['atts']

        #deal with encoding (particularly for kml/gml)
        #there's some wackiness with a unicode char and mongo (and also a bad char in the data, see fid 6284858)
        #convert atts to name, value tuples so we only have to deal with the wackiness once
        datavalues = [(a['name'], convert_by_ogrtype(a['val'], ogr.OFTString, geometry_format) if isinstance(a['val'], str) or isinstance(a['val'], unicode) else a['val']) for a in atts]

        #append the observed timestamp
        datavalues.append((u'observed', observed))

        return {"id": int(record_id), "geom": geom_repr, "datavalues": datavalues}

    def move_vectors(self, to_mongo_uri, from_mongo_uri):
        """

        move docs from one collection to the other (as defined by the mongo_uri collection)
        for a dataset

        so switch from embargoed to public (vectors) data or active to inactive data

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """

        #TODO: add a check to make sure that the #docs 
        #      from the source is the #docs in the sink

        #get the vectors
        from_gm = gMongo(from_mongo_uri)
        from_data = from_gm.query({'d.id': self.id})

        #insert into the other collection
        to_gm = gMongo(to_mongo_uri)
        
        for f in from_data:
            to_gm.insert(f)

        #drop it from the first collection
        removed = from_gm.remove({'d.id': self.id})


