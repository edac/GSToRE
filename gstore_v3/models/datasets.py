from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey
from sqlalchemy import Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
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

from ..lib.utils import get_all_formats, get_all_services, create_zip
from ..lib.spatial import *
from ..lib.mongo import gMongo

from ..models.features import Feature

import os, tempfile, shutil


'''
gstoredata.datasets
'''
#TODO: set up the schema for the engine (or metadata, somewhere) but 
#      check first - not required for 10, for example

#could autoload but there's a lot of old junk in there that we don't really care about
class Dataset(Base):
    __table__ = Table('datasets', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('description', String(200)),
        Column('taxonomy', String(50)),
        Column('feature_count', Integer),
        Column('record_count', Integer),
        Column('dateadded', TIMESTAMP, default="now()"),
        Column('basename', String(100)),
        Column('geom', String),
        Column('geomtype', String),
        Column('formats_cache', String),
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
        Column('project_id', Integer, ForeignKey('gstoredata.projects.id')),
        Column('uuid', UUID), # we aren't setting this in postgres anymore
        schema='gstoredata' #THIS IS KEY
    ) 

    #category join
    #note: the table name should be the metadata tablename which i guess includes the schema
    categories = relationship('Category', 
                    secondary='gstoredata.categories_datasets',
                    backref='datasets')

    #TODO: add a join condition where sources.active only (, primaryjoin='and_(Dataset.id==Source.dataset_id, Source.active==True)')
    #relate to sources
    sources = relationship('Source', backref='datasets')

    #relate to attributes
    attributes = relationship('Attribute', backref='dataset')

    #relate to relationships (meta)

    #relate to shapes
    shapes = relationship('Feature')

    #relate to collections
    collections = relationship('Collection', 
                    secondary='gstoredata.collections_datasets',
                    backref='datasets')

    #TODO: update this when the new schema is in place. this is an intermediate step
    #relate to the metadata
    original_metadata = relationship('OriginalMetadata')

    #relate to projects
    projects = relationship('Project', backref='datasets')
      
    def __init__(self, description):
        self.description = description

        #TODO: generate the uuid for this on create (but overwrite for the tristate sharing datasets, so remember that)
        #that way it at least has a uuid no matter what
        
    def __repr__(self):
        return '<Dataset (%s, %s)>' % (self.description, self.uuid)

    #get the available formats for the dataset
    #use the excluded_formats & the taxonomy_list defaults to get the actual supported formats
    def get_formats(self, req):
        if not self.is_available:
            return []
        lst = get_all_formats(req)
        exc_lst = self.excluded_formats

        #get all from one not in the other
        fmts = [i for i in lst if i not in exc_lst]
        return fmts

    #get the supported web services for the dataset
    def get_services(self, req):
        if not self.is_available:
            return []

        lst = get_all_services(req)
        exc_lst = self.excluded_services

        #get all from one not in the other
        svcs = [i for i in lst if i not in exc_lst]
        return svcs

    #return a source by set & extension for this dataset
    #default to active sources only
    def get_source(self, aset, extension, active=True):
        src = [s for s in self.sources if s.extension == extension and s.set == aset and s.active == active]
        return src[0] if src else None


    #get the source location (path) for the mapfile
    #return the source id (for the mapfile) and the data filepath
    def get_mapsource(self, fmtpath='', mongo_uri=None, srid=4326):
        '''
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

        '''

        if self.taxonomy == 'file':
            return None
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
            success, message = self.build_vector('shp', os.path.join(fmtpath, str(self.uuid), 'shp'), mongo_uri, srid)
            if success != 0: 
                return None, None
                
            return None, fmtfile

        #and there's nothing left
        return None, None

    #build the dict for the full service description
    #see the service view, search v3 results
    def get_full_service_dict(self, base_url, req):
        #update the url
        base_url += str(self.uuid)
    
        results = {'id': self.id, 'uuid': self.uuid, 'description': self.description, 
                'spatial': {'bbox': string_to_bbox(self.box), 'epsg': 4326}, 'lastupdate': self.dateadded.strftime('%Y%m%d'), 'name': self.basename, 'taxonomy': self.taxonomy,
                'categories': [{'theme': t.theme, 'subtheme': t.subtheme, 'groupname': t.groupname} for t in self.categories]}

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
                dlds = [(s.set, s.extension) for s in srcs if not s.is_external]

                links = [{s.extension: s.get_location()} for s in srcs if s.is_external]
            elif self.taxonomy == 'vector':
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
                    if sf.is_external:
                        links.append({f: sf.get_location()})
                    else:
                        dlds.append((sf.set, f))
            elif self.taxonomy == 'services':
                #TODO: figure out what to put here
                pass

            #combine the links (don't change url) with downloads (build url)
            dlds = [{s[1]: '%s/%s.%s.%s' % (base_url, self.basename, s[0], s[1]) for s in dlds}] if dlds else []
            dlds = links + dlds

            results.update({'services': [{s: '%s/services/ogc/%s' % (base_url, s) for s in svcs}] if svcs else [], 'downloads': dlds})

            #add the link to the mapper 
            #TODO: when the mapper moves, get rid of this
            if self.taxonomy in ['geoimage', 'vector']:
                mapper = '%s/mapper' % (base_url)
                results.update({'preview': mapper})

        else:
            results.update({'services': [], 'downloads': [], 'preview': '', 'availability': False})

        #check on the metadata
        if self.has_metadata_cache:
            standards = ['fgdc']
            exts = ['html', 'xml'] 
            #removing txt format as per karl (8/21/2012) - transform doesn't work properly and provides little benefit
            '''
            as {fgdc: {ext: url}}
            '''
            mt = [{s: {e: '%s/metadata/%s.%s' % (base_url, s, e) for e in exts} for s in standards}]

        else:
            mt = []
        results.update({'metadata': mt})
        

        #TODO: add the html card view also maybe

        #TODO: add related datasets

        #TODO: add link to collections it's in?

        #TODO: add project

        return results

    #build any vector format that comes from OGR (shp, kml, csv, gml, geojson)
    def build_vector(self, format, basepath, mongo_uri, epsg):
        '''
        pull the data from mongo
        get the attribute data
        create an ogr dataset
        populate

        note: we shouldn't care about whether the vector has one geometry for the recordset or multiple geometries
              for the recordset. the wkb is in each mongo document or the fid so we have the info we need 
              based on the mongo query resultset.
        '''
        

        #check the base location
        if os.path.abspath(basepath) != basepath or not os.path.isdir(basepath):
            return (1, 'invalid base path') #do something for a reasonable error

        if format == 'xls':
            #do something else
            return self.build_excel(basepath, mongo_uri)

        #get the data
        gm = gMongo(mongo_uri)
        vectors = gm.query({'d.id': self.id})

        #for everything else, we can use the ogr file formats
        driver = ogr.GetDriverByName(format_to_filetype(format))
        if driver is None:
            return (1, 'bad driver %s' % format)

        #set up a temp location
        tmp_path = tempfile.mkdtemp()
#        if format == 'shp':
#            tmp_file = tmp_path
#        else:
        tmp_file = os.path.join(tmp_path, '%s.%s' % (self.basename, format))

        datasource = driver.CreateDataSource(str(tmp_file))

        #get the default projection
        sr = epsg_to_sr(epsg)
        
        #set up the layer
        lyrtype = postgis_to_ogr(self.geomtype)
        layer = datasource.CreateLayer(str(self.basename), sr, lyrtype)

        layer.CreateField(ogr.FieldDefn('FID', ogr.OFTInteger))

        #deal with the fields
        #FID AND SHAPE are always set except for csv
        flds = self.attributes
        for fld in flds:
            layer.CreateField(fld.att_to_fielddefn(format))

        #TODO: add the observed field

        lyrdef = layer.GetLayerDefn()

        #now add data
        for v in vectors:
            feature = ogr.Feature(lyrdef)

            fid = str(v['f']['id'])
            feature.SetField('FID', int(fid))

            if format not in ['csv']:
                #add the geometry
                #TODO: test this
                if not 'geom' in v or ('geom' in v and not 'g' in v['geom']):
                    #go get it
                    shape = DBSession.query(Feature).filter(Feature.fid==int(fid)).first()
                    if not shape:
                        #there's no geometry!
                        continue
                    wkb = shape.geom
                else:    
                    wkb = v['geom']['g']
                geom = wkb_to_geom(wkb, epsg)
                feature.SetGeometry(geom)
                geom.Destroy()
            #TODO: think about GIDs but not very hard since we decided, what with the snotel, that they were not so meaningful

            #get the attribute data from the mongo doc
            atts = v['atts']
            
            #add the attribute data to the feature
            for fld in flds:
                att = [a for a in atts if a['name'] == fld.name]
                if att:
                    value = att[0]['val']  
                    #convert it to the right type based on the field with string (and encoded to utf-8) as default
                    value = convert_by_ogrtype(value, fld.ogr_type)
                    feature.SetField(str(fld.name), value)

            #TODO: add the observed value   
                

            #add the feature to the layer
            layer.CreateFeature(feature)
            feature.Destroy()

        #create a spatial index for the shapefile
        if format == 'shp':
            datasource.ExecuteSQL('CREATE SPATIAL INDEX ON %s' % (str(self.basename)))

        datasource.Destroy()
        layer = None

        #now the metadata and the projection file
        #just for shapes? or add metadata to all in the zip
        if format == 'shp':
            #write out the .prj file
            #TODO: add the basepath
            prjfile = open('%s.prj' % (os.path.join(tmp_path, self.basename)), 'w')
            sr.MorphToESRI()
            prjfile.write(sr.ExportToWkt())
            prjfile.close()

            #and the metadata
            #TODO: NEED SOME METADATA BUILDER THAT IS NOT RESPONSE TEMPLATE FOR THIS
            if self.has_metadata_cache:
                mt = str(self.original_metadata[0].original_xml.encode('utf8'))
                mtfile = open('%s.shp.xml' % (os.path.join(tmp_path, self.basename)), 'w')
                mtfile.write(mt)
                mtfile.close()
                
        #set up the formats directory WITHOUT BASENAMES
        #NO - move this to the view so that we can build vector formats wherever we want

        filename = os.path.join(basepath, '%s.%s.zip' % (self.uuid, format))
        #get the files for the zip
        #and move them to the formats cache
        files = []
        if format=='shp':
            exts = ['shp', 'shx', 'dbf', 'prj', 'shp.xml', 'sbn', 'sbx']
            for e in exts:
                if os.path.isfile(os.path.join(tmp_path, '%s.%s' % (self.basename, e))):
                    files.append(os.path.join(tmp_path, '%s.%s' % (self.basename, e)))
        else:
            files.append(os.path.join(tmp_path, '%s.%s' % (self.basename, format)))
        output = create_zip(filename, files)

        #and copy everything in files to the formats cache
        for f in files:
            outfile = f.replace(tmp_path, basepath)
            shutil.copyfile(f, outfile)
            

        #TODO: tidy up tmp (or figure out where it is and cron job?)
        
        return (0, 'success')

    #build an excel file for the data
    def build_excel(self, basepath, mongo_uri):
        style = xlwt.easyxf('font: name Times New Roman, color-index black, bold on', num_format_str='#,##0.00')
        workbook = xlwt.Workbook()
        sheetname = self.basename[0:29]

        worksheet = workbook.add_sheet(sheetname)
        x = 0
        y = 0

        #get the data
        gm = gMongo(mongo_uri)

        #only request as many as excel can handle
        vectors = gm.query({'d.id': self.id}, {}, 65534, 0)

        #build the header
        atts = self.attributes
        for att in atts:
            worksheet.write(y, x, att.name, style=style)
            x += 1
            
       #TODO: add observed timestamp column

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
                    value = str(va[0]['val'])
                 
                #convert it to the right type based on the field with string as default
                value = convert_by_ogrtype(value, att.ogr_type)

                worksheet.write(y, x, value)
                x += 1
                
            #TODO: add the observed timestamp data
            
            y += 1

#            #TODO: do something else for this, instead of just stopping partway through
#            if y > 65535:
#                break

        #write the file
        filename = os.path.join(basepath, '%s.xls' % (self.basename))
        workbook.save(filename)

        #just to be consistent with all of the other types
        #let's pack up a zip file
        output = create_zip(os.path.join(basepath, '%s.xls.zip' % (self.uuid)), [filename])
        
        return (0, 'success')
    

'''
gstoredata.categories and the join table
'''
categories_datasets = Table('categories_datasets', Base.metadata,
    Column('category_id', Integer, ForeignKey('gstoredata.categories.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)

class Category(Base):
    __table__ = Table('categories', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('theme', String(150)),
        Column('subtheme', String(150)),
        Column('groupname', String(150)),
        Column('apps', ARRAY(String)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )
    #relate to datasets handled in the datasets backref

    def __repr__(self):
        return '<Category (%s, %s, %s, %s)>' % (self.id, self.theme, self.subtheme, self.groupname)

'''
gstoredata.collections and join table
'''
collections_datasets = Table('collections_datasets', Base.metadata,
    Column('collection_id', Integer, ForeignKey('gstoredata.collections.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)

class Collection(Base):
    __table__ = Table('collections', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('description', String(200)),
        Column('apps', ARRAY(String)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

    #relate with datasets (see Dataset)

    #relate with categories with the backref
    categories = relationship('Category',
                    secondary='gstoredata.categories_collections',
                    backref='collections')

    def __init__(self, name, apps):  
        self.name = name
        self.apps = apps

    def __repr__(self):
        return '<Collection (%s, %s, %s)>' % (self.id, self.name, self.uuid)

#and the collection-to-category join
collections_categories = Table('categories_collections', Base.metadata,
    Column('collection_id', Integer, ForeignKey('gstoredata.collections.id')),
    Column('category_id', Integer, ForeignKey('gstoredata.categories.id')),
    schema='gstoredata'
)


'''
gstoredata.relationships
'''

class DatasetRelationship(Base):
    __table__ = Table('relationships', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('base_dataset', Integer),
        Column('related_dataset', Integer),
        Column('relationship', String(100)),
        Column('uuid', UUID, FetchedValue()),
        schema='gstoredata'
    )

    def __init__(self, base_dataset, related_dataset, relationship):
        self.base_dataset = base_dataset
        self.related_dataset = related_dataset
        self.relationship = relationship

    def __repr__(self):
        return '<Relationship (%s, %s, %s, %s)>' % (self.id, self.base_dataset, self.related_dataset, self.relationship)



'''
gstoredata.projects
'''
class Project(Base):
    __table__ = Table('projects', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(200)),
        Column('description', String(1000)),
        Column('acknowledgments', String(500)),
        Column('funder', String(200)),
        schema='gstoredata'
    )

