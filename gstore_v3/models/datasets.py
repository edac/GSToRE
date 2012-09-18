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
import subprocess, re
from xml.sax.saxutils import escape


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
            #TODO: why is this showing up in the logs as an error? even though the vectors are generated and diplayed correctly?
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


        ##TODO: need to escape the description text - this:
        #1935 15' Quad #217 Aerial Photo Mosaic Index - AZ
        #returns a result but doesn't parse
    
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

    #TODO: add a vector cache directory check + mkdir method


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

        #TODO: change to a datetime field and fix the format is necessary
        layer.CreateField(ogr.FieldDefn('Observed', ogr.OFTString))

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

            #TODO: check on the format (it's utc, but maybe we want a specific structure)   
            obs = str(v['obs']) if 'obs' in v else ''
            if obs:
                feature.SetField('Observed', obs)

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
            prjfile = open('%s.prj' % (os.path.join(tmp_path, self.basename)), 'w')
            sr.MorphToESRI()
            prjfile.write(sr.ExportToWkt())
            prjfile.close()

        #and the metadata
        if self.has_metadata_cache:
            mt = str(self.original_metadata[0].original_xml.encode('utf8'))
            mtfile = open('%s/%s.%s.xml' % (tmp_path, self.basename, format), 'w')
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
        files.append('%s/%s.%s.xml' % (tmp_path, self.basename, format))
        output = create_zip(filename, files)

        #and copy everything in files to the formats cache
        for f in files:
            outfile = f.replace(tmp_path, basepath)
            shutil.copyfile(f, outfile)
            
        
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
                    value = str(va[0]['val'])
                 
                #convert it to the right type based on the field with string as default
                value = convert_by_ogrtype(value, att.ogr_type)

                worksheet.write(y, x, value)
                x += 1
                
            #TODO: deal with the utc format and make sure it's what we want
            obs = v['obs'] if 'obs' in v else ''
            if obs:
                worksheet.write(y, x, obs)
            
            y += 1

        #write the file
        filename = os.path.join(basepath, '%s.xls' % (self.basename))
        workbook.save(filename)

        #just to be consistent with all of the other types
        #let's pack up a zip file
        if self.has_metadata_cache:
            mt = str(self.original_metadata[0].original_xml.encode('utf8'))
            mtfile = open('%s.xls.xml' % (os.path.join(tmp_path, self.basename)), 'w')
            mtfile.write(mt)
            mtfile.close()
            
        output = create_zip(os.path.join(basepath, '%s.xls.zip' % (self.uuid)), [filename, '%s.xls.xml' % (os.path.join(tmp_path, self.basename))])
        
        return (0, 'success')


    def stream_vector(self, format, basepath, mongo_uri, epsg, baseurl=''):
        '''
        optimization to speed up the file generation, especially for large vector datasets
        processing time goes from 15 minutes to 1 and change for a 100,000 feature dataset   

        if the request is for gml, json, csv, or kml -> stream those
        if the request is for xls -> run the original excel builder (nothing we can do about that one)
        if the request is for a shapefile -> stream as gml and convert with ogr2ogr 

        then grab the metadata and pack everything as a zip for delivery
        and copy all of the files to the formats cache for mapserver, etc
        '''

        #check the base location
        if os.path.abspath(basepath) != basepath or not os.path.isdir(basepath):
            return (1, 'invalid base path')

        #send the excel off for processing
        if format == 'xls':
            return self.build_excel(basepath, mongo_uri)    

        fields = self.attributes
        encode_as = 'utf-8'

        fmt = 'gml' if format == 'shp' else format

        #gml generator
        def generate_stream():
            #set up the head, tail, folder info, etc
            folder_head = ''
            field_set = ''
            folder_tail = ''
            delimiter = '\n'
            schema_url = ''
            if fmt == 'geojson':
                head = """{"type": "FeatureCollection", "features": ["""
                tail = "\n]}"
                delimiter = ',\n'
            elif fmt == 'kml':
                head = """<?xml version="1.0" encoding="UTF-8"?>
                                <kml xmlns="http://earth.google.com/kml/2.2">
                                <Document>"""
                tail = """\n</Document>\n</kml>"""
                folder_head = "<Folder><name>%s</name>" % (self.description)
                folder_tail = "</Folder>"

                schema_url = baseurl + '%s/attributes.kml' % (self.uuid)

                kml_flds = [{'type': ogr_to_kml_fieldtype(f.ogr_type), 'name': f.name} for f in fields]
                kml_flds.append({'type': 'string', 'name': 'observed'})
                field_set = """<Schema name="%(name)s" id="%(id)s">%(sfields)s</Schema>""" % {'name': str(self.uuid), 'id': str(self.uuid), 
                    'sfields': '\n'.join(["""<SimpleField type="%s" name="%s"><displayName>%s</displayName></SimpleField>""" % (k['type'], k['name'], k['name']) for k in kml_flds])
                }     
            elif fmt == 'csv':
                head = '' 
                tail = ''
                delimiter = '\n'

                field_set = ','.join([f.name for f in fields]) + ',fid,dataset,observed\n'
            elif fmt == 'gml':
                head = """<?xml version="1.0" encoding="UTF-8"?>
                                        <gml:FeatureCollection 
                                            xmlns:gml="http://www.opengis.net/gml" 
                                            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                                            xmlns:xlink="http://www.w3.org/1999/xlink"
                                            xmlns:ogr="http://ogr.maptools.org/">
                                        <gml:description>GSTORE API 3.0 Vector Stream</gml:description>\n""" 
                tail = """\n</gml:FeatureCollection>"""
            elif fmt == 'json':
                head = """{"features": ["""
                tail = "]}"
                delimiter = ',\n'
        
            #get the vectors
            gm = gMongo(mongo_uri)
            vectors = gm.query({'d.id': self.id})
            total = vectors.count()

            yield head + folder_head + field_set
            cnt = 0
            for vector in vectors:
                result = convert(vector, schema_url, epsg)
                if cnt < total - 1 and total > 1:
                    result += delimiter

                cnt += 1
                yield result.encode(encode_as)

            yield folder_tail + tail

        #convert the mongo doc to a feature
        def convert(vector, schema_url, epsg):
            #convert the mongo to a chunk of something based on the format
            fid = int(vector['f']['id'])
            did = int(vector['d']['id'])
            obs = vector['obs'] if 'obs' in vector else ''

            #get the geometry
            if not fmt in ['csv', 'json']:
                wkb = vector['geom']['g'] if 'geom' in vector else ''
                if not wkb:
                    #need to get it from shapes
                    feat = DBSession.query(Feature).filter(Feature.fid==fid).first()
                    wkb = feat.geom
                    
                #and convert to geojson, kml, or gml
                geom_repr = wkb_to_output(wkb, epsg, fmt)
                if not geom_repr:
                    return ''

            atts = vector['atts']
            #there's some wackiness with a unicode char and mongo (and also a bad char in the data, see fid 6284858)
            #convert atts to name, value tuples so we only have to deal with the wackiness once
            atts = [(a['name'], unicode(a['val']).encode('ascii', 'xmlcharrefreplace')) for a in atts]

            #TODO: add observed to kml/gml and field list (also double check attribute schema for kml for observed)
            if fmt == 'kml': 
                #make sure we've encoded the value string correctly for kml
                feature = "\n".join(["""<SimpleData name="%s">%s</SimpleData>""" % (v[0], re.sub(r'[^\x20-\x7E]', '', escape(str(v[1])))) for v in atts])

                #and no need for a schema url since it can be an internal schema linked by uuid here
                feature = """<Placemark id="%s">
                            <name>%s</name>
                            %s\n%s
                            <ExtendedData><SchemaData schemaUrl="%s">%s</SchemaData></ExtendedData>
                            <Style><LineStyle><color>ff0000ff</color></LineStyle><PolyStyle><fill>0</fill></PolyStyle></Style>
                            </Placemark>""" % (fid, fid, geom_repr, '', schema_url, feature)
            elif fmt == 'gml':
                #going to match the gml from the dataset downloader
                #need a list of values as <ogr:{att name}>VAL</ogr:{att name}>
                vals = ''.join(['<ogr:%s>%s</ogr:%s>' % (a[0], re.sub(r'[^\x20-\x7E]', '', escape(str(a[1]))), a[0]) for a in atts])
                feature = """<gml:featureMember><ogr:g_%(basename)s><ogr:geometryProperty>%(geom)s</ogr:geometryProperty>%(values)s</ogr:g_%(basename)s></gml:featureMember>""" % {
                        'basename': self.basename, 'geom': geom_repr, 'values': vals} 
            elif fmt == 'geojson':
                vals = dict([(a[0], a[1]) for a in atts])
                vals.update({'fid':fid, 'dataset_id': did, 'observed': obs})
                feature = json.dumps({"type": "Feature", "properties": vals, "geometry": json.loads(geom_repr)})
            elif fmt == 'csv':
                vals = []
                for f in fields:
                    att = [a for a in atts if a[0] == f.name]
                    if not att:
                        continue
                    vals.append(att[0][1])
                vals += [str(fid), str(did), obs]
                feature = ','.join(vals)
            elif fmt == 'json':
                #no geometry, just attributes (good for timeseries requests)
                vals = dict([(a[0], a[1]) for a in atts])
                feature = json.dumps({'fid': fid, 'dataset_id': str(vector['d']['u']), 'properties': vals, 'observed': obs})
            else:
                feature = ''
                    
            return feature

        #run the generator for the streaming
        #set up the temporary location
        tmp_path = tempfile.mkdtemp()
        tmp_file = os.path.join(tmp_path, '%s.%s' % (self.basename, fmt))
        for g in generate_stream():
            #append a tmp file
            with open(tmp_file, 'a') as f:
                f.write(g)  
                      

        #pack up the results, with metadata, as a zip
        filename = os.path.join(basepath, '%s.%s.zip' % (self.uuid, format))
        files = []
        if format == 'shp':
            #convert it first
            s = subprocess.Popen(['ogr2ogr', '-f', 'ESRI Shapefile', os.path.join(basepath, '%s.shp' % (self.basename)), tmp_file], shell=False)    
            status = s.wait()
            #note: the prj file should be generated already
            #TODO: add a spatial index
        
            exts = ['shp', 'shx', 'dbf', 'prj', 'shp.xml', 'sbn', 'sbx']
            for e in exts:
                if os.path.isfile(os.path.join(basepath, '%s.%s' % (self.basename, e))):
                    files.append(os.path.join(basepath, '%s.%s' % (self.basename, e)))
        else:
            files.append(os.path.join(tmp_path, '%s.%s' % (self.basename, format)))

        #return (1, ','.join(files))
            
        #add a metadata file if there's any metadata to add
        if self.original_metadata:
            xml = self.original_metadata[0].original_xml.encode(encode_as)
            mtfile = open('%s.%s.xml' % (os.path.join(tmp_path, self.basename), format), 'w')
            mtfile.write(xml)
            mtfile.close()
            files.append('%s.%s.xml' % (os.path.join(tmp_path, self.basename), format))

        #create the zip file
        output = create_zip(filename, files)

        #copy to the formats cache
        if format != 'shp':
            for f in files:
                outfile = f.replace(tmp_path, basepath)
                shutil.copyfile(f, outfile)        

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

    def __init__(self, theme, subtheme, groupname, apps):
        self.theme = theme
        self.subtheme = subtheme
        self.groupname = groupname
        self.apps = apps

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

    def __init__(self, name, description, funder):
        self.name = name
        self.description = description
        self.funder = funder

    def __repr__(self):
        return '<Project (%s, %s, %s)>' % (self.id, self.name, self.funder)

