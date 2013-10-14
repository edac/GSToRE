from pyramid.view import view_config
from pyramid.response import Response, FileResponse
from pyramid.renderers import render_to_response
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError, HTTPBadRequest, HTTPServiceUnavailable

from sqlalchemy.exc import DBAPIError

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import *
from ..models.sources import Source, SourceFile, MapfileSetting
from ..models.metadata import OriginalMetadata, DatasetMetadata
from ..models.apps import GstoreApp

import os, json, re, tempfile
from xml.sax.saxutils import escape

from ..lib.utils import *
from ..lib.spatial import *
from ..lib.database import *
from ..lib.mongo import gMongoUri
from ..lib.es_indexer import DatasetIndexer


'''
datasets
'''
#TODO: add dataset statistics view - min/max per attribute, histogram info, etc

def return_fileresponse(output, mimetype, filename):
    fr = FileResponse(output, content_type=mimetype)
    fr.content_disposition = 'attachment; filename=%s' % filename
    
    #TODO: may want to reconsider the cookie age
    '''
    This is specifically for the fileDownload jquery plugin in RGIS/EPSCoR.
    It acts like a flag to close the modal popup and only works if the urls
    from gstore are rewritten to interface_host/datasets... so that the cookie is set
    for the client as is then usable there. Otherwise it's a whole CORS thing that we
    don't really want to get into. Basically, don't change the key and don't chuck the cookie
    as long as rgis/epscor use that plugin.
    '''
    fr.set_cookie(key='fileDownload', value='true', max_age=31536000, path='/')
    return fr

#REMOVED: run this functionality from the interface and use the services.json
###TODO: add the html renderer to this
#@view_config(route_name='html_dataset', renderer='dataset_card.mako')
#def show_html(request):
#    dataset_id = request.matchdict['id']
#    app = request.matchdict['app']

#    d = get_dataset(dataset_id)

#    if not d:
#        return HTTPNotFound()

#    #http://129.24.63.66/gstore_v3/apps/rgis/datasets/8fc27d61-285d-45f6-8ef8-83785d62f529/soils83.html
#    #http://{load_balancer}/apps/.....

#    load_balancer = request.registry.settings['BALANCER_URL']
#    base_url = '%s/apps/%s/datasets/' % (load_balancer, app)
#    
#    rsp = d.get_full_service_dict(base_url, request)

#    return rsp
    

@view_config(route_name='zip_dataset')
@view_config(route_name='dataset')
def dataset(request):
    #use the original dataset_id structure 
    #or the new dataset uuid structure

    app = request.matchdict['app']
    dataset_id = request.matchdict['id']
    format = request.matchdict['ext']
    datatype = request.matchdict['type'] #original vs. derived
    basename = request.matchdict['basename']

    #limited to ignore-cache (T/F) for now
    params = normalize_params(request.params)

    #go get the dataset
    d = get_dataset(dataset_id)

    if not d:
        return HTTPNotFound()

    #make sure it's available
    #TODO: replace this with the right status code (not sure what that code is though)
    if d.is_available == False:
        return HTTPServiceUnavailable()

    if d.is_embargoed:
        return HTTPNotFound('This dataset is embargoed.')     

    if app not in d.apps_cache:
        return HTTPBadRequest()

    #so now we have the dataset
    #let's get the source for the set + extension combo
    #need the mimetype, list of files
    '''
    sources - get sources by set + extension (extension == format)
            && by type (original | derived; and as part of zip filename for being nice) 

    if sources && services - redirect to the location
    if not sources && vector - check the cache
    if not sources && vector && no cache - generate cache file
    '''  

    #ignore the cache on request or if the dataset is set to ignore cache (is_cacheable==False)
    if not d.is_cacheable:
        ignore_cache = True
    else:
        ignore_cache = params['ignore-cache'].lower() == 'true' if 'ignore-cache' in params else False

    xslt_path = request.registry.settings['XSLT_PATH']
    fmtpath = request.registry.settings['FORMATS_PATH']
    tmppath = request.registry.settings['TEMP_PATH']
    base_url = '%s/apps/%s/datasets/' % (request.registry.settings['BALANCER_URL'], app)
    
    #check for a requested metadata standard
    #if there isn't one, get the app preferred ordered list and go for the best match
    #get the supported standard for fgdc for the given dataset (plain or rse)
    supported_standards = d.get_standards(request)

    std = ''    
    if 'standard' in params:
        std = params['standard']
        std = std if std in supported_standards else ''
        
    if not std:
        req_app = DBSession.query(GstoreApp).filter(GstoreApp.route_key==app.lower()).first()
        if not req_app:
            app_prefs = ['FGDC-STD-001-1998','FGDC-STD-012-2002','ISO-19115:2003']
        else:
            app_prefs = req_app.preferred_metadata_standards    
        std = next(s for s in app_prefs if s in supported_standards)

    #TODO: what happens if standard is null?
    
    metadata_info = {'app': app, 'base_url': base_url, 'standard': std, "xslt_path": xslt_path + '/xslts', 'validate': False}
    
    taxonomy = str(d.taxonomy)
    if taxonomy in ['services']:
        src = d.get_source(datatype, format)
        
        if not src:
            #not valid source information for the dataset
            return HTTPNotFound()

        loc = src.get_location()
        if not loc:
            return HTTPNotFound()
            
        return HTTPFound(location=loc)

    #check for a source for everyone
    src = d.get_source(datatype, format)
    if not src and d.taxonomy in ['geoimage', 'file']:
        return HTTPNotFound()

    #outside link so redirect
    if src and src.is_external:
        loc = src.get_location()
        return HTTPFound(location=loc)

    mimetype = str(src.file_mimetype) if src else 'application/x-zip-compressed'

    #return things that shouldn't be zipped (pdfs, etc)
    if format != 'zip' and mimetype != 'application/x-zip-compressed':
        output = src.get_location(format)
        return return_fileresponse(output, mimetype, output.split('/')[-1])

    #return the already packed zip (this assumes that everything set to zip is already a zip)
    if format == 'zip':
        output = src.get_location(format)

        ext = output.split('.')[-1]
        if ext == format:
            #if it really is a zip file.
            #otherwise we want to pack it with the redundant _zip.zip structure
            return return_fileresponse(output, mimetype, output.split('/')[-1])
    
    #check the cache for a zip
    output = os.path.join(fmtpath, str(d.uuid), format, '%s_%s.zip' % (str(d.basename), format))
    if os.path.isfile(output) and not ignore_cache:
        return return_fileresponse(output, mimetype, output.split('/')[-1])

    #first check for the uuid + format subdirectories in the formats dir
    if ignore_cache:
        #create a tmp directory
        output_path = tempfile.mkdtemp()
    else:
        output_path = os.path.join(fmtpath, str(d.uuid), format)
        if not os.path.isdir(output_path):
            if not os.path.isdir(os.path.join(fmtpath, str(d.uuid))):
                os.mkdir(os.path.join(fmtpath, str(d.uuid)))
            os.mkdir(output_path)

    outname = '%s_%s.zip' % (d.basename, format)
    output_file = os.path.join(output_path, '%s_%s.zip' % (str(d.basename), format))

    #TODO: add some check for derived v original for the vector datasets
    #TODO: and also, what to do about that if there are in fact datasets with original shp and derived shp in clusterdata?

    #no zip. need to pack it up (raster/file) or generate it (vector)
    if taxonomy in ['geoimage', 'file']:
        #pack up the zip to the formats cache
        output = src.pack_source(output_path, outname, xslt_path, metadata_info)
        
        return return_fileresponse(output, mimetype, outname)
    elif taxonomy in ['vector']:
        #generate the file and pack the zip
        #note that the kml isn't being packed as kmz - we include metadata with every download here

        #set up the mongo connection
        mconn = request.registry.settings['mongo_uri']
        mcoll = request.registry.settings['mongo_collection']
        mongo_uri = gMongoUri(mconn, mcoll)

        srid = int(request.registry.settings['SRID'])

        #for the original write to ogr directly build option
        #success = d.build_vector(format, cached_path, mongo_uri, srid, metadata_info)

        #for the new stream to ogr2ogr option (or just stream if not shapefile)
        load_balancer = request.registry.settings['BALANCER_URL']
        base_url = '%s/apps/%s/datasets/' % (load_balancer, app)
#        TODO: don't forget the metadata_info HERE!
        success = d.stream_vector(format, output_path, mongo_uri, srid, metadata_info)

        #check the response for failure
        if success[0] != 0:
            return HTTPServerError()    

        #TODO: the vectors are returning as uuid.format.zip instead of basename.format.zip
        return return_fileresponse(output_file, mimetype, outname)    

    #if we're here something really bad is happening
    return HTTPNotFound()

@view_config(route_name='dataset_streaming')
def stream_dataset(request):
    '''
    stream dataset as json, kml, csv, geojson, gml
    for improved access options (pull in json for a table on a webpage, etc)
    BUT only for vector datasets

    params:
        bbox (return features intersecting box)
        datetime (return features within time range (sensor data, etc))
        
    '''

    app = request.matchdict['app']
    dataset_id = request.matchdict['id']
    format = request.matchdict['ext']

    if format not in ['json', 'geojson', 'csv', 'kml', 'gml']:
        return HTTPBadRequest()

    #TODO: add the parmaeter searches
    params = normalize_params(request.params)

    #go get the dataset
    d = get_dataset(dataset_id)    

    if not d:
        return HTTPNotFound()

    if d.taxonomy != 'vector' or d.inactive or app not in d.apps_cache or d.is_embargoed:
        return HTTPBadRequest()

    if not d.is_available:
        return HTTPServiceUnavailable()

    connstr = request.registry.settings['mongo_uri']
    collection = request.registry.settings['mongo_collection']
    mongo_uri = gMongoUri(connstr, collection)
    gm = gMongo(mongo_uri)

    encode_as = 'utf-8'   
    epsg = int(request.registry.settings['SRID'])
    
    fields = d.attributes

    folder_head = ''
    folder_tail = ''
    delimiter = '\n'
    schema_url = ''
    field_set = ''
    
    if format == 'geojson':
        content_type = 'application/json; charset=UTF-8'
        head = """{"type": "FeatureCollection", "features": ["""
        tail = "\n]}"
        delimiter = ',\n'
    elif format == 'json':
        content_type = 'application/json; charset=UTF-8'
        head = """{"features": ["""
        tail = "]}"
        delimiter = ',\n'
    elif format == 'kml':
        content_type = 'application/vnd.google-earth.kml+xml; charset=UTF-8'
        head = """<?xml version="1.0" encoding="UTF-8"?>
                        <kml xmlns="http://earth.google.com/kml/2.2">
                        <Document>"""
        tail = """\n</Document>\n</kml>"""
        folder_head = "<Folder><name>%s</name>" % (d.description)
        folder_tail = "</Folder>"

        load_balancer = request.registry.settings['BALANCER_URL']
        base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

        kml_flds = [{'type': ogr_to_kml_fieldtype(f.ogr_type), 'name': f.name} for f in fields]
        kml_flds.append({'type': 'string', 'name': 'observed'})
        field_set = """<Schema name="%(name)s" id="%(id)s">%(sfields)s</Schema>""" % {'name': str(d.uuid), 'id': str(d.uuid), 
            'sfields': '\n'.join(["""<SimpleField type="%s" name="%s"><displayName>%s</displayName></SimpleField>""" % (k['type'], k['name'], k['name']) for k in kml_flds])
        }

        schema_url = '%s%s/attributes.kml' % (base_url, d.uuid)

    elif format == 'gml':
        content_type = 'application/xml; subtype="gml/3.1.1; charset=UTF-8"'
        head = """<?xml version="1.0" encoding="UTF-8"?>
                                <gml:FeatureCollection 
                                    xmlns:gml="http://www.opengis.net/gml" 
                                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                                    xmlns:xlink="http://www.w3.org/1999/xlink"
                                    xmlns:ogr="http://ogr.maptools.org/">
                                <gml:description>GSTORE API 3.0 Vector Stream</gml:description>\n""" 
        tail = """\n</gml:FeatureCollection>"""
    elif format == 'csv':
        content_type = 'text/csv; charset=UTF-8'
        head = '' 
        tail = ''
        delimiter = '\n'
        field_set = ','.join([f.name for f in fields]) + ',fid,dataset,observed\n'
    else:
        return HTTPBadRequest()

    vectors = gm.query({'d.id': d.id})
    total = vectors.count()

    def yield_results():
        yield head

        cnt = 0
        for vector in vectors:
            vector_result = convert_vector(vector, fields, format, d.basename, schema_url, epsg)
            if cnt == 0:
                vector_result = folder_head + field_set + vector_result + delimiter
            elif cnt == total - 1:
                vector_result += folder_tail
            else:
                vector_result += delimiter

            cnt += 1
            yield vector_result.encode(encode_as)

        yield tail

    #TODO: figure out a better solution (and revise the feature streamer) that can call some generic method 
    #      and return a formatted vector chunk
    def convert_vector(vector, fields, fmt, basename, schema_url, epsg):
        '''
        for notes, see the feature streamer methods
        '''
        fid = int(vector['f']['id'])
        did = int(vector['d']['id'])
        obs = vector['obs'] if 'obs' in vector else ''
        obs = obs.strftime('%Y-%m-%dT%H:%M:%S+00') if obs else ''

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
        atts = [(a['name'], unicode(a['val']).encode('ascii', 'xmlcharrefreplace')) for a in atts]
        atts.append(('observed', obs))

        if fmt == 'kml':
            feature = "\n".join(["""<SimpleData name="%s">%s</SimpleData>""" % (v[0], re.sub(r'[^\x20-\x7E]', '', escape(str(v[1])))) for v in atts])
            feature = """<Placemark id="%s">
                        <name>%s</name>
                        %s\n%s
                        <ExtendedData><SchemaData schemaUrl="%s">%s</SchemaData></ExtendedData>
                        <Style><LineStyle><color>ff0000ff</color></LineStyle><PolyStyle><fill>0</fill></PolyStyle></Style>
                        </Placemark>""" % (fid, fid, geom_repr, '', schema_url, feature)
        elif fmt == 'gml':
            vals = ''.join(['<ogr:%s>%s</ogr:%s>' % (a[0], re.sub(r'[^\x20-\x7E]', '', escape(str(a[1]))), a[0]) for a in atts])
            feature = """<gml:featureMember><ogr:g_%(basename)s><ogr:geometryProperty>%(geom)s</ogr:geometryProperty>%(values)s</ogr:g_%(basename)s></gml:featureMember>""" % {'basename': basename, 'geom': geom_repr, 'values': vals} 
        elif fmt == 'json':
            vals = dict([(a[0], a[1]) for a in atts])
            feature = json.dumps({'fid': fid, 'dataset_id': str(vector['d']['u']), 'properties': vals})
        elif fmt == 'geojson':
            vals = dict([(a[0], a[1]) for a in atts])
            vals.update({'fid':fid, 'dataset_id': did})
            feature = json.dumps({"type": "Feature", "properties": vals, "geometry": json.loads(geom_repr)})
        elif fmt == 'csv':
            vals = []
            for f in fields:
                att = [a for a in atts if a[0] == f.name]
                #need to be sure to handle the fact that if the value for an attribute is null, it's not in mongo
                #i hope that doesn't turn out to be really bad.
                v = att[0][1] if att else ""
                vals.append(v)
            vals += [str(fid), str(did), obs]
            feature = ','.join(vals)
        else:
            feature = ''

        return feature
                
    response = Response()
    response.content_type = content_type
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.app_iter = yield_results()
    return response
   

#TODO: add params for including styles with output (so render from gstore for niceness or just deliver html structure for epscor/rgis, etc)
#@view_config(route_name='dataset_services', renderer='json')
@view_config(route_name='dataset_services', renderer='dataset.mako')
#@view_config(route_name='dataset_services', match_param="ext=json", renderer='prettyjson')
def services(request):
    #return .json (or whatever) with all services for dataset defined 
    #i.e. links to the ogc services, links to the downloads, etc
    #same format as stuff from search request?

    app = request.matchdict['app']
    dataset_id = request.matchdict['id']
    format = request.matchdict['ext']

    #go get the dataset
    d = get_dataset(dataset_id)    

    if not d:
        return HTTPNotFound()

    if d.is_embargoed or d.inactive:
        return HTTPNotFound()

    #ogc services as {host}/apps/{app}/datasets/{id}/services/{service_type}/{service}
    #downloads as {host}/apps/{app}/datasets/{id}.{set}.{ext}
    #metadata as {host}/apps/{app}/datasets/{id}/metadata/{standard}.{ext}
    '''
    {
        id:
        uuid:
        description: 
        categories: [] as {theme, subtheme, groupname}
        bbox: 
        epsg: 
        downloads: [] as {fmt: url}
        services: [] as {wxs: url}
        metadata: [] as {standard: {fmt: url}}
    }
    '''

#    #get the host url
#    host = request.host_url
#    g_app = request.script_name[1:]
#    base_url = '%s/%s/apps/%s/datasets/' % (host, g_app, app)

    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

    rsp = d.get_full_service_dict(base_url, request)

    if format == 'json':
        response = render_to_response('json', rsp, request=request)
        response.content_type='application/json'
        return response
    elif format == 'html':
        #TODO: split out into 2 html formats: one basic one for the kml and one complete nice looking one for everything else?
        return rsp

@view_config(route_name='dataset_statistics')
def statistics(request):
    '''
    return some dataset-level stats:

    vector:
        for each attribute (that is int or real)
            min/max
            basic histogram info

    raster
        min/max
        basic histogram info


    in part to help with classification (although that requires sld support)
    '''
    return Response()

@view_config(route_name='dataset_indexer', renderer='json')
def indexer(request):
    '''
    return the document for the elasticsearch index

    THIS IS NOT really for production but intersecting everything to get a list of quads will explode the database
    '''

    app = request.matchdict['app']
    dataset_id = request.matchdict['id']

    #go get the dataset
    d = get_dataset(dataset_id)    

    if not d:
        return HTTPNotFound()

        
    ''' 
    {
    "dataset": {
        "properties": {
            "abstract": {
                "type": "string"
            },
            "active": {
                "type": "boolean"
            },
            "aliases": {
                "type": "string",
                "index_name": "alias"
            },
            "applications": {
                "type": "string",
                "index_name": "application"
            },
            "area": {
                "type": "double"
            },
            "available": {
                "type": "boolean"
            },
            "category": {
                "properties": {
                    "apps": {
                        "type": "string"
                    },
                    "groupname": {
                        "type": "string"
                    },
                    "subtheme": {
                        "type": "string"
                    },
                    "theme": {
                        "type": "string"
                    }
                }
            },
            "category_facets": {
                "type": "nested",
                "properties": {
                    "apps": {
                        "type": "string"
                    },
                    "groupname": {
                        "type": "string",
                        "index": "not_analyzed",
                        "omit_norms": true,
                        "index_options": "docs"
                    },
                    "subtheme": {
                        "type": "string",
                        "index": "not_analyzed",
                        "omit_norms": true,
                        "index_options": "docs"
                    },
                    "theme": {
                        "type": "string",
                        "index": "not_analyzed",
                        "omit_norms": true,
                        "index_options": "docs"
                    }
                }
            },
            "date_added": {
                "type": "date",
                "format": "YYYY-MM-dd"
            },
            "embargo": {
                "type": "boolean"
            },
            "formats": {
                "type": "string"
            },
            "geomtype": {
                "type": "string"
            },
            "isotopic": {
                "type": "string"
            },
            "location": {
                "properties": {
                    "bbox": {
                        "type": "geo_shape",
                        "tree": "quadtree",
                        "tree_levels": 40
                    },
                    "counties": {
                        "type": "string",
                        "index_name": "county"
                    },
                    "quads": {
                        "type": "string",
                        "index_name": "quad"
                    }
                }
            },
            "services": {
                "type": "string"
            },
            "taxonomy": {
                "type": "string"
            },
            "title": {
                "type": "string"
            }
        }
    }
}
    '''

    index = d.get_index_doc(request)

    return {"dataset": index}

'''
dataset maintenance
'''
@view_config(route_name='add_dataset', request_method='POST')
def add_dataset(request):
    app = request.matchdict['app']

    #generate uuid here, not through postgres - need to use 
    #outside uuids for data replication (nv/id data as local dataset with pointer to their files)


    '''
    we are skipping the file upload - no one wanted to do that (or no one wanted it to post to ibrix)
    so maybe add it again later if it comes up, but we're starting with the basic json post functionality

    {
        'description':
        'basename':
        'dates': {
            'start': 
            'end':
        }
        'uuid': 
        'taxonomy': 
        'spatial': {
            'geomtype':
            'epsg':
            'bbox':
            'geom': 
            'features':
            'records':
        }
        'metadata': {
            "xml":
            "standard":
            "upgrade": t/f
        },
        'project': 
        'apps': []
        'formats': []
        'services': []
        'standards': []
        'categories': [
            {
                'theme':
                'subtheme':
                'groupname':
            }
        ]
        'sources': [
            {
                'set':
                'extension':
                'external':
                'mimetype':
                'identifier':
                'identifier_type':
                'files': [],
                'settings': {'basic': {'WCS-NODATA': 'some value'}, 'classes': {'class': {style stuff here}}}
                
            }
        ]
        'embargo': {
            'release_date': 
            'embargoed': 
        }
    }

    '''

    #TODO: finish the settings insert (class & style)

    #get the data as json
    post_data = request.json_body

    SRID = int(request.registry.settings['SRID'])
    excluded_formats = get_all_formats(request)
    excluded_services = get_all_services(request)
    excluded_standards = get_all_standards(request)

    #do stuff
    description = post_data['description']
    basename = post_data['basename']
    taxonomy = post_data['taxonomy']
    apps = post_data['apps'] if 'apps' in post_data else []
    validdates = post_data['dates'] if 'dates' in post_data else {}
    spatials = post_data['spatial']
    formats = post_data['formats']
    services = post_data['services']
    categories = post_data['categories']
    sources = post_data['sources']
    metadatas = post_data['metadata']
    standards = post_data['standards'] if 'standards' in post_data else []
    acquired = post_data['acquired'] if 'acquired' in post_data else ''

    box = map(float, spatials['bbox'].split(','))
    epsg = spatials['epsg']
    geomtype = spatials['geomtype'] if 'geomtype' in spatials else ''
    geom = spatials['geom'] if 'geom' in spatials else ''
    features = spatials['features'] if 'features' in spatials else 0
    records = spatials['records'] if 'records' in spatials else 0

    #add the inactive flag
    active = post_data['active'].lower() if 'active' in post_data else ''

    project = post_data['project'] if 'project' in post_data else ''

    embargo = post_data['embargo'] if 'embargo' in post_data else {}
    #this is not good
    embargoed = True if 'embargoed' in embargo else False
    embargo_release = embargo['release_date'] if embargo else ''
    

    #we may have instances where we have an external dataset (tri-state replices for example)
    #and we want to keep the uuid for that dataset so we can provide a uuid or make one here
    provided_uuid = post_data['uuid'] if 'uuid' in post_data else generate_uuid4()

    #like make the new dataset
    new_dataset = Dataset(description)
    new_dataset.basename = basename
    new_dataset.taxonomy = taxonomy
    if taxonomy == 'vector':
        new_dataset.geomtype = geomtype
        new_dataset.feature_count = features
        new_dataset.record_count = records
    new_dataset.orig_epsg = epsg
    new_dataset.inactive = False if active == 'true' else True

    if embargoed == 'true':
        #need to set is_embargoed and the release date so the dataset is unavailable through gstore
        new_dataset.is_embargoed = True
        new_dataset.embargo_release_date = embargo_release

    if not geom:
        #go make one
        geom = bbox_to_wkb(box, SRID)

    new_dataset.geom = geom
    new_dataset.box = box
    
    new_dataset.apps_cache = [app] + apps

    #TODO: get rid of formats_cache (once v2 tools issue is resolved in search datasets)
    #new_dataset.formats_cache = ','.join(formats)
    new_dataset.excluded_formats = [f for f in excluded_formats if f not in formats]
    new_dataset.excluded_services = [s for s in excluded_services if s not in services]
    new_dataset.excluded_standards = [s for s in excluded_standards if s not in standards]

    new_dataset.uuid = provided_uuid

    #add the category set (if not in categories) and assign to dataset
    for category in categories:
        theme = category['theme']
        subtheme = category['subtheme']
        groupname = category['groupname']

        c = DBSession.query(Category).filter(and_(Category.theme==theme, Category.subtheme==subtheme, Category.groupname==groupname)).first()
        if not c:
            #we'll need to add a new category BEFORE running this (?)
            return HTTPBadRequest('Missing category triplet')

        new_dataset.categories.append(c)

    if validdates:
        #TODO: add some date checking
        validstart = validdates['start'] if 'start' in validdates else None
        validend = validdates['end'] if 'end' in validdates else None
        new_dataset.begin_datetime = validstart
        new_dataset.end_datetime = validend

    if acquired:
        new_dataset.date_acquired = acquired

    #add the metadata    
    #get the xml, standard (and it should be in supported list), upgrade flag
    original_xml = metadatas['xml'] if 'xml' in metadatas else ''
    original_std = metadatas['standard'] if 'standard' in metadatas else ''
    upgrade_to_gstore = metadatas['upgrade'] if 'upgrade' in metadatas else ''
    upgrade_to_gstore = True if upgrade_to_gstore.lower() == 'true' else False
    if metadatas:
        if original_xml and original_std and original_std != 'GSTORE':
            #dump the xml in the table and tag the standard
            o = OriginalMetadata()
            o.original_xml = original_xml
            o.original_xml_standard = original_std
            new_dataset.original_metadata.append(o)

            if upgrade_to_gstore:
                #need to convert but not with the original method (dataset has not been committed, original metadata has not been committed)
                xslt_path = request.registry.settings['XSLT_PATH'] + '/xslts'
                gstore_xml = o.convert_to_gstore_metadata(xslt_path, False)
                if not gstore_xml:
                    return HTTPServerError('Upgrade to gstore failed')

                g = DatasetMetadata()
                g.gstore_xml = gstore_xml
                new_dataset.gstore_metadata.append(g)

        elif original_xml and original_std == 'GSTORE':
            #validate the xml
            #if valid gstore, put in gstore

            valid = validate_xml(original_xml)
            if 'error' in valid.lower():
                return HTTPBadRequest('Invalid GSTORE metadata')

            g = DatasetMetadata()
            g.gstore_xml = original_xml
            new_dataset.gstore_metadata.append(g)
            
        else:
            return HTTPBadRequest('Bad metadata definition')
    else:
        return HTTPBadRequest('No metadata')
        
           
    #add the sources to sources
        #add the source_files to the source
    for src in sources:
        ext = src['extension']
        srcset = src['set']
        external = src['external']
        external = True if external.upper() == 'TRUE' else False
        mimetype = src['mimetype']
        s = Source(srcset, ext)
        s.file_mimetype = mimetype
        s.is_external = external
        s.active = True

        settings = src['settings'] if 'settings' in src else {}

        files = src['files']
        for f in files:
            sf = SourceFile(f)
            s.src_files.append(sf)

        #TODO: finish implementing the settings (classes, styles)
        if settings and 'basic' in settings:
            new_settings = {}
            for key in settings['basic'].iterkeys():
                new_settings.update({str(key): str(settings['basic'][key])})
            new_settings = MapfileSetting(new_settings)
            s.map_settings.append(new_settings)

        new_dataset.sources.append(s)        

    if project:
        #this should be the unique project name
        p = DBSession.query(Project).filter(Project.name==project).first()
        if p:
            #new_dataset.project_id = p.id
            new_dataset.projects.append(p)

    #create the new dataset with all its pieces
    try:
        DBSession.add(new_dataset)
        DBSession.commit()
        DBSession.flush()
        DBSession.refresh(new_dataset)
    except Exception as err:
        return HTTPServerError(err)

    #add the dataset to the index
    es_description = {
        "host": request.registry.settings['es_root'],
        "index": request.registry.settings['es_dataset_index'], 
        "type": 'dataset',
        "user": request.registry.settings['es_user'].split(':')[0],
        "password": request.registry.settings['es_user'].split(':')[-1]
    } 

    indexer = DatasetIndexer(es_description, new_dataset, request)  
    #TODO: update the list for facets
    indexer.build_document([])
    #add to the index
    try:
        indexer.put_document()
    except:
        return HTTPServerError('failed to put index document for %s' % new_dataset.uuid)
 
    #and just for kicks, return the uuid
    return Response(str(new_dataset.uuid))

#TODO: change this back to PUT and figure out why pycurl hung up on it.
@view_config(route_name='update_dataset', request_method='POST')
def update_dataset(request):
    '''
    add version value
    activate/deactivate

    update bbox + geom
    update metadata xml (original_metadata)

    add dataset to tileindex | bundle | collection | some other thing
    '''
    dataset_id = request.matchdict['id']
    d = get_dataset(dataset_id)
    if not d:
        return HTTPNotFound()

    post_data = request.json_body

    #set of elasticsearch elements to update for this dataset's doc
    '''
    options:
    bbox
    taxonomy/geomtype
    activate
    embargo
    available
    metadata.abstract & metadata.isotopic
    formats
    services
    '''
    elements_to_update = {}

    keys = post_data.keys()
    for key in keys:
        #so we can update all the things
        
        if key == 'metadata':
            xml = post_data[key]
            if not xml:
                return HTTPBadRequest()
            #replace the original_metadata.xml with the metadata included here
            if not d.original_metadata:
                #we need to make one
                o = OriginalMetadata()
                o.original_xml = xml
                d.original_metadata.append(o)
            else:
                #just update the xml field
                d.original_metadata[0].original_xml = xml
        elif key == "convert_metadata":
            '''
            get the list of standards to support

            then go get the original_xml for the dataset
            and run the converter
            '''

            supported_standards = post_data[key]['standards'] if 'standards' in post_data[key] else []

            if supported_standards:
                excluded_standards = get_all_standards(request)
                d.excluded_standards = [f for f in excluded_standards if f not in supported_standards and f != 'GSTORE']

                om = d.original_metadata[0] if d.original_metadata else None

                if om:
                    xslt_path = request.registry.settings['XSLT_PATH'] + '/xslts'
                    try:
                        converted = om.convert_to_gstore_metadata(xslt_path)
                    except Exception as e:
                        raise
                   
            
        elif key == 'activate':
            #TODO: add es updater for flag in index doc
        
            active = post_data[key]
            if not active:
                return HTTPBadRequest()
            inactive = True if active.lower() == 'false' else False
            d.inactive = inactive

            #move the vector data from gstore.vectors to gstore.inactive if FALSE
            #move from gstore.inactive to gstore.vectors if TRUE

            if d.taxonomy in ['vector']:
                connstr = request.registry.settings['mongo_uri']
                live_collection = request.registry.settings['mongo_collection']
                inactive_collection = request.registry.settings['mongo_inactive_collection']
                
                if inactive == False:
                    # move from inactives to vectors
                    to_mongo_uri = gMongoUri(connstr, live_collection)
                    from_mongo_uri = gMongoUri(connstr, inactive_collection)
                else:
                    #move from vectors to inactives
                    to_mongo_uri = gMongoUri(connstr, inactive_collection)
                    from_mongo_uri = gMongoUri(connstr, live_collection)
                
                d.move_vectors(to_mongo_uri, from_mongo_uri)


            #yes, this is json/dict, but the update is a string script widget and cannot deal with True.
            elements_to_update.update({"active": str(not inactive).lower()})
            
        elif key == 'available':
            #TODO: add es updater for flag in index doc
            available = post_data[key]
            if not available:
                return HTTPBadRequest()
            available = True if available == 'True' else False
            d.is_available = available

            elements_to_update.update({"available": str(available).lower()})
        elif key == 'embargo':
            #TODO: add es updater for flag in index doc
            embargo = post_data[key]
            if not embargo:
                return HTTPBadRequest()

            is_embargoed = embargo['embargoed']
            embargo_date = embargo['release_date'] if 'release_date' in embargo else ''     

            is_embargoed = True if is_embargoed.lower() == 'true' else False
            d.is_embargoed = is_embargoed

            if embargo_date:
                #add it to the table, if one isn't supplied, it's embargoed indefinitely or we can add a date
                d.embargo_release_date = embargo_date

            if d.taxonomy in ['vector']:
                #move the documents if it's a vector
                connstr = request.registry.settings['mongo_uri']
                live_collection = request.registry.settings['mongo_collection']
                embargo_collection = request.registry.settings['mongo_embargo_collection']
                
                if is_embargoed == False:
                    # move from inactives to vectors
                    to_mongo_uri = gMongoUri(connstr, live_collection)
                    from_mongo_uri = gMongoUri(connstr, embargo_collection)
                else:
                    #move from vectors to inactives
                    to_mongo_uri = gMongoUri(connstr, embargo_collection)
                    from_mongo_uri = gMongoUri(connstr, live_collection)
                
                d.move_vectors(to_mongo_uri, from_mongo_uri)

            elements_to_update.update({"embargo": str(embargo).lower()})
        elif key == 'bbox':
            parts = post_data[key]
            if 'geom' not in parts and 'box' not in parts:
                return HTTPBadRequest()

            SRID = int(request.registry.settings['SRID'])
            box = parts['box'] if 'box' in parts else ''
            geom = parts['geom'] if 'geom' in parts else ''

            if not geom and box:
                box = map(float, box.split(','))
                geom = bbox_to_wkb(box, SRID)
            else:
                return HTTPBadRequest()

            d.box = box
            d.geom = geom

            area = d.geom.GetArea()
            if area == 0.:
                loc = {"type": "Point", "coordinates": [box[0], box[1]]}
            else:
                loc = {
                    "type": "Polygon",
                    "coordinates": [[[box[0], box[1]], [box[2], box[1]], [box[2], box[3]], [box[0], box[3]], [box[0], box[1]]]]
                }

            elements_to_update.update({"location": {"bbox": loc}, "area": area})
            
        elif key == 'epsg':
            epsg = post_data['epsg']
            d.orig_epsg = epsg
        elif key == 'sources':
            new_sources = post_data[key]

            '''
            'sources': [
                {
                    'set':
                    'extension':
                    'external':
                    'mimetype':
                    'identifier':
                    'identifier_type':
                    'files': [],
                    'settings': {'basic': {'WCS-NODATA': 'some value'}, 'classes': {'class': {style stuff here}}}
                    
                }
            ]
            '''
            for src in new_sources:
                ext = src['extension']
                srcset = src['set']
                external = src['external']
                external = True if external.upper() == 'TRUE' else False
                mimetype = src['mimetype']
                s = Source(srcset, ext)
                s.file_mimetype = mimetype
                s.is_external = external
                s.active = True

                settings = src['settings'] if 'settings' in src else {}

                files = src['files']
                for f in files:
                    sf = SourceFile(f)
                    s.src_files.append(sf)

                #TODO: finish implementing the settings (classes, styles)
                if settings and 'basic' in settings:
                    new_settings = {}
                    for key in settings['basic'].iterkeys():
                        new_settings.update({str(key): str(settings['basic'][key])})
                    new_settings = MapfileSetting(new_settings)
                    s.map_settings.append(new_settings)

                d.sources.append(s)

        elif key == 'formats':
            #list of formats to support
            formats = post_data['formats']
            excluded_formats = get_all_formats(request)
            d.excluded_formats = [f for f in excluded_formats if f not in formats]


            elements_to_update.update({"formats": formats})

        elif key == 'services':
            services = post_data['services']
            excluded_services = get_all_services(request)
            d.excluded_services = [s for s in excluded_services if s not in services]

            elements_to_update.update({"services": services})
        elif key == 'taxonomy':  
            taxo = post_data['taxonomy']

            taxonomy = taxo['taxonomy']
            geomtype = taxo['geomtype'] if 'geomtype' in taxo else ''

            if taxonomy == 'vector' and not geomtype:
                continue

            d.taxonomy = taxonomy.lower()
            if geomtype and taxonomy.lower() == 'vector':
                d.geomtype = geomtype.upper()

            elements_to_update.update({"taxonomy": taxonomy.lower()})
            if geomtype and taxonomy.lower() == 'vector':
                elements_to_update.update({"geomtype": geomtype.upper()})

        elif key == 'records':
            records = post_data['records']
            if records > 0:
                d.record_count = records

        elif key == 'features':
            features = post_data['features']
            if features > 0:
                d.feature_count = features

        elif key == 'mapfile':
            new_settings = post_data[key]

        elif key == 'project':
            project = post_data[key]

            #TODO: finish this one
        elif key == 'citations':
            '''
            as:

                "citations": ['citation a', 'citation b']

            '''
            citations = post_data[key]

            for citation in citations:
                c = Citation(citation)
                d.citations.append(c)
       
    try:
        DBSession.commit()
    except Exception as err:
        return HTTPServerError(err)


    #now push the updates to elasticsearch
    es_description = {
        "host": request.registry.settings['es_root'],
        "index": request.registry.settings['es_dataset_index'], 
        "type": 'dataset',
        "user": request.registry.settings['es_user'].split(':')[0],
        "password": request.registry.settings['es_user'].split(':')[-1]
    } 

    indexer = DatasetIndexer(es_description, d, request)  
    try:
        #es_url, data = 
        indexer.update_document(elements_to_update)
        #return Response(json.dumps({"url": es_url, "data": data}))
    except Exception as ex:
        return HTTPServerError('failed to update document (%s)' % ex)

    return Response('updated')





	
