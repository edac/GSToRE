from pyramid.view import view_config
from pyramid.response import Response, FileResponse
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError, HTTPBadRequest

from sqlalchemy.exc import DBAPIError

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import *
from ..models.sources import Source, SourceFile
from ..models.metadata import OriginalMetadata


from ..lib.utils import *
from ..lib.spatial import *
from ..lib.database import *
from ..lib.mongo import gMongoUri


'''
datasets
'''
#TODO: add dataset statistics view - min/max per attribute, histogram info, etc

def return_fileresponse(output, mimetype, filename):
    fr = FileResponse(output, content_type=mimetype)
    fr.content_disposition = 'attachment; filename=%s' % filename
    
    #TODO: may want to reconsider the cookie age
    #for the jquery file download progress widget
    fr.set_cookie(key='fileDownload', value='true', max_age=31536000, path='/')
    return fr

##TODO: add the html renderer to this
@view_config(route_name='html_dataset', renderer='dataset_card.mako')
def show_html(request):
    dataset_id = request.matchdict['id']
    app = request.matchdict['app']

    d = get_dataset(dataset_id)

    if not d:
        return HTTPNotFound()

    #http://129.24.63.66/gstore_v3/apps/rgis/datasets/8fc27d61-285d-45f6-8ef8-83785d62f529/soils83.html
    #http://{load_balancer}/apps/.....

#    #get the host url
#    host = request.host_url
#    g_app = request.script_name[1:]
#    base_url = '%s/%s/apps/%s/datasets/' % (host, g_app, app)

    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/apps/%s/datasets/' % (load_balancer, app)
    
    rsp = d.get_full_service_dict(base_url, request)

    return rsp
    #return Response('html ' + str(d.uuid))
    

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

    #go get the dataset
    d = get_dataset(dataset_id)

    if not d:
        return HTTPNotFound()

    #make sure it's available
    #TODO: replace this with the right status code
    if d.is_available == False:
        return HTTPNotFound('Temporarily unavailable')

    #TODO: change this to be the right formats filter (given format in remainder of all formats - excluded formats)
    #TODO: add the format check. except better than this: .filter(Dataset.formats_cache.ilike('%'+format+'%')))

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
    xslt_path = request.registry.settings['XSLT_PATH']
    fmtpath = request.registry.settings['FORMATS_PATH']
    tmppath = request.registry.settings['TEMP_PATH']

    #return things that shouldn't be zipped (pdfs, etc)
    if format != 'zip' and mimetype != 'application/x-zip-compressed':
        output = src.get_location(format)
        return return_fileresponse(output, mimetype, output.split('/')[-1])

    #return the already packed zip (this assumes that everything set to zip is already a zip)
    if format == 'zip':
        output = src.get_location(format)
        return return_fileresponse(output, mimetype, output.split('/')[-1])
    
    #check the cache for a zip
    output = os.path.join(fmtpath, str(d.uuid), format, '%s_%s.zip' % (str(d.basename), format))
    if os.path.isfile(output):
        return return_fileresponse(output, mimetype, output.split('/')[-1])

    #first check for the uuid + format subdirectories in the formats dir
    cached_path = os.path.join(fmtpath, str(d.uuid), format)
    if not os.path.isdir(cached_path):
        if not os.path.isdir(os.path.join(fmtpath, str(d.uuid))):
            os.mkdir(os.path.join(fmtpath, str(d.uuid)))
        os.mkdir(cached_path)

    outname = '%s_%s.zip' % (d.basename, format)
    cached_file = os.path.join(cached_path, '%s_%s.zip' % (str(d.basename), format))

    #no zip. need to pack it up (raster/file) or generate it (vector)
    if taxonomy in ['geoimage', 'file']:
        #pack up the zip to the formats cache
        output = src.pack_source(cached_path, outname, xslt_path)
        
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
#        success = d.build_vector(format, cached_path, mongo_uri, srid)

        #for the new stream to ogr2ogr option (or just stream if not shapefile)
        load_balancer = request.registry.settings['BALANCER_URL']
        base_url = '%s/apps/%s/datasets/' % (load_balancer, app)
        success = d.stream_vector(format, cached_path, mongo_uri, srid, base_url)

        #check the response for failure
        if success[0] != 0:
            return HTTPServerError()    

        #TODO: the vectors are returning as uuid.format.zip instead of basename.format.zip
        return return_fileresponse(cached_file, mimetype, outname)    

    #if we're here something really bad is happening
    return HTTPNotFound()
 

@view_config(route_name='dataset_services', renderer='json')
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

#can still show the basic info
#    if d.is_available == False:
#        return HTTPNotFound('Temporarily unavailable')

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
        metadata: [] as {standard: url}
    }
    '''

#    #get the host url
#    host = request.host_url
#    g_app = request.script_name[1:]
#    base_url = '%s/%s/apps/%s/datasets/' % (host, g_app, app)

    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

    rsp = d.get_full_service_dict(base_url, request)

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
    pass

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
        'metadata': ''
        'project': 
        'apps': []
        'formats': []
        'services': []
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
                'files': []
                
            }
        ]
    }

    '''

    #get the data as json
    post_data = request.json_body

    SRID = int(request.registry.settings['SRID'])
    excluded_formats = get_all_formats(request)
    excluded_services = get_all_services(request)

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

    box = map(float, spatials['bbox'].split(','))
    epsg = spatials['epsg']
    geomtype = spatials['geomtype'] if 'geomtype' in spatials else ''
    geom = spatials['geom'] if 'geom' in spatials else ''
    features = spatials['features'] if 'features' in spatials else 0
    records = spatials['records'] if 'records' in spatials else 0

    #add the inactive flag
    active = post_data['active'].lower() if 'active' in post_data else ''
    inactive = False if active == 'true' else True

    project = post_data['project'] if 'project' in post_data else ''

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

    if not geom:
        #go make one
        geom = bbox_to_wkb(box, SRID)

    new_dataset.geom = geom
    new_dataset.box = box
    
    new_dataset.apps_cache = [app] + apps

    #TODO: get rid of formats_cache (once v2 tools issue is resolved in search datasets)
    new_dataset.formats_cache = ','.join(formats)
    new_dataset.excluded_formats = [f for f in excluded_formats if f not in formats]
    new_dataset.excluded_services = [s for s in excluded_services if s not in services]

    new_dataset.uuid = provided_uuid

    #add the category set (if not in categories) and assign to dataset
    for category in categories:
        theme = category['theme']
        subtheme = category['subtheme']
        groupname = category['groupname']

        c = DBSession.query(Category).filter(and_(Category.theme==theme, Category.subtheme==subtheme, Category.groupname==groupname)).first()
        if not c:
            #we'll need to add a new category BEFORE running this (?)
            return HTTPBadRequest()

        new_dataset.categories.append(c)

    if validdates:
        #TODO: add some date checking
        validstart = validdates['start'] if 'start' in validdates else None
        validend = validdates['end'] if 'end' in validdates else None
        new_dataset.begin_datetime = validstart
        new_dataset.end_datetime = validend


    #add the metadata
    #TODO: fix this when we may have multiple metadata streams coming in. this just handles our current v2 situation
    if metadatas:
        o = OriginalMetadata()
        o.original_xml = metadatas
        new_dataset.original_metadata.append(o)

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

        files = src['files']
        for f in files:
            sf = SourceFile(f)
            s.src_files.append(sf)

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
        elif key == 'activate':
            active = post_data[key]
            if not active:
                return HTTPBadRequest()
            inactive = True if active == 'False' else False
            d.inactive = inactive
        elif key == 'available':
            available = post_data[key]
            if not available:
                return HTTPBadRequest()
            available = True if available == 'True' else False
            d.is_available = available
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
        elif key == 'sources':
            new_sources = post_data[key]
            #TODO: add the code to add a new source for a dataset

        elif key == 'mapfile':
            new_settings = post_data[key]

        elif key == 'project':
            project = post_data[key]
       
    try:
        DBSession.commit()
    except Exception as err:
        return HTTPServerError(err)
       

    return Response('updated')





	
