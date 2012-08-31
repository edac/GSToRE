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
    if d.taxonomy in ['services']:
        src = d.get_source(datatype, format)
        
        if not src:
            #not valid source information for the dataset
            return HTTPNotFound()

        loc = src.get_location()
        if not loc:
            return HTTPNotFound()
            
        return HTTPFound(location=loc)

    #TODO: refactor the heck out of this  

    xslt_path = request.registry.settings['XSLT_PATH']
    if d.taxonomy in ['geoimage', 'file']:
        src = d.get_source(datatype, format)
        if not src:
            #not valid source information for the dataset
            return HTTPNotFound()

        if src.is_external:
            loc = src.get_location()
            #redirect and bail
            return HTTPFound(location=loc)

        #get the mimetype (not as unicode)
        mimetype = str(src.file_mimetype)


        #for things that we don't actually want to emit as zips (pdf, etc)
        if format != 'zip' and mimetype != 'application/x-zip-compressed':
            #it's not a file we want to deliver as a zip anyway
            f = src.get_location(format)

            fr = FileResponse(f, content_type=mimetype)
            fr.content_disposition = 'attachment; filename=%s' % (f.split('/')[-1])
            return fr
            
        #zip'em up unless it's already a zip
        if format != 'zip':

            #to test: http://129.24.63.66/gstore_v3/apps/rgis/datasets/ccfc9523-4b9e-4c58-8cf5-7d727fc8a807.original.tif
            tmppath = request.registry.settings['TEMP_PATH']
            if not tmppath:
                return HTTPNotFound()
            #get the name of the file from the url
            outname = '%s.%s.%s.zip' % (d.basename, datatype, format)
            zippath = os.path.join(tmppath, str(dataset_id))

            if not os.path.isdir(zippath):
                os.mkdir(zippath)

            #make the zipfile (which returns the path that we already know. whatever.)
            output = src.pack_source(zippath, outname, xslt_path)
        else:
            #it should already be a zip
            output = src.get_location(format)
            outname = output.split('/')[-1]

        fr = FileResponse(output, content_type=mimetype)
        fr.content_disposition = 'attachment; filename=%s' % (outname)
        return fr

    else:
        # get file
        # zip file (or if kml, kmz it)
        #deliver
        
        #check for the existing file in sources
        src = d.get_source(datatype, format)
        if src:
            #zip or not
            if src.is_external:
                loc = src.get_location(format)
                #redirect and bail
                return HTTPFound(location=loc)

            #get the mimetype (not as unicode)
            mimetype = str(src.file_mimetype)

            #zip'em up unless it's already a zip
            if format != 'zip':

                #to test: http://129.24.63.66/gstore_v3/apps/rgis/datasets/ccfc9523-4b9e-4c58-8cf5-7d727fc8a807/{basename}.original.tif
                tmppath = request.registry.settings['TEMP_PATH']
                if not tmppath:
                    return HTTPNotFound()
                outname = '%s.%s.%s.zip' % (d.basename, datatype, format)
                zippath = os.path.join(tmppath, str(dataset_id), outname)
                #make the zipfile
                output = src.pack_source(zippath, outname, xslt_path)
            else:
                #it should already be a zip
                output = files[0].location
                outname = files[0].location.split('/')[-1]

            fr = FileResponse(output, content_type=mimetype)
            fr.content_disposition = 'attachment; filename=%s' % (outname)
            return fr

        #at this point it's going to be an existing cached zip or a new zip
        mimetype = 'application/x-zip-compressed'

        #TODO: probably something about the KML -> KMZ situation (UNLESS we're packing some metadata in the zip)
        #check for the existing file in formats
        fmtpath = request.registry.settings['FORMATS_PATH']
        cachepath = os.path.join(fmtpath, str(d.uuid), format)
        #don't forget the actual packed zip
        cachefile = os.path.join(cachepath, str(d.uuid) + '.' + format + '.zip')
        if os.path.isfile(cachefile):
            fr = FileResponse(cachefile, content_type=mimetype)
            fr.content_disposition = 'attachment; filename=%s' % (str(d.basename) + '.' + format + '.zip')
            return fr

        #build the file
        #at the cache path
        if not os.path.isdir(cachepath):
            #make a new one and this is stupid
            if not os.path.isdir(os.path.join(fmtpath, str(d.uuid))):
                os.mkdir(os.path.join(fmtpath, str(d.uuid)))
            os.mkdir(os.path.join(fmtpath, str(d.uuid), format))


        #set up the mongo connection
        mconn = request.registry.settings['mongo_uri']
        mcoll = request.registry.settings['mongo_collection']
        mongo_uri = gMongoUri(mconn, mcoll)

        srid = int(request.registry.settings['SRID'])
        
        success = d.build_vector(format, cachepath, mongo_uri, srid)
        if success[0] != 0:
            return HTTPServerError()

        #return the file (already been zipped) and only has metadata if it's a shapefile
        fr = FileResponse(cachefile, content_type=mimetype)
        fr.content_disposition = 'attachment; filename=%s' % (str(d.basename) + '.' + format + '.zip')
        return fr
        

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
    active = post_data['active'] if 'active' in post_data else ''
    inactive = False if active == 'True' else True

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
            new_dataset.project_id = p[0].id

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
    option = post_data['option'].lower() if 'option' in post_data else ''
    if not option:
        return HTTPBadRequest()

    #TODO: change this to accept multiple options at once

    if option == 'metadata':
        xml = post_data['xml'] if 'xml' in post_data else ''
        if not 'xml':
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
    elif option == 'activate':
        active = post_data['active'] if 'active' in post_data else ''
        if not active:
            return HTTPBadRequest()
        inactive = True if active == 'False' else False
        d.inactive = inactive
    elif option == 'available':
        available = post_data['available'] if 'available' in post_data else ''
        if not available:
            return HTTPBadRequest()
        available = True if available == 'True' else False
        d.is_available = available
    elif option == 'bbox':
        if 'geom' not in post_data and 'box' not in post_data:
            return HTTPBadRequest()

        SRID = int(request.registry.settings['SRID'])
        box = map(float, post_data['box'].split(','))
        geom = post_data['geom'] if 'geom' in post_data else ''

        if not geom and box:
            geom = bbox_to_wkb(box, SRID)
        else:
            return HTTPBadRequest()

        d.box = box
        d.geom = geom
        
    try:
        DBSession.commit()
    except Exception as err:
        return HTTPServerError(err)
    
    #TODO: add the other options
    #      

    return Response('updated')





	
