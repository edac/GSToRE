from pyramid.view import view_config
from pyramid.response import Response, FileResponse
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError

from sqlalchemy.exc import DBAPIError

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )


from ..lib.utils import *
from ..lib.database import *
from ..lib.mongo import gMongoUri

'''
datasets
'''

##TODO: add the html renderer to this
@view_config(route_name='html_dataset', renderer='dataset_card.mako')
def show_html(request):
    dataset_id = request.matchdict['id']
    app = request.matchdict['app']

    d = get_dataset(dataset_id)

    if not d:
        return HTTPNotFound('No results')

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
        return HTTPNotFound('No results')

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
            return HTTPNotFound('No results')

        loc = src.get_location()
        if not loc:
            return HTTPNotFound('')
            
        return HTTPFound(location=loc)

    #TODO: refactor the heck out of this  

    xslt_path = request.registry.settings['XSLT_PATH']
    if d.taxonomy in ['geoimage', 'file']:
        src = d.get_source(datatype, format)
        if not src:
            #not valid source information for the dataset
            return HTTPNotFound('No results')

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
            zippath = os.path.join(tmppath, str(dataset_id), outname)

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
                    return HTTPNotFound('where is the temp!')
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
            return HTTPServerError('failed to build vector')

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
        return HTTPNotFound('No results')

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

    description
    taxonomy
    geomtype
    valid start
    valid end
    bbox
    epsg

    {uuid} (optional and add it later)

    apps
    formats
    services
    
    record count
    feature count

    metadata (stays the same until we have the migration widget running)
    sources
    categories
    
    
    '''

    #get the data as json
    post_data = request.json_body

    #do stuff


    #create the new dataset


    #add the category set (if not in categories) and assign to dataset


    #add the metadata
    

    #add the sources to the sources
    
    

    return Response('hooray for post')

@view_config(route_name='update_dataset', request_method='PUT')
def update_dataset(request):
    '''
    add version value
    activate/deactivate

    add dataset to tileindex | bundle | collection | some other thing
    '''
    dataset_id = request.matchdict['id']
    d = get_dataset(dataset_id)
    if not d:
        return HTTPNotFound()

    #get the dataset by the filter

	return Response('put!')
