from pyramid.view import view_config
from pyramid.response import Response, FileResponse
from pyramid.httpexceptions import HTTPNotFound, HTTPFound

from pyramid.threadlocal import get_current_registry

from sqlalchemy.exc import DBAPIError

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )


from ..lib.utils import *
from ..lib.database import *

'''
datasets
'''

#TODO: figure out a route that doesn't conflict with the download route
##TODO: add the html renderer to this
#@view_config(route_name='dataset', match_param='ext=html')
#def show_html(request):
#    dataset_id = request.matchdict['id']

#    if not isinstance(dataset_id, (int, long)): 
#        #it's the uuid
#        dfilter = 'uuid = %s' % (dataset_id)
#    else:
#        dfilter = 'id = %s' % (dataset_id)

#    #get the dataset by the filter

#    return Response('html ' + dfilter)
#    

@view_config(route_name='dataset')
def dataset(request):
    #use the original dataset_id structure 
    #or the new dataset uuid structure

    dataset_id = request.matchdict['id']
    format = request.matchdict['ext']
    datatype = request.matchdict['type'] #original vs. derived

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
    if taxonomy in ['services']:
        src = [s for s in d.sources if s.extension == format and s.set == datatype and s.active]
        if not src:
            #not valid source information for the dataset
            return HTTPNotFound('No results')

        src = src[0]
        loc = src.src_files[0].location
        return HTTPFound(location=loc)
      
    if d.taxonomy in ['geoimage', 'file']:
        src = [s for s in d.sources if s.extension == format and s.set == datatype and s.active]
        if not src:
            #not valid source information for the dataset
            return HTTPNotFound('No results')

        src = src[0]
        if src.is_external:
            loc = src.src_files[0].location
            #redirect and bail
            return HTTPFound(location=loc)

        #get the mimetype (not as unicode)
        mimetype = str(src.file_mimetype)
        
        #get the files
        files = src.src_files
        if not files:
            return HTTPNotFound('why are there no files?')

        #TODO: add the metadata to the zip someday

        #for things that we don't actually want to emit as zips (pdf, etc)
        if format != 'zip' and mimetype != 'application/x-zip-compressed':
            #it's not a file we want to deliver as a zip anyway

            if len(files) > 1:
                #we want the right one
                f = [g for g in files if g.location.split('.')[-1] == format]
                f = f[0] if f else None
            else:
                f = files[0]
            fr = FileResponse(f, content_type=mimetype)
            fr.content_disposition = 'attachment; filename=%s' % (f.split('/')[-1])
            return fr
            
        #zip'em up unless it's already a zip
        if format != 'zip':

            #to test: http://129.24.63.66/gstore_v3/apps/rgis/datasets/ccfc9523-4b9e-4c58-8cf5-7d727fc8a807.original.tif
            tmppath = get_current_registry().settings['TEMP_PATH']
            if not tmppath:
                return HTTPNotFound('where is the temp!')
            #get the name of the file from the url
            zippath = os.path.join(tmppath, '%s.%s.%s.zip' % (dataset_id, datatype, format))
            #make the zipfile
            files = [f.location for f in files]
            output = createZip(zippath, files)
            outname = zippath.split('/')[-1]
        else:
            #it should already be a zip
            output = files[0].location
            outname = files[0].location
        
        #Response.content_disposition = 'attachment; filename=%s' % (outname)
        #Response.content_type = mimetype
        fr = FileResponse(output, content_type=mimetype)
        fr.content_disposition = 'attachment; filename=%s' % (outname)
        return fr
        #return FileResponse(output, content_type=mimetype)
    else:
        #TODO: deal with the vectors once mongo is running and there's data
        #TODO: what about formats not in sources (always derived)? bounce if uuid.original.gml?

        # get file
        # zip file (or if kml, kmz it)
        #deliver
        return Response('Not doing vectors today. Come back later. kthxbye.')



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

    if d.is_available == False:
        return HTTPNotFound('Temporarily unavailable')

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

    #get the host url
    host = request.host_url
    g_app = request.script_name[1:]

    base_url = '%s/%s/apps/%s/datasets/%s' % (host, g_app, app, d.uuid)

    #all the basic info
    rsp = {'id': d.id, 'uuid': d.uuid, 'description': d.description, 
                'spatial': {'bbox': [float(s) for s in d.box], 'epsg': 4326}, 
                'categories': [{'theme': t.theme, 'subtheme': t.subtheme, 'groupname': t.groupname} for t in d.categories]}
    
    #base the services on the taxonomy
    #TODO: change this once the excluded list is ready
    #base the downloads on formats + the sources.set combination (if format in sources, otherwise for vector it's all derived except the zip)
    svcs = []
    dlds = []
    fmts = d.formats_cache.split(',')
    srcs = d.sources
    srcs = [s for s in srcs if s.active]
    
    if d.taxonomy == 'geoimage':
        svcs = ['wms', 'wcs']

        #add the downloads by source
        #TODO: maybe compare to the formats list?
        dlds = [(s.set, s.extension) for s in srcs]
#        for s in srcs:
#            dlds.append((s.set, s.extension))
    elif d.taxonomy == 'vector':
        svcs = ['wms', 'wfs']

        #get the formats
        #check for a source
        #if none, derived + fmt
        #if one, set + fmt
        for f in fmts:
            sf = [s for s in srcs if s.extension == f]
            st = sf[0].set if sf else 'derived'
            dlds.append((st, f))
    elif d.taxonomy == 'file':
        #just the formats
        for f in fmts:
            sf = [s for s in srcs if s.extension == f]
            if sf:
                #if it's not in there, that's a whole other problem (i.e. why is it listed in the first place?)
                dlds.append((sf[0], f))
                
    #update the response dict
    rsp.update({'services': [{s: '%s/services/ogc/%s' % (base_url, s) for s in svcs}], 'downloads': [{s[1]: '%s.%s.%s' % (base_url, s[0], s[1]) for s in dlds}]})
    
    #then just add the metadata
    if d.has_metadata_cache:
        standards = ['fgdc']
        exts = ['html', 'txt', 'xml']
        '''
        as {fgdc: {ext: url}}
        '''
        mt = [{s: {e: '%s/metadata/%s.%s' % (base_url, s, e) for e in exts} for s in standards}]
      
        rsp.update({'metadata': mt})

    #TODO: add the html card view also maybe

    #TODO: add related datasets

    return rsp


'''
dataset maintenance
'''
@view_config(route_name='add_dataset', request_method='POST')
def add_dataset(request):
    app = request.matchdict['app']

    #generate uuid here, not through postgres - need to use 
    #outside uuids for data replication (nv/id data as local dataset with pointer to their files)
    

    return Response('hooray for post')

@view_config(route_name='update_dataset', request_method='PUT')
def update_dataset(request):
    '''
    add version value
    activate/deactivate

    add dataset to tileindex | bundle | collection | some other thing
    '''
    dataset_id = request.matchdict['id']
    if not isinstance(dataset_id, (int, long)): 
        #it's the uuid
        dfilter = 'uuid = %s' % (dataset_id)
    else:
        dfilter = 'id = %s' % (dataset_id)

    #get the dataset by the filter

	return Response('put!')
