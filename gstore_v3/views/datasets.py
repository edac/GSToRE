from pyramid.view import view_config
from pyramid.response import Response, FileResponse
from pyramid.renderers import render_to_response
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError, HTTPBadRequest, HTTPServiceUnavailable

from sqlalchemy.exc import DBAPIError

from ..models import DBSession
from ..models.datasets import *
from ..models.sources import Source, SourceFile, MapfileSetting
from ..models.metadata import OriginalMetadata, DatasetMetadata
from ..models.apps import GstoreApp

import os, json, tempfile
from xml.sax.saxutils import escape

from ..lib.utils import *
from ..lib.spatial import *
from ..lib.database import *
from ..lib.mongo import gMongoUri
from ..lib.es_indexer import DatasetIndexer
from ..lib.spatial_streamer import *


#TODO: add dataset statistics view - min/max per attribute, histogram info, etc

def return_fileresponse(output, mimetype, filename):
<<<<<<< HEAD
=======
    print "return_fileresponse"
>>>>>>> gstore/master
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    fr = FileResponse(output, content_type=mimetype)
    fr.content_disposition = 'attachment; filename=%s' % filename

    #add a no-robots to see if that helps (we would like to have the metadata crawled)
    fr.headers['X-Robots-Tag'] = 'noindex'
    
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

@view_config(route_name='dataset')
def dataset(request):
<<<<<<< HEAD
=======
    print "dataset() called..."
>>>>>>> gstore/master
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
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
<<<<<<< HEAD

=======
    print "Datset ID is:"
    print d.id
    print "Dataset UUID is :"
    print d.uuid
>>>>>>> gstore/master
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

    if format not in d.get_formats(request):
        return HTTPNotFound()    

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
    base_url = request.registry.settings['BALANCER_URL']
    
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
    
    metadata_info = {'app': app, 'base_url': base_url, 'standard': std, "xslt_path": xslt_path + '/xslts', 'validate': False, "request": request}
    
    taxonomy = str(d.taxonomy)

    #check for a source for everyone
    src = d.get_source(datatype, format)
    if not src and d.taxonomy in ['geoimage', 'file', 'service']:
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
    elif taxonomy in ['vector', 'table']:
        #generate the file and pack the zip
        #note that the kml isn't being packed as kmz - we include metadata with every download here

        #set up the mongo connection
        mconn = request.registry.settings['mongo_uri']
        mcoll = request.registry.settings['mongo_collection']
        mongo_uri = gMongoUri(mconn, mcoll)

        srid = int(request.registry.settings['SRID'])
        
        #for the new stream to ogr2ogr option (or just stream if not shapefile)
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
<<<<<<< HEAD
=======
    print "stream_dataset"
>>>>>>> gstore/master
    """
    stream dataset as json, kml, csv, geojson, gml
    for improved access options (pull in json for a table on a webpage, etc)
    BUT only for vector datasets

    params:
        bbox (return features intersecting box)
        datetime (return features within time range (sensor data, etc))

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    app = request.matchdict['app']
    dataset_id = request.matchdict['id']
    format = request.matchdict['ext']

    if format not in ['json', 'geojson', 'csv', 'kml', 'gml']:
        return HTTPBadRequest()

    #TODO: add the parameter searches
    params = normalize_params(request.params)

    #go get the dataset
    d = get_dataset(dataset_id)    

    if not d:
        return HTTPNotFound()

    if d.taxonomy not in ['vector', 'table'] or d.inactive or app not in d.apps_cache or d.is_embargoed:
        return HTTPBadRequest()

    if not d.is_available:
        return HTTPServiceUnavailable()

    supported_formats = d.get_formats(request)
    if format not in supported_formats:
        return HTTPNotFound()    

    connstr = request.registry.settings['mongo_uri']
    collection = request.registry.settings['mongo_collection']
    mongo_uri = gMongoUri(connstr, collection)
    gm = gMongo(mongo_uri)

    epsg = int(request.registry.settings['SRID'])

    is_spatial = False if format in ['json', 'csv'] else True

    #IF THIS IS A PROBLEM, take out everything two the second vectors def and uncomment that.    
    limit = int(params['limit']) if 'limit' in params else d.record_count + 100
    offset = int(params['offset']) if 'offset' in params else 0
    sort = []
    if 'sort' in params and 'order' in params:
        #NOTE: this is only going to work for observed AS OBS right now
        sort = [(params['sort'].lower(), 1 if params['order'] == 'asc' else -1)]

    #add the basic parameters (limit, offset, sort)
    vectors = gm.query({'d.id': d.id}, None, sort, limit, offset)
<<<<<<< HEAD

#    vectors = gm.query({'d.id': d.id})
=======
    countTotal = gm.count({'d.id': d.id}, None, sort, limit, offset)
    countSubtotal=0
    if countTotal<limit:
        countSubtotal=countTotal
    else:
        countSubtotal=limit

    #vectors = gm.query({'d.id': d.id})
>>>>>>> gstore/master
    
    records = [d.convert_doc_to_record(vector, epsg, format, is_spatial) for vector in vectors]

    fields = [{"name": f.name, "type": f.ogr_type, "len": f.ogr_width} for f in d.attributes]

    if [a for a in records[0]['datavalues'] if a[0] == 'observed']:
        #this is not a good plan but we don't have the flag for "dataset contains observation timestamp" today
        #and it's as a string for now
        fields.append({"name": "observed", "type": 4, "len": 20})

    if format == 'kml' and d.taxonomy in ['vector']:
        streamer = KmlStreamer(fields)
        streamer.update_description(d.description)
    elif format =='gml' and d.taxonomy in ['vector']:
        streamer = GmlStreamer(fields)
        streamer.update_description(d.description)
        streamer.update_namespace(d.basename)
    elif format == 'geojson' and d.taxonomy in ['vector']:
        streamer = GeoJsonStreamer(fields)
    elif format == 'json':
        streamer = JsonStreamer(fields)
<<<<<<< HEAD
=======
        streamer.update_description(str(countTotal),str(countSubtotal))
>>>>>>> gstore/master
    elif format == 'csv':
        streamer = CsvStreamer(fields)
    else:
        return HTTPBadRequest()
        
                
    response = Response()
    response.content_type = streamer.content_type
    response.headers['X-Robots-Tag'] = 'noindex'
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.app_iter = streamer.yield_set(records)
    return response
   

#TODO: try prettyjson format in latest pyramid. this just scares people
#TODO: add params for including styles with output (so render from gstore for niceness or just deliver html structure for epscor/rgis, etc)
@view_config(route_name='dataset_services', renderer='dataset.mako')
def services(request):
    """

    return .json (or whatever) with all services for dataset defined 
    i.e. links to the ogc services, links to the downloads, etc
    same format as stuff from search request?

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

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
    
    load_balancer = request.registry.settings['BALANCER_URL']

    rsp = d.get_full_service_dict(load_balancer, request, app)

 
    if format == 'json':
        response = render_to_response('json', rsp, request=request)
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.content_type='application/json'
        return response
    elif format == 'html':
        #TODO: split out into 2 html formats: one basic one for the kml and one complete nice looking one for everything else?
        return rsp

@view_config(route_name='dataset_statistics')
def statistics(request):
    """

    return some dataset-level stats:

    vector:
        for each attribute (that is int or real)
            min/max
            basic histogram info

    raster
        min/max
        basic histogram info


    in part to help with classification (although that requires sld support)

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    return Response()

@view_config(route_name='dataset_indexer', renderer='json')
def indexer(request):
    """

    return the document for the elasticsearch index

    THIS IS NOT really for production but intersecting everything to get a list of quads will explode the database

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    app = request.matchdict['app']
    dataset_id = request.matchdict['id']

    #go get the dataset
    d = get_dataset(dataset_id)    

    if not d:
        return HTTPNotFound()

    #add the dataset to the index
    es_description = {
        "host": request.registry.settings['es_root'],
        "index": request.registry.settings['es_dataset_index'], 
        "type": 'dataset',
        "user": request.registry.settings['es_user'].split(':')[0],
        "password": request.registry.settings['es_user'].split(':')[-1]
    } 

    indexer = DatasetIndexer(es_description, d, request)  
    #TODO: update the list for facets
    indexer.build_document([])
    #add to the index
    try:
        indexer.put_document()
    except:
        return HTTPServerError('failed to put index document for %s' % d.uuid)

'''
dataset maintenance
'''
@view_config(route_name='add_dataset', request_method='POST')
def add_dataset(request):
<<<<<<< HEAD
=======
    print "add_dataset() called"

>>>>>>> gstore/master
    """

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

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    app = request.matchdict['app']

    #generate uuid here, not through postgres - need to use 
    #outside uuids for data replication (nv/id data as local dataset with pointer to their files)

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
<<<<<<< HEAD
    apps = post_data['apps'] if 'apps' in post_data else []
    validdates = post_data['dates'] if 'dates' in post_data else {}
    spatials = post_data['spatial'] if 'spatial' in post_data else []
    formats = post_data['formats']
=======
    dataone_archive = post_data['dataone_archive']
    author = post_data['author']

    is_embargoed = post_data['is_embargoed']
    releasedate = post_data['releasedate']  
    print "\nDataone_archive: %s" % dataone_archive
    print "DataOne Release Date: %s" % releasedate
    print "Is Embargoed: %s" % is_embargoed

    apps = post_data['apps'] if 'apps' in post_data else []
    validdates = post_data['dates'] if 'dates' in post_data else {}
#    print "Valid date 0: %s" % post_data['dates']
#    print "Valid date 1: %s" % validdates["start"]
    print "Validdates:",validdates
    spatials = post_data['spatial'] if 'spatial' in post_data else []
    formats = post_data['formats']
    print "Formats: %s" % formats
>>>>>>> gstore/master
    services = post_data['services']
    categories = post_data['categories']
    sources = post_data['sources']
    metadatas = post_data['metadata']
    standards = post_data['standards'] if 'standards' in post_data else []
    acquired = post_data['acquired'] if 'acquired' in post_data else ''
    records = post_data['records'] if 'records' in post_data else 0

    geom = ''
    epsg = int(request.registry.settings['SRID'])
    if spatials:
        box = map(float, spatials['bbox'].split(','))
        epsg = spatials['epsg']
        geomtype = spatials['geomtype'] if 'geomtype' in spatials else ''
        geom = spatials['geom'] if 'geom' in spatials else ''
        features = spatials['features'] if 'features' in spatials else 0
        
        
    #add the inactive flag
    active = post_data['active'].lower() if 'active' in post_data else ''

    project = post_data['project'] if 'project' in post_data else ''

<<<<<<< HEAD
    embargo = post_data['embargo'] if 'embargo' in post_data else {}
    #this is not good
    embargoed = True if 'embargoed' in embargo else False
    embargo_release = embargo['release_date'] if embargo else ''
=======
#    embargo = post_data['is_embargoed'] if 'is_embargoed'
    #this is not good
#    embargoed = True if 'embargoed' in embargo else False
#    embargo_release = embargo['release_date'] if embargo else ''
>>>>>>> gstore/master
    

    #we may have instances where we have an external dataset (tri-state replices for example)
    #and we want to keep the uuid for that dataset so we can provide a uuid or make one here
    provided_uuid = post_data['uuid'] if 'uuid' in post_data else generate_uuid4()

<<<<<<< HEAD
    #like make the new dataset
=======
    #Instantiate a new dataset
>>>>>>> gstore/master
    new_dataset = Dataset(description)
    new_dataset.basename = basename
    new_dataset.taxonomy = taxonomy
    new_dataset.record_count = records
<<<<<<< HEAD
=======

>>>>>>> gstore/master
    if taxonomy == 'vector':
        new_dataset.geomtype = geomtype
        new_dataset.feature_count = features
    new_dataset.orig_epsg = epsg
        
    new_dataset.inactive = False if active == 'true' else True

<<<<<<< HEAD
    if embargoed == 'true':
        #need to set is_embargoed and the release date so the dataset is unavailable through gstore
        new_dataset.is_embargoed = True
        new_dataset.embargo_release_date = embargo_release
=======

    #DataOne Capabilities
    new_dataset.dataone_archive=dataone_archive
    new_dataset.embargo_release_date=releasedate
    if is_embargoed == 'True':
	print "Yes, it's true!"
        new_dataset.is_embargoed = True

    print "\ndataone_archive",new_dataset.dataone_archive
    print "embargoReleaseDate",new_dataset.embargo_release_date
    print "is_embargoed",new_dataset.is_embargoed


    #Adding in the author/PI
    new_dataset.author=author
    print "author", new_dataset.author
>>>>>>> gstore/master

    if not geom and taxonomy not in ['table']:
        #go make one
        geom = bbox_to_wkb(box, SRID)

    if taxonomy not in ['table']:
        new_dataset.geom = geom
        new_dataset.box = box
    
    new_dataset.apps_cache = [app] + apps

    #TODO: get rid of formats_cache (once v2 tools issue is resolved in search datasets)
    #new_dataset.formats_cache = ','.join(formats)
<<<<<<< HEAD
=======
    format=','.join(formats)
    formatList=[]
    formatList.append(format)
    print formatList
    new_dataset.formats=formatList
>>>>>>> gstore/master
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
            new_dataset.projects.append(p)

    #TODO: add the publication date
    new_dataset.date_published = datetime.now()
<<<<<<< HEAD

    #create the new dataset with all its pieces
=======
    if(is_embargoed=='False'):
	    new_dataset.embargo_release_date=new_dataset.date_published
    else:
	    new_dataset.embargo_release_date=releasedate

    print "new_dataset values...."
    print new_dataset

    #*******************************CREATE THE NEW DATASET IN POSTGRESQL************************************************************
    #*****************************************************************************************************************************

>>>>>>> gstore/master
    try:
        DBSession.add(new_dataset)
        DBSession.commit()
        DBSession.flush()
        DBSession.refresh(new_dataset)
    except Exception as err:
        return HTTPServerError(err)

    dataset_uuid = str(new_dataset.uuid)

<<<<<<< HEAD
    #add the dataset to the index
=======

#***********************************NOW CREATE NEW DOCUMENT FOR THE DATASET IN ELASTICSEARCH************************************************************


    #add the dataset to the ELASTICSEARCH index
>>>>>>> gstore/master
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
<<<<<<< HEAD
=======

>>>>>>> gstore/master
    #add to the index
    try:
        indexer.put_document()
    except:
        return HTTPServerError('failed to put index document for %s' % dataset_uuid)

    DBSession.close()
 
    #and just for kicks, return the uuid
    return Response(dataset_uuid)

#TODO: change this back to PUT and figure out why pycurl hung up on it.
@view_config(route_name='update_dataset', request_method='POST')
def update_dataset(request):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    
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
    elements_to_update = []

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

            #update the es doc for the metadata elements now that it's gstore schema       
            elements_to_update.append('abstract')
            elements_to_update.append('isotopic')
            elements_to_update.append('keywords')
        elif key == 'activate':
            #TODO: add es updater for flag in index doc
        
            active = post_data[key]
            if not active:
                return HTTPBadRequest()
            inactive = True if active.lower() == 'false' else False
            d.inactive = inactive

            #move the vector data from gstore.vectors to gstore.inactive if FALSE
            #move from gstore.inactive to gstore.vectors if TRUE

            if d.taxonomy in ['vector', 'table']:
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

            elements_to_update.append("active")
            
        elif key == 'available':
            #TODO: add es updater for flag in index doc
            available = post_data[key]
            if not available:
                return HTTPBadRequest()
            available = True if available == 'True' else False
            d.is_available = available

            elements_to_update.append("available")
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

            if d.taxonomy in ['vector', 'table']:
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

            elements_to_update.append("embargo")
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

            elements_to_update.append("location")
            
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


            elements_to_update.append("formats")

        elif key == 'services':
            services = post_data['services']
            excluded_services = get_all_services(request)
            d.excluded_services = [s for s in excluded_services if s not in services]

            elements_to_update.append("services")

        elif key == 'repositories':
            #TODO: add the app+repo to postgres and update the doc for es
            pass
        elif key == 'taxonomy':  
            taxo = post_data[key]

            taxonomy = taxo['taxonomy']
            geomtype = taxo['geomtype'] if 'geomtype' in taxo else ''

            if taxonomy == 'vector' and not geomtype:
                continue

            d.taxonomy = taxonomy.lower()
            if geomtype and taxonomy.lower() == 'vector':
                d.geomtype = geomtype.upper()

            elements_to_update.append("taxonomy")

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
        elif key == 'publication_date':
            '''
            use with embargo/activate -
            if releasing the embargo, include flag to use this datetime (instead of date_added)
            if activating for the first time, include flag to use this datetime (instead of date_added)
                 
            '''
            pdate = datetime.now()
            d.date_published = pdate
            elements_to_update.append("date_published")
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

    #TODO: don't trigger this if no updates to the index doc
    indexer = DatasetIndexer(es_description, d, request)  
    try:
        indexer.build_partial(elements_to_update)
        indexer.update_partial()

    except Exception as ex:
        return HTTPServerError('failed to update document (%s)' % ex)

    finally:
        DBSession.close()

    return Response('updated')

@view_config(route_name='update_dataset_index', request_method='POST')
def update_dataset_index(request):
    """

    just update the es index doc in place (not updating/inserting anything to postgres)

    for postgres database changes that need to be mapped to es but weren't changed through the update dataset route

    url?elements=key1,key2,key3

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    dataset_id = request.matchdict['id']
    d = get_dataset(dataset_id)
    if not d:
        return HTTPNotFound()

    params = normalize_params(request.params)

    param_keys = params['elements'].split(',') if 'elements' in params else []

    if not param_keys:
        return HTTPBadRequest()

    es_description = {
        "host": request.registry.settings['es_root'],
        "index": request.registry.settings['es_dataset_index'], 
        "type": 'dataset',
        "user": request.registry.settings['es_user'].split(':')[0],
        "password": request.registry.settings['es_user'].split(':')[-1]
    } 

    indexer = DatasetIndexer(es_description, d, request)  
    try:
        indexer.build_partial(param_keys)
        indexer.update_partial()
    except Exception as ex:
        return HTTPServerError('failed to update document (%s)' % ex)

    return Response('updated')


	
