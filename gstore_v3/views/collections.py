from pyramid.view import view_config
from pyramid.response import Response
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError, HTTPBadRequest, HTTPServiceUnavailable

from sqlalchemy.exc import DBAPIError

from ..models import DBSession
from ..models.datasets import (
    Dataset,
    Collection
    )


from ..lib.utils import *
from ..lib.spatial import *
from ..lib.database import *
from ..lib.spatial_streamer import *

'''
collections
'''

@view_config(route_name='collections', renderer='json')
def collections(request):
    '''
    
    '''
    
    app = request.matchdict['app']
    collection_id = request.matchdict['id']
    format = request.matchdict['ext']

    dlist = request.params.get('list', 'basic')

    c = get_collection(collection_id)

    if not c:
        return HTTPNotFound()

    base_url = request.registry.settings['BALANCER_URL']

    response = render_to_response('json', c.get_full_service_dict(base_url, request, app), request=request)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.content_type='application/json'
    return response

@view_config(route_name='collection_footprint')
def generate_footprint(request):
    '''
    return the footprint (union) or the bboxes (if reasonable)
    '''

    app = request.matchdict['app']
    collection_id = request.matchdict['id']
    format = request.matchdict['ext']

    if format not in ['geojson', 'kml', 'gml']:
        return HTTPBadRequest()

    #TODO: footprint v bboxes of datasets
    params = normalize_params(request.params)
    
    c = get_collection(collection_id)
    if not c:
        return HTTPNotFound()

    if not c.is_spatial:
        return HTTPBadRequest()

    if format not in ['geojson', 'kml', 'gml']:
        return HTTPBadRequest()

    if not c.is_available:
        return HTTPServiceUnavailable()

    epsg = int(request.registry.settings['SRID'])

    #deal with the geometry
    geom_repr = c.get_footprint(epsg, format)
    
    records = [{"geom": geom_repr, "id": 1, "datavalues": [("Collection", c.name)]}]

    fields = [{"name": "Collection", "type": 4, "len": 100}]

    if format == 'kml':
        streamer = KmlStreamer(fields)
        streamer.update_description(c.name)
    elif format =='gml':
        streamer = GmlStreamer(fields)
        streamer.update_description(c.name)
        streamer.update_namespace(c.name.replace(' ', '_'))
    elif format == 'geojson':
        streamer = GeoJsonStreamer(fields)
    else:
        return HTTPBadRequest()
                     
    response = Response()
    response.content_type = streamer.content_type
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.app_iter = streamer.yield_set(records)
    return response


'''
collection maintenance
'''
@view_config(route_name='add_collection', request_method='POST')
def add_collection(request):
    '''
    add collection

    {
        title: ''
        description: ''
        apps: []
        datasets: []
        categories: [{theme:, subtheme:, groupname:}]
        standards: []
        active: t/f
        embargoed: t/f
        metadata: xml  (gstore only)
    }
    '''

    app = request.matchdict['app']
    post_data = request.json_body

    excluded_standards = get_all_standards(request)

    #prep the inputs
    title = post_data['title']
    description = post_data['description'] if 'description' in post_data else ''
    apps = post_data['apps'] if 'apps' in post_data else []
    datasets = post_data['datasets']
    categories = post_data['categories']
    standards = post_data['standards'] if 'standards' in post_data else []
    xml = post_data['metadata']

    is_active = bool(post_data['active']) if 'active' in post_data else True
    is_embargoed = bool(post_data['embargoed']) if 'embargoed' in post_data else False

    #TODO: add the tile index flags/descriptors

    apps = apps + [app] if app not in apps else apps
    new_collection = Collection(title, apps)
    new_collection.is_embargoed = is_embargoed
    new_collection.is_active = is_active
    new_collection.excluded_standards = [s for s in excluded_standards if s not in standards]
    new_collection.description = description
    
    #add the categories
    for category in categories:
        theme = category['theme']
        subtheme = category['subtheme']
        groupname = category['groupname']

        c = DBSession.query(Category).filter(and_(Category.theme==theme, Category.subtheme==subtheme, Category.groupname==groupname)).first()
        if not c:
            #we'll need to add a new category BEFORE running this (?)
            return HTTPBadRequest('Missing category triplet')

        new_collection.categories.append(c)

    #add the metadata object (must be gstore)
    valid = validate_xml(xml)
    if 'ERROR' in valid:
        return HTTPBadRequest('Not valid GSTORE metadata: %s' % valid)

    new_metadata = CollectionMetadata()
    new_metadata.gstore_metadata = xml
    new_collection.append(new_metadata)

    for dataset in datasets:
        #this is silly, who cares
        d = DBSession.query(Dataset).filter(Dataset.id==dataset).first()
        if d:
            new_collection.datasets.append(d)

    #commit the new collection object
    try:
        DBSession.add(new_collection)
        DBSession.commit()
        DBSession.flush()
        DBSession.refresh(new_collection)
    except Exception as err:
        DBSession.rollback()
        return HTTPSErverError(err)

    #once the datasets are added, build the geometries
    new_collection.update_geometries()

    #and update the valid dates based on the dataset dates
    new_collection.update_date_range()

    #TODO: update the dataset taxonomy list (unique taxonomies for dataset set)

    #add to elasticsearch
    es_description = {
        "host": request.registry.settings['es_root'],
        "index": request.registry.settings['es_dataset_index'], 
        "type": 'collection',
        "user": request.registry.settings['es_user'].split(':')[0],
        "password": request.registry.settings['es_user'].split(':')[-1]
    }
    indexer = CollectionIndexer(es_description, new_collection, request)
    indexer.build_document()
    try:
        indexer.put_document()
    except:
        return HTTPServerError('failed to put index document for %s' % new_collection.uuid)

    #update all of the datasets to include the new collection uuid
    #also silly and really needs a better widget
    errors = []
    for dataset in datasets:
        d = DBSession.query(Dataset).filter(Dataset.id==dataset).first()
        if d:
            dataset_indexer = DatasetIndexer(es_description, d, request)
            try:
                dataset_indexer.update_document({"collections": d.collections})
            except Exception as ex:
                errors.append({d.id: ex})

    rsp = {"uuid": str(new_collection.uuid)}
    if errors:
        rsp.update({"errors": errors})

    return Response(json.dumps(rsp))

@view_config(route_name='update_collection', request_method='PUT')
def update_collection(request):
    '''
    modify an existing collection
    '''
    collection_id = request.matchdict['id']

    c = get_collection(collection_id)
    if not c:
        return HTTPNotFound()
    
    return Response('updated collection')
