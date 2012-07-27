from pyramid.view import view_config
from pyramid.response import Response
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError

from pyramid.threadlocal import get_current_registry

import json
from ..models import DBSession
from ..models.features import (
    Feature,
    )

from ..lib.mongo import gMongo


'''
features
'''
#TODO: add formats? why would anyone want it as a shapefile though?
#TODO: update some indexes for better performance in both queries
@view_config(route_name='feature', renderer='json')
def feature(request):
    '''
    get a feature
    /apps/{app}/features/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}
    /apps/rgis/features/e74a1e0d-3e75-44dd-bb4c-3328a2425856.json


    now what did we forget? that uuids ar enot good indexes in mongo (too big)
    so ping shapes for the fid before pinging mongo (and then we already have our geom just in case)
    '''
    feature_id = request.matchdict['id']

    feature = DBSession.query(Feature).filter(Feature.uuid==feature_id).first()
    if not feature:
        return HTTPNotFound('Invalid feature request')

    #get the feature from mongo
    connstr = get_current_registry().settings['mongo_uri']
    gm = gMongo(connstr, 'vectors')
    vectors = gm.query({'f.id': feature.fid})

    if not vectors:
        return HTTPServerError('no cursor')

    vector = vectors[0]

    #add the check for the geometry (larger than 1.5mb not stored there - go to postgres)
    if not 'geom' in vector:        
        geom = feature.geom if feature else ''     
    else:
        geom = vector['geom']['g']

    #'observed': vector['obs'],

    #rebuild some json from the other json (i like it)
    results = {'dataset': {'id': vector['d']['id'], 'uuid': vector['d']['u']}, 'feature': {'id': vector['f']['id'], 'uuid': vector['f']['u']}, 
                'attributes': vector['atts'], 'geometry': geom}

    return results

@view_config(route_name='features')
def features(request):
    '''
    feature streamer

    PARAMS:
    app
    
    limit
    offset
    
    start_time
    end_time
    valid_start
    valid_end
    
    epsg
    box
    theme, subtheme, groupname - category
    query - keyword

    parameter (or parameters + units + frequency)

    format
    geomtype !!! required

    
    '''

    #TO START, GEOMETRY TYPE IS REQUIRED
    #id the datasets that match the filters
    app = request.matchdict['app']

    #pagination
    limit = int(request.params.get('limit')) if 'limit' in request.params else 25
    offset = int(request.params.get('offset')) if 'offset' in request.params else 0

    #check for valid utc datetime
    start_added = request.params.get('start_time') if 'start_time' in request.params else ''
    end_added = request.params.get('end_time') if 'end_time' in request.params else ''

    #check for valid utc datetime
    start_valid = request.params.get('valid_start') if 'valid_start' in request.params else ''
    end_valid = request.params.get('valid_end') if 'valid_end' in request.params else ''

    #check for OUTPUT format
    format = request.params.get('format', '')
    
    #check for geomtype
    geomtype = request.params.get('geomtype', '')

    #keyword search
    keyword = request.params.get('query') if 'query' in request.params else ''
    keyword = keyword.replace(' ', '%').replace('+', '%')

    #TODO: set up for the georelevance sorting
    #sort geometry
    box = request.params.get('box') if 'box' in request.params else ''
    epsg = request.params.get('epsg') if 'epsg' in request.params else ''

    #category params
    theme = request.params.get('theme') if 'theme' in request.params else ''
    subtheme = request.params.get('subtheme') if 'subtheme' in request.params else ''
    groupname = request.params.get('groupname') if 'groupname' in request.params else ''

    #set up the postgres checks
    dataset_clauses = [Dataset.inactive==False, "'%s'=ANY(apps_cache)" % (app)]


    #TODO: QUERY SHAPES BY FID? limit to the number of fids in a list - check on that (cannot query for millions of things)

    #use the filters to build the mongo filters


    #export as the format somehow
    
    return Response('features')

@view_config(route_name='add_features', request_method='POST')
def add_feature(request):
    '''
    add features to a dataset
    '''
    dataset_id = request.matchdict['id']

    return Response('added new features')

@view_config(route_name='update_feature', request_method='PUT')
def update_feature(request):
    '''
    modify an existing feature - add qualty flag or something
    '''
    return Response('')
