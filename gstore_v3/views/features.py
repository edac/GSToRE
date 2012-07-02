from pyramid.view import view_config
from pyramid.response import Response

'''
features
'''
@view_config(route_name='feature')
def features(request):
    '''
    get a feature
    '''
    feature_id = request.matchdict['id']
    return Response('feature')

@view_config(route_name='features')
def features(request):
    '''
    feature streamer
    '''
    
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