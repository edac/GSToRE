from pyramid.view import view_config
from pyramid.response import Response

'''
tileindexes
'''
@view_config(route_name='tileindexes')
def tileindexes(request):
    '''
    tileindexes
    '''
    
    return Response('tileindex')

@view_config(route_name='add_tileindex', request_method='POST')
def add_tileindex(request):
    '''
    add tileindex
    '''
    
    return Response('added new tile index')

@view_config(route_name='update_tileindex', request_method='PUT')
def update_tileindex(request):
    '''
    modify an existing tile index

    update bbox for union of all tile bboxes
    add dataset to tile index

    '''
    return Response('')