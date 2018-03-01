from pyramid.view import view_config
from pyramid.response import Response

from ..lib.utils import *
from ..lib.database import *
from ..models.tileindexes import *

import json

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

@view_config(route_name='update_tileindex', request_method='POST')
def update_tileindex(request):
    '''
    modify an existing tile index

    1. update bbox for union of all tile bboxes
    2. add dataset to tile index
    3. generate the index shapefile 
    4. active t/f
    

    '''

    tile_id = request.matchdict['id']
    app = request.matchdict['app']

    

    #get the tile index by id/uuid
    tile = get_tileindex(tile_id)

    
    if not tile:
        return HTTPNotFound()

    post_data = request.json_body

    SRID = int(request.registry.settings['SRID'])

    keys = post_data.keys()
    rsp = {}
    for key in keys:
        #so we can update all the things
        
        if key == 'activate':
            
            pass
        elif key == 'build_shapefile':
            #TODO: change this to match the other default paths
            base_path = request.registry.settings['BASE_DATA_PATH'] + '/tileindexes'
            tile.generate_index_shapefiles(base_path)
            rsp.update({key: 'success'})
    
    return Response(json.dumps(rsp))
