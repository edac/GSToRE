from pyramid.view import view_config
from pyramid.response import Response
from pyramid.httpexceptions import HTTPNotFound, HTTPFound


from sqlalchemy.exc import DBAPIError

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    Collection
    )


from ..lib.utils import *
from ..lib.database import *

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

    c = DBSession.query(Collection).filter(Collection.uuid==collection_id).first()

    if not c:
        return HTTPNotFound()

    #do stuff and junk
    rsp = {'uuid': c.uuid, 'name': c.name, 'description': c.description, 'metadata': 'some url here'}

    #TODO: add this back when the image bits are added to postgres and the models
#    if c.imagepath is not None:
#        rsp.update('img': {'full': 'some img path here', 'thmb': 'some other img path here'})


    #add some dataset info
    ds = []
    if dlist == 'basic':
        #it's just minimum dataset info
        ds = [d.uuid for d in c.datasets if d.inactive == False]
    else:
        #it's more, so much more

        g_app = request.script_name[1:]
        
        ds = [{'uuid': d.uuid, 'description': d.description, 'link': '%s/%s/apps/%s/datasets/%s/services.json' % (request.host_url, g_app, app, d.uuid)} for d in c.datasets]
#        for d in c.datasets:
#            ds.append({'uuid': d.uuid, 'description': d.description, 'link': 'some link to the services list?'})

    rsp.update({'datasets': ds})

    #TODO: override the format if we want to (will it ever be not json?)
    
    return rsp




'''
collection maintenance
'''
@view_config(route_name='add_collection', request_method='POST')
def add_collection(request):
    '''
    add collection
    '''

    return Response('added new collection')

@view_config(route_name='update_collection', request_method='PUT')
def update_collection(request):
    '''
    modify an existing collection
    '''
    return Response('updated collection')
