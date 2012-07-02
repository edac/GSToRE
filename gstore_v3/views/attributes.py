from pyramid.response import Response
from pyramid.view import view_config
from pyramid.httpexceptions import HTTPNotFound, HTTPFound

from pyramid.threadlocal import get_current_registry

from sqlalchemy.exc import DBAPIError

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.attributes import (
    Attribute,
    )


from ..lib.utils import *
from ..lib.database import *
from ..lib.spatial import *

'''
attribute
'''
@view_config(route_name='dataset_attributes', renderer='json')
def attributes(request):
    '''
    return attributes for a dataset by dataset uuid

    name, original name, description, data type (postgis not ogr to make it readable), (nodata, parameter listing if parameter)
    '''

    dataset_id = request.matchdict['id']
    format = request.matchdict['ext'] #that we are ignoring right now (what should the formats be?)

    d = get_dataset(dataset_id)
    if not d:
        return HTTPNotFound('No dataset')

    atts = d.attributes

    if not atts:
        return {'total': 0, 'results': []}

    rsp = {'total': len(atts), 'dataset': {'id': d.id, 'uuid': d.uuid}}
    res = [{'uuid': a.uuid, 'name': a.name, 'original_name': a.orig_name, 'description': a.description, 'datatype': 'insert type'} for a in atts] #ogr_to_psql(a.ogr_type)
    rsp.update({'results': res})
    
    return rsp


@view_config(route_name='attributes', renderer='json')
def attribute(request):
    '''
    return an attribute by its uuid    
    '''
    attribute_id = request.matchdict['id']
    format = request.matchdict['ext']

    #TODO: add the regex check for fun?

    #it's the uuid        
    a = DBSession.query(Attribute).filter(Attribute.uuid==attribute_id).first()   

    if not a:
        return HTTPNotFound('No attribute')

    rsp = {'total': 1, 'dataset': {'id': a.dataset.id, 'uuid': a.dataset.uuid}}
    res = [{'uuid': a.uuid, 'name': a.name, 'original_name': a.orig_name, 'description': a.description, 'datatype': 'insert type'} ] #ogr_to_psql(a.ogr_type)
    rsp.update({'results': res})

    return rsp


'''
attribute maintenance
'''
@view_config(route_name='add_attributes', request_method='POST')
def attribute_new(request):
    return Response('added new attribute')
