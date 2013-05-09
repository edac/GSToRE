from pyramid.response import Response
from pyramid.view import view_config
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError

from sqlalchemy.exc import DBAPIError

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.attributes import (
    Attribute,
    )

import json

from ..lib.utils import *
from ..lib.database import *
from ..lib.spatial import *

'''
attribute
'''
@view_config(route_name='dataset_attributes')
def attributes(request):
    '''
    return attributes for a dataset by dataset uuid

    name, original name, description, data type (postgis not ogr to make it readable), (nodata, parameter listing if parameter)
    '''

    dataset_id = request.matchdict['id']
    format = request.matchdict['ext'] 

    if format not in ['json', 'kml']:
        return HTTPNotFound()

    d = get_dataset(dataset_id)
    if not d:
        return HTTPNotFound()

    if d.is_embargoed or d.inactive:
        return HTTPNotFound()

    fields = d.attributes

    if not fields:
        return HTTPNotFound()

    if format == 'json':
        rsp = {'total': len(fields), 'dataset': {'id': d.id, 'uuid': d.uuid}}
        res = [{'uuid': a.uuid, 'name': a.name, 'original_name': a.orig_name, 'description': a.description, 'datatype': ogr_to_kml_fieldtype(a.ogr_type)} for a in fields] #ogr_to_psql(a.ogr_type)
        rsp.update({'results': res})

        rsp = json.dumps(rsp)

        content_type = 'application/json'
    elif format == 'kml':
        #do we need a kml schema response?
        #maybe?
        kml_flds = [{'type': ogr_to_kml_fieldtype(f.ogr_type), 'name': f.name} for f in fields]
        kml_flds.append({'type': 'string', 'name': 'observed'})
        rsp = """<?xml version="1.0" encoding="UTF-8"?><Schema name="%(name)s" id="%(id)s">%(sfields)s</Schema>""" % {'name': str(d.uuid), 
                    'id': str(d.uuid), 
                    'sfields': '\n'.join(["""<SimpleField type="%s" name="%s"><displayName>%s</displayName></SimpleField>""" % (k['type'], k['name'], k['name']) for k in kml_flds])
        }
        content_type = 'application/vnd.google-earth.kml+xml'
    
    return Response(rsp, content_type=content_type, charset='UTF-8')


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
        return HTTPNotFound()

    if a.dataset.isembargoed or a.dataset.inactive:
        return HTTPNotFound()

    rsp = {'total': 1, 'dataset': {'id': a.dataset.id, 'uuid': a.dataset.uuid}}
    res = [{'uuid': a.uuid, 'name': a.name, 'original_name': a.orig_name, 'description': a.description, 'datatype': 'insert type'} ] #ogr_to_psql(a.ogr_type)
    rsp.update({'results': res})

    return rsp


'''
attribute maintenance
'''
@view_config(route_name='add_attributes', request_method='POST', renderer='json')
def attribute_new(request):
    '''
    add a new set of attributes
    for a dataset
    {   
        dataset: 
        fields:
            [
                {
                    name:
                    description:
                    orig_name:
                    ogr_type:

                    nodata:
                    ogr_precision:
                    ogr_width:
                    ogr_justify:
                },
            ]
    }
    '''

    #TODO: add the link to a parameter
    #TODO: add the link to a representation (i.e. odm, etc)

    post_data = request.json_body
    if not 'dataset' in post_data:
        return HTTPNotFound()
        
    dataset_id = post_data['dataset']
    the_dataset = get_dataset(dataset_id)
    if not the_dataset:
        return HTTPNotFound()
    dataset_id = the_dataset.id

    fields = post_data['fields'] if 'fields' in post_data else []
    if not fields:
        return HTTPNotFound()

    new_fields = []
    for d in fields:
        name = d['name']
        desc = d['description']
        orig_name = d['orig_name'] if 'orig_name' in d else ''

        #accept the integer enum or, if not int, get the enum based on the string, defaulting to string (4) if that fails
        try :
            ogr_type = int(d['ogr_type'])
        except:
            ogr_type = psql_to_ogr(d['ogr_type'])

        ogr_precision = d['ogr_precision'] if 'ogr_precision' in d else ''
        ogr_width = d['ogr_width'] if 'ogr_width' in d else ''
        ogr_justify = d['ogr_justify'] if 'ogr_justify' in d else ''
        nodata = d['nodata'] if 'nodata' in d else ''

        field = Attribute(name, ogr_type)
        field.description = desc
        field.orig_name = orig_name

        if ogr_precision or ogr_precision == 0:
            field.ogr_precision = ogr_precision
        if ogr_width:
            field.ogr_width = ogr_width
        if ogr_justify:
            field.ogr_justify = ogr_justify

        if nodata:
            field.nodata = nodata

        field.dataset_id = dataset_id
        
        new_fields.append(field)
    try:
        DBSession.add_all(new_fields)
        DBSession.commit()
        DBSession.flush()
        
    except Exception as err:
        return HTTPServerError(err)

    return {'attributes': [{'name': n.name, 'uuid': n.uuid} for n in new_fields]}







    
