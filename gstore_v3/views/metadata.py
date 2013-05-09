from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPFound

from sqlalchemy.exc import DBAPIError

import os, json
from lxml import etree

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )

from ..models.metadata import OriginalMetadata

from ..lib.database import *

'''
metadata
'''

'''
standard return metadata as X for a dataset

note: is_available doesn't apply - metadata still accessible
'''

#*****FOR THE CURRENT STRUCTURE 
#TODO: think about doing the HTML/txt part of this with renderers instead? (when it's all db parts, then building dict is better than building fgdc xml and transforming)
@view_config(route_name='metadata_fgdc')
@view_config(route_name='metadata_v2')
def metadata(request):
    #/apps/{app}/datasets/{id}/metadata/{standard}.{ext}
    '''
    note: for iso, return data with xlinks to lineage, entity info/attributes, contacts, possibly also spatial ref
    '''

    app = request.matchdict['app'] #doesn't actually mean anything right now
    dataset_id = request.matchdict['id']
    standard = request.matchdict.get('standard', 'fgdc') #for the v2 uri
    format = request.matchdict['ext']

    if standard not in ['fgdc']:
        return HTTPNotFound()
    if format not in ['html', 'xml']:
        #removing (, 'txt') from list - transform is busted and it provides little that's different from the html representation (as per karl 8/21/2012)
        return HTTPNotFound() 
    
    #go get the dataset
    d = get_dataset(dataset_id)    

    if not d:
        return HTTPNotFound()

    #TODO: replace this when the schema is complete & populated
    #and make sure there's metadata
    if d.has_metadata_cache == False:
        return HTTPNotFound()

    if d.is_embargoed or d.inactive:
        return HTTPNotFound()

    #this should only be valid xml (<?xml or <metadata)
    #get the xml metadata
    #TODO: the standard is a lie
    xslt_path = request.registry.settings['XSLT_PATH']
    base_url = '%s/apps/%s/datasets/' % (request.registry.settings['BALANCER_URL'], app) if standard == 'fgdc' else ''
    
    om = [o for o in d.original_metadata if o.original_xml_standard == standard]
    if not om:
        return HTTPNotFound()
    
    output, content_type = om[0].transform(format, xslt_path, {'app': app, 'standard': standard, 'base_url': base_url}) 
    if not output:
        return HTTPBadRequest()

    return Response(output, content_type=content_type)

@view_config(route_name='metadata')
def generate_metadata(request):
    '''
    the new routing for the gstore schema-based metadata structure
    '''
    return Response(json.dumps({"standard": request.matchdict['standard']}), content_type='application/json')


@view_config(route_name='metadata_resolved')
def metadata_resolver(request):
    '''
    this really only matters for ISO right now
    but, if, iso, return full version of metadata, no xlinks
    if not iso, return same as basic metadata view
    '''
    return Response()


#TODO: deal with this once we have linkable components in new schema
#TODO: modify routes to have some {metadata object}/{uuid}.{ext} thing?
@view_config(route_name='xlink_metadata')
def xlink(request):
    return Response('metadata = %s' % (request.matchdict['id']))


'''
other
'''
#TODO: this is probably not necessary (see the metadata method instead) 
# unless we just want something that ignores the dataset (why would we want that?)
#@view_config(route_name='get_metadata')
#def show(request):
#    return Response('metadata = %s' % (request.matchdict['id']))

#return a deprecation warning for v1 api requests
@view_config(route_name='schema')
def schema(request):
    return Response('Deprecated. Do not use.')
