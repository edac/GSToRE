from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPFound

from pyramid.threadlocal import get_current_registry

from sqlalchemy.exc import DBAPIError

import os
from lxml import etree

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )

from ..models.metadata import DatasetMetadata

from ..lib.database import *

'''
metadata
'''

'''
standard return metadata as X for a dataset

note: is_available doesn't apply - metadata still accessible
'''

#TODO: think about doing the HTML/txt part of this with renderers instead? (when it's all db parts, then building dict is better than building fgdc xml and transforming)
@view_config(route_name='metadata')
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
        return HTTPNotFound('Only running FGDC now')
    if format not in ['html', 'xml', 'txt']:
        return HTTPNotFound('Only FGDC as html, xml, or text.')
    
    #go get the dataset
    d = get_dataset(dataset_id)    

    if not d:
        return HTTPNotFound('No results')

    #TODO: replace this when the schema is complete & populated
    #and make sure there's metadata
    if d.has_metadata_cache == False:
        return HTTPNotFound('No metadata')

    #this should only be valid xml (<?xml or <metadata)
    #get the xml metadata
    #TODO: the standard is a lie
    output, content_type = d.dataset_metadata[0].transform(standard, format) 

    return Response(output, content_type=content_type)

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
metadata maintenance
'''
@view_config(route_name='add_metadata', request_method='POST')
def add_metadata(request):
    '''
    so could be one of several different imports:

    1) POST edac xml file (need schema that can be validated by metadata crew) that can be parsed here into table structure

    2) POST valid fgdc/iso(?) plus some extra JSON elements that is stored in original_metadata but that could be parsed later using the extra JSON bits

    3) POST text that isn't anything and is stored in original_metadata.original_text that we do next to nothing to
    '''

    return Response('metadata = %s' % (request.matchdict['id']))

#i have no idea what would get updated though
@view_config(route_name='add_metadata', request_method='PUT')
def update_metadata(request):
    return Response('metadata = %s' % (request.matchdict['id']))


'''
other
'''
#TODO: this is probably not necessary (see the metadata method instead) 
# unless we just want something that ignores the dataset (why would we want that?)
@view_config(route_name='get_metadata')
def show(request):
    return Response('metadata = %s' % (request.matchdict['id']))

#return a deprecation warning for v1 api requests
@view_config(route_name='schema')
def schema(request):
    return Response('Deprecated. Do not use.')
