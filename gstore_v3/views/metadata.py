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

    app = request.matchdict['app'] #doesn't actually mean anything right now
    dataset_id = request.matchdict['id']
    standard = request.matchdict.get('standard', 'fgdc')
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
    xml = d.dataset_metadata[0].original_xml

    #get the xml metadata
    #TODO: update this for iso, fgdc, dc, etc
    #TODO: xml-to-text transform iffy? or esri metadata in database so not valid fgdc
    xslt_path = get_current_registry().settings['XSLT_PATH']
    xslt = 'fgdc_classic_rgis.xsl' if format == 'html' else 'xml-to-text.xsl'

    if format == 'xml':
        #dump the xml and encode so that everything matches
        return Response(xml.encode('utf8'), content_type='text/xml; charset=UTF-8')
    else:
        #transform the xml
        #TODO: update this for iso, fgdc, dc, etc
        content_type = 'text/html; charset=UTF-8' if format == 'html' else 'text; charset=UTF-8'

        xslt_path = os.path.join(xslt_path, xslt)
        xsltfile = open(xslt_path, 'r')

        try:
            xslt = etree.parse(xsltfile)
            transform = etree.XSLT(xslt)
            xml_enc = etree.XML(xml.encode('utf8'))
            output = transform(xml_enc)
            output = etree.tostring(output, encoding='utf8')
        except Exception as e:
            #failover to the raw xml
            content_type='text/xml; charset=UTF-8'
            output = xml.encode('utf8')
            return Response(str(e))

        return Response(output, content_type=content_type)

#TODO: deal with this once we have linkable components in new schema
@view_config(route_name='xlink_metadata')
def xlink(request):
    return Response('metadata = %s' % (request.matchdict['id']))

'''
metadata maintenance
'''
@view_config(route_name='add_metadata', request_method='POST')
def add_metadata(request):
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
