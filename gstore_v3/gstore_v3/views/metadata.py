from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError

from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.expression import and_

import os, json
from lxml import etree

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )

from ..models.metadata import OriginalMetadata, MetadataStandards
from ..models.apps import GstoreApp

from ..lib.database import *

'''
standard return metadata as X for a dataset

note: is_available doesn't apply - metadata still accessible
'''


@view_config(route_name='metadata', match_param='datatype=datasets')
def generate_metadata(request):
    '''
    either transform from gstore or just return an unmodified xml blob depending on what the dataset has
    '''
    
    app = request.matchdict['app']
    dataset_id = request.matchdict['id']
    standard = request.matchdict.get('standard') 
    format = request.matchdict['ext']

    d = get_dataset(dataset_id) 

    if not d:
        return HTTPNotFound('Dataset not found')

    if d.is_embargoed or d.inactive:
        return HTTPNotFound('dataset is embargoed or inactive')

    #set up the transformation
    xslt_path = request.registry.settings['XSLT_PATH']

    base_url = request.registry.settings['BALANCER_URL']


    #check for the kind of metadata: gstore (standard + format check) | original (standard & format == xml) | bail
    if d.gstore_metadata:
        #check the standard and format
        
        #check the requested standard against the default list - excluded_standards
        supported_standards = d.get_standards(request)
        if (standard not in supported_standards and '19119' not in standard) or ('19119' in standard and standard.split(':')[0] not in supported_standards):
            return HTTPNotFound("Standard is not supported. Supported standards are: %s" % supported_standards)

        #and check the format of the requested standard
        ms = DBSession.query(MetadataStandards).filter(and_(MetadataStandards.alias==standard, "'%s'=ANY(supported_formats)" % format.lower()))
        if not ms:
            return HTTPNotFound('Format issues...')

        #transform and return
        gstoreapp = DBSession.query(GstoreApp).filter(GstoreApp.route_key==app).first()
        if not gstoreapp:
            #default to rgis and fingers crossed i guess
            gstoreapp = DBSession.query(GstoreApp).filter(GstoreApp.route_key=='rgis').first()

        metadata_info = {"base_url": base_url, "app-name": gstoreapp.full_name, "app-url": gstoreapp.url, "app": app, "request": request}

        if '19119' in standard:
            service = standard.split(':')[-1]
            supported_services = d.get_services(request)
            if service.lower() not in supported_services:
                return HTTPNotFound('Not a supported service')
            metadata_info.update({"service": service})
            standard = standard.split(':')[0]

        gm = d.gstore_metadata[0]
        output = gm.transform(standard, format, xslt_path + '/xslts', metadata_info, False)

        if not output:
            return HTTPServerError('Invalid output')
        if output == 'No matching stylesheet':
            #no xslt for the standard + format output
            return HTTPNotFound('No matching xslt')

        content_type = 'text/html' if format == 'html' else 'application/xml'
        
        r = Response(output, content_type=content_type)
        r.headers['X-Robots-Tag'] = 'nofollow'
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r

    elif not d.gstore_metadata and d.original_metadata and format.lower() == 'xml':
        #check for some xml with that standard
        om = [o for o in d.original_metadata if o.original_xml_standard == standard and o.original_xml]
        if not om:
            return HTTPNotFound('check for some xml with that standard failed')

        r = Response(om[0].original_xml, content_type='application/xml')
        r.headers['X-Robots-Tag'] = 'nofollow'
        r.headers['Access-Control-Allow-Origin'] = '*'
        return r

    #otherwise, who knows, we got nothing.
    return HTTPNotFound('LOL')




@view_config(route_name='metadata', match_param='datatype=collections')
def generate_collection_metadata(request):
    '''
    build the collection level metadata (iso ds, not great fgdc)
    '''

    app = request.matchdict['app'] #doesn't actually mean anything right now
    collection_id = request.matchdict['id']
    standard = request.matchdict.get('standard') 
    format = request.matchdict['ext']

    c = get_collection(collection_id) 

    if not c:
        return HTTPNotFound()

    if c.is_embargoed or not c.is_active:
        return HTTPNotFound()

    #set up the transformation
    xslt_path = request.registry.settings['XSLT_PATH']
    base_url = request.registry.settings['BALANCER_URL']

    if c.gstore_metadata:
        #check the requested standard against the default list - excluded_standards
        supported_standards = c.get_standards(request)
        if (standard not in supported_standards and '19119' not in standard) or ('19119' in standard and standard.split(':')[0] not in supported_standards):
            return HTTPNotFound()

        #and check the format of the requested standard
        ms = DBSession.query(MetadataStandards).filter(and_(MetadataStandards.alias==standard, "'%s'=ANY(supported_formats)" % format.lower()))
        if not ms:
            return HTTPNotFound()

        #transform and return
        gstoreapp = DBSession.query(GstoreApp).filter(GstoreApp.route_key==app).first()
        if not gstoreapp:
            #default to rgis and fingers crossed i guess
            gstoreapp = DBSession.query(GstoreApp).filter(GstoreApp.route_key=='rgis').first()

        metadata_info = {"base_url": base_url, "app-name": gstoreapp.full_name, "app-url": gstoreapp.url, "app": app, "request": request}

        gm = c.gstore_metadata[0]
        output = gm.transform(standard, format, xslt_path + '/xslts', metadata_info, False)

        if not output:
            return HTTPServerError('Invalid output')
        if output == 'No matching stylesheet':
            #no xslt for the standard + format output
            return HTTPNotFound()

        content_type = 'text/html' if format == 'html' else 'application/xml'

        r = Response(output, content_type=content_type)
        r.headers['X-Robots-Tag'] = 'nofollow'
        return r


    return HTTPNotFound()
    
'''
other
'''

#return a deprecation warning for v1 api requests
@view_config(route_name='schema')
def schema(request):
    return Response('Deprecated. Do not use.')
