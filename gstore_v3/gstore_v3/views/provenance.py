from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError

from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.expression import and_

import os, json

#from the models init script
from ..models import DBSession
from ..models.provenance import *

from ..lib.database import *


'''
'provenance_trace': '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/prov/{ontology}.{ext}'
'provenance_base': '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/prov/{ontology}/{standard}.{ext}'
'''


@view_config(route_name='provenance_trace')
def get_trace(request):
    '''
    return the rdf trace (PROV) for the dataset obj
    if the dataset has a prov obj for the ontology
    '''

    app_key = request.matchdict['app']
    dataset_id = request.matchdict['id']
    ontology_key = request.matchdict['ontology']
    format = request.matchdict['ext']

    # get a couple of bits
    d = get_dataset(dataset_id)   
    if not d:
        return HTTPNotFound()

    app = get_app(app_key)
    if not app:
        return HTTPNotFound()

    # need the mapping to decide if we can do anything or not (i.e. identify the mapping for ontology + app + 
    # output format to get the base info using the same ontology + app + standard key + standard format)
    ontology = DBSession.query(ProvOntology).filter(and_(ProvOntology.ontology_key==ontology_key, ProvOntology.app_id==app.id)).first()
    if not ontology:
        return HTTPNotFound()

    ontology_mapping = DBSession.query(ProvMapping).filter(and_(ProvMapping.ontology_id==ontology.id, ProvMapping.output_format==format)).first()
    if not ontology_mapping:
        return HTTPNotFound()    


    #check for the prov base
    prov_base = DBSession.query(ProvBase).join(ProvBase.ontologies).\
        join(ProvBase.datasets).\
        filter(and_(ProvBase.dataset_id==d.id, 
            ProvOntology.ontology_key==ontology_key,
            ProvOntology.app_id==app.id,
            ProvBase.inputstandards_id==ontology_mapping.inputstandards_id)).\
        first()

    if not prov_base:
        return HTTPNotFound()

    base_path = request.registry.settings['PROV_PATH']
    xslt_path = request.registry.settings['XSLT_PATH'] + '/xslts/prov'

    output = prov_base.transform_base(format, xslt_path, base_path)

    #TODO: update the mimetype if we ever have other types
    mimetype = "application/rdf+xml"

    return Response(output, content_type=mimetype)


@view_config(route_name='provenance_base')
def get_trace_source(request):
    '''
    return the source (if there is one) for the prov
    for the dataset and ontology

    i.e. return the iso ds xml for the modis dataset for elseweb

    '''

    app_key = request.matchdict['app']
    dataset_id = request.matchdict['id']
    ontology_key = request.matchdict['ontology']
    standard = request.matchdict['standard']
    format = request.matchdict['ext']

    
    # get a couple of bits
    d = get_dataset(dataset_id)   
    if not d:
        return HTTPNotFound()

    app = get_app(app_key)
    if not app:
        return HTTPNotFound()

    #check for the prov base
    prov_base = DBSession.query(ProvBase).join(ProvBase.ontologies).\
        join(ProvBase.base_standards).join(ProvBase.datasets).\
        filter(and_(ProvBase.dataset_id==d.id, 
            ProvOntology.ontology_key==ontology_key,
            ProvOntology.app_id==app.id,
            ProvInputStandard.standard_key==standard, 
            ProvInputStandard.standard_format==format)).\
        first()

    if not prov_base:
        return HTTPNotFound()

    base_path = request.registry.settings['PROV_PATH']

    source = prov_base.get_base(base_path)

    #TODO: change mimetype to not be xml depending on the theoretical presence of other things
    mimetype = 'application/xml'

    return Response(source, content_type=mimetype)










    
