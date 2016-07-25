from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPFound

from sqlalchemy.exc import DBAPIError

import os, json

#from the models init script
from ..models import DBSession
from ..models.vocabs import *


@view_config(route_name='available_vocabs', renderer='json')
def list_vocabs(request):
    '''
    return a list of vocabs available through gstore
    that may only be used by us but you never know

    /apps/{app}/vocabs/{type}.{ext}
    ''' 
    app = request.matchdict['app']
    cvs = ['units', 'timeunits', 'censorcodes', 'datatypes', 'generalcategories', 'samplemediums', 'sampletypes', 'speciation', 'valuetypes', 'variablenames', 'parameters', 'qualitycontrollevels', 'parametersources']

    base_url = '%s/apps/%s/vocabs' % (request.registry.settings['BALANCER_URL'], app)

    #baseurl, name
    tmp = '%s/%s.json'
    return [{"name": c, "url": tmp % (base_url, c)} for c in cvs]

@view_config(route_name='vocabs', renderer='json')
def vocab(request):
    '''
    return the list of values for the specified controlled vocab
    '''
    #TODO: add limit/offset? is it necessary?
    
    cv_type = request.matchdict['type'].lower()

    if cv_type == 'units':
        recs = DBSession.query(Units).all()
    elif cv_type == 'timeunits':
        recs = DBSession.query(TimeUnits).all()
    elif cv_type == 'censorcodes':
        recs = DBSession.query(cvCensorCode).all()
    elif cv_type == 'datatypes':
        recs = DBSession.query(cvDataType).all()  
    elif cv_type == 'generalcategories':
        recs = DBSession.query(cvGeneralCategory).all() 
    elif cv_type == 'parametersources':
        recs = DBSession.query(cvParameterSource).all()
        output = [{'name': r.name, 'description': r.description, 'id': r.uuid} for r in recs]
    elif cv_type == 'qualitycontrollevels':
        recs = DBSession.query(cvQualityControlLevel).all()  
        output = [{'code': r.code, 'definition': r.definition, 'id': r.uuid} for r in recs]
    elif cv_type == 'samplemediums':
        recs = DBSession.query(cvSampleMedium).all() 
    elif cv_type == 'sampletypes':
        recs = DBSession.query(cvSampleType).all() 
    elif cv_type == 'speciation':
        recs = DBSession.query(cvSpeciation).all()
    elif cv_type == 'valuetypes':
        recs = DBSession.query(cvValueType).all()
    elif cv_type == 'variablenames':
        recs = DBSession.query(cvVariableName).all()
    elif cv_type == 'parameters':
        #TODO: add parameters
        output = []
    else:
        output = []

    if recs and cv_type in ['censorcodes', 'datatypes', 'generalcategories', 'samplemediums', 'sampletypes', 'speciation', 'valuetypes', 'variablenames']:
        output = [{'term': r.term, 'description': r.description, 'id': r.uuid} for r in recs]
    if recs and cv_type in ['units', 'timeunits']:
        output = [{'name': r.name, 'code': r.code, 'abbr': r.abbreviation, 'id': r.uuid} for r in recs]
        
    return output

@view_config(route_name='vocab', renderer='json')
def show(request):
    '''
    return a single cv item

    i can't think of a reason for this, but it's here anyway
    
    /apps/{app}/vocabs/{type}/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}
    '''

    cv = request.matchdict['type']
    uuid = request.matchdict['id']
    ext = request.matchdict['ext']

    #TODO: implement other formats? why? what?
    ext = 'json'

    if cv_type == 'units':
        recs = DBSession.query(Units).filter(Units.uuid==uuid).first()
    elif cv_type == 'timeunits':
        recs = DBSession.query(TimeUnits).filter(TimeUnits.uuid==uuid).first()
    elif cv_type == 'censorcodes':
        recs = DBSession.query(cvCensorCode).filter(cvCensorCode.uuid==uuid).first()
    elif cv_type == 'datatypes':
        recs = DBSession.query(cvDataType).filter(cvDataType.uuid==uuid).first()
    elif cv_type == 'generalcategories':
        recs = DBSession.query(cvGeneralCategory).filter(cvGeneralCategory.uuid==uuid).first() 
    elif cv_type == 'parametersources':
        recs = DBSession.query(cvParameterSource).filter(cvParameterSource.uuid==uuid).first()
        output = [{'name': r.name, 'description': r.description, 'id': r.uuid} for r in recs]
    elif cv_type == 'qualitycontrollevels':
        recs = DBSession.query(cvQualityControlLevel).filter(cvQualityControlLevel.uuid==uuid).first()
        output = [{'code': r.code, 'definition': r.definition, 'explanation': r.explanation, 'id': r.uuid} for r in recs]
    elif cv_type == 'samplemediums':
        recs = DBSession.query(cvSampleMedium).filter(cvSampleMedium.uuid==uuid).first()
    elif cv_type == 'sampletypes':
        recs = DBSession.query(cvSampleType).filter(cvSampleType.uuid==uuid).first()
    elif cv_type == 'speciation':
        recs = DBSession.query(cvSpeciation).filter(cvSpeciation.uuid==uuid).first()
    elif cv_type == 'valuetypes':
        recs = DBSession.query(cvValueType).filter(cvValueType.uuid==uuid).first()
    elif cv_type == 'variablenames':
        recs = DBSession.query(cvVariableName).filter(cvVariableName.uuid==uuid).first()
    elif cv_type == 'parameters':
        #TODO: add parameters
        output = []
    else:
        output = []

    if recs and cv_type in ['censorcodes', 'datatypes', 'generalcategories', 'samplemediums', 'sampletypes', 'speciation', 'valuetypes', 'variablenames']:
        output = [{'term': r.term, 'description': r.description, 'id': r.uuid} for r in recs]
    if recs and cv_type in ['units', 'timeunits']:
        output = [{'name': r.name, 'code': r.code, 'abbr': r.abbreviation, 'id': r.uuid} for r in recs]
    
    return output

@view_config(route_name='add_vocab', request_method='POST')
def add_vocab(request):
    return Response('')
