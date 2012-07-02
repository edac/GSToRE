from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound

'''
REST services waterml 1.1

http://nmhis.unm.edu/RGET/REST/waterml_1_1.svc/siteinfo?location=RioGrandeET:ALF
http://nmhis.unm.edu/RGET/REST/waterml_1_1.svc/variables?variable=RioGrandeET:Wind_Speed_Ave
http://nmhis.unm.edu/RGET/REST/waterml_1_1.svc/datavalues?location=RioGrandeET:ALF&variable=RioGrandeET:svpma&startDate=2008-11-08T07:00:00Z&endDate=2008-11-11T07:00:00Z
'''
@view_config(route_name='hydroserver', match_param=('method=siteinfo', 'version=1_1, app=hydroserver'))
def sites(request):
    #site
    site = request.params.get('location') if 'location' in request.params else ''
    if not site:
        return HTTPNotFound('No site information')

    #split site into network:site code
    network = site.split(':')[0]
    site = site.split(':')[1]

    return Response('hydroserver sites')

@view_config(route_name='hydroserver', match_param=('method=variables', 'version=1_1', 'app=hydroserver'))
def variables(request):
    #variable
    variable = request.params.get('variable') if 'variable' in request.params else ''
    if not variable:
        return HTTPNotFound('No variable information')

    #split varaible into network:variable code
    network = variable.split(':')[0]
    variable = variable.split(':')[1]

    return Response('hydroserver variables')

@view_config(route_name='hydroserver', match_param=('method=datavalues', 'version=1_1', 'app=hydroserver'))
def datavalues(request):
    #check for valid utc datetime
    startDate = request.params.get('startDate') if 'startDate' in request.params else ''
    endDate = request.params.get('endDate') if 'endDate' in request.params else ''

    #site
    site = request.params.get('location') if 'location' in request.params else ''
    if not site:
        return HTTPNotFound('No site information')

    #split site into network:site code
    network = site.split(':')[0]
    site = site.split(':')[1]

    #variable
    variable = request.params.get('variable') if 'variable' in request.params else ''
    if not variable:
        return HTTPNotFound('No variable information')

    #split varaible into network:variable code
    network = variable.split(':')[0]
    variable = variable.split(':')[1]
    return Response('hydroserver values')

'''
REST services waterml 2

http://nmhis.unm.edu/RGET/REST/waterml_2.svc/featureOfInterest?location=RioGrandeET:ALF
http://nmhis.unm.edu/RGET/REST/waterml_2.svc/observedProperty?variable=RioGrandeET:Wind_Speed_Ave
http://nmhis.unm.edu/RGET/REST/waterml_2.svc/values?location=RioGrandeET:ALF&variable=RioGrandeET:RH_Min&startDate=2008-11-08T07:00:00Z&endDate=2008-11-11T07:00:00Z
'''
@view_config(route_name='hydroserver', match_param=('method=featureOfInterest', 'version=2', 'app=hydroserver'))
def sitesV2(request):
    #site
    site = request.params.get('location') if 'location' in request.params else ''
    if not site:
        return HTTPNotFound('No site information')

    #split site into network:site code
    network = site.split(':')[0]
    site = site.split(':')[1]

    return Response('hydroserver sites')

@view_config(route_name='hydroserver', match_param=('method=observedProperty', 'version=2', 'app=hydroserver'))
def variablesV2(request):
    #variable
    variable = request.params.get('variable') if 'variable' in request.params else ''
    if not variable:
        return HTTPNotFound('No variable information')

    #split varaible into network:variable code
    network = variable.split(':')[0]
    variable = variable.split(':')[1]

    return Response('hydroserver variables')

@view_config(route_name='hydroserver', match_param=('method=values', 'version=2', 'app=hydroserver'))
def datavaluesV2(request):

    #check for valid utc datetime
    startDate = request.params.get('startDate') if 'startDate' in request.params else ''
    endDate = request.params.get('endDate') if 'endDate' in request.params else ''

    #site
    site = request.params.get('location') if 'location' in request.params else ''
    if not site:
        return HTTPNotFound('No site information')

    #split site into network:site code
    network = site.split(':')[0]
    site = site.split(':')[1]

    #variable
    variable = request.params.get('variable') if 'variable' in request.params else ''
    if not variable:
        return HTTPNotFound('No variable information')

    #split varaible into network:variable code
    network = variable.split(':')[0]
    variable = variable.split(':')[1]
    
    return Response('hydroserver values')

#TODO: capabilities services, getsites, etc, for lookups


