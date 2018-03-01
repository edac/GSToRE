from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPBadRequest

from ..models.statistics import *

#TODO: add aggregation options
@view_config(route_name='app_stats', match_param='stat=added')
def get_datasets_by_dateadded(request):
    '''
    return a blob of dataset counts by time period (month, count or something)

    ex: http://129.24.63.115/apps/rgis/statistics/added.csv
    '''

    app = request.matchdict['app']
    format = request.matchdict['ext']

    if format.lower() not in ['csv']:
        return HTTPNotFound()

    appstats = AppStatistics(app)
    stats = appstats.get_dataset_counts_by_dateadded(format)

    if stats:
        return Response(stats, content_type='text/csv')

    return HTTPBadRequest('Unable to generate statistics')
    
    

    

