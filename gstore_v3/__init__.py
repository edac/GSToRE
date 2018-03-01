from pyramid.config import Configurator
from sqlalchemy import engine_from_config
from sqlalchemy.pool import NullPool

from pyramid.response import Response
from pyramid.view import notfound_view_config
from pyramid.httpexceptions import HTTPNotFound, HTTPNotImplemented


from .models import DBSession

#for the cleanup/locking problem
from pyramid.events import subscriber
from pyramid.events import NewRequest


#custom error methods
@notfound_view_config(request_method='GET')
def notfound_get(request):
	return HTTPNotFound('Invalid GET request.')

#custom notfound method
@notfound_view_config(request_method='POST')
def notfound_post(request):
	return HTTPNotFound('Invalid POST request.')

'''
custom predicates for some quick url validation before hitting the views
'''

#add a custom predicate to limit routes to just the apps listed
def any_of(segment_name, *allowed):
    def predicate(info, request):
        if info['match'][segment_name] in allowed:
            return True
    return predicate
applist = any_of('app', 'gstore')

#check for the dataset type (original vs. derived) for downloads
def any_type(segment_name, *allowed):
    def predicate(info, request):
        if info['match'][segment_name] in allowed:
            return True
    return predicate
typelist = any_type('type', 'original', 'derived')

#check for valid service requests
def any_service(segment_name, *allowed):
    def predicate(info, request):
        if info['match'][segment_name] in allowed:
            return True
    return predicate
servicelist = any_service('service', 'wms', 'wfs', 'wcs', 'map', 'wms_tiles', 'clip')

#check for valid metadata standards
def any_standard(segment_name, *allowed):
    def predicate(info, request):
        if info['match'][segment_name] in allowed:
            return True
    return predicate
standardslist = any_standard('standard', 'fgdc', 'iso', 'dc')

#check for the geolookup type 
def any_geolookup(segment_name, *allowed):
    def predicate(info, request):
        if info['match'][segment_name] in allowed:
            return True
    return predicate
geolookuplist = any_type('geolookup', 'nm_counties', 'nm_quads', 'nm_gnis')


'''
we shouldn't need this! we have pyramid_tm!
but apparently we do. otherwise we just generate locks every which way
until the system dies

and this apparently does jack for the streaming (app_iter) 
responses (OR there is some issue with timing, esp with pgpool)
'''
def cleanup_callback(request):
    DBSession.close()

@subscriber(NewRequest)
def add_cleanup_callback(event):
    event.request.add_finished_callback(cleanup_callback)    


'''
all the routing
'''
def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """

    config = Configurator(settings=settings)
    config.include('pyramid_chameleon')
    config.include('pyramid_mako')
    config.scan('.models')
    engine = engine_from_config(settings, 'sqlalchemy.', pool_reset_on_return='commit', poolclass=NullPool)

    #add the dataone logging engine (in its own postgres connection)
    dataone_engine = engine_from_config(settings, 'dataone.')
    models.initialize_sql([engine, dataone_engine])

    #config.add_static_view('static', 'static', cache_max_age=3600)
    config.add_static_view(name='xslts', path='gstore_v3:../resources/xslts')
    config.add_static_view(name='docs', path='gstore_v3:../resources/docs')


    config.add_route('home', '/')    


# graph route
    config.add_route('analyticsdata', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/analytics.{ext}')

# authors route : lists all authors in an app
    config.add_route('authors','/apps/{app}/authors.{ext}')
    config.add_route('datasetsreport','/apps/{app}/datasetsreport.{ext}')

#app routes (stats, etc)
    config.add_route('app_stats', 'apps/{app}/statistics/{stat}.{ext}', custom_predicates=(applist,))

#to the attributes
    config.add_route('attributes', '/apps/{app}/attributes/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('dataset_attributes', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/attributes.{ext}', custom_predicates=(applist,))
    config.add_route('add_attributes', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/attributes', custom_predicates=(applist,)) #POST

#to the features
    config.add_route('add_features', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/features', custom_predicates=(applist,)) #POST
    config.add_route('add_feature_attributes', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/featureattributes', custom_predicates=(applist,)) #POST
    config.add_route('update_feature', '/apps/{app}/features/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}', custom_predicates=(applist,)) #PUT
    config.add_route('feature', '/apps/{app}/features/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('features', '/apps/{app}/features.{ext}', custom_predicates=(applist,))

#to the metadata

    #config.add_route('metadata_sitemap', '/apps/{app}/sitemap.html', custom_predicates=(applist,))

    config.add_route('metadata', '/apps/{app}/{datatype}/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/metadata/{standard}.{ext}', custom_predicates=(applist,))

#to the provenance
    '''
    trace: generated prov (only supporting rdf now)
    base: the source file for generating the rdf (in our case, the source ds xml)
    '''
    config.add_route('provenance_trace', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/prov/{ontology}.{ext}', custom_predicates=(applist,))
    config.add_route('provenance_base', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/prov/{ontology}/{standard}.{ext}', custom_predicates=(applist,))    

#to the search
    config.add_route('search_categories', '/apps/{app}/search/categories.json', custom_predicates=(applist,))
    config.add_route('search_features', '/apps/{app}/search/features.json', custom_predicates=(applist,))

    config.add_route('searches', '/apps/{app}/search/{doctypes}.{ext}', custom_predicates=(applist,))

    config.add_route('search_within_collection', '/apps/{app}/search/collection/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/datasets.{ext}', custom_predicates=(applist,))

    config.add_route('search_facets', '/apps/{app}/search/facets/{facet}.{ext}', custom_predicates=(applist,)) 


#to ogc services (datasets | tile indexes)
    #for the base layers
    config.add_route('base_services', '/apps/{app}/datasets/base/services/{service_type}/{service}', custom_predicates=(applist,servicelist,))

    config.add_route('services', '/apps/{app}/{type}/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/services/{service_type}/{service}', custom_predicates=(applist,servicelist,))




#to the mapper
    config.add_route('mapper', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/mapper', custom_predicates=(applist,))

#to the deprecated schema call
    config.add_route('schema', '/apps/{app}/datasets/{id:\d+}/schema.{ext}', custom_predicates=(applist,))

#to digitalcommons
    config.add_route('digitalcommons', '/apps/{app}/dc/{doctypes}.{ext}', custom_predicates=(applist,))

#to dataone
    config.add_route('dataone_noversion', '/dataone')
    config.add_route('dataone_noversion_slash', '/dataone/')
    config.add_route('dataone', '/dataone/v1')
    config.add_route('dataone_slash', '/dataone/v1/')
    config.add_route('dataone_node', '/dataone/v1/node')
    config.add_route('dataone_node_slash', '/dataone/v1/node/')
    config.add_route('dataone_ping', '/dataone/v1/monitor/ping')
    config.add_route('dataone_ping_slash', '/dataone/v1/monitor/ping/')
    config.add_route('dataone_log', '/dataone/v1/log')
    config.add_route('dataone_log_slash', '/dataone/v1/log/')
    config.add_route('dataone_search', '/dataone/v1/object')
    config.add_route('dataone_search_slash', '/dataone/v1/object/')
    config.add_route('dataone_object', '/dataone/v1/object/{pid:.*}')
    config.add_route('dataone_object_slash', '/dataone/v1/object/{pid:.*}/')
    config.add_route('dataone_meta', '/dataone/v1/meta/{pid:.*}')
    config.add_route('dataone_meta_slash', '/dataone/v1/meta/{pid:.*}/')
    config.add_route('dataone_checksum', '/dataone/v1/checksum/{pid:.*}')
    config.add_route('dataone_checksum_slash', '/dataone/v1/checksum/{pid:.*}/')
    config.add_route('dataone_replica', '/dataone/v1/replica/{pid:.*}')
    config.add_route('dataone_replica_slash', '/dataone/v1/replica/{pid:.*}/')
    config.add_route('dataone_error', '/dataone/v1/error') 
    config.add_route('dataone_error_slash', '/dataone/v1/error/') 

    #maintenance routes
    config.add_route('dataone_add', '/dataone/v1/{object}/add')
    config.add_route('dataone_update', '/dataone/v1/{object}/update')

#to the dataset
    #use the integer dataset_id or the uuid
    config.add_route('dataset', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/{basename}.{type}.{ext}', custom_predicates=(applist, typelist,))
    config.add_route('add_dataset', '/apps/{app}/datasets', custom_predicates=(applist,)) #POST
    config.add_route('update_dataset', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}', custom_predicates=(applist,)) #PUT
    config.add_route('update_dataset_index', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/index', custom_predicates=(applist,))
    config.add_route('dataset_services', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/services.{ext}', custom_predicates=(applist,))

    config.add_route('dataset_streaming', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/dataset.{ext}', custom_predicates=(applist,))
    config.add_route('dataset_statistics', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/statistics.{ext}', custom_predicates=(applist,))

    #elasticsearch builder
    config.add_route('dataset_indexer', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/index.json', custom_predicates=(applist,))

#to repository services
    config.add_route('repositories', '/apps/{app}/repositories.json', custom_predicates=(applist,))
    config.add_route('repository', '/apps/{app}/repository/{repo}.json', custom_predicates=(applist,))
    config.add_route('search_repo', '/apps/{app}/repository/{repo}/{doctypes}/{standard}.{ext}', custom_predicates=(applist,))

#to hydroserver
    config.add_route('hydroserver', '/apps/{app}/{odm}/REST/waterml_{version}.svc/{method}', custom_predicates=(applist,))

#to tile index
    config.add_route('tileindexes', '/apps/{app}/tileindexes/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('add_tileindex', '/apps/{app}/tileindexes', custom_predicates=(applist,))
    config.add_route('update_tileindex', '/apps/{app}/tileindexes/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}', custom_predicates=(applist,))

#to collections
    config.add_route('collections', '/apps/{app}/collections/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/services.{ext}', custom_predicates=(applist,))
    config.add_route('collection_footprint', '/apps/{app}/collections/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/footprint.{ext}', custom_predicates=(applist,))
    config.add_route('add_collection', '/apps/{app}/collections', custom_predicates=(applist,))
    config.add_route('update_collection', '/apps/{app}/collections/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}', custom_predicates=(applist,))

#to controlled vocabs
    config.add_route('available_vocabs', '/apps/{app}/vocabs', custom_predicates=(applist,))
    config.add_route('vocabs', '/apps/{app}/vocabs/{type}.{ext}', custom_predicates=(applist,))
    config.add_route('vocab', '/apps/{app}/vocabs/{type}/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('add_vocab', '/apps/{app}/vocabs/{type}', custom_predicates=(applist,))

    config.scan('gstore_v3')
    return config.make_wsgi_app()

