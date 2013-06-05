from pyramid.config import Configurator
from sqlalchemy import engine_from_config

from pyramid.response import Response
from pyramid.view import notfound_view_config
from pyramid.httpexceptions import HTTPNotFound, HTTPNotImplemented

from .models import DBSession

'''
to run like paster

cd /opt/modwsgi/gstore_v3_env/
bin/pshell gstore_v3/development.ini

'''

#TODO: set up some nice 404 pages
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
applist = any_of('app', 'rgis', 'epscor', 'hydroserver', 'epht', 'elseweb', 'tristate')
#removed dataone, see route changes below

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
servicelist = any_service('service', 'wms', 'wfs', 'wcs', 'map', 'wms_tiles')

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

#TODO: check for version query param as custom predicate 
#def any_version():
#versionlist = any_version('version', 2, 3)

'''
all the routing
'''
def main(global_config, **settings):
    """ This function returns a Pyramid WSGI application.
    """
    
    config = Configurator(settings=settings)    
    config.scan('.models')
    engine = engine_from_config(settings, 'sqlalchemy.')

    #add the dataone logging engine (in its own postgres connection)
    dataone_engine = engine_from_config(settings, 'dataone.')
    models.initialize_sql([engine, dataone_engine])
    #models.initialize_sql(engine)

    config.add_static_view('static', 'static', cache_max_age=3600)
    config.add_static_view(name='xslts', path='gstore_v3:../resources/xslts')
    config.add_route('home', '/')    

#    #TEST ROUTE
#    config.add_route('test', '/test/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}')
#    config.add_route('test_url', '/test/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/{basename}.{type}.{ext}')
#    config.add_route('test_wcs', '/test/services/wcs')
#    config.add_route('test_fmt', '/test/services/{fmt}')
#    config.add_route('test_fidsearch', '/test/mongo')
#    config.add_route('test_insert', '/test/insert')
#    config.add_route('test_bulkinsert', '/test/chunk/{id}/{amount}')

    #config.add_route('test_urlencoding', '/test/encoder/{test}')
    
    #to the attributes
    config.add_route('attributes', '/apps/{app}/attributes/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('dataset_attributes', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/attributes.{ext}', custom_predicates=(applist,))
    config.add_route('add_attributes', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/attributes', custom_predicates=(applist,)) #POST

#to the features
    #config.add_route('features', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/features.{ext}')
    config.add_route('add_features', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/features', custom_predicates=(applist,)) #POST
    config.add_route('add_feature_attributes', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/featureattributes', custom_predicates=(applist,)) #POST
    config.add_route('update_feature', '/apps/{app}/features/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}', custom_predicates=(applist,)) #PUT
    config.add_route('feature', '/apps/{app}/features/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('features', '/apps/{app}/features.{ext}', custom_predicates=(applist,))

#to the metadata
    #the unresolved (iso) metadata
    config.add_route('metadata_fgdc', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/metadata/fgdc.{ext}', custom_predicates=(applist,))
    config.add_route('metadata', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/metadata/{standard}.{ext}', custom_predicates=(applist,))
    #the resolved, no xlink version. maybe this only applies to the iso? or return transform of whatever but it will be the same as the file from above
    config.add_route('metadata_resolved', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/metadata/resolved/{standard}.{ext}', custom_predicates=(applist,standardslist,))
    
    #route to maintain the v2 urls
    config.add_route('metadata_v2', '/apps/{app}/datasets/{id:\d+}/metadata/{did:\d+}.{ext}', custom_predicates=(applist,))
    #config.add_route('add_metadata', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/metadata', custom_predicates=(applist,))
    #TODO: have a think about this route and what it needs to do (i.e. probably not necessary)
    #config.add_route('get_metadata', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/metadata/{mid:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('xlink_metadata', '/apps/{app}/metadata/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))

#to the search
    config.add_route('search_datasets', '/apps/{app}/search/datasets.{ext}', custom_predicates=(applist,))
    config.add_route('search_features', '/apps/{app}/search/features.json', custom_predicates=(applist,))
    config.add_route('search_geolookups', '/apps/{app}/search/{geolookup}.json', custom_predicates=(applist,geolookuplist,))
    config.add_route('search_categories', '/apps/{app}/search/categories.json', custom_predicates=(applist,))

#to ogc services (datasets | tile indexes)
    #for the base layers
    config.add_route('base_services', '/apps/{app}/datasets/base/services/{service_type}/{service}', custom_predicates=(applist,servicelist,))

    config.add_route('services', '/apps/{app}/{type}/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/services/{service_type}/{service}', custom_predicates=(applist,servicelist,))

    

#TODO: deprecate this (move functionality to the interfaces where it belongs)
#to the mapper
    config.add_route('mapper', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/mapper', custom_predicates=(applist,))

#to the deprecated schema call
    config.add_route('schema', '/apps/{app}/datasets/{id:\d+}/schema.{ext}', custom_predicates=(applist,))

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
#    config.add_route('dataone_object', '/apps/{app}/v1/object/{pid}', custom_predicates=(applist,))
#    config.add_route('dataone_object_slash', '/apps/{app}/v1/object/{pid}/', custom_predicates=(applist,))
    config.add_route('dataone_meta', '/dataone/v1/meta/{pid:.*}')
    config.add_route('dataone_meta_slash', '/dataone/v1/meta/{pid:.*}/')
    config.add_route('dataone_checksum', '/dataone/v1/checksum/{pid:.*}')
    config.add_route('dataone_checksum_slash', '/dataone/v1/checksum/{pid:.*}/')
    config.add_route('dataone_replica', '/dataone/v1/replica/{pid:.*}')
    config.add_route('dataone_replica_slash', '/dataone/v1/replica/{pid:.*}/')
    
    config.add_route('dataone_error', '/dataone/v1/error') 
    config.add_route('dataone_error_slash', '/dataone/v1/error/') 

    #maintenance routes
    config.add_route('dataone_addcore', '/dataone/v1/core/add')
    config.add_route('dataone_addmetadata', '/dataone/v1/metadata/add')
    config.add_route('dataone_addvector', '/dataone/v1/vector/add')
    config.add_route('dataone_addsource', '/dataone/v1/source/add')
    config.add_route('dataone_addpackage', '/dataone/v1/package/add')
    config.add_route('dataone_addobsolete', '/dataone/v1/obsolete/add')
    config.add_route('dataone_updatepackage', '/dataone/v1/package/{pid:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/update')

#to the dataset
    #use the integer dataset_id or the uuid
    config.add_route('dataset', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/{basename}.{type}.{ext}', custom_predicates=(applist, typelist,))
    #removed: moving this type of functionality to the interface side of things (i.e. use the services.json request)
    #config.add_route('html_dataset', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/{basename}.html', custom_predicates=(applist,))
    config.add_route('zip_dataset', '/apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/{basename}.{type}.{ext}.zip', custom_predicates=(applist, typelist,))
    config.add_route('add_dataset', '/apps/{app}/datasets', custom_predicates=(applist,)) #POST
    config.add_route('update_dataset', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}', custom_predicates=(applist,)) #PUT
    config.add_route('dataset_services', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/services.{ext}', custom_predicates=(applist,))

    config.add_route('dataset_streaming', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/dataset.{ext}', custom_predicates=(applist,))
    config.add_route('dataset_statistics', '/apps/{app}/datasets/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/statistics.{ext}', custom_predicates=(applist,))

#to hydroserver
    #services (MODIFY FOR WSDL VS REST)
    config.add_route('hydroserver', '/apps/{app}/{odm}/REST/waterml_{version}.svc/{method}', custom_predicates=(applist,))
    #capabilities (if we need it)

#to tile index
    config.add_route('tileindexes', '/apps/{app}/tileindexes/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('add_tileindex', '/apps/{app}/tileindexes', custom_predicates=(applist,))
    config.add_route('update_tileindex', '/apps/{app}/tileindexes/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}', custom_predicates=(applist,))

#to collections
    config.add_route('collections', '/apps/{app}/collections/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('add_collection', '/apps/{app}/collections', custom_predicates=(applist,))
    config.add_route('update_collection', '/apps/{app}/collections/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}', custom_predicates=(applist,))

#to controlled vocabs
    config.add_route('available_vocabs', '/apps/{app}/vocabs', custom_predicates=(applist,))
    config.add_route('vocabs', '/apps/{app}/vocabs/{type}.{ext}', custom_predicates=(applist,))
    config.add_route('vocab', '/apps/{app}/vocabs/{type}/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}', custom_predicates=(applist,))
    config.add_route('add_vocab', '/apps/{app}/vocabs/{type}', custom_predicates=(applist,))
    
    config.scan('gstore_v3')
    #config.scan()
    return config.make_wsgi_app()

