"""Routes configuration

The more specific and detailed routes should be defined first so they
may take precedent over the more generic routes. For more information
refer to the routes manual at http://routes.groovie.org/docs/
"""
from routes import Mapper

def make_map(config):
    """Create, configure and return the routes Mapper"""
    map = Mapper(directory=config['pylons.paths']['controllers'],
                 always_scan=config['debug'])
    map.minimization = False
    map.explicit = False

    # The ErrorController route (handles 404/500 error pages); it should
    # likely stay at the top, ensuring it can always be resolved
    map.connect('/error/{action}', controller='error')
    map.connect('/error/{action}/{id}', controller='error')

    # CUSTOM ROUTES HERE

    # Static data
    map.connect('/data/{what}/{category}', controller='data', action='search',
        conditions=dict(method=['POST']))
    map.connect('/data/{what}', controller='data', action='search', category = None,
        conditions=dict(method=['POST']))

    # Dataset resource
    map.resource('app', 'apps')
   
    map.resource('dataset', 'datasets', collection={'footprints': 'GET'}, 
        parent_resource = dict(member_name = 'app', collection_name='apps'))

    # Dataset-Metadata resource
    map.resource('metadata', 'metadata',
        path_prefix = '/apps/:app_id/datasets/:dataset_id',
        name_prefix = 'metadata_')

    # list all metadata records
    map.connect('/apps/{app_id}/metadata.{format}', controller = 'metadata', action = 'index')

    # synch all metadata records in the webdav folder
    map.connect('/metadata/synch_webdav', controller = 'metadata', action = 'metadata_synch_webdav',
        conditions=dict(method=['POST'])
    )   
    
    map.connect('/apps/{app_id}/features.{format}', controller = 'features_bundle', action = 'index', dataset_id = None) 
    # Dataset-vector features resource
    map.resource('feature', 'features',
        controller = 'features_bundle',
        path_prefix = '/apps/:app_id/datasets/:dataset_id',
        name_prefix = 'feature_')


    # Dataset categories
    map.connect('datasets/categories', controller='datasets', action='categories')

    # Vector fields schema if any
    map.connect('/apps/{app_id}/datasets/{id}/schema.{format}', 
        controller = 'datasets', action = 'schema', 
        conditions=dict(method=['GET']))

    # Dataset-services
    map.connect('/apps/{app_id}/datasets/{id}/services/{service_type}/{service}',
        controller='datasets', action='services',
        conditions=dict(method=['GET']))

    # printing services are post only
    map.connect('/apps/{app_id}/datasets/{id}/services/print/{service}',
        controller='datasets', action='services',
        conditions=dict(method=['POST']))

    # Dataset-mapper
    map.connect('/apps/{app_id}/datasets/{id}/mapper', controller='datasets', action='mapper',
        conditions=dict(method=['GET']))

    # Search controller
    map.connect('/apps/{app_id}/search/datasets.json', controller='search', action='search_datasets')
    map.connect('/apps/{app_id}/search/{resource}.json', controller='search', action='index')

    # Root controller
    map.connect('/', controller='index', action='index')

    return map
