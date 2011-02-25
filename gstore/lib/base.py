"""The base Controller API

Provides the BaseController class for subclassing.
"""
from pylons.controllers import WSGIController
from pylons.templating import render_mako as render

from pylons import app_globals 

from gstore import model


class BaseController(WSGIController):
    def __call__(self, environ, start_response):
        """Invoke the Controller"""
        # WSGIController.__call__ dispatches to the Controller method
        # the request is routed to. This routing information is
        # available in environ['pylons.routes_dict']
        try:
            return WSGIController.__call__(self, environ, start_response)
        finally:
            model.meta.Session.close()

    def load_dataset(self, dataset_id):
        @app_globals.cache.region('short_term','datasetsbyid')
        def fetch_dataset(dataset_id):
            dataset = model.meta.Session.query(model.Dataset).get(dataset_id)
            if dataset.taxonomy == 'vector':
                assert(len(dataset.attributes_ref) > 0)
            return dataset

        return fetch_dataset(dataset_id)

