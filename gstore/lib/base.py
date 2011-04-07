"""The base Controller API

Provides the BaseController class for subclassing.
"""
from pylons.controllers import WSGIController
from pylons.templating import render_mako as render

from pylons import app_globals 

from gstore import model


class BaseController(WSGIController):
    streaming_mode = False
    def __before__(self):
        model.meta.Session.close()

    def __call__(self, environ, start_response):
        """Invoke the Controller"""
        # WSGIController.__call__ dispatches to the Controller method
        # the request is routed to. This routing information is
        # available in environ['pylons.routes_dict']
        try:
            return WSGIController.__call__(self, environ, start_response)
        finally:
            if not self.streaming_mode:
                model.meta.Session.close()
