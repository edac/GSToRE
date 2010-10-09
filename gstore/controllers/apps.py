import logging

from pylons import request, response, session, tmpl_context as c, url
from pylons.controllers.util import abort, redirect

from gstore.lib.base import BaseController, render

log = logging.getLogger(__name__)

class AppsController(BaseController):
    """REST Controller styled on the Atom Publishing Protocol"""
    # To properly map this controller, ensure your config/routing.py
    # file has a resource setup:
    #     map.resource('app', 'apps')

    def index(self, format='html'):
        """GET /apps: All items in the collection"""
        # url('apps')

    def create(self):
        """POST /apps: Create a new item"""
        # url('apps')

    def new(self, format='html'):
        """GET /apps/new: Form to create a new item"""
        # url('new_app')

    def update(self, id):
        """PUT /apps/id: Update an existing item"""
        # Forms posted to this method should contain a hidden field:
        #    <input type="hidden" name="_method" value="PUT" />
        # Or using helpers:
        #    h.form(url('app', id=ID),
        #           method='put')
        # url('app', id=ID)

    def delete(self, id):
        """DELETE /apps/id: Delete an existing item"""
        # Forms posted to this method should contain a hidden field:
        #    <input type="hidden" name="_method" value="DELETE" />
        # Or using helpers:
        #    h.form(url('app', id=ID),
        #           method='delete')
        # url('app', id=ID)

    def show(self, id, format='html'):
        """GET /apps/id: Show a specific item"""
        # url('app', id=ID)

    def edit(self, id, format='html'):
        """GET /apps/id/edit: Form to edit an existing item"""
        # url('edit_app', id=ID)
