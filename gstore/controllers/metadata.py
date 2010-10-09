import logging

from pylons import config, request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect

from gstore.lib.base import BaseController, render
from gstore.model.cached import load_dataset
from lxml import etree

log = logging.getLogger(__name__)

class MetadataController(BaseController):
    """REST Controller styled on the Atom Publishing Protocol
    This is routed as a child resource of datasets. This means
    dataset_id must be passed on pertinent actions.
    
    """
    
    
    def index(self, format='html'):
        """GET /metadata: All items in the collection"""
        # url('metadata')

    def create(self):
        """POST /metadata: Create a new item"""
        # url('metadata')

    def new(self, dataset_id,  format='html'):
        """GET /metadata/new: Form to create a new item"""
        # url('new_metadata')
        return dataset_id

    def update(self, id):
        """PUT /metadata/id: Update an existing item"""
        # Forms posted to this method should contain a hidden field:
        #    <input type="hidden" name="_method" value="PUT" />
        # Or using helpers:
        #    h.form(url('metadata', id=ID),
        #           method='put')
        # url('metadata', id=ID)

    def delete(self, id):
        """DELETE /metadata/id: Delete an existing item"""
        # Forms posted to this method should contain a hidden field:
        #    <input type="hidden" name="_method" value="DELETE" />
        # Or using helpers:
        #    h.form(url('metadata', id=ID),
        #           method='delete')
        # url('metadata', id=ID)

    def show(self, dataset_id, id, format='html'):
        """GET /metadata/id: Show a specific item"""

        d = load_dataset(dataset_id)
        if d and d.metadata_xml:
            if format == 'txt':
                content_type = 'text'
                xslt_sheet = open(config['METADATA_XSLT_TXT'], 'r')
                try:
                    xslt_fgdc = etree.parse(xslt_sheet)
                    transform = etree.XSLT(xslt_fgdc)
                    metadata_doc = etree.XML(d.metadata_xml.encode('utf8'))
                    content = transform(metadata_doc)
                    content = etree.tostring(content, enconding = 'utf8')
                except:
                    content = d.metadata_xml

            elif format == 'html':
                content_type = 'text/html'
                xslt_sheet = open(config['METADATA_XSLT_HTML'], 'r')
                try:
                    xslt_fgdc = etree.parse(xslt_sheet)
                    transform = etree.XSLT(xslt_fgdc)
                    metadata_doc = etree.XML(d.metadata_xml.encode('utf8'))
                    content = transform(metadata_doc)
                    content = etree.tostring(content, encoding ='utf8')
                except:
                    content = d.metadata_xml
            else:
                content_type = 'text/xml'
                content = d.metadata_xml

            response.headers['Content-Type'] = '%s; charset=UTF-8' % content_type
            return content 


    def edit(self, id, format='html'):
        """GET /metadata/id/edit: Form to edit an existing item"""
        # url('edit_metadata', id=ID)
