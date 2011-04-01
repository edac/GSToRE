import logging
import os, sys, time
import codecs
import commands

from pylons import config, request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect

from gstore.lib.base import BaseController, render
from lxml import etree

from gstore.model import meta, Dataset
from gstore.model.caching_query import FromCache

log = logging.getLogger(__name__)

class MetadataController(BaseController):
    """REST Controller styled on the Atom Publishing Protocol
    This is routed as a child resource of datasets. This means
    dataset_id must be passed on pertinent actions.
    
    """
    
    def index(self, app_id, format='html'):
        """GET /metadata: All items in the collection"""
        l = []
        for dataset in meta.Session.query(Dataset.id, Dataset.has_metadata).\
            filter(Dataset.has_metadata == True).options(
            FromCache('short_term', 'bydatasetid')):
            l.append("<a href='http://129.24.63.99:9999/apps/rgis/datasets/%(id)s/metadata/%(id)s.xml'>%(id)s.xml</a>" % { 'id': dataset.id})

        return '<br />\n'.join(l)

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

        d = meta.Session.query(Dataset).options(FromCache('short_term', 'bydatasetid')).get(dataset_id)
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

    def metadata_synch_webdav(self, transform_to_iso = False):
        token = request.params.get('secret_token',None)
        app_id = request.params.get('app_id', None)
        if token != '1239hj23420349u10293i1293':
            return abort(403)
        
    
        folder = config.get('METADATA_WEBDAV_ROOT')
        l = []
        for dataset in meta.Session.query(Dataset).\
            filter(Dataset.has_metadata == True).yield_per(10):
            filepath = os.path.join(folder, '%s.xml' % dataset.id)
            if True or not os.path.isfile(filepath):
                metadata_file = codecs.open(filepath, encoding = 'utf-8', mode ='w')
                metadata_file.write(dataset.metadata_xml)
                metadata_file.close()
                os.utime(filepath, (int(time.mktime(time.localtime())), int(time.mktime(dataset.dateadded.timetuple()))))

                for app in dataset.apps_cache:
                    if app_id is not None and app != app_id:
                        continue
                    metadata_fgdc_filepath = os.path.join(os.path.join(folder, app), 'fgdc')
                    metadata_fgdc_filepath = os.path.join(metadata_fgdc_filepath, '%s.xml' % dataset.id)
                    metadata_fgdc_file = codecs.open(metadata_fgdc_filepath, encoding = 'utf-8', mode ='w')
                    # Start FGDC metadata trasnformation pipeline
                    metadata_xml =  dataset.metadata_xml
                    # step 1. Uppercase <lineage>
                    #metadata_xml = metadata_xml.replace('<lineage>','<Lineage>')
                    #metadata_xml = metadata_xml.replace('</lineage>','</Lineage>') 
                    # step 2. Collapse times to date range
                    # pass
                    onlinks = ['http://rgis.unm.edu/gstore/datasets/%s/metadata/%s.xml' % (dataset.id, dataset.id) ]

                    for fmt in Dataset.get_format(dataset):
                        onlinks.append("http://gstore.unm.edu/apps/%s/datasets/%s.%s" % (app, dataset.id, fmt))
                    for svc in Dataset.get_services(dataset):
                        if svc == 'wms':
                            onlinks.append("http://gstore.unm.edu/apps/%s/datasets/%s/services/ogc/wms?VERSION=1.1.1&amp;SERVICE=WMS&amp;REQUEST=GetCapabilities" % (app, dataset.id))
                        if svc == 'wfs':
                            onlinks.append("http://gstore.unm.edu/apps/%s/datasets/%s/services/ogc/wfs?VERSION=1.0.0&amp;SERVICE=WFS&amp;REQUEST=GetCapabilities" % (app, dataset.id))
                        if svc == 'wcs':
                            onlinks.append("http://gstore.unm.edu/apps/%s/datasets/%s/services/ogc/wcs?SERVICE=WCS&amp;REQUEST=GetCapabilities" % (app, dataset.id))
                    if dataset.taxonomy == 'vector':
                        geoform = 'vector digital data'
                    elif dataset.taxonomy == 'geoimage':
                        geoform = 'raster digital data'
                    else:
                        geoform = 'document'
                    onlinks = '\n'.join(['<onlink>%s</onlink>' % onlink for onlink in onlinks])
                    gstore_citation = """
                            <citeinfo>
                                <origin>University of New Mexico</origin>
                                <title>%(description)s</title>
                                <pubdate>%(dateadded)s</pubdate>
                                <pubinfo>
                                    <pubplace>Albuquerque, NM</pubplace>
                                    <publish>Earth Data Analysis Center</publish>
                                </pubinfo>
                                <geoform>%(geoform)s</geoform>
                                %(onlinks)s 
                            </citeinfo>
                    """ % {
                        'onlinks': onlinks, 
                        'geoform': geoform,
                        'description': dataset.description,
                        'dateadded': dataset.dateadded.strftime('%Y%m%d')
                    }
                    metadata_xml = metadata_xml.replace("<citeinfo>","<citeinfo>%s" % onlinks, 1) 
                    
                    metadata_fgdc_file.write(metadata_xml)
                    metadata_fgdc_file.close()
                    os.utime(metadata_fgdc_filepath, (int(time.mktime(time.localtime())), int(time.mktime(dataset.dateadded.timetuple()))))

                if transform_to_iso:
                    xslt_fgdc2iso = config['METADATA_XSLT_FGDC2ISO']
                    # /usr/bin/saxonb-xslt -s:/tmp/106448.xml -xsl:/saxonsamples/csdgm2iso19115-2.xslt
                    cmd = "/usr/bin/saxonb-xslt -s:%s -xsl:%s" % (filepath, xslt_fgdc2iso)
                    (res, metadata_xml_iso) = commands.getstatusoutput(cmd)
                    for app in dataset.apps_cache:
                        metadata_iso_filepath = os.path.join(os.path.join(folder, app), 'iso')
                        metadata_iso_filepath = os.path.join(metadata_iso_filepath, '%s.xml' % dataset.id)
                        metadata_iso_file = codecs.open(metadata_iso_filepath, encoding ='utf-8', mode = 'w')
                        metadata_iso_file.write(metadata_xml_iso.decode('utf-8'))
                        metadata_iso_file.close()
                        os.utime(metadata_iso_filepath, (int(time.mktime(time.localtime())), int(time.mktime(dataset.dateadded.timetuple()))))
            else: 
                stats = os.stat(filepath)

    def edit(self, id, format='html'):
        """GET /metadata/id/edit: Form to edit an existing item"""
        # url('edit_metadata', id=ID)
