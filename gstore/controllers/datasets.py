from __future__ import with_statement

import logging
import simplejson

from pylons import request, response, session, tmpl_context as c
from pylons.controllers.util import abort, redirect
from pylons import config
from pylons.templating import render_mako as render
from pylons.decorators import jsonify

from gstore.lib.base import BaseController

from gstore.model.caching_query import FromCache
from sqlalchemy.sql import func

from gstore.model import meta, Dataset
from gstore.model.tindices import VectorTileIndexDataset, RasterTileIndexDataset
from gstore.model.rasters import RasterDataset
from gstore.model.shapes import VectorDataset
from gstore.model.geoutils import bbox_to_polygon, transform_to, transform_bbox

from gstore.lib.ogc import OGC
from gstore.lib.tiles import wsgi_tilecache as tilecache

from urllib2 import urlparse
import simplejson
import zipfile, tempfile, os, re

import osgeo.osr as osr
import osgeo.ogr as ogr

import shutil


log = logging.getLogger(__name__)

SRID = int(config['SRID'])
MAP_EPSG = int(config['MAP_EPSG'])
FORMATS_PATH = config.get('FORMATS_PATH', '/tmp')

def ziplist(zipfilename, filelist):
    def clean_tempdir(tempdir, remove_dir = False):
        """
        Clean temporary files in the relative 'tempdir' directory.
        """
        # Ed 1/6/2011 jbrown:  No long necessarily a relative directory.  This
        # handles the fact that most things are stored in /tmp/

        #while '/' == tempdir[0]:
        #    tempdir = tempdir[1:]
        #clean_dir(tempdir, remove_dir)
        shutil.rmtree(tempdir)

    # filelist: [(path, filename, content = None)]
    tempdir = tempfile.mkdtemp()
    tempzipfile = zipfile.ZipFile(os.path.join(tempdir, zipfilename), mode='w', compression=zipfile.ZIP_STORED)

    for (path, filename, content) in filelist:
        if os.path.isfile(os.path.join(path,filename)) and not content:
            tempzipfile.write(os.path.join(path,filename), filename)
        elif content:
            tempzipfile.writestr(filename, content)
    tempzipfile.close()
    tempzipfile = open(os.path.join(tempdir, zipfilename),'r')
    zipfilecontents = tempzipfile.read()
    tempzipfile.close()
    clean_tempdir(tempdir, remove_dir = True)
    return zipfilecontents


class DatasetsController(BaseController):
    readonly = True
    def __init__(self):
        #self.protocol = RGISDatasetProtocol(meta.Session, DatasetFootprint, self.readonly)
        pass


    def index(self, app_id, format='html'):
        """GET /datasets: All items in the collection"""
        pass

    def create(self):
        """POST /datasets: Create a new item"""
        # url('datasets')

    def new(self, format='html'):
        """GET /apps/app_id:/datasets/new: Form to create a new item"""
        # url('new_dataset')

    def update(self, id):
        """PUT /apps/app_id:/datasets/id: Update an existing item"""
        # Forms posted to this method should contain a hidden field:
        #    <input type="hidden" name="_method" value="PUT" />
        # Or using helpers:
        #    h.form(url('dataset', id=ID),
        #           method='put')
        # url('dataset', id=ID)

    def delete(self, id):
        """DELETE /apps/app_id:/datasets/id: Delete an existing item"""
        # Forms posted to this method should contain a hidden field:
        #    <input type="hidden" name="_method" value="DELETE" />
        # Or using helpers:
        #    h.form(url('dataset', id=ID),
        #           method='delete')
        # url('dataset', id=ID)

    def show(self, app_id, id, format='html'):
        """GET /apps/app_id:/datasets/id: Show a specific item"""
        dataset = meta.Session.query(Dataset).\
                  options(FromCache('short_term', 'bydatasetid')).\
                  get(id)

        if format == 'kmz':
            format = 'kml'

        if format not in Dataset.get_formats(dataset):
            abort(404)

        if dataset.sources_ref:
            for src in dataset.sources_ref:
                if src.active:
                    break
            if format == 'zip' or dataset.taxonomy not in ['geoimage', 'vector']:
                url = src.location
                if urlparse.urlparse(url).scheme in ['file','']:
                    response.headers['Content-Type'] = 'application/x-zip-compressed'
                    with open(str(url), 'r') as f:
                        shutil.copyfileobj(f, response) 
                    return 
                else:
                    redirect(url)

        if format == 'xls':
            compressed = False
        else:
            compressed = True

        filename = dataset.get_filename(format, compressed = compressed)

        if dataset.taxonomy != 'vector':
            response.headers['Content-Type'] = 'application/x-zip-compressed'
            response.headers['Content-Disposition'] = str('attachment; filename=%s.zip' % dataset.get_filename(format))
            filelist = dataset.clip_zip(format)
            return ziplist(filename, filelist)
        else:
            response.headers['Content-Disposition'] = str('attachment; filename=%s' % filename)
            basepath = os.path.join(FORMATS_PATH, str(dataset.id))
            basepath = os.path.join(basepath, format)
            filename = str(os.path.join(basepath, filename))
            if not os.path.isfile(filename):
                vd = VectorDataset(dataset, meta.Session, config)
                vd.write_vector_format(format, FORMATS_PATH)
            tf = open(filename, 'r')
            contents = tf.read()
            tf.close()
            meta.Session.remove()
            return contents

        # url('dataset', id=ID)

    def edit(self, id, format='html'):
        """GET /apps/app_id/datasets/id/edit: Form to edit an existing item"""
        # url('edit_dataset', id=ID)


    # Vector schema URI
    def schema(self, app_id, id, format):
        response.headers['Content-Type'] = 'application/xml; charset=utf8'
        return meta.Session.query(Dataset).options(FromCache('short_term', 'bydatasetid')).get(id).get_schema(format)

    # Services
    def services(self, app_id, id, service_type, service):
        # OGC Bunch
        kargs = self._get_method_args()
        params = request.params

        if service_type == 'ogc':
            if id == 'base':
                if service == 'wms':
                    myogc = OGC(app_id, config, 'base')
                    content_type, content = myogc.wms(params)
                    response.headers['Content-Type'] = content_type
                    myogc = None
                    return content

                elif service == 'wfs':
                    myogc = OGC(app_id, config, 'base')
                    content_type, content = myogc.wfs(params)
                    response.headers['Content-Type'] = content_type
                    myogc = None

                elif service == 'wms_tiles':
                    meta.Session.remove()
                    return tilecache(None, app_id, config, kargs, is_base = True)

                else:
                    abort(404)
                meta.Session.remove()
                return content

            elif id.isdigit():
                dataset = meta.Session.query(Dataset).\
                      options(FromCache('short_term', 'bydatasetid')).\
                      get(id)


                if dataset is None:
                    abort(404)
                elif dataset.taxonomy == 'vector':
                    ds = VectorDataset(dataset, meta.Session, config)
                    if not os.path.isfile(ds.shapefile):
                        ds.write_vector_format('shp', FORMATS_PATH)
                elif dataset.taxonomy == 'geoimage':
                    ds = RasterDataset(dataset)
                elif dataset.taxonomy == 'rtindex':
                    ds = RasterTileIndexDataset(dataset, config)
                elif dataset.taxonomy == 'vtindex':
                    ds = VectorTileIndexDataset(dataset, config)
                else:
                    ds = None

                if ds is None or not ds.is_mappable:
                    abort(404)
                elif service == 'wms':
                    myogc = OGC(app_id, config, [ds])
                    myogc.buildService()
                    content_type, content = myogc.wms(params)
                    response.headers['Content-Type'] = content_type
                    myogc = None
                elif service == 'wfs':
                    myogc = OGC(app_id, config, [ds])
                    myogc.buildService()
                    content_type, content = myogc.wfs(params)
                    response.headers['Content-Type'] = content_type
                    myogc = None
                elif service == 'wcs':
                    myogc = OGC(app_id, config, [ds])
                    myogc.buildService()
                    content_type, content = myogc.wcs(params)
                    response.headers['Content-Type'] = content_type
                    myogc = None
                elif service == 'wms_tiles':
                    meta.Session.remove()
                    return tilecache(ds, app_id, config, kargs, is_base = False)
                else:
                    abort(404)

                meta.Session.remove() 
                return content

            else:
                abort(404)

    def mapper(self, app_id, id):

        wms_req_params = 'VERSION=1.1.1&SERVICE=WMS&REQUEST=GetCapabilities'
        wfs_req_params = 'VERSION=1.0.0&SERVICE=WFS&REQUEST=GetCapabilities'
        dataset = meta.Session.query(Dataset).\
                  options(FromCache('short_term', 'bydatasetid')).\
                  get(id)

        if dataset is None:
            abort(404)
        elif dataset.taxonomy == 'vector':
            ds = VectorDataset(dataset, meta.Session, config)
            taxonomy = 'Vector dataset'
        elif dataset.taxonomy == 'geoimage':
            ds = RasterDataset(dataset)
            taxonomy = 'Raster image'
        elif dataset.taxonomy == 'rtindex':
            ds = RasterTileIndexDataset(dataset, config)
            taxonomy = 'Raster Virtual Mosaic'
        elif dataset.taxonomy == 'vtindex':
            ds = VectorTileIndexDataset(dataset, config)
            taxonomy = 'Vector Virtual Mosaic'
        else:
            ds = None
            taxonomy = 'File'

        if ds is None:
            abort(404)

        if dataset.taxonomy == 'vector':
            feature_attributes = dataset.get_light_attributes()
        else:
            feature_attributes = None

        formats = []
        for fmt in Dataset.get_formats(dataset):
            if fmt == 'kml':
                fmt = 'kmz'
            formats.append({
                'title': fmt,
                'text':  config.get('BASE_URL') + '/apps/%s/datasets/%s.%s' % (app_id, dataset.id, fmt)
            })
        services = [{
            'title' : 'WMS',
            'text' : config.get('BASE_URL') + '/apps/%s/datasets/%s/services/ogc/wms?%s' % (app_id, dataset.id, wms_req_params)
        }, {
            'text' : config.get('BASE_URL') + '/apps/%s/datasets/%s/services/ogc/wfs?%s' % (app_id, dataset.id, wfs_req_params),
            'title':'WFS'
        }]
        metadata_xml = [{
            'title': 'XML',
            'text': config.get('BASE_URL') + '/apps/%(app_id)s/datasets/%(id)s/metadata/%(id)s.xml' %  { 'app_id': app_id, 'id': dataset.id}
        },{
            'title': 'TXT',
            'text': config.get('BASE_URL') + '/apps/%(app_id)s/datasets/%(id)s/metadata/%(id)s.txt' %  {'app_id': app_id, 'id': dataset.id}
        },{
            'title': 'HTML',
            'text': config.get('BASE_URL') + '/apps/%(app_id)s/datasets/%(id)s/metadata/%(id)s.html' %  {'app_id': app_id, 'id': dataset.id}
        }]

        layers = [{
            'layer' : dataset.basename,
            'id' : dataset.id,
            'title' : dataset.description ,
            'feature_attributes' : feature_attributes,
            'maxExtent': dataset.get_box(dataset.orig_epsg, SRID)
        }]

        description = {
            'what' : 'dataset',
            'title' : dataset.description,
            'id' : dataset.id ,
            'singleTile' : False,
            'layers' : [dataset.basename],
            'services' : services,
            'formats' : formats, 
            'metadata': metadata_xml,
            'taxonomy': dataset.taxonomy,
            'taxonomy_desc': taxonomy
        }

        c.D = dataset
        c.DX = ds
        c.Layers = layers
        c.Description = description
        c.AppId = app_id
        c.MEDIA_URL = config.get('MEDIA_URL')

        meta.Session.remove()

        return render('mapper.html')
