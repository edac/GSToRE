import logging

from pylons import request, response, session, tmpl_context as c, config
from pylons.controllers.util import abort, redirect

from sqlalchemy.sql import func

from rgis2.lib.base import BaseController, render

from rgis2.model.shapes import ShapesVector
from rgis2.model import meta
from rgis2.model.shapes_util import *
from rgis2.model.geobase import DatasetFootprint
from rgis2.model.cached import load_dataset
from rgis2.model.postgis import Spatial

import osgeo.ogr as ogr
import osgeo.osr as osr

import simplejson

SRID = int(config['SRID'])
log = logging.getLogger(__name__)

class Marker(Spatial):
    pass

class FeaturesController(BaseController):
    """REST Controller styled on the Atom Publishing Protocol"""
    readonly = True
    def __init__(self):
        pass
    def index(self, dataset_id, format='json'):
        """GET /dataset/{id}/features: All items in the collection"""
        
        query = meta.Session.query(ShapesVector).filter(ShapesVector.dataset_id == dataset_id)

        bbox = request.params.get('bbox')
        lon = request.params.get('lon')
        lat = request.params.get('lat')
        tolerance = request.params.get('tolerance',1000)

        start = request.params.get('start')
        end = request.params.get('end')

        epsg = request.params.get('epsg', SRID)
        epsg = int(epsg)
        limit = request.params.get('limit')
        d = load_dataset(dataset_id)

        if bbox:
            bbox = map(float, bbox.split(','))
            if epsg and epsg != SRID:
                bbox = transform_bbox(bbox, epsg, SRID)
            box = bbox_to_polygon(bbox) 
            search_box = func.GeomFromText(box.ExportToWkt())
            query = query.filter(func.intersects(ShapesVector.geom, search_box))
        elif lon is not None and lat is not None:
            try:
                lon = float(lon)
                lat = float(lat)
                tolerance = float(tolerance)
            except ValueError:
                log.debug('Wrong lot lon parameters passed')
                abort(404)
            
            point = ogr.Geometry(ogr.wkbPoint)
            point.AddPoint(lon, lat)    
            tolerance = .10    
            if epsg and epsg != SRID:
                transform_to(point, epsg, SRID)
            search_point = func.GeomFromText(point.ExportToWkt())
            query = query.filter(func.st_dwithin(ShapesVector.geom, search_point, tolerance))                    

        if limit and limit.isdigit():
            limit = int(limit)
            if limit > 30:
                limit = 30
        else:
            limit = 30

        if start and start.isdigit():
            offset = int(start)*limit
            query = query.offset(offset)
        else:
            offset = 0

        query = query.limit(limit)
        query = query.add_column(func.astext(func.centroid(ShapesVector.geom)))

        results = query.all()
    
        response.headers['Content-Type'] = 'application/json'
        if results is None:
            return ''
        else:
            features = []
            M = Marker()

            for result in results:
                vector = result[0]
                centroid_wkt = result[1]
                properties = {}
                g = ogr.CreateGeometryFromWkt(centroid_wkt)
                if epsg and epsg != SRID:
                    transform_to(g, SRID, epsg)

                for att in d.attributes_ref:
                    properties[att.name] = vector.values[att.array_id-1]
                    
                features.append(M.to_geojson(geom = g, properties = properties))

            return simplejson.dumps({'totalRecords': len(results), 'type': 'FeatureCollection', 'features': features })

    def create(self):
        """POST /datasets/features: Create a new item"""
        # url('features')

    def new(self, id, format='html'):
        """GET /datasets/{id}/features/new: Form to create a new item"""
        # url('new_feature')

    def update(self, id, gid):
        """PUT /datasets/{id}/features/{gid}: Update an existing item"""
        # Forms posted to this method should contain a hidden field:
        #    <input type="hidden" name="_method" value="PUT" />
        # Or using helpers:
        #    h.form(url('feature', id=ID),
        #           method='put')
        # url('feature', id=ID)

    def delete(self, id, gid):
        """DELETE /datasets/{id}features/{gid}: Delete an existing item"""
        # Forms posted to this method should contain a hidden field:
        #    <input type="hidden" name="_method" value="DELETE" />
        # Or using helpers:
        #    h.form(url('feature', id=ID),
        #           method='delete')
        # url('feature', id=ID)

    def show(self, id, format='html'):
        """GET /datasets/{id}/features/{gid}: Show a specific item"""
        # url('feature', id=ID)

    def edit(self, id, format='html'):
        """GET /datasets/{id}/features/{gid}/edit: Form to edit an existing item"""
        # url('edit_feature', id=ID)
