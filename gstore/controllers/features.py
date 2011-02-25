import logging

from pylons import request, response, session, tmpl_context as c, config
from pylons.controllers.util import abort, redirect

from sqlalchemy.sql import func

from gstore.lib.base import BaseController, render

from gstore.model import meta
from gstore.model.geoutils import *
from gstore.model import Dataset, ShapesVector

import osgeo.ogr as ogr
import osgeo.osr as osr

import json
import shutil

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

SRID = int(config['SRID'])
log = logging.getLogger(__name__)

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
        
        epsg = request.params.get('epsg', SRID)
        epsg = int(epsg)
        limit = request.params.get('limit')
        offset = request.params.get('offset', 0)

        d = self.load_dataset(dataset_id)

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

        # User defined limit
        if limit and limit.isdigit():
            limit = int(limit)
            query = query.limit(limit)
        else:
            # Set initial row count limit for streaming
            limit = 30

        total = query.count()

        query = query.offset(offset)
        query = query.add_column(func.asewkt(ShapesVector.geom))
   
        if total:
            response.headers['Content-Type'] = 'application/json'
            features = []
            def stream_geojson():
                row_chunk_size = limit 
                newoffset = offset
                i = 1
                yield """{'totalRecords': %s, 'type': 'FeatureCollection', 'features': [""" % total
                while newoffset < total:
                    avg_sizes = []
                    for result in query.offset(newoffset).limit(row_chunk_size):
                        vector = result[0]
                        geom_wkt = result[1]
                        properties = {}
                        g = ogr.CreateGeometryFromWkt(geom_wkt)
                        if epsg and epsg != SRID:
                            transform_to(g, SRID, epsg)

                        for att in d.attributes_ref:
                            properties[att.name] = vector.values[att.array_id-1]
                        if i == total:
                            st = "%s"
                        else:
                            st = "%s, "                          
                        gj =  st % json.dumps(to_geojson(g.ExportToWkt(), properties = properties))
                        i += 1
                        avg_sizes.append(len(gj))
                        yield gj

                    newoffset += row_chunk_size
                    max_batch_size = min(avg_sizes) * row_chunk_size
                    if max_batch_size < 100000:
                        row_chunk_size = int(100000.0/min(avg_sizes))
                yield "]}"

            return stream_geojson()

        else:
            return

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
