import logging

from pylons import request, response, session, tmpl_context as c, config
from pylons.controllers.util import abort, redirect

from sqlalchemy.sql import func

from gstore.lib.base import BaseController, render

from gstore.model import meta
from gstore.model.caching_query import FromCache
from gstore.model.geoutils import *
from gstore.model import Dataset, ShapesVector

import osgeo.ogr as ogr
import osgeo.osr as osr

import json
import csv

try:
    from cStringIO import StringIO
except:
    from StriongIO import StringIO

SRID = int(config['SRID'])
log = logging.getLogger(__name__)


def write_json_row(g, properties, isfinal):
    tail = '%s\n' if isfinal else '%s,\n'
    return tail % json.dumps(to_geojson(g.ExportToWkt(), properties = properties))
    
def write_csv_row(g, properties, isfinal):
    """
    Return generic csv row from properties (dict)
    """
    r = StringIO()
    csv.writer(r).writerow(properties.values())
    row = r.getvalue()
    r.close()
    return row

class FeaturesController(BaseController):
    """REST Controller styled on the Atom Publishing Protocol"""
    readonly = True
    templates = {
        'json': {
            'content_type': 'application/json',
            'head': lambda d: """{'totalRecords': %(total)s, 'type': 'FeatureCollection', 'features': [\n""" % d,
            'feature': write_json_row, 
            'tail': ']}'
        }, 
        'geojson': {
            'content_type': 'application/json',
            'head': lambda d: """{'type': 'FeatureCollection', 'features': [\n""", 
            'feature': write_json_row, 
            'tail': ']}'
        },
        'kml': {
            'content_type': 'application/vnd.google-earth.kml+xml',
            'head': '',
            'feature': '',
            'tail':''
        },
        'csv': {
            'content_type': 'text/csv',
            'head': lambda d: ','.join(d['attributes']) + '\r\n',
            'feature': write_csv_row,
            'tail': ''
        }
    }
    def __init__(self):
        pass
    def index(self, dataset_id, format='json'):
        """GET /dataset/{id}/features: All items in the collection"""
        
        
        dataset_ids = request.params.get('dataset_ids',[dataset_id])

        bbox = request.params.get('bbox')
        lon = request.params.get('lon')
        lat = request.params.get('lat')
        tolerance = request.params.get('tolerance', 1000)
        template = request.params.get('format', format)        
 
        epsg = request.params.get('epsg', SRID)
        epsg = int(epsg)
        limit = request.params.get('limit')
        offset = request.params.get('offset', 0)


        query = meta.Session.query(ShapesVector).filter(ShapesVector.dataset_id == dataset_id)
        d = meta.Session.query(Dataset).\
              options(FromCache('short_term', 'bydatasetid')).\
              get(id)
        if epsg != SRID and epsg.isdigit():
            geom_col = func.asewkt(func.transform(func.setsrid(ShapesVector.geom, SRID), epsg))
        else:
            geom_col = func.asewkt(ShapesVector.geom)

        if bbox:
            bbox = map(float, bbox.split(','))
            if epsg and epsg != SRID:
                bbox = transform_bbox(bbox, epsg, SRID)
            box = bbox_to_polygon(bbox) 
            search_box = func.GeomFromText(box.ExportToWkt())
            query = query.filter(func.intersects(ShapesVector.geom, search_box))
            box.Destroy()
        elif lon is not None and lat is not None:
            try:
                lon = float(lon)
                lat = float(lat)
                tolerance = float(tolerance)
            except ValueError:
                log.debug('Incorrect latitude and longitude parameters passed')
                abort(404)
            
            point = ogr.Geometry(ogr.wkbPoint)
            point.AddPoint(lon, lat)    
            tolerance = .10    
            if epsg and epsg != SRID:
                transform_to(point, epsg, SRID)
            search_point = func.GeomFromText(point.ExportToWkt())
            query = query.filter(func.st_dwithin(ShapesVector.geom, search_point, tolerance))                    
            point.Destroy()

        # User defined limit
        if limit and limit.isdigit():
            limit = int(limit)
            query = query.limit(limit)
        else:
            # Set initial row count limit for streaming
            limit = 30

        total = query.count()

        query = query.offset(offset).execution_options(stream_results = True)    
   
        if total:
            def stream_features(template_format):
                i = 1
                template = self.templates[template_format]
                
                metadata = {
                    'total': total,
                    'attributes': [ att.name for att in d.attributes_ref],
                    'crs': epsg
                }
       
                yield template['head'](metadata)
 
                for result in query.values(ShapesVector.values, geom_col, ShapesVector.time): 
                    properties = {'timestamp': result[2].isoformat()}
                    g = ogr.CreateGeometryFromWkt(result[1])

                    for att in d.attributes_ref:
                        properties[att.name] = result[0][att.array_id-1]
                   
                    gj = template['feature'](g, properties, i == total) 
                    g.Destroy()
                    i += 1
                    yield gj
 
                meta.Session.close()
                yield template['tail']

            response.headers['Content-Type'] = self.templates[template]['content_type']
            return stream_features(template)

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
