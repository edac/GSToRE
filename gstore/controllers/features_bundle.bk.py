import logging

from pylons import request, response, config, app_globals
from pylons.controllers.util import abort, redirect

from sqlalchemy.sql import func, and_

from gstore.lib.base import BaseController, render

from gstore.model import meta, caching_query, Dataset, ShapesVector, ShapesAttribute

import json
import csv

try:
    from cStringIO import StringIO
except:
    from StriongIO import StringIO

SRID = int(config['SRID'])
log = logging.getLogger(__name__)

def write_csv_row(values):
    """
    Return generic csv row from ordered values list. 
    """
    r = StringIO()
    csv.writer(r).writerow(values)
    row = r.getvalue()
    r.close()
    return row

class FeaturesBundleController(BaseController):
    """REST Controller styled on the Atom Publishing Protocol"""
    readonly = True
    streaming_mode = True
    def __init__(self):
        pass
    def index(self, dataset_id, format='json'):
        """GET /dataset/{id}/features: All items in the collection"""
       
        try:
            if request.params.get('dataset_ids'): 
                dataset_ids = map(int, request.params.get('dataset_ids').split(','))
            else:
                dataset_ids = [dataset_id]
        except:
            raise Exception('Wrong dataset id parameters')
    
        bbox = request.params.get('box')
        lon = request.params.get('lon')
        lat = request.params.get('lat')
        tolerance = request.params.get('tolerance', 1000)
        template = request.params.get('format', format)        
 
        epsg = request.params.get('epsg', SRID)
        epsg = int(epsg)
        limit = request.params.get('limit')
        offset = request.params.get('offset', 0)

        strip_collection_markup = request.params.get('strip_collection_markup', False)

        def coerce_timestamp(t):
            if t is None:
                return None
            try:
                nt = float(t)  
            except:
                nt = None
            return nt

        def get_time_filter(column, the_time, start_time, end_time):
            """
            If the_time is specified, the rest is ignored. Pointwise equality.
            """
            t_filter = None
            if the_time is not None:
                t_filter = column == func.to_timestamp(the_time)  
            elif start_time is not None and end_time is None:
                t_filter = column >= func.to_timestamp(start_time)
            elif start_time is None and end_time is not None:
                t_filter = column < func.to_timestamp(end_time)
            elif start_time is not None and end_time is not None and start_time < end_time:
                t_filter = and_(column >= func.to_timestamp(start_time), column < func.to_timestamp(end_time))
            return t_filter
                 
        start_time = coerce_timestamp(request.params.get('start_time'))
        end_time = coerce_timestamp(request.params.get('end_time'))
        the_time = coerce_timestamp(request.params.get('time'))
        t_filter = get_time_filter(ShapesVector.time, the_time, start_time, end_time) 

        query = meta.Session.query(ShapesVector)
        if t_filter is not None:
            query = query.filter(t_filter)

        if bbox:
            bbox = map(float, bbox.split(','))
            search_box = func.setsrid(func.makebox2d(func.makepoint(bbox[0],bbox[1]), func.makepoint(bbox[2],bbox[3])), epsg)

            if epsg and epsg != SRID:
                search_box = func.setsrid(func.transform(search_box, SRID), -1)
            query = query.filter(func.intersects(ShapesVector.geom, search_box))

        elif lon is not None and lat is not None:
            try:
                lon = float(lon)
                lat = float(lat)
                tolerance = float(tolerance)
            except ValueError:
                log.debug('Incorrect latitude and longitude parameters passed')
                abort(404)
            
            search_point = func.makepoint(lon, lat, epsg)
            if epsg and epsg != SRID:
                 search_point = func.transform(search_point, SRID)
            query = query.filter(func.st_dwithin(ShapesVector.geom, search_point, tolerance))                    

        if epsg != SRID: 
            geom_col = func.transform(func.setsrid(ShapesVector.geom, SRID), epsg)
        else:
            geom_col = func.setsrid(ShapesVector.geom, SRID)

        if format == 'json':
            geom_col = func.st_asgeojson(geom_col)
        elif format == 'kml':
            geom_col = func.st_askml(geom_col)
        elif format == 'gml':
            geom_col = func.st_asgml(geom_col)
        else: 
            geom_col = func.text(None)
            if format == 'csv' and len(dataset_ids) > 1:
                abort(404)

        query = query.add_column(geom_col) 
        
        datasets_attributes = {}

 
        def stream_features():
        
            if not strip_collection_markup:
                if format == 'json':
                    yield "{'type': 'FeatureCollection', 'features': ["
                elif format == 'kml':  
                    yield """<?xml version="1.0" encoding="UTF-8"?>
                    <kml xmlns="http://earth.google.com/kml/2.2">
                    <Folder>
                        <name>GSTORE API 1.0 Vector Stream</name>
                        <open>1</open>"""
                elif format == 'gml':
                    yield """<?xml version="1.0" encoding="UTF-8"?>
                            <gml:FeatureCollection 
                                xmlns:gml="http://www.opengis.net/gml" 
                                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                                xmlns:xlink="http://www.w3.org/1999/xlink">
                            <gml:description>GSTORE API 1.0 Vector Stream</gml:description>\n"""
                else:
                    pass 
            for d_id in dataset_ids:
                q = query
                if d_id is not None:
                    q = q.filter(ShapesVector.dataset_id == d_id)

                # User defined limit number of records found per dataset
                if limit and limit.isdigit():
                    q = q.limit(limit)
                q = q.offset(offset).yield_per(int(config.get('YIELD_PER_ROWS')))

                i = 1
 
                properties = {}
                for result in q:
                    d_id = result[0].dataset_id
                    if not properties.has_key(d_id):
                        properties[d_id] = {}
                        for att in meta.Session.query(ShapesAttribute).\
                            filter(ShapesAttribute.dataset_id == d_id).\
                            options(caching_query.FromCache('short_term', 'bydatasetid')):
                            properties[d_id][att.array_id-1] = att.name

                        len_properties = len(properties[d_id])
                        properties[d_id].update({
                            len_properties : 'gstore_time', 
                            len_properties+1 : 'gstore_dataset_id'
                        })
                            
                    # For now CSV is the only format that requires preserving 
                    # ordering in attribute columns, the rest gets dump from htore dicts.
                    if format == 'csv' and i == 1:
                        yield ','.join([properties[d_id][k] for k in xrange(len(properties))]) + '\n'

                    feature_timestamp = None if not result[0].time else result[0].time.isoformat() 
                    pg = result[1].split(';') if result[1] else ('','')
                    geom = pg[0] if len(pg) == 1 else pg[1] 

                    values = dict(result[0].values, gstore_time = feature_timestamp, gstore_dataset_id = d_id)

                    if not strip_collection_markup and i != 1 and format == 'json':
                        nl = ',\n'
                    else:
                        nl = ''
                    fid = "%s_%s" % (d_id, result[0].gid)

                    if format == 'json':
                        feat = """%s{"type": "Feature", "geometry": %s, "properties": %s}""" % (nl, geom, json.dumps(values))
                    elif format == 'kml':
                        if feature_timestamp is not None:
                            fts = """<TimeStamp><when>%s</when></TimeStamp>""" % feature_timestamp
                        else:
                            fts = ""
                        desc = "<![CDATA[\n" + ''.join([ '<p>%s: %s</p>\n' % (k,v) for k,v in values.iteritems()]) + "]]>"
                                 
                        feat = """<Placemark id="gstore_%s">
                                    <name>%s</name>
                                    <description>%s</description>
                                    %s%s\n%s</Placemark>
                               """ % ( fid, fid, desc, nl, geom, fts)
                    elif format == 'gml': 
                        feat = ''.join((nl, geom))
                    elif format == 'csv':
                        feat = write_csv_row([values[properties[d_id][k]] for k in xrange(len(properties[d_id]))])
                    else:
                        break
            
                    i += 1
                    yield feat

            meta.Session.connection().detach()
            meta.Session.close()

            if format == 'json':
                content_type = 'application/json'
                if not strip_collection_markup:
                    yield "\n]}"
            elif format == 'kml':  
                content_type = 'application/vnd.google-earth.kml+xml' 
                if not strip_collection_markup:
                    yield """\n</Folder>\n</kml>"""
            elif format == 'gml':
                content_type = 'application/xml; subtype="gml/3.1.1"'
                if not strip_collection_markup:
                    yield """\n</gml:FeatureCollection>"""
            else:
                pass

        if format == 'json':
            content_type = 'application/json'
        elif format == 'kml':  
            content_type = 'application/vnd.google-earth.kml+xml' 
        elif format == 'gml':
            content_type = 'application/xml; subtype="gml/3.1.1"'
        elif format == 'csv':
            content_type = 'text/csv'
        else:
            abort(404)
    
        response.headers['Content-Type'] = content_type
 
        return stream_features()


