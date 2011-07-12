import logging

from pylons import request, response, config, app_globals
from pylons.controllers.util import abort, redirect

from sqlalchemy.sql import func, and_

from gstore.lib.base import BaseController, render

from gstore.model import meta, caching_query, Dataset, ShapesVector 

import re
import json
import csv
from xml.sax.saxutils import escape

try:
    from cStringIO import StringIO
except:
    from StriongIO import StringIO

SRID = int(config['SRID'])
BASE_URL = config['BASE_URL']

OPERATORS = ['>=','<=','=','>','<']

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
    def index(self, app_id, dataset_id, format='json'):
        """GET /dataset/{id}/features: All items in the collection"""
      
        param_filters = request.params.get('filters','')
        hstore_filters = []

        for keyval in param_filters.split(','):
            for op in OPERATORS:
                kv = keyval.split(op)
                key = ''
                if kv[0] != keyval:
                    key = re.sub(r'\W+', '', kv[0])
                    val = kv[1].replace("'","").replace('"',"").strip()   
                else:
                    continue

                if not key:
                    break      

                if op != '=':
                    try:
                        _ = float(val)  
                        hstore_filters.append("(values->'%s')::numeric %s %s" % (key, op, val))
                    except:
                        continue
                else:
                    hstore_filters.append("values->'%s' == '%s'" % (key, val))
                break
                                 
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
        flatten = True if request.params.get('flatten') else False 

        if len(dataset_ids) == 1 and dataset_ids[0] is not None:
            flatten = True

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
            query = query.filter(func.st_intersects(ShapesVector.geom, search_box))

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
      
        for h_filter in hstore_filters:
            query = query.filter(h_filter)
 
        if format == 'json':
            content_type = 'application/json; charset=UTF-8'
            head = "{'type': 'FeatureCollection', 'features': ["
            tail = "\n]}"
        elif format == 'kml':  
            content_type = 'application/vnd.google-earth.kml+xml; charset=UTF-8'
            head = """<?xml version="1.0" encoding="UTF-8"?>
                        <kml xmlns="http://earth.google.com/kml/2.2">
                        <Document>"""
            tail = """\n</Document>\n</kml>"""
        elif format == 'gml':
            content_type = 'application/xml; subtype="gml/3.1.1; charset=UTF-8"'
            head = """<?xml version="1.0" encoding="UTF-8"?>
                                <gml:FeatureCollection 
                                    xmlns:gml="http://www.opengis.net/gml" 
                                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                                    xmlns:xlink="http://www.w3.org/1999/xlink">
                                <gml:description>GSTORE API 1.0 Vector Stream</gml:description>\n"""
            tail = """\n</gml:FeatureCollection>"""
        elif format == 'csv':
            content_type = 'text/csv; charset=UTF-8'
            head = ''
            tail = ''
        else:
            abort(404)
   
 
        response.headers['Content-Type'] = content_type 
     
        if not flatten: 
            query0 = query
            if dataset_ids == [None]:
                actual_dataset_ids = [d[0] for d in query0.values(ShapesVector.dataset_id.distinct())]
            else: 
                actual_dataset_ids = dataset_ids

            ids = []
            for d_id in actual_dataset_ids: 
                ids.append((escape(meta.Session.query(Dataset).\
                        options(caching_query.FromCache('short_term', 'bydatasetid')).get(d_id).description), d_id))
    
            output = head
            query_string = '&amp;'.join([ '%s=%s' % (k,escape(v)) for (k, v) in request.params.iteritems() if k != 'dataset_ids'])
               
            def get_query_url(d_id):
                return '%s/apps/%s/datasets/%s/features.%s?%s' % (BASE_URL, app_id, d_id, format, query_string)
 
            if format == 'kml':
                output += '\n'.join([
                    """<Folder>
                    <name>%s</name>
                    <visibility>0</visibility>
                    <open>0</open>
                    <NetworkLink>
                        <visibility>0</visibility>
                        <refreshVisibility>1</refreshVisibility>
                        <Link>
                            <viewRefreshMode>never</viewRefreshMode>
                            <href>%s</href>
                        </Link>
                    </NetworkLink>
                    </Folder>""" % (desc, get_query_url(d_id)) 
                 for (desc, d_id) in ids])
                output += tail
            else:
                output = ''

            return output     

        else:
             
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
         
            if dataset_ids != [None]:
                query = query.filter(ShapesVector.dataset_id.in_(dataset_ids))
 
            def stream_features():
                if not strip_collection_markup:
                    yield head

                current_d_id = 0

                q = query
                #q = q.order_by(ShapesVector.dataset_id, ShapesVector.gid.asc())
                #q = q.order_by(ShapesVector.dataset_id)
        
                # User defined limit number of records found per dataset
                if limit and limit.isdigit():
                    q = q.limit(limit)

                q = q.offset(offset).yield_per(int(config.get('YIELD_PER_ROWS')))

                i = 1

                attributes = {}

                for result in q:
                    d_id = result[0].dataset_id
                    if not flatten and current_d_id != d_id and not strip_collection_markup and format == 'kml':
                        if current_d_id == 0:
                            first_bit = ""
                        else:
                            first_bit = "\n</Folder>\n"
                
                        yield first_bit + "<Folder><name>" + escape(meta.Session.query(Dataset).\
                        options(caching_query.FromCache('short_term', 'bydatasetid')).\
                        get(d_id).description) + "</name>"

                    current_d_id = d_id

                    if not attributes.has_key(d_id):
                        attributes[d_id] = [ 
                            (-2, ('gstore_time', 'string')), # KML does not support time SimpleField type
                            (-1, ('gstore_dataset_id', 'int')) 
                        ]
                        attributes[d_id].extend(meta.Session.query(Dataset).\
                            options(caching_query.FromCache('short_term', 'bydatasetid')).\
                            get(d_id).get_light_attributes())

                    # For now CSV is the only format that requires preserving 
                    # ordering in attribute columns, the rest gets dump from htore dicts.
                    if format == 'csv' and i == 1:
                        yield ','.join([unicode(k[1][0]) for k in attributes[d_id]]) + '\n'
                    # If this is a single dataset we are querying and collection markup is not stripped
                    # inject Schema to the stream
                    if format == 'kml' and len(dataset_ids) == 1 and not strip_collection_markup and i == 1:
                        yield """<Schema name="%(name)s" id="%(name)sID">
                                %(simplefields)s
                            </Schema>""" % { 
                            'name': 'gstore_dataset_%s' % d_id,
                            'simplefields': '\n'.join([
                                """<SimpleField type="%(type)s" name="%(name)s"><displayName>%(name)s</displayName></SimpleField>""" % 
                                    dict(type = att[1][1], name = att[1][0]) for att in attributes[d_id]
                                ])
                            }
                    
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
                        #desc = "<![CDATA[\n" + ''.join([ '<b>%s</b>: %s<br />\n' % (k,re.sub(r'\W+','', escape(unicode(v)))) for k,v in values.iteritems()]) + "]]>"
                        
                        simpledata = "\n".join([
                            """<SimpleData name="%s">%s</SimpleData>""" % 
                            #(att[1][0], re.sub(r'[\W_]+', '', escape(str(values[att[1][0]])))) for att in attributes[d_id] if att[1][0] in values.keys()
                            (att[1][0], re.sub(r'[^\x20-\x7E]', '', escape(str(values[att[1][0]])))) for att in attributes[d_id] if att[1][0] in values.keys()
                        ])  
                                      #<SchemaData schemaUrl="#gstore_dataset_%sID">%s</SchemaData> 
                        # <description>%s</description>
                        feat = """<Placemark id="gstore_%s">
                                   <name>%s</name>
                                   %s%s\n%s
                                   <ExtendedData>
                                      <SchemaData schemaUrl="%s/apps/%s/datasets/%s/schema.kml">%s</SchemaData>
                                   </ExtendedData>
                                   <Style><LineStyle><color>ff0000ff</color></LineStyle>  <PolyStyle><fill>0</fill></PolyStyle></Style>
                               </Placemark>
                               """ % ( fid, fid, nl, geom, fts, BASE_URL, app_id, d_id, simpledata)
                    elif format == 'gml': 
                        feat = ''.join((nl, geom))
                    elif format == 'csv':
                        feat = write_csv_row([values[att[1][0]] for att in attributes[d_id] if att[1][0] in values.keys()])
                    else:
                        break
            
                    i += 1
                    yield feat

                meta.Session.connection().detach()
                meta.Session.close()

                if not strip_collection_markup:
                    yield tail  

            return stream_features()
