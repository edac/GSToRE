# -*- coding: utf-8 -*-
"""Static Data Controller"""
import logging
import re

from pylons import request, response, config
from pylons.controllers.util import abort
from pylons.decorators import jsonify

from sqlalchemy import select, func, and_

from gstore.lib.base import BaseController
from gstore.model import meta
from gstore.model import Dataset, GeoLookup
from gstore.model.geoutils import transform_bbox, bbox_to_polygon

import json

log = logging.getLogger(__name__)

__all__ = ['SearchController']

SRID = int(config['SRID'])
APPS = config.get('APPS').split(',')

def convert_string_box_numeric(box):
    try: 
        if isinstance(box, basestring):
            bbox = map(float, box.split(','))
        # else assume list/tuple
        else:
            bbox = map(float, box)
    except:
        bbox = ''

    return bbox
    
def prepare_box(box, epsg_from, epsg_to):
    bbox = convert_string_box_numeric(box) 
    try: 
        # Careful this is an Openlayers bbox: (minX, minY, maxX, maxY)
        if epsg_from and epsg_from != epsg_to:
            bbox = transform_bbox(bbox, epsg_from, epsg_to)
    except:
        # The worst we expect here is a GDAL "Exception: EPSG PCS/GCS code X 
        # not found in EPSG support files. Is this a valid EPSG coordinate system?"
        pass
 
    return bbox

class SearchController(BaseController):
    streaming_mode = True
 
    def index(self, app_id, resource):
        """
        kw  dict:   Usually a copy of request.params.
        app_id string:  Application identifier.
        """ 
        if app_id not in APPS:
            abort(404)

        kw = request.params

        response.headers['Content-Type'] = 'application/json; charset=utf-8'

        if resource == 'datasets':
            total, results = self.search_datasets(app_id, kw)
            return dict(total = total, results =results)
        elif resource == 'dataset_categories':
            return self.search_datasets_categories(app_id, kw)
        else:
            return self.search_geolookups(app_id, kw)

    @jsonify    
    def search_geolookups(self, app_id, kw):
        """
        kw  dict:   Usually a copy of request.params.
        app_id string:  Application identifier.
        """ 
        if app_id not in APPS:
            abort(404)
        what = kw.get('layer', '')

        # Pagination related parameters
        limit = 25
        offset = 0
        direction = None

        if kw.has_key('limit'):
            limit = kw['limit']

        if kw.has_key('offset'):
            offset = kw['offset']

        if kw.has_key('dir'):
            if kw['dir'].upper() == 'ASC':
                direction = 1
            else:
                direction = 0

        if kw.has_key('query'):
            query = kw['query']
        else:
            query = ''
        query = '%'.join([ w for w in re.split('\s', query) if w ])

        epsg = kw.get('epsg', SRID)
        epsg = int(epsg)

        results = meta.Session.query(GeoLookup.box, GeoLookup.description).\
            filter(GeoLookup.description.ilike('%'+query+'%')).\
            filter(GeoLookup.what.ilike(what)).\
            filter("'%s' = ANY(app_ids)" % app_id).limit(limit).offset(offset)
   
        return dict(results = [{'text': r.description, 'box': prepare_box(r.box, SRID, epsg)} for r in results ])


    def search_datasets_categories(self, app_id, kw):
        """
        kw  dict:   Usually a copy of request.params.
        app_id string:  Application identifier.
        """ 
        if app_id not in APPS:
            abort(404)

        if kw.has_key('query'):
            query = kw['query']
        else:
            query = ''
        query = '%'.join([ w for w in re.split('\s', query) if w ])

        empty = [{'category' : '--------------'}]

        if query == '':
            return json.dumps(dict(results = empty))

        th = Dataset.theme
        st = Dataset.subtheme
        gn = Dataset.groupname
        
        results = meta.Session.query(Dataset).filter(Dataset.inactive == False).\
            filter("'%s' = ANY(apps_cache)" % app_id).\
            filter((th + st + gn).ilike('%'+query+'%')).distinct().\
            values(th, st, gn)
    
        return dict(results = empty + [{'text': r.theme + '-' + r.subtheme + '-' + r.groupname } for r in results])

    def search_datasets(self, app_id):
        """
        kw  dict:   Usually a copy of request.params.
        app_id string:  Application identifier.
        """ 

        if app_id not in APPS:
            abort(404)

        kw = request.params

        res_list = []
        filters = []

        D = Dataset

        # Simple keyword query search
        query = ''
        if kw.has_key('query'):
            query = kw['query'].replace(' ','%')
            query = kw['query'].replace('+','%')

        # Pagination related parameters
        limit = 25
        offset = 0
        direction = None

        if kw.has_key('limit'):
            limit = int(kw['limit'])

        if kw.has_key('offset'):
            offset = int(kw['offset'])

        if kw.has_key('dir'):
            if kw['dir'].upper() == 'ASC':
                direction = 1
            else:
                direction = 0

        def coerce_timestamp(t):
            if t is None:
                return None
            try:
                nt = float(t)  
            except:
                nt = None
            return nt

        def get_time_filter(column, start_time, end_time):
            t_filter = None
            if start_time is not None and end_time is None:
                t_filter = column >= func.to_timestamp(start_time)
            elif start_time is None and end_time is not None:
                t_filter = column < func.to_timestamp(end_time)
            elif start_time is not None and end_time is not None and start_time < end_time:
                t_filter = and_(column >= func.to_timestamp(start_time), column < func.to_timestamp(end_time))
            return t_filter
                 
        start_time = coerce_timestamp(kw.get('start_time'))
        end_time = coerce_timestamp(kw.get('end_time'))
        t_filter = get_time_filter(D.dateadded, start_time, end_time) 

        if t_filter is not None:
            filters.append(t_filter)

        # Sorting related parameters
        orderby = None

        if kw.has_key('sort'):
            if kw['sort'] == 'lastupdate':
                o = 'dateadded'
            elif kw['sort'] == 'text':
                o = 'description'
            else:
                o = kw['sort']
            if o in ['description', 'theme', 'subtheme', 'groupname', 'dateadded']:
                orderby = getattr(D, o)

        # Spatial query parameters
        epsg = kw.get('epsg', SRID)
        epsg = int(epsg)

        #log.debug(kw.get('box'))
        if kw.has_key('box'):
            box = convert_string_box_numeric(kw['box'])
        else:
            box = ''

        search_box = None
        if box and epsg:
            bbox = prepare_box(box, epsg, SRID)
            box = bbox_to_polygon(bbox)
            if box is None:
                abort(404)
            search_box = func.GeomFromText(box.ExportToWkt())
            search_box = func.setsrid(search_box, -1)

            filters.append(func.st_intersects(D.geom, search_box))

        #categories bool: Flag to indicate whether the dictionary result should only 
        #                contain distinct theme, subtheme and groupnames categories. 
        #                This is applicable to feed tree nodes in the browse search view.
        categories = kw.get('categories', False)
        node = kw.get('node', 'root')

        # We have to assign variables to these categorical fields for being able to 
        # modify them as distinct in the query if the categories = True is passed. 
        theme = D.theme   
        subtheme = D.subtheme
        groupname = D.groupname

        filters.append("'%s' = ANY(apps_cache)" % app_id)

        if orderby is not None:
            if direction:
                orderby_nongeo = orderby.asc()
            else:
                orderby_nongeo = orderby.desc()
        else:
            orderby_nongeo = D.dateadded.desc()

        if query:
            filters.append(D.description.ilike('%'+query+'%'))
   
        # Filling up tree node with categories, applying distinct
        levels = [] 
        if kw.has_key('theme') and kw['theme']:
            levels.append(kw['theme'])
            filters.append(D.theme.ilike(levels[0]))
            if kw.has_key('subtheme') and kw['subtheme']:
                levels.append(kw['subtheme'])
                filters.append(D.subtheme.ilike(levels[1]))
                if kw.has_key('groupname') and kw['groupname']:
                    levels.append(kw['groupname'])
                    filters.append(D.groupname.ilike(levels[2]))

        cat = None
        if levels and categories:
            if len(levels) == 1:
                cat = D.subtheme.distinct().label('text')
            elif len(levels) == 2:
                cat = D.groupname.distinct().label('text')
        elif categories:
            node = list(node.split('__|__'))
            if node[0] != 'root':
                if len(node) == 1:
                    filters.append(D.theme.ilike(node[0]))
                    cat = D.subtheme.distinct().label('text')
                elif len(node) == 2:
                    filters.append(D.theme.ilike(node[0]))
                    filters.append(D.subtheme.ilike(node[1]))
                    cat = D.groupname.distinct().label('text') 
                else:
                    cat = 'NULL';
            else:       
                node = [] 
                cat = D.theme.distinct().label('text')    
        # Target columns
        if cat is None:
            res = meta.Session.query(D).filter(Dataset.inactive == False)
            C = [D.description, D.theme, D.subtheme, D.groupname, D.id, \
                    D.taxonomy, D.formats, D.box, D.dateadded, D.has_metadata]
 
            if search_box is not None:
                C.append(func.geo_relevance(D.geom, search_box).label('geo_relevance'))
                if orderby is not None:
                    res = res.order_by(orderby_nongeo)
                    res = res.order_by('geo_relevance DESC ')
                else:
                    res = res.order_by('geo_relevance DESC ')
                    # Geo-relevance has priority in order
                    res = res.order_by(orderby_nongeo)
            else:
                res = res.order_by(orderby_nongeo)
                

        else:
            C = [cat]
            res = meta.Session.query(cat).filter(Dataset.inactive == False).order_by('text ASC')

        for f in filters:
            res = res.filter(f)
    
        #res = res.values(*C)
        res_list = []

        response.headers['Content-Type'] = 'application/json'
        # don't limit/offset results when we are looking for distinct categories
        if categories:
            if cat != 'NULL':
                for category in res.values(*C):
                    d = {
                        'text': category.text,
                        'leaf': False, 
                        'id': '__|__'.join(node +  [category.text])
                    }
                    if len(node) == 2:
                        d['cls'] = 'folder'
                        d['leaf'] = True
                    res_list.append(d)

            # ExtJS Tree only accepts lists as JSON
            return json.dumps(dict(total = 0, results = res_list))

        else:
            total = res.count()
            res = res.limit(limit).offset(offset)
            #for dataset in res.execution_options(stream_results = True).values(*C):
            def stream_search_results():
                i = 1
                yield """{"total": %s, "results": [""" % total
                #for dataset in res.yield_per(10).values(*C):
                for dataset in res.values(*C):
                    if search_box is not None:
                        geo_relevance = dataset.geo_relevance
                    else:
                        geo_relevance = 0
                    if i == limit or i == total - offset:
                        st = "%s\n"
                    else:
                        st = "%s, \n"
                    i += 1
                    yield st % json.dumps({ 
                        'text': dataset.description, 
                        'categories': dataset.theme + '__|__' + dataset.subtheme + '__|__' + dataset.groupname,
                        'config': { 
                            'id' : dataset.id,
                            'what' : 'dataset',
                            'taxonomy': dataset.taxonomy,
                            'formats' : Dataset.get_formats(dataset),
                            'services' : Dataset.get_services(dataset),
                            'tools'	: Dataset.get_tools(dataset)
                        },
                        'box': prepare_box(dataset.box, SRID, epsg),
                        'lastupdate': dataset.dateadded.strftime('%d%m%D')[4:],
                        'id': dataset.id,
                        'gr': float(geo_relevance)
                    })
                yield "]}"
                meta.Session.connection().detach()
                meta.Session.close()

            return stream_search_results() 
