# -*- coding: utf-8 -*-
"""Static Data Controller"""
import logging
import re

from pylons import request, response, config
from pylons.controllers.util import abort
from pylons.decorators.cache import beaker_cache

from sqlalchemy import select, func, and_

from gstore.lib.base import BaseController
from gstore.model import meta
from gstore.model.geobase import Dataset, GeoLookup
from gstore.model.shapes_util import transform_bbox, bbox_to_polygon

try:
    import json
except:
    import simplejson as json

log = logging.getLogger(__name__)

__all__ = ['SearchController']

SRID = int(config['SRID'])
APPS = ['rgis', 'epscor']

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
            callback = kw.get("callback", None) 
            results = self.search_datasets(app_id, kw)
            if callback:
                return "%s(%s)" % (callback, json.dumps(results) )
            else:
                return json.dumps(results)
        elif resource == 'dataset_categories':
            return json.dumps(self.search_datasets_categories(app_id, kw))
        else:
            return json.dumps(self.search_geolookups(app_id, kw))

        
    def search_geolookups(self, app_id, kw):
        """
        kw  dict:   Usually a copy of request.params.
        app_id string:  Application identifier.
        """ 
        if app_id not in APPS:
            abort(404)
        what = kw.get('what', '')

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
            filter("'%s' = ANY(app_ids)" % app_id).limit(limit).offset(offset).all()
    
        meta.Session.close()

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

        th = Dataset.__table__.c.theme
        st = Dataset.__table__.c.subtheme
        gn = Dataset.__table__.c.groupname
        
        cat = (th + ' - ' + st + ' - ' + gn).label('category') 
        results = meta.Session.query(cat).filter(Dataset.__table__.c.inactive == False).\
            filter("'%s' = ANY(apps_cache)" % app_id).\
            filter((th + st + gn).ilike('%'+query+'%')).distinct().all()
        meta.Session.close()
    
        return dict(results = empty + [{'text': r.category } for r in results ])


    # this cache needs to be done by query parameter
    #@beaker_cache(expire=3600)
    def search_datasets(self, app_id, kw):
        """
        kw  dict:   Usually a copy of request.params.
        app_id string:  Application identifier.
        """ 
        if app_id not in APPS:
            abort(404)

        res_list = []
        filters = []

        # Let's query the table directly
        D = Dataset.__table__

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
            limit = kw['limit']

        if kw.has_key('offset'):
            offset = kw['offset']

        if kw.has_key('dir'):
            if kw['dir'].upper() == 'ASC':
                direction = 1
            else:
                direction = 0

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
                orderby = getattr(D.c, o)

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

            filters.append(func.intersects(D.c.geom, search_box))

        #categories bool: Flag to indicate whether the dictionary result should only 
        #                contain distinct theme, subtheme and groupnames categories. 
        #                This is applicable to feed tree nodes in the browse search view.
        categories = kw.get('categories', False)
        node = kw.get('node', [])

        # We have to assign variables to these categorical fields for being able to 
        # modify them as distinct in the query if the categories = True is passed. 
        theme = D.c.theme   
        subtheme = D.c.subtheme
        groupname = D.c.groupname

        filters.append("'%s' = ANY(apps_cache)" % app_id)

        if orderby is not None:
            if direction:
                orderby = orderby.asc()
            else:
                orderby = orderby.desc()
        else:
            orderby = D.c.dateadded.desc()

        if query:
            filters.append(D.c.description.ilike('%'+query+'%'))
   
        # Filling up tree node with categories, applying distinct
        levels = [] 
        if kw.has_key('theme') and kw['theme']:
            levels.append(kw['theme'])
            filters.append(D.c.theme.ilike(levels[0]))
            if kw.has_key('subtheme') and kw['subtheme']:
                levels.append(kw['subtheme'])
                filters.append(D.c.subtheme.ilike(levels[1]))
                if kw.has_key('groupname') and kw['groupname']:
                    levels.append(kw['groupname'])
                    filters.append(D.c.groupname.ilike(levels[2]))

        cat = None
        if levels and categories:
            if len(levels) == 1:
                cat = D.c.subtheme.distinct().label('text')
            elif len(levels) == 2:
                cat = D.c.groupname.distinct().label('text')
        elif categories:
            node = list(node.split('__|__'))
            if node[0] != 'root':
                if len(node) == 1:
                    filters.append(D.c.theme.ilike(node[0]))
                    cat = D.c.subtheme.distinct().label('text')
                elif len(node) == 2:
                    filters.append(D.c.theme.ilike(node[0]))
                    filters.append(D.c.subtheme.ilike(node[1]))
                    cat = D.c.groupname.distinct().label('text') 
                else:
                    cat = 'NULL';
            else:       
                node = [] 
                cat = D.c.theme.distinct().label('text')    
        # Target columns
        if cat is None:
            C = [
                D.c.id, 
                D.c.description,
                D.c.theme, D.c.subtheme, D.c.groupname, 
                D.c.box, 
                D.c.has_metadata_cache.label('has_metadata'), 
                D.c.formats_cache.label('formats'), 
                D.c.taxonomy, 
                D.c.dateadded
            ]
            res = meta.Session.query(*C)
            if search_box is not None:
                res = res.add_column(func.geo_relevance(D.c.geom, search_box).label('geo_relevance'))
                res = res.order_by('geo_relevance DESC ')
            # Geo-relevance always has priority in order
            res = res.order_by(orderby)
        else:
            res = meta.Session.query(cat)

        for f in filters:
            res = res.filter(f)
    
        res_list = []

        # don't limit/offset results when we are looking for distinct categories
        if categories:
            if cat != 'NULL':
                results = res.all()
                log.debug(len(node))
                for category in results:
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
            return res_list            

        else:
            total = res.count()
            results = res.limit(limit).offset(offset).all()
            for dataset in results:
                if search_box is not None:
                    geo_relevance = dataset.geo_relevance
                else:
                    geo_relevance = 0
                d = { 
                    'text': dataset.description, 
                    'categories': '__|__'.join([dataset.theme, dataset.subtheme, dataset.groupname]),
                    'config': { 
                        'id' : dataset.id,
                        'what' : 'dataset',
                        'taxonomy': dataset.taxonomy,
                        'formats' : dataset.formats.split(','),
                        'services' : Dataset.get_services(dataset),
                        'tools'	: Dataset.get_tools(dataset)
                    },
                    'box': prepare_box(dataset.box, SRID, epsg),
                    'lastupdate': dataset.dateadded.strftime('%d%m%D')[4:],
                    'id': dataset.id,
                    'gr': float(geo_relevance)
                }

                res_list.append(d)

            return {'total': total, 'results': res_list}
