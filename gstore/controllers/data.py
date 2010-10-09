# -*- coding: utf-8 -*-
"""Static Data Controller"""
import logging
import re

from pylons import request, response, config
from pylons.controllers.util import abort

from sqlalchemy import select, and_

from gstore.lib.base import BaseController

from datetime import datetime

from gstore.model import meta

from gstore.model.geobase import Dataset, DatasetFootprint
from gstore.model.static_geotables import GeoLookup 

from pylons.decorators import jsonify
from pylons.decorators.cache import beaker_cache


log = logging.getLogger(__name__)

__all__ = ['DataController']


class DataController(BaseController):

    @jsonify
    def search(self, what, category):
        kw = request.params
        res = []

        if what == 'tree' and category  == 'themes':
            theme = ''
            subtheme = ''
            groupname = ''
            filter = ''
            limit = 25
            offset = 0

            if kw.has_key('filter'):
                filter = kw['filter'].replace(' ','%')
            
            if kw.has_key('limit'):
                limit = kw['limit']

            if kw.has_key('offset'):
                offset = kw['offset']

            if kw.has_key('node'):
                node = kw['node'].split('_|_')
                if len(node) == 1:
                    theme = kw['node']
                elif len(node) == 2:
                    theme = node[0]
                    subtheme = node[1]
                elif len(node) == 3:
                    theme = node[0]
                    subtheme = node[1]
                    groupname = node[2]
        
                if groupname:
                    self.insert_logentry()
                    (total, results) = Dataset.category_search(groupname = groupname, theme = theme, subtheme = subtheme, filter = filter, limit = limit, offset = offset)
                    for dataset in results:
                        d = { 
                            'text': dataset.description, 
                            'allowDrag': True,
                            'allowDrop': True,
                            'config': { 
                                'id' : dataset.id,
                                'what' : 'dataset',
                                'taxonomy': dataset.taxonomy,
                                'formats' : dataset.formats.split(','),
                                'services' : Dataset.get_services(dataset),
                                'tools'	: Dataset.get_tools(dataset)
                            },
                            'lastupdate': dataset.dateadded.strftime('%d%m%D')[4:],
                            'id': dataset.id 
                        }
                        res.append(d)
                    return dict(results = res, total = total)
            
                elif subtheme:
                    results = Dataset.category_search(subtheme = subtheme, theme = theme, filter = filter)
                elif theme and node != 'root': # theme is not empty
                    results = Dataset.category_search(theme = theme, filter = filter)
                    id = theme
                else:
                    results = Dataset.category_search(filter = filter)
                    id = None
            
                for category in results:
                    d = { 
                        'text' : category.text, 
                        'leaf' : False,
                        'allowDrag': False, 
                        'options': '-',
                        'allowDrop': False 
                    }
                    if subtheme:
                        d['leaf'] = True		
                        d['id'] = '_|_'.join([theme,subtheme, category.text])
                    elif theme and theme != 'root':
                        d['id'] = '_|_'.join([theme, category.text])
                    else:
                        d['id'] = category.text

                    res.append(d)
        else:
            if kw.has_key('filter'):
                results = Dataset.category_search(filter = filter)	

        if what == 'tree' and category == 'bundles':
            d = { 'text': 'New Mexico All Boundaries (Sample Bundle)', 
                 'allowDrag': True,
                'allowDrop': True,
                'options': [ 86, True, False ],
                'lastupdate': '05/01/09',
                'leaf' : True,
                'what' : 'bundle',
                'id': 86 }
            res.append(d)

        if what == 'combo' and category == 'themes':
            query = meta.Session.query(
                Dataset.id, 
                Dataset.description, 
                Dataset.theme,
                Dataset.subtheme,
                Dataset.groupname
            )
        
            if kw.has_key('theme'):
                query = query.filter(Dataset.theme.ilike(kw['theme']+'%')).cache()
            if kw.has_key('subtheme'):
                query = query.filter(Dataset.subtheme.ilike(kw['subtheme']+'%')).cache()
            if kw.has_key('groupname'):
                query = query.filter(Dataset.groupname.ilike(kw['groupname']+'%')).cache()

        if what in ['counties', 'quads', 'gnis', 'categories']:
            if 'query' in kw.keys():
                query = kw['query']
            else:
                query = None

            if what == 'categories':
                What = Dataset
            elif what in ['gnis', 'quads', 'counties']:
                What = GeoLookup
            else:
                What = None

            if What:
                if What == Dataset:
                    self.insert_logentry()
                    results = []
                    empty = [{'category' : '--------------'}]
                    if kw.has_key('query'):
                        query = kw['query']
                    else:
                        query = ''
                    query = '%'.join([ w for w in re.split('\s', query) if w ])

                    if query == '':
                        return dict(results = empty)
                    results = meta.Session.query(DatasetFootprint.category).filter(DatasetFootprint.category.ilike('%'+query+'%')).distinct().all()
                    results = dict(results = empty + [ { 'category' : r.category } for r in results ])
                    meta.Session.close()
                    return results 

                else:
                    sql = meta.Session.query(What).order_by(What.name)
                    sql = sql.filter(What.what == what)
                    if query:
                        sql = sql.filter(What.name.ilike(query+'%'))
                    resultproxy = sql.all()
                    
            else:
                resultproxy = None
            
            if resultproxy:
                results = []
                for row in resultproxy:
                    if What == Dataset:
                        box = ''
                        if category == 'groupname': 
                            full_label = row.groupname
                        elif category == 'subtheme': 
                            full_label = row.subtheme
                        else: 
                            full_label = row.theme
                        id = full_label
                    else:
                        full_label = row.name
                        box = row.box
                        id = row.id
                    results.append(dict(id = id, name = full_label, box = box))
                res = dict(results = results)
            else:
                res = None

        return res
