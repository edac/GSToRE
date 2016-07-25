from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPServerError, HTTPBadRequest

import sqlalchemy
from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_, or_, cast
from sqlalchemy.sql import between

import json
from datetime import datetime

import requests

from ..models import DBSession
from ..models.datasets import (
    Dataset,
    Category
    )
from ..models.features import Feature

from ..models.vocabs import geolookups
from ..lib.spatial import *
from ..lib.mongo import *
from ..lib.utils import *
from ..lib.database import get_dataset, get_collection
from ..lib.es_searcher import *



def return_no_results(ext='json'):
    """empty response

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    if ext == 'json':
        response = Response(json.dumps({"total": 0, "results": []}))
        response.content_type = 'application/json'
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response

    #TODO: generate empty KML response
    return Response()

'''
doctype response
'''
#TODO: replace the streamers here with the other streamer? meh, the other json is a different structure so maybe not.
def generate_search_response(searcher, request, app, limit, base_url, ext, version=3):
    """generate the streamer for the search results for doctypes

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    total = searcher.get_result_total()

    if total < 1:
        return return_no_results(ext)

    #TODO: figure out what to do if, horrifically, the es uuid count does not match the dataset count
    search_objects = searcher.get_result_ids()
    
    subtotal = len(search_objects)
    if subtotal < 1:
        return return_no_results(ext)

    limit = subtotal if subtotal < limit else limit

    response = Response()
    response.headers['Access-Control-Allow-Origin'] = '*'
    if ext == 'json':
        json_streamer = StreamDoctypeJson(app, base_url, request, total, version, limit)
        response.content_type = 'application/json'
        response.app_iter = json_streamer.yield_set(search_objects, limit)
    elif ext == 'kml':
        kml_streamer = StreamDoctypeKml(app, base_url) 
        response.content_type = 'application/vnd.google-earth.kml+xml; charset=UTF-8'
        response.app_iter = kml_streamer.yield_set(search_objects, limit)
    else:
        return HTTPNotFound()

    return response

@view_config(route_name='search_categories', renderer='json')
def search_categories(request):
    """return the category tree


    return distinct themes if no node or if 
        distinct subthemes for theme if node is one chunk
        distinct groupnames for theme + subtheme if node = two chunks (__|__ delimiter)

    root:
    {
        "total": 0, 
            "results": [
                    {"text": "Area Code Change - New Mexico", "leaf": false, "id": "Area Code Change - New Mexico"}, 
                    {"text": "Boundaries", "leaf": false, "id": "Boundaries"}, 
                    {"text": "Cadastral", "leaf": false, "id": "Cadastral"}, 
                    {"text": "Census Data", "leaf": false, "id": "Census Data"}, 
                    {"text": "Cities and Towns", "leaf": false, "id": "Cities and Towns"}
                ]
        }
    theme (node=Boundaries):
    {"total": 0, "results": [{"text": "General", "leaf": false, "id": "Boundaries__|__General"}]}

    subtheme (node=Cadastral__|__NSDI):
    {"total": 0, "results": [{"text": "PLSS V1", "leaf": true, "id": "Cadastral__|__NSDI__|__PLSS V1", "cls": "folder"}]}

    groupname (node=Climate__|__General__|__United%States)
    (what exactly would be the point?)
    {"total": 0, "results": []}

    doctypes: datasets, collections, etc

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    
    #TODO: allow other formats (kml, etc) 
    #IT IS POST FROM RGIS FOR SOME REASON (geoext/ext reasons)
    #get the starting location for the tree
    #root OR Census Data OR Census Data__|__2008 TIGER
    #root OR theme OR theme__|__subtheme
    app = request.matchdict['app']

    params = normalize_params(request.params)
    node = params.get('node', '')

    doctypes = params.get('doctypes', 'datasets').split(',')
    doctypes = ','.join([d[:-1] for d in doctypes])
    

    #set up the elasticsearch connection
    es_connection = request.registry.settings['es_root']
    es_index = request.registry.settings['es_dataset_index']
    #TODO: change this to the combined search options
    es_type = doctypes
    es_user = request.registry.settings['es_user'].split(':')[0]
    es_password = request.registry.settings['es_user'].split(':')[-1]

    es_url = es_connection + es_index + '/' + es_type +'/_search'

    #set up the basic query with embargo/active flags at the dataset level BUT not the app here 
    #because the categories could be different for the apps (i.e. 'climate' for epscor and 'nrcs' for rgis (don't do that, though))
    query = {
        "size": 0,
        "query": {
            "filtered": {
                "filter": {
                    "and": [
                        {"term": {"embargo": False}},
                        {"term": {"active": True}}
                    ]
                }
            }
        }
    }

    #running with es
    #TODO: add the checks for embargoed, inactive for this (BUT ADD THEM TO THE STUPID INDEX FIRST)
    level = 0
    parts = []
    if node and node != 'root':
        parts = node.split('__|__')
        if len(parts) == 1:
            #get subthemes
            facets = {
                "categories": {"terms": {"field": "subtheme", "size": 100, "order": "term"},
                    "nested": "category_facets",
                    "facet_filter": {
                        "query": {
                            "filtered": {
                                "query": {"match_all": {}},
                                "filter": {
                                    "and": [
                                        {"term": {"apps": app.lower()}},
                                        {"term": {"theme": parts[0]}}
                                    ]
                                }
                            }
                        }
                    }
                }
            }
            
            query.update({"facets": facets})

            level = 1
        elif len(parts) == 2:
            #get groupnames
            facets = {
                "categories": {"terms": {"field": "groupname", "size": 100, "order": "term"},
                    "nested": "category_facets",
                    "facet_filter": {
                        "query": {
                            "filtered": {
                                "query": {"match_all": {}},
                                "filter": {
                                    "and": [
                                        {"term": {"apps": app.lower()}},
                                        {"term": {"theme": parts[0]}},
                                        {"term": {"subtheme": parts[1]}}
                                    ]
                                }
                            }
                        }
                    }
                }
            }
            query.update({"facets": facets})

            level = 2
        else:
            return return_no_results()
    else:
        #get themes with datasets in the app
        '''
        {
	        "size": 0,
            "query": {
            	    "match_all": {}
            },
            "facets": {
                "categories": {
                    "terms": {"field": "theme", "size": 600, "order": "term"},
                    "nested": "category_facets",
                    "facet_filter": {
                    	"query": {
                        	"filtered": {
                            	"query": {
                                	"match_all": {}
                                },
                                "filter": {
                                	    "and": [
                                    	    {"term": {"apps": "epscor"}}
                                    ]
                                }
                            }
                        }
                    	}
                }
            }
        }
        '''
        #the field of the nested set, the size (for now) is larger than the set, and order by the term alphabetically
        facets = {
            "categories": {"terms": {"field": "theme", "size": 700, "order": "term"},
                "nested": "category_facets",
                "facet_filter": {
                    "query": {
                        "filtered": {
                            "query": {"match_all": {}},
                            "filter": {
                                "and": [
                                    {"term": {"apps": app.lower()}}
                                ]
                            }
                        }
                    }
                }
            }
        }
        query.update({"facets": facets})

    if 'check' in params:
        #for testing - get the elasticsearch json request
        return query

    results = requests.post(es_url, data=json.dumps(query), auth=(es_user, es_password))
    
    data = results.json()
    if 'facets' not in data:
        return return_no_results()

    #TODO: get rid of the cls element (not used, kinda stupid)
    facets = data['facets']['categories']['terms']
    resp = {"total": len(facets)}
    rslts = []
    if level == 0:
        rslts = [{"text": facet['term'], "leaf": False, "id": facet['term']} for facet in facets]
    elif level == 1:
        rslts = [{"text": facet['term'], "leaf": False, "id": '%s__|__%s' % (parts[0], facet['term'])} for facet in facets]
    elif level == 2:
        rslts = [{"text": facet['term'], "leaf": True, "cls": "folder", "id": '%s__|__%s__|__%s' % (parts[0], parts[1], facet['term'])} for facet in facets]
    resp.update({"results": rslts})

    response = Response(json.dumps(resp))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.content_type="application/json"    
    return response


#search for any of the doctypes in es 
@view_config(route_name='searches')
def search_doctypes(request):
    """

    PARAMS:
    limit
    offset
    dir (ASC | DESC)
    start_time yyyyMMddThh:mm:ss
    end_time
    valid_start
    valid_end
    sort (lastupdate | text |theme | subtheme | groupname) #datasets not sorted by theme|subtheme|groupname?
    epsg
    box
    theme, subtheme, groupname - category
    query - keyword

    format
    web service (wms|wcs|wfs)
    taxonomy
    geomtype

    /search/datasets.json?query=property&offset=0&sort=lastupdate&dir=desc&limit=15&theme=Boundaries&subtheme=General&groupname=New+Mexico

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    ext = request.matchdict['ext']
    app = request.matchdict['app']

    doctypes = request.matchdict['doctypes']

    #reset doctypes from the route-required plural to the doctype-required singular
    doctypes = ','.join([dt[:-1] for dt in doctypes.split(',')])

    params = normalize_params(request.params)

    #get version (not for querying, just for the output) 
    version = int(params.get('version')) if 'version' in params else 2

    #and we still like the limit here
    limit = int(params['limit']) if 'limit' in params else 15

    #set up the elasticsearch search object
    searcher = EsSearcher(
        {
            "host": request.registry.settings['es_root'], 
            "index": request.registry.settings['es_dataset_index'], 
            "type": doctypes, 
            "user": request.registry.settings['es_user'].split(':')[0], 
            "password": request.registry.settings['es_user'].split(':')[-1]
        }
    )
    try:
        searcher.parse_basic_query(app, params)
    except Exception as ex:
        return HTTPBadRequest(json.dumps({"query": searcher.query_data, "msg": ex.message}))

    if 'check' in params:
        #for testing - get the elasticsearch json request
        return Response(json.dumps({"search": searcher.get_query(), "url": searcher.es_url}), content_type = 'application/json')

    try:
        searcher.search()
    except Exception as ex:
        return HTTPServerError(ex.message)

    base_url = request.registry.settings['BALANCER_URL']
    
    return generate_search_response(searcher, request, app, limit, base_url, ext, version)

@view_config(route_name='search_within_collection')
def search_within_collection(request):
    """

    search for datasets within a single collection object

    - by date range
    - by bbox
    - by keywords
    - by category?

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    app = request.matchdict['app']
    collection_id = request.matchdict['id']
    ext = request.matchdict['ext']

    c = get_collection(collection_id)
    if not c:
        return HTTPNotFound()

    params = normalize_params(request.params)

    #get version (not for querying, just for the output) 
    version = int(params.get('version')) if 'version' in params else 2

    #and we still like the limit here
    limit = int(params['limit']) if 'limit' in params else 15
    
    #set up the elasticsearch search object
    searcher = CollectionWithinSearcher(
        {
            "host": request.registry.settings['es_root'], 
            "index": request.registry.settings['es_dataset_index'], 
            "type": 'dataset', 
            "user": request.registry.settings['es_user'].split(':')[0], 
            "password": request.registry.settings['es_user'].split(':')[-1]
        }
    )
    try:
        searcher.parse_basic_query(app, params)
        searcher.update_collection_filter(c.uuid)
    except:
        return HTTPBadRequest()

    if 'check' in params:
        #for testing - get the elasticsearch json request
        return Response(json.dumps({"search": searcher.get_query(), "url": searcher.es_url}), content_type = 'application/json')

    try:
        searcher.search()
    except Exception as ex:
        return HTTPServerError()

    base_url = request.registry.settings['BALANCER_URL']
    
    return generate_search_response(searcher, request,app, limit, base_url, ext, version)

@view_config(route_name='searches', match_param="doctypes=nm_quads", renderer='json')
@view_config(route_name='searches', match_param="doctypes=nm_gnis", renderer='json')
@view_config(route_name='searches', match_param="doctypes=nm_counties", renderer='json')  
def search(request):
    """

    quad = /search/geolookups.json?query=albuquer&layer=nm_quads&limit=20
    placename = /search/geolookups.json?query=albu&layer=nm_gnis&limit=20

    Notes:
   
    Args:
        
    Returns:
    
    Raises:
    """
    geolookup = request.matchdict['doctypes']

    #pagination
    limit = int(request.params.get('limit')) if 'limit' in request.params else 25
    offset = int(request.params.get('offset')) if 'offset' in request.params else 0

    #sort direction
    sortdir = request.params.get('dir').upper() if 'dir' in request.params else 'DESC'
    direction = 1 if sortdir == 'DESC' else 0

    #keyword
    keyword = request.params.get('query') if 'query' in request.params else ''
    keyword = keyword.replace('+', ' ') if keyword else keyword
    
    #get the epsg for the returned results
    epsg = request.params.get('epsg') if 'epsg' in request.params else ''

    order_clause = geolookups.c.description.asc() if direction else geolookups.c.description.desc()
    
    #TODO: add the rest of the filtering
    keyword = '%'+keyword+'%'
    geos = DBSession.query(geolookups).filter(geolookups.c.what==geolookup).filter(or_(geolookups.c.description.ilike(keyword), "array_to_string(aliases, ',') like '%s'" % keyword)).order_by(order_clause).limit(limit).offset(offset)
    #dump the results
    #TODO: check for anything weird about the bbox (or deal with reprojection, etc)
    response = Response(json.dumps({'results': [{'text': g.description, 'box': [float(b) for b in g.box]} for g in geos]}))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.content_type="application/json"    
    return response


#TODO: finish this
@view_config(route_name='search_features', renderer='json')
def search_features(request):
    """

    return a listing of fids that match the filters (for potentially some interface later or as an option to the streamer)

    return fids for the features that match the params
    this is NOT the streamer (see views.features)

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    app = request.matchdict['app']

    params = normalize_params(request.params)
    
    #pagination
    limit = int(params.get('limit')) if 'limit' in params else 25
    offset = int(params.get('offset')) if 'offset' in params else 0

    #check for valid utc datetime
    start_valid = params.get('valid_start') if 'valid_start' in params else ''
    end_valid = params.get('valid_end') if 'valid_end' in params else ''

    #sort parameter
    #TODO: sort params for features - by param or dataset or what?
    sort = params.get('sort') if 'sort' in params else 'observed'
    if sort not in ['observed']:
        return HTTPNotFound()

    #geometry type so just points, polygons or lines or something
    geomtype = params.get('geomtype', '')

    #sort direction
    sortdir = params.get('dir', 'desc').upper()
    direction = 0 if sortdir == 'DESC' else 1
    
    #sort geometry
    box = params.get('box', '')
    epsg = params.get('epsg', '') 

    #category search
    theme = params.get('theme', '')
    subtheme = params.get('subtheme', '')
    groupname = params.get('groupname', '')

    #parameter search
    #TODO: add the other bits to this and implement it
    param = params.get('param', '')
    frequency = params.get('freq', '')
    units = params.get('units', '')

    #need to have all three right now?
    if param and not frequency and not units:
        return HTTPNotFound()
    
    #go for the dataset query first UNLESS there's a list of datasets
    #then ignore geomtype, theme/subtheme/groupname
    dataset_clauses = [Dataset.inactive==False, "'%s'=ANY(apps_cache)" % (app)]
    if geomtype and geomtype.upper() in ['POLYGON', 'POINT', 'LINESTRING', 'MULTIPOLYGON', '3D POLYGON', '3D LINESTRING']:
        dataset_clauses.append(Dataset.geomtype==geomtype.upper())

    #and the valid data range to limit the datasets
    if start_valid or end_valid:
        c = get_overlap_date_clause(Dataset.begin_datetime, Dataset.end_datetime, start_valid, end_valid)
        if c is not None:
            dataset_clauses.append(c)

    query = DBSession.query(Dataset.id).filter(and_(*dataset_clauses))

    category_clauses = []
    if theme:
        category_clauses.append(Category.theme.ilike(theme))
    if subtheme:
        category_clauses.append(Category.subtheme.ilike(subtheme))
    if groupname:
        category_clauses.append(Category.groupname.ilike(groupname))

    #join to categories if we need to
    if category_clauses:
        query = query.join(Dataset.categories).filter(and_(*category_clauses))

    dataset_ids = []

    #need to go get the datasets
    dataset_ids = [d.id for d in query]

    shp_fids = []
    shape_clauses = []
    if dataset_ids:
        #TODO: actually, if it's not bbox related, just push to mongo (it seems quicker with the number of ids)
        shape_clauses.append(Feature.dataset_id.in_(dataset_ids))

    if box:
        #or go hit up shapes, bad idea, very bad idea
        srid = int(request.registry.settings['SRID'])
        #make sure we have a valid epsg
        epsg = int(epsg) if epsg else srid
        
        #convert the box to a bbox
        bbox = string_to_bbox(box)

        #and to a geom
        bbox_geom = bbox_to_geom(bbox, epsg)

        #and reproject to the srid if the epsg doesn't match the srid
        if epsg != srid:
            reproject_geom(bbox_geom, epsg, srid)

        #now intersect on shapes and with dataset_id in dataset_ids
        shape_clauses.append(func.st_intersects(func.st_setsrid(Feature.geom, srid), func.st_geometryfromtext(geom_to_wkt(bbox_geom, srid))))

    #just return the fid field. makes it much faster (geoms are big) and defer is not fast, either.
    shps = DBSession.query(Feature.fid).filter(and_(*shape_clauses))
    shp_fids = [s.fid for s in shps]

    mongo_fids = []
    #TODO: add the attribute part to this (if att.name == x and att.val != null or something)
    #TODO: ADD DATETIME clause builder for before, after, between 
    if start_valid or end_valid:
        #go hit up mongo, high style    
        connstr = request.registry.settings['mongo_uri']
        collection = request.registry.settings['mongo_collection']
        mongo_uri = gMongoUri(connstr, collection)
        gm = gMongo(mongo_uri)
  
        mongo_clauses = {'d.id': {'$in': dataset_ids}}

        #add the date clauses
        #db.tests.find({$and: [{s: {$lte: end}}, {e: {$gte: start}}]})
        #haha don't care. observed is a singleton
        #d: {$gte: start, $lt: end}

        #TODO: check date format
        if start_valid and end_valid:
            mongo_clauses.append({'obs': {'$gte': start_valid, '$lte': end_valid}})
        elif start_valid and not end_valid:
            mongo_clauses.append({'obs': {'$gte': start_valid}})
        elif not start_valid and end_valid:
            mongo_clauses.append({'obs': {'$lte': end_valid}})

        #need to set up the AND
        if len(mongo_clauses) > 1:
            mongo_clauses = {'$and': mongo_clauses}

        #run the query and just return the fids (we aren't interested in anything else here)
        vectors = gm.query(mongo_clauses, {'f.id': 1})
        #and convert to a list without the objectids
        mongo_fids = [v['f']['id'] for v in vectors]
    
    #intersect the two lists IF there's something in both
    if shp_fids and not mongo_fids:
        fids = shp_fids
    elif not shp_fids and mongo_fids:
        fids = mongo_fids
    else:
        shp_set = set(shp_fids)
        fids = shp_set.intersection(mongo_fids)
        fids = list(fids)

    #return a honking big list
    s = offset
    e = limit + offset

    #and run the offset, limit against the list
    return {'total': len(fids), 'features': fids[s:e]}


'''
search output options
'''    
class StreamDoctypeJson():
    """generate the json output for the search results

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    head = '{"total": %s, "subtotal": %s, "results": ['
    tail = ']}'
    
    def __init__(self, app, base_url, request, total, version, subtotal):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        self.app = app
        self.base_url = base_url
        self.request = request
        self.head = self.head % (total, subtotal)
        self.version = version

        

    def yield_set(self, object_tuples, limit):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        yield self.head

        cnt = 0

        for object_tuple in object_tuples:
            if object_tuple[1] == 'dataset' and self.version == 2:
                to_yield = json.dumps(self.build_v2(object_tuple)) + ','
            elif self.version == 3:
                to_yield = json.dumps(self.build_v3(object_tuple)) + ','
            else:
                to_yield = '{},'

            if cnt == limit - 1:
                to_yield = to_yield[:-1]

            cnt += 1
            yield to_yield

        yield self.tail

        #to close the conn and release the postgres locks
        #THE FACTORY DOES NOT WORK WITH THE STREAMING RESPONSES!@#!#$
        #TODO: see kml note
        DBSession.close()

    #TODO: deprecate this.
    def build_v2(self, object_tuple):
        """
        
        haha. nope.

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """

        if object_tuple[1] != 'dataset':
            return {}

        d = get_dataset(object_tuple[0])
        if not d:
            return {}

        services = d.get_services(request)
        fmts = d.get_formats(request)

        tools = [0 for i in range(6)]
        if fmts:
            tools[0] = 1
        if d.taxonomy in ['vector', 'geoimage']:
            tools[1] = 1
            tools[2] = 1
            tools[3] = 1
        if d.has_metadata_cache:
            tools[2] = 1 

        return {"text": d.description, "categories": '%s__|__%s__|__%s' % 
                                (d.categories[0].theme, d.categories[0].subtheme, d.categories[0].groupname),
                                "config": {"id": d.id, "what": "dataset", "taxonomy": d.taxonomy, "formats": fmts, "services": services, "tools": tools},
                                "box": [float(b) for b in d.box] if d.box else [], "lastupdate": d.dateadded.strftime('%d%m%D')[4:], "id": d.id}
        
    def build_v3(self, object_tuple):
        """

        still ridiculous

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        o = None
        if object_tuple[1] == 'collection':
            o = get_collection(object_tuple[0])
        elif object_tuple[1] == 'dataset':
            o = get_dataset(object_tuple[0])

        if not o:
            return {}

        return o.get_full_service_dict(self.base_url, self.request, self.app)

class StreamDoctypeKml():
    """generate the kml output for the search results

    Notes:
        
    Atrributes:

    """

    head = """<?xml version="1.0" encoding="UTF-8"?>\n<kml xmlns="http://earth.google.com/kml/2.2">\n<Document>"""

    tail = """\n</Document>\n</kml>"""

    folder_head = """<Folder><name>Search Results</name>"""

    folder_tail = """</Folder>"""

    field_tmpl = '<SimpleField type="string" name="%(fieldname)s"><displayName>%(displayname)s</displayName></SimpleField>'
    data_tmpl = '<SimpleData name="%(fieldname)s">%(data)s</SimpleData>'

    default_fields = [
        ("Type", "Data Type"),
        ("UUID", "UUID"),
        ("Name", "Name"),
        ("Category", "Category"),
        ("Services", "Service URL")
    ]
    
    def __init__(self, app, base_url, fields=[]):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        self.app = app
        self.base_url = base_url
        self.fields = fields #as a list of tuples (name, display)
        self.field_set = self.generate_fields()

    def generate_fields(self):
        """

        build the field schema for the set of fields
        of course, if it's not the default fields, the builder will be wrong

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        flds = self.fields if self.fields else self.default_fields
        return '<Schema name="searchFields">' + ''.join([self.field_tmpl % {'fieldname': f[0], 'displayname': f[1]} for f in flds]) + '</Schema>'

    def yield_set(self, object_tuples, limit):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        yield self.head

        cnt = 0

        for obj in object_tuples:
            kml = self.build_item(obj)
            
            if cnt == 0:
                kml = self.folder_head + self.field_set + kml + '\n'
            elif cnt == limit - 1:
                kml += self.folder_tail
            else:
                kml += '\n'

            cnt += 1
            yield kml.encode('utf-8')

        yield self.tail

        #to close the conn and release the postgres locks
        #THE FACTORY DOES NOT WORK WITH THE STREAMING RESPONSES!@#!#$
        #TODO: except it crashed the other streamer so it may not be necessary here
        DBSession.close()


    def build_item(self, object_tuple):
        """

        get the bits we need for the field set

        hooray for consistency

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """

        data = {}
        if object_tuple[1] == 'dataset':
            d = get_dataset(object_tuple[0])
            if not d:
                return ''

            if d.taxonomy in ['table']:
                return ''

            bbox = string_to_bbox(d.box)
            geom = d.geom

            uuid = d.uuid
            obj_id = d.id
            description = d.description
            dateadded = d.dateadded

            obj_data = {"Type": "Dataset", "UUID": uuid, "Name": description, "Category": ' | '.join([d.categories[0].theme, d.categories[0].subtheme, d.categories[0].groupname]), "Services": self.base_url+ build_service_url(self.app, 'datasets', uuid)}
            
        elif object_tuple[1] == 'collection':
            c = get_collection(object_tuple[0])
            if not c:
                return ''

            bbox = string_to_bbox(c.bbox)
            geom = c.bbox_geom

            obj_id = c.id
            uuid = c.uuid
            description = c.name 
            dateadded = c.date_added   

            obj_data = {"Type": "Collection", "UUID": uuid, "Name": description, "Category": ' | '.join([c.categories[0].theme, c.categories[0].subtheme, c.categories[0].groupname]), "Services": self.base_url+ build_service_url(self.app, 'collections', uuid)}
        else:
            return ''

        if not check_for_valid_extent(bbox):
            geom = wkt_to_geom('POINT (%s %s)' % (bbox[0], bbox[1]), 4326)
            geom = geom_to_wkb(geom)

        data = {"id": obj_id, "uuid": uuid, "description": description, "data": obj_data, "dateadded": dateadded}            

        return self.build_feature(data, geom)


    def build_feature(self, object_data, object_geometry):
        """generate the kml chunk

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """

        geom_repr = wkb_to_output(object_geometry, 4326, 'kml')

        field_data = object_data['data']
        data = '\n'.join(["""<SimpleData name="%(fieldname)s">%(fielddata)s</SimpleData>""" % {'fieldname': k, 'fielddata': v} for k, v in field_data.iteritems()])

        feature = """<Placemark id="%s">
                    <name>%s</name>
                    <TimeStamp><when>%s</when></TimeStamp>
                    %s\n
                    <ExtendedData><SchemaData schemaUrl="#searchFields">%s</SchemaData></ExtendedData>
                    <Style><LineStyle><color>ff0000ff</color></LineStyle><PolyStyle><fill>0</fill></PolyStyle></Style>
                    </Placemark>""" % (object_data['id'], object_data['description'].replace('&', '&amp;') if '&amp;' not in object_data['description'] else object_data['description'], object_data['dateadded'].strftime('%Y-%m-%d'), geom_repr, data)

        return feature























           
