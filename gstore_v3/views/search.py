from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPServerError

import sqlalchemy
from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_, or_, cast
from sqlalchemy.sql import between

import json
from datetime import datetime

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    Category
    )
from ..models.features import Feature

from ..models.vocabs import geolookups
from ..lib.spatial import *
from ..lib.mongo import *
from ..lib.utils import normalize_params, convert_timestamp, get_single_date_clause, get_overlap_date_clause, match_pattern
from ..lib.database import get_dataset

#starting with requests instead - unclear if you can concat query + query_raw to handle the dot field name concats
#from elasticutils import S, F

import requests

'''
search
'''
#return the category tree
@view_config(route_name='search_categories', renderer='json')
#@view_config(route_name='search', match_param='resource=datasets', request_param='categories=1', renderer='json')
def search_categories(request):
    #TODO: allow other formats (kml, etc) 
    #IT IS POST FROM RGIS FOR SOME REASON
    #get the starting location for the tree
    #root OR Census Data OR Census Data__|__2008 TIGER
    #root OR theme OR theme__|__subtheme
    app = request.matchdict['app']

    params = normalize_params(request.params)
    node = params.get('node', '')

    '''
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
    '''

    #set up the elasticsearch connection
    es_connection = request.registry.settings['es_root']
    es_index = request.registry.settings['es_dataset_index']
    #TODO: change this to the combined search options
    es_type = 'dataset'
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
            resp = {'total': 0, 'results': []}

            level = 3
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
        resp = {'total': 0, 'results': []} 
    else:
        facets = data['facets']['categories']['terms']
        resp = {"total": len(facets)}
        rslts = []
        if level == 0:
            rslts = [{"text": facet['term'], "leaf": False, "id": facet['term']} for facet in facets]
        elif level == 1:
            rslts = [{"text": facet['term'], "leaf": False, "id": '%s__|__%s' % (parts[0], facet['term'])} for facet in facets]
        elif level == 2:
            rslts = [{"text": facet['term'], "True": False, "cls": "folder", "id": '%s__|__%s__|__%s' % (parts[0], parts[1], facet['term'])} for facet in facets]
        resp.update({"results": rslts})

    response = Response(json.dumps(resp))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.content_type="application/json"    
    return response

#return datasets
#TODO: maybe not renderer - firefox open with?   
#@view_config(route_name='search', match_param='resource=datasets', renderer='json')
@view_config(route_name='search_datasets')
def search_datasets(request):
    '''
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

    uuid


    /search/datasets.json?query=property&offset=0&sort=lastupdate&dir=desc&limit=15&theme=Boundaries&subtheme=General&groupname=New+Mexico
    '''
    ext = request.matchdict['ext']
    app = request.matchdict['app']

    params = normalize_params(request.params)

    #pagination
    limit = int(params.get('limit')) if 'limit' in params else 15
    offset = int(params.get('offset')) if 'offset' in params else 0

    #get version 
    version = int(params.get('version')) if 'version' in params else 2

    #category params
    theme = params.get('theme') if 'theme' in params else ''
    subtheme = params.get('subtheme') if 'subtheme' in params else ''
    groupname = params.get('groupname') if 'groupname' in params else ''

    theme = theme.replace('+', ' ')
    subtheme = subtheme.replace('+', ' ' )
    groupname = groupname.replace('+', ' ' )
    
    #check for valid utc datetime
    start_added = params.get('start_time') if 'start_time' in params else ''
    end_added = params.get('end_time') if 'end_time' in params else ''

    #check for valid utc datetime
    start_valid = params.get('valid_start') if 'valid_start' in params else ''
    end_valid = params.get('valid_end') if 'valid_end' in params else ''

    #TODO: add the uuid back in (but as a prefix? search)
    #check for uuid (and append wildcard if not match to the regex)
    search_uuid = params.get('uuid', '')


    format = params.get('format', '')
    taxonomy = params.get('taxonomy', '')
    geomtype = params.get('geomtype', '').replace('+', ' ')
    service = params.get('service', '')


    sort = params.get('sort') if 'sort' in params else 'lastupdate'
    if sort not in ['lastupdate', 'description', 'geo_relevance']:
        return HTTPNotFound()

    sort = 'date_added' if sort == 'lastupdate' else sort
    sort = 'title' if sort == 'description' else sort

    sortdir = params.get('dir').upper() if 'dir' in params else 'DESC'
    order = 'desc' if sortdir == 'DESC' else 'asc'

    #TODO: needs a better query param structure (also +, -, etc, for es)
    keyword = params.get('query') if 'query' in params else ''
    keyword = keyword.replace('+', ' ')


    box = params.get('box') if 'box' in params else ''
    epsg = params.get('epsg') if 'epsg' in params else ''

    #set up the elasticsearch connection
    es_connection = request.registry.settings['es_root']
    es_index = request.registry.settings['es_dataset_index']
    #TODO: modify this to run multiple doctypes (like dataset and collections)
    es_type = 'dataset'
    es_user = request.registry.settings['es_user'].split(':')[0]
    es_password = request.registry.settings['es_user'].split(':')[-1]

    es_url = es_connection + es_index + '/' + es_type + '/_search'

    #set up the json for the request
    #where we only want the _ids (uuids) back
    query_request = {"size": limit, "from": offset, "fields": ["_id"]}   

    #TODO: this should probably be revised to not have two sets of filtered
    #filtered = {"query": {"term": {"applications": app.lower()}}}
    filtered = {}

    #set up the filters, with the mandatory app part
    #currently everything is AND
    f_and = [{"term": {"applications": app.lower()}}, {"term": {"embargo": False}}, {"term": {"active": True}}]
    
    #add some category stuff
    if theme:
        f_and.append({"query": {"match": {"category.theme": {"query": theme, "operator": "and"}}}})
    if subtheme:
        f_and.append({"query": {"match": {"category.subtheme": {"query": subtheme, "operator": "and"}}}})
    if groupname:
        f_and.append({"query": {"match": {"category.groupname": {"query": groupname, "operator": "and"}}}})

    if format:
        f_and.append({"query": {"term": {"formats": format.lower()}}})

    if service:
        f_and.append({"query": {"term": {"services": service.lower()}}})

    if taxonomy:
        f_and.append({"query": {"term": {"taxonomy": taxonomy.lower()}}})

        #NOTE: geomtype is not currently in the indexed docs
        if geomtype and geomtype.upper() in ['POLYGON', 'POINT', 'LINESTRING', 'MULTIPOLYGON', '3D POLYGON', '3D LINESTRING']:
            f_and.append({"query": {"term": {"geomtype": geomtype.lower()}}})

    if keyword:
        #TODO: this may be a little extreme
#        f_and.append({"query": {"match": {"title": {"query": keyword, "operator": "and"}}}})
#        f_and.append({"query": {"match": {"aliases": }}})
        key_search = {
            "query": {
                
                    "filtered": {
                        "filter": {
                            "or": [
                                {"query": {"match": {"title": {"query": keyword, "operator": "and"}}}},
                                {"query": {"match": {"aliases": {"query": keyword, "operator": "and"}}}}
                            ]
                        }
                    }
                }
        }
        f_and.append(key_search)

    #fun with dates
    dfmt = '%Y-%m-%d'
    if start_added or end_added:
        range_request = {}
        started = convert_timestamp(start_added)
        ended = convert_timestamp(end_added)
        if started and not ended:
            range_request.update({"gte": started.strftime(dfmt)})
        if not started and ended:
            range_request.update({"lte": ended.strftime(dfmt)})
        if started and ended:
            range_request.update({"gte": started.strftime(dfmt), "lte": ended.strftime(dfmt)})
        f_and.append({"range": {"date_added": range_request}})

    #TODO: this is not actually in the indexes
#    if start_valid or end_valid:    
#        range_request = {}
#        if start_valid and not end_valid:
#            range_request.update({"gte": convert_timestamp(start_valid).strftime(dfmt)})
#        if not start_valid and end_valid:
#            range_request.update({"lte": convert_timestamp(start_valid).strftime(dfmt)})
#        if start_valid and end_valid:
#            range_request.update({"from": convert_timestamp(start_valid).strftime(dfmt), "to": convert_timestamp(start_valid).strftime(dfmt)})
#        f_and.append({"range": {"date_valid": range_request}})


    #set up the initial sort
    sort_arr = [{"dataset." + sort : {"order": order.lower()}}]
    if sort != 'title':
        sort_arr.append({"dataset.title": {"order": "asc"}})
    s = {"sort": sort_arr}

    
    search_area = 0.  #ha, this is so bad (div by zero later)
    spatial_search = False
    #Like nailing jelly to kittens
    if box:
        #build the query for the bbox search
        srid = int(request.registry.settings['SRID'])
        epsg = int(epsg) if epsg else srid

        #TODO: could probably do this with a to_geojson ogr method. (the output polygon array, i mean)
        bbox = string_to_bbox(box)
        bbox_geom = bbox_to_geom(bbox, epsg)
        if epsg != srid:
            reproject_geom(bbox_geom, epsg, srid)

        search_area = bbox_geom.Area()

        coords = [[[bbox[0], bbox[1]],[bbox[0],bbox[3]],[bbox[2],bbox[3]],[bbox[2],bbox[1]],[bbox[0],bbox[1]]]]
        
        geo_shape = {
            "location.bbox" : {
                "shape": {
                    "type": "Polygon",
                    "coordinates": coords
                }
            }
        }

        f_and.append({"geo_shape": geo_shape})

        #add the sort by georelevance
        s = {"sort": [{"_score": order.lower()}]}
        spatial_search = True

    #finish building the POST data
    if f_and:
        #don't include an empty AND array (returns no results)
        filtered.update({"filter": {"and": f_and}})
        
    if spatial_search:
        #need to wrap the one query in a custom_score query widget instead
        i_query = {
            "custom_score": {
                "query": {"filtered": filtered},
                "params": {
                    "search_area": search_area
                },
                "script": "_source.dataset.area / search_area"
            } 
        }

        query_request.update({"query": i_query})
    else:
        query_request.update({"query": {"filtered": filtered}})

    #add the sort
    query_request.update(s)
    

    if 'check' in params:
        #for testing - get the elasticsearch json request
        return Response(json.dumps({"search": query_request, "url": es_url}), content_type = 'application/json')

    results = requests.post(es_url, data=json.dumps(query_request), auth=(es_user, es_password))

    if results.status_code != 200:
        #return HTTPServerError('%s, %s' % (results.status_code, results.text))
        return Response(json.dumps({"total": 0, "results": []}), content_type = 'application/json')
    
    total = results.json()['hits']['total'] if 'hits' in results.json() else 0

    if total < 1:
        #TODO: add the cors to this
        return Response(json.dumps({"total": 0, "results": []}), content_type = 'application/json')

    #TODO: figure out what to do if, horrifically, the es uuid count does not match the dataset count
    dataset_ids = [i['_id'] for i in results.json()['hits']['hits']]

    has_georel = False

    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

    #NOTE: calls to get_current_registry during the app_iter yield part returns NONE so there's an error and we can't get what we need
    head = """{"total": %s, "results": [""" % (total)
    tail = ']}'

    if ext=='kml':
        head = """<?xml version="1.0" encoding="UTF-8"?>
                        <kml xmlns="http://earth.google.com/kml/2.2">
                        <Document>"""
        tail = """\n</Document>\n</kml>"""
        folder_head = "<Folder><name>Search Results</name>"
        folder_tail = "</Folder>"
        field_set = """<Schema name="searchFields"><SimpleField type="string" name="DatasetUUID"><displayName>Dataset UUID</displayName></SimpleField><SimpleField type="string" name="DatasetName"><displayName>Dataset Name</displayName></SimpleField><SimpleField type="string" name="Category"><displayName>Category</displayName></SimpleField><SimpleField type="string" name="DatasetServices"><displayName>Dataset Information</displayName></SimpleField></Schema>"""

    subtotal = len(dataset_ids)
    if subtotal < 1:
        if ext == 'json':
            return {"total": 0, "results": []}
        else:
            #TODO: return empty kml set
            return Response()
    limit = subtotal if subtotal < limit else limit

    def yield_results():
        #query for the datasets
        datasets = DBSession.query(Dataset).filter(Dataset.uuid.in_(dataset_ids))
    
    
        #note: georelevance is added not as an extra field but as the second element in a tuple. the first element is the dataset object. hence the wonkiness.
        if version == 2:
            '''
            {"box": [-109.114059, 31.309483, -102.98925, 37.044096000000003], "lastupdate": "02/29/12", "gr": 0.0, "text": "NM Property Tax Rates - September 2011", "config": {"what": "dataset", "taxonomy": "vector", "formats": ["zip", "shp", "gml", "kml", "json", "csv", "xls"], "services": ["wms", "wfs"], "tools": [1, 1, 1, 1, 0, 0], "id": 130043}, "id": 130043, "categories": "Boundaries__|__General__|__New Mexico"}
            ''' 

            cnt = 0
            for d in datasets:
                #d = get_dataset(ds)
#                if not d:
#                    continue
                    
                if has_georel:
                    gr = ds[1]
                else:
                    gr = 0.0

                services = d.get_services(request)
                fmts = d.get_formats(request)
            
                #TODO: not this REVISE 
                tools = [0 for i in range(6)]
                if fmts:
                    tools[0] = 1
                if d.taxonomy in ['vector', 'geoimage']:
                    tools[1] = 1
                    tools[2] = 1
                    tools[3] = 1
                if d.has_metadata_cache:
                    tools[2] = 1

                    
                #let's build some json
                rst = json.dumps({"text": d.description, "categories": '%s__|__%s__|__%s' % 
                                (d.categories[0].theme, d.categories[0].subtheme, d.categories[0].groupname),
                                "config": {"id": d.id, "what": "dataset", "taxonomy": d.taxonomy, "formats": fmts, "services": services, "tools": tools},
                                "box": [float(b) for b in d.box], "lastupdate": d.dateadded.strftime('%d%m%D')[4:], "id": d.id, "gr": gr})

                to_yield = ''
                if cnt == 0:
                    to_yield = head

                to_yield += rst + ','

                if cnt == limit - 1:
                    to_yield = to_yield[:-1] + tail
                
                cnt += 1

                yield to_yield
        elif version == 3:
            '''
            new format
            '''
            cnt = 0
            
#            for ds in dataset_ids:
#                d = get_dataset(ds)
#                if not d:
#                    continue
            for d in datasets:
                    
                if has_georel:
                    gr = ds[1]
                else:
                    gr = 0.0
                    
                rst = d.get_full_service_dict(base_url, request)
                rst.update({'gr': gr})
                rst = json.dumps(rst)
                
                to_yield = ''
                if cnt == 0:
                    to_yield = head

                to_yield += rst + ','

                if cnt == limit - 1:
                    to_yield = to_yield[:-1] + tail

                cnt += 1
                yield to_yield                    

    def yield_kml():
        '''
        geometry = bbox
        description = html chunk with description, link to services.html(?), downloads, services?, metadata, some other stuff

        point test = http://129.24.63.115/apps/rgis/search/datasets.kml?theme=Climate&subtheme=SNOTEL&limit=100
        polygon test = http://129.24.63.115/apps/rgis/search/datasets.kml?theme=Boundaries&limit=50
        '''
        yield head

        cnt = 0

        for ds in datas:
            kml = build_kml(ds)

            if cnt == 0:
                kml = folder_head + field_set + kml + '\n'
            elif cnt == limit - 1:
                kml += folder_tail
            else:
                kml += '\n'

            cnt += 1
            yield kml.encode('utf-8')
        yield tail      
        

    def build_kml(d):
        bbox = [float(b) for b in d.box]
        geom = d.geom
        if not check_for_valid_extent(bbox):
            #the extent area == 0, it's a point so let's just use the point
            geom = wkt_to_geom('POINT (%s %s)' % (bbox[0], bbox[1]), 4326)
            geom = geom_to_wkb(geom)
        geom_repr = wkb_to_output(geom, 4326, 'kml')

        rst = d.get_full_service_dict(base_url, request)
        
        flds = """<SimpleData name="DatasetUUID">%s</SimpleData><SimpleData name="DatasetName">%s</SimpleData><SimpleData name="Category">%s</SimpleData><SimpleData name="DatasetServices">%s</SimpleData>""" % (d.uuid, d.description.replace('&', '&amp;') if '&amp;' not in d.description else d.description, rst['categories'][0]['theme'] + ' | ' + rst['categories'][0]['subtheme'] + ' | ' + rst['categories'][0]['groupname'], '/'.join([base_url, d.uuid, 'services.json']))
        
        feature = """<Placemark id="%s">
                    <name>%s</name>
                    <TimeStamp><when>%s</when></TimeStamp>
                    %s\n
                    <ExtendedData><SchemaData schemaUrl="#searchFields">%s</SchemaData></ExtendedData>
                    <Style><LineStyle><color>ff0000ff</color></LineStyle><PolyStyle><fill>0</fill></PolyStyle></Style>
                    </Placemark>""" % (d.id, d.description.replace('&', '&amp;') if '&amp;' not in d.description else d.description, d.dateadded.strftime('%Y-%m-%d'), geom_repr, flds)
                    
        return feature

    

    response = Response()
    response.headers['Access-Control-Allow-Origin'] = '*'
    if ext == 'json':
        response.content_type = 'application/json'
        response.app_iter = yield_results()
    elif ext == 'kml':
        response.content_type = 'application/vnd.google-earth.kml+xml; charset=UTF-8'
        response.app_iter = yield_kml()
    else:
        return HTTPNotFound()
    return response


#TODO: finish this
#return fids for the features that match the params
#this is NOT the streamer (see views.features)
@view_config(route_name='search_features', renderer='json')
def search_features(request):
    '''
    return a listing of fids that match the filters (for potentially some interface later or as an option to the streamer)
    '''
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

    #TODO: let's add a search by dataset uuid?
#    dataset_uuids = request.params.get('datasets', '')
#    dataset_uuids = dataset_uuids.split(',') if dataset_uuids else ''
    

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
        #TODO: actually , if it's not bbox related, just push to mongo (it seems quicker with the number of ids)
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

    #db.vectors.find({'d.id': {$in: [52208, 52209, 56282, 56350]}}, {'f.id': 1})
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


#TODO: replace this with FACET search route thing
#NOTE: we chucked the geolookups structure completely to just keep a cleaner url moving forward.
whats = ["nm_counties", "nm_gnis", "nm_quads"]
#return geolookup data
@view_config(route_name='search_geolookups', renderer='json')
def search(request):
    '''
    quad = /search/geolookups.json?query=albuquer&layer=nm_quads&limit=20
    placename = /search/geolookups.json?query=albu&layer=nm_gnis&limit=20

    current working request = http://129.24.63.66/gstore_v3/apps/rgis/search/nm_quads.json?query=albu
    '''
    geolookup = request.matchdict['geolookup']
    if geolookup not in whats:
        return HTTPNotFound()

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

    #TODO: add the rest of the filtering
    keyword = '%'+keyword+'%'
    geos = DBSession.query(geolookups).filter(geolookups.c.what==geolookup).filter(or_(geolookups.c.description.ilike(keyword), "array_to_string(aliases, ',') like '%s'" % keyword))

    #dump the results
    #TODO: check for anything weird about the bbox (or deal with reprojection, etc)
    return {'results': [{'text': g.description, 'box': [float(b) for b in g.box]} for g in geos]}
