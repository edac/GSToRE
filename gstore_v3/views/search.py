from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound

from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_
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
from ..lib.mongo import gMongo
from ..lib.utils import normalize_params

'''
search
'''


'''
date utils
dates as yyyyMMdd{THHMMss} (date with time optional)
and UTC time - interfaces should do the conversion
'''
def convertTimestamp(in_timestamp):
    sfmt = '%Y%m%dT%H:%M:%S'
    if not in_timestamp:
        return None
    try:
        if 'T' not in in_timestamp:
            in_timestamp += 'T00:00:00'
        out_timestamp = datetime.strptime(in_timestamp, sfmt)
        return out_timestamp
    except:
        return None
#to compare a date (single column) with a search range
def getSingleDateClause(column, start_range, end_range):
    start_range = convertTimestamp(start_range)
    end_range = convertTimestamp(end_range)

    if start_range and not end_range:
        clause = column >= start_range
    elif not start_range and end_range:
        clause = column < end_range
    elif start_range and end_range:
        clause = between(column, start_range, end_range)
    else:
        clause = None
    return clause
#to compare two sets of date ranges, one in table and one from search
def getOverlapDateClause(start_column, end_column, start_range, end_range):
    start_range = convertTimestamp(start_range)
    end_range = convertTimestamp(end_range)

    if start_range and not end_range:
        clause = start_column >= start_range
    elif not start_range and end_range:
        clause = end_column < end_range
    elif start_range and end_range:
        clause = and_(start_column <= end_range, end_column >= start_range)
    else:
        clause = None
    return clause

#return the category tree
@view_config(route_name='search', match_param='resource=categories', renderer='json')
@view_config(route_name='search', match_param='resource=datasets', request_param='categories=1', renderer='json')
def search_categories(request):
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

    if node and node != 'root':
        parts = node.split('__|__')

        #TODO: deal with any html encoding (%, etc)
        
        if len(parts) == 1:
            #clicked on theme so get the distinct subthemes
            cats = DBSession.query(Category).filter("'%s'=ANY(apps)" % (app)).filter(Category.theme==parts[0]).distinct(Category.subtheme).order_by(Category.subtheme.asc()) 

            resp = {"total": 0, "results": [{"text": c.subtheme, "leaf": False, "id": '%s__|__%s' % (c.theme, c.subtheme)} for c in cats]}
        elif len(parts) == 2:
            #clicked on the subtheme
            cats = DBSession.query(Category).filter("'%s'=ANY(apps)" % (app)).filter(Category.theme==parts[0]).filter(Category.subtheme==parts[1]).order_by(Category.groupname.asc()) 

            resp = {"total": 0, "results": [{"text": c.groupname, "leaf": True, "id": '%s__|__%s__|__%s' % (c.theme, c.subtheme, c.groupname), "cls": "folder"} for c in cats]}
        else:
            #clicked on the groupname or something
            #return nothing right now, it isn't meaningful
            #and apparently not called from rgis
            resp = {'total': 0, 'results': []}
            
    else:
        #just pull all of the categories for the app
        cats = DBSession.query(Category).filter("'%s'=ANY(apps)" % (app)).distinct(Category.theme).order_by(Category.theme.asc()).order_by(Category.subtheme.asc()).order_by(Category.groupname.asc()) 
        resp = {"total": 0, "results": [{"text": c.theme, "leaf": False, "id": c.theme} for c in cats]}

    return resp

#return datasets
#TODO: maybe not renderer - firefox open with?   
@view_config(route_name='search', match_param='resource=datasets', renderer='json')
def search_datasets(request):
    '''
    PARAMS:
    limit
    offset
    dir (ASC | DESC)
    start_time
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
    '''

    app = request.matchdict['app']

    params = normalize_params(request.params)

    #pagination
    limit = int(params.get('limit')) if 'limit' in params else 25
    offset = int(params.get('offset')) if 'offset' in params else 0

    #get version 
    version = int(params.get('version')) if 'version' in params else 2

    #check for valid utc datetime
    start_added = params.get('start_time') if 'start_time' in params else ''
    end_added = params.get('end_time') if 'end_time' in params else ''

    #check for valid utc datetime
    start_valid = params.get('valid_start') if 'valid_start' in params else ''
    end_valid = params.get('valid_end') if 'valid_end' in params else ''

    #check for format
    format = params.get('format', '')

    #check for taxonomy
    taxonomy = params.get('taxonomy', '')
    
    #check for geomtype
    geomtype = params.get('geomtype', '')

    #TODO: add some explicit service field for this
    #check for avail services
    service = params.get('service', '')

    #sort parameter
    sort = params.get('sort') if 'sort' in params else 'lastupdate'
    #if sort not in ['lastupdate', 'text', 'theme', 'subtheme', 'groupname']:
    #this includes geo-relevance even though it is not used (it is part of the request from rgis though)
    if sort not in ['lastupdate', 'text', 'geo_relevance']:
        return HTTPNotFound('Bad sort parameter')
    sort = 'dateadded' if sort == 'lastupdate' else sort
    sort = 'description' if sort == 'text' else sort

    #sort direction
    sortdir = params.get('dir').upper() if 'dir' in params else 'DESC'
    #TODO: check on the sort direction (maybe backwards?)
    direction = 0 if sortdir == 'DESC' else 1

    #keyword search
    keyword = params.get('query') if 'query' in params else ''
    keyword = keyword.replace(' ', '%').replace('+', '%')

    #sort geometry
    box = params.get('box') if 'box' in params else ''
    epsg = params.get('epsg') if 'epsg' in params else ''

    #category params
    theme = params.get('theme') if 'theme' in params else ''
    subtheme = params.get('subtheme') if 'subtheme' in params else ''
    groupname = params.get('groupname') if 'groupname' in params else ''

    '''
    #from pshell
    from gstore_v3.models import *
    from sqlalchemy.sql.expression import and_
    #get the initial dataset filters
    query = DBSession.query(datasets.Dataset).filter(and_(datasets.Dataset.inactive==False, datasets.Dataset.is_available==True))
    #build up a list of filters
    clauses = [datasets.Category.theme=='Boundaries', datasets.Category.subtheme=='General', datasets.Category.groupname=='New Mexico']
    #join and filter again
    query2 = query.join(datasets.Dataset.categories).filter(and_(*clauses))

    #except for the formats (not_ func.any generates bad sql or what sqlalchemy thinks is bad sql)
    #this works
    query = DBSession.query(datasets.Dataset).filter("not 'pdf'= ANY(excluded_formats)")
    #if we want to have some set (arrays overlap)
    use this - not '{zip,kml}' && excluded_formats
    '''

    #set up the basic dataset clauses
    #always exclude deactivated datasets and the app from the url
    dataset_clauses = [Dataset.inactive==False, "'%s'=ANY(apps_cache)" % (app)]
    if format:
        #check that it's a supported format
        default_formats = get_current_registry().settings['DEFAULT_FORMATS'].split(',')
        if format not in default_formats:
            return HTTPNotFound('Invalid request') 
        #add the filter
        dataset_clauses.append("not '%s' = ANY(excluded_formats)" % format)

    if taxonomy:
        dataset_clauses.append(Dataset.taxonomy==taxonomy)

    if geomtype and geomtype.upper() in ['POLYGON', 'POINT', 'LINESTRING', 'MULTIPOLYGON', '3D POLYGON', '3D LINESTRING']:
        dataset_clauses.append(Dataset.geomtype==geomtype.upper())

    if keyword:
        dataset_clauses.append(Dataset.description.ilike('%' + keyword + '%'))     
  
    #add the dateadded
    if start_added or end_added:
        c = getSingleDateClause(Dataset.dateadded, start_added, end_added)
        if c is not None:
            dataset_clauses.append(c)

    #and the valid data range
    if start_valid or end_valid:
        c = getOverlapDateClause(Dataset.begin_datetime, Dataset.end_datetime, start_valid, end_valid)
        if c is not None:
            dataset_clauses.append(c)

    '''
    all the spatial query bits
    '''
    #TODO: move this somewhere more general for feature and feature streamer search
    #can't check for existence of binary expression widget apparently so add a flag
    georel_column = None
    has_georel = False
    if box:
        srid = int(get_current_registry().settings['SRID'])
        #make sure we have a valid epsg
        epsg = int(epsg) if epsg else srid
        
        #convert the box to a bbox
        bbox = string_to_bbox(box)

        #and to a geom
        bbox_geom = bbox_to_geom(bbox, epsg)

        #and reproject to the srid if the epsg doesn't match the srid
        if epsg != srid:
            reproject_geom(bbox_geom, epsg, srid)

        if bbox_geom:
            #TODO: look into pulling some of geoalchemy over or something
            #setsrid may not matter? but probably should
            bbox_wkt = geom_to_wkt(bbox_geom, srid)
            dataset_clauses.append(func.st_intersects(func.st_setsrid(Dataset.geom, srid), func.st_geometryfromtext(bbox_wkt)))

            georel_column = func.st_area(func.st_setsrid(Dataset.geom, srid)) / func.st_area(func.st_geometryfromtext(bbox_wkt))
            has_georel = True


    #set up the dataset query
    query = DBSession.query(Dataset).filter(and_(*dataset_clauses))

    #TODO: levels + categories? don't get it yet
    #category search
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

    #TODO : figure out why the app filter also returns objects where app == null    
    #TODO: revise output format for is_available T/F (if F no downloads, no services)

    total = query.count()
    if total < 1:
        return {"total": 0, "results": []}

    #add the georelevance bit to the results so we don't calculate it for everything we don't need
    if has_georel:
        #so add geom area / search area to create a dataset tuple of goodness
        query = query.add_columns(georel_column)

    #set up the sorting
    #TODO: theme, subtheme, groupname sorting? (or was that just for the category tree?)
    if sort:
        if sort == 'description':
            sort_clause = Dataset.description
        else:
            #run with dateadded
            sort_clause = Dataset.dateadded
        if direction == 0:
            sort_clause = sort_clause.desc()
        else:
            sort_clause = sort_clause.asc()
    else:
        #it's the descending dateadded
        sort_clause = Dataset.dateadded.desc()
    sort_clauses = [sort_clause]
    
    if has_georel:
        #add the georelevance
        sort_clauses.insert(0, georel_column.asc())

    #and run the limit/offset/sort
    datas = query.order_by(*sort_clauses).limit(limit).offset(offset)

#    #get the host url
#    host = request.host_url
#    g_app = request.script_name[1:]
#    base_url = '%s/%s/apps/%s/datasets/' % (host, g_app, app)

    load_balancer = get_current_registry().settings['BALANCER_URL']
    base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

    #TODO: sort out yield and streaming results (this threw an error - can't return generator as response)
    #def stream_results():
    #yield """{"total": %s, "results": [""" % total

    rsp = {"total": total}
    results = []
    #note: georelevance is added not as an extra field but as the second element in a tuple. the first element is the dataset object. hence the wonkiness.
    if version == 2:
        '''
        {"box": [-109.114059, 31.309483, -102.98925, 37.044096000000003], "lastupdate": "02/29/12", "gr": 0.0, "text": "NM Property Tax Rates - September 2011", "config": {"what": "dataset", "taxonomy": "vector", "formats": ["zip", "shp", "gml", "kml", "json", "csv", "xls"], "services": ["wms", "wfs"], "tools": [1, 1, 1, 1, 0, 0], "id": 130043}, "id": 130043, "categories": "Boundaries__|__General__|__New Mexico"}
        ''' 
        
        for ds in datas:
            if has_georel:
                d = ds[0]
                gr = ds[1]
            else:
                d = ds
                gr = 0.0
        
            #TODO: not this REVISE 
            tools = [0 for i in range(6)]
            if d.formats_cache:
                tools[0] = 1
            if d.taxonomy in ['vector', 'geoimage']:
                tools[1] = 1
                tools[2] = 1
                tools[3] = 1
            if d.has_metadata_cache:
                tools[2] = 1

            services = d.get_services()
            fmts = d.get_formats()
                
            #let's build some json
            results.append({"text": d.description, "categories": '%s__|__%s__|__%s' % (d.categories[0].theme, d.categories[0].subtheme, d.categories[0].groupname),
                            "config": {"id": d.id, "what": "dataset", "taxonomy": d.taxonomy, "formats": fmts, "services": services, "tools": tools},
                            "box": [float(b) for b in d.box], "lastupdate": d.dateadded.strftime('%d%m%D')[4:], "id": d.id, "gr": gr})

#            yield json.dumps({"text": d.description, "categories": '%s__|__%s__|__%s' % (d.categories[0].theme, d.categories[0].subtheme, d.categories[0].groupname),
#                            "config": {"id": d.id, "what": "dataset", "taxonomy": d.taxonomy, "formats": fmts, "services": services, "tools": tools},
#                            "box": [float(b) for b in d.box], "lastupdate": d.dateadded.strftime('%d%m%D')[4:], "id": d.id, "gr": 0.0})
    elif version == 3:
        '''
        new format
        '''
        for ds in datas:
            if has_georel:
                d = ds[0]
                gr = ds[1]
            else:
                d = ds
                gr = 0.0
            rst = d.get_full_service_dict(base_url)
            rst.update({'gr': gr})
            results.append(rst)
            #rsp = d.get_full_service_dict(base_url)
            #yield json.dumps(rsp)
            
    #yield "]}"

    #return stream_results()
    rsp.update({"results": results})
    return rsp


#TODO: finish this
#return fids for the features that match the params
#this is NOT the streamer (see views.features)
@view_config(route_name='search', match_param='resource=features', renderer='json')
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
        return HTTPNotFound('Bad sort parameter')

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
        return HTTPNotFound('Bad parameter request')
    
    #go for the dataset query first UNLESS there's a list of datasets
    #then ignore geomtype, theme/subtheme/groupname
    dataset_clauses = [Dataset.inactive==False, "'%s'=ANY(apps_cache)" % (app)]
    if geomtype and geomtype.upper() in ['POLYGON', 'POINT', 'LINESTRING', 'MULTIPOLYGON', '3D POLYGON', '3D LINESTRING']:
        dataset_clauses.append(Dataset.geomtype==geomtype.upper())

    #and the valid data range to limit the datasets
    if start_valid or end_valid:
        c = getOverlapDateClause(Dataset.begin_datetime, Dataset.end_datetime, start_valid, end_valid)
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
        srid = int(get_current_registry().settings['SRID'])
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
        connstr = get_current_registry().settings['mongo_uri']
        collection = get_current_registry().settings['mongo_collection']
        gm = gMongo(connstr, collection)

        
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


#NOTE: we chucked the geolookups structure completely to just keep a cleaner url moving forward.
whats = ["nm_counties", "nm_gnis", "nm_quads"]
#return geolookup data
@view_config(route_name='search', renderer='json')
def search(request):
    '''
    quad = /search/geolookups.json?query=albuquer&layer=nm_quads&limit=20
    placename = /search/geolookups.json?query=albu&layer=nm_gnis&limit=20

    current working request = http://129.24.63.66/gstore_v3/apps/rgis/search/nm_quads.json?query=albu
    '''
    geolookup = request.matchdict['resource']
    if geolookup not in whats:
        return HTTPNotFound('There is no such resource')

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
    geos = DBSession.query(geolookups).filter(geolookups.c.what==geolookup).filter(geolookups.c.description.ilike('%'+keyword+'%'))

    #dump the results
    #TODO: check for anything weird about the bbox (or deal with reprojection, etc)
    return {'results': [{'text': g.description, 'box': [float(b) for b in g.box]} for g in geos]}
