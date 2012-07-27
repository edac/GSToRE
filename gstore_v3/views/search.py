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

from ..models.vocabs import geolookups
from ..lib.spatial import *

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
    node = request.params.get('node') if 'node' in request.params else None

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

    #pagination
    limit = int(request.params.get('limit')) if 'limit' in request.params else 25
    offset = int(request.params.get('offset')) if 'offset' in request.params else 0

    #get version 
    version = int(request.params.get('version')) if 'version' in request.params else 2

    #check for valid utc datetime
    start_added = request.params.get('start_time') if 'start_time' in request.params else ''
    end_added = request.params.get('end_time') if 'end_time' in request.params else ''

    #check for valid utc datetime
    start_valid = request.params.get('valid_start') if 'valid_start' in request.params else ''
    end_valid = request.params.get('valid_end') if 'valid_end' in request.params else ''

    #check for format
    format = request.params.get('format', '')

    #check for taxonomy
    taxonomy = request.params.get('taxonomy', '')
    
    #check for geomtype
    geomtype = request.params.get('geomtype', '')

    #TODO: add some explicit service field for this
    #check for avail services
    service = request.params.get('service', '')

    #sort parameter
    sort = request.params.get('sort') if 'sort' in request.params else 'lastupdate'
    #if sort not in ['lastupdate', 'text', 'theme', 'subtheme', 'groupname']:
    if sort not in ['lastupdate', 'text']:
        return HTTPNotFound('Bad sort parameter')
    sort = 'dateadded' if sort == 'lastupdate' else sort
    sort = 'description' if sort == 'text' else sort

    #sort direction
    sortdir = request.params.get('dir').upper() if 'dir' in request.params else 'DESC'
    #TODO: check on the sort direction (maybe backwards?)
    direction = 0 if sortdir == 'DESC' else 1

    #keyword search
    keyword = request.params.get('query') if 'query' in request.params else ''
    keyword = keyword.replace(' ', '%').replace('+', '%')

    #TODO: set up for the georelevance sorting
    #sort geometry
    box = request.params.get('box') if 'box' in request.params else ''
    epsg = request.params.get('epsg') if 'epsg' in request.params else ''

    #category params
    theme = request.params.get('theme') if 'theme' in request.params else ''
    subtheme = request.params.get('subtheme') if 'subtheme' in request.params else ''
    groupname = request.params.get('groupname') if 'groupname' in request.params else ''

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
            dataset_clauses.append(func.st_intersects(func.st_setsrid(Dataset.geom, srid), func.st_geometryfromtext(geom_to_wkt(bbox_geom, srid))))
        

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
#    rsp = {"total": datas.count()}
#    results = []

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
    

    #and run the limit/offset/sort
    datas = query.order_by(sort_clause).limit(limit).offset(offset)

    #get the host url
    host = request.host_url
    g_app = request.script_name[1:]
    base_url = '%s/%s/apps/%s/datasets/' % (host, g_app, app)

    #TODO: deal with georelevance
    #TODO: sort out yield and streaming results (this threw an error - can't return generator as response)
    #def stream_results():
    #yield """{"total": %s, "results": [""" % total

    rsp = {"total": total}
    results = []
    if version == 2:
        '''
        {"box": [-109.114059, 31.309483, -102.98925, 37.044096000000003], "lastupdate": "02/29/12", "gr": 0.0, "text": "NM Property Tax Rates - September 2011", "config": {"what": "dataset", "taxonomy": "vector", "formats": ["zip", "shp", "gml", "kml", "json", "csv", "xls"], "services": ["wms", "wfs"], "tools": [1, 1, 1, 1, 0, 0], "id": 130043}, "id": 130043, "categories": "Boundaries__|__General__|__New Mexico"}
        ''' 
        
        for d in datas:
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

            #TODO: also not this
            #services = ['wms', 'wfs'] if d.taxonomy == 'vector' else ['wms', 'wcs']
            #services = [] if d.taxonomy in ['file', 'services'] else services
            
            services = d.get_services()

            #TODO: and maybe not even this
            #fmts = d.formats_cache.split(',')
            fmts = d.get_formats()
                
            #let's build some json
            results.append({"text": d.description, "categories": '%s__|__%s__|__%s' % (d.categories[0].theme, d.categories[0].subtheme, d.categories[0].groupname),
                            "config": {"id": d.id, "what": "dataset", "taxonomy": d.taxonomy, "formats": fmts, "services": services, "tools": tools},
                            "box": [float(b) for b in d.box], "lastupdate": d.dateadded.strftime('%d%m%D')[4:], "id": d.id, "gr": 0.0})

#            yield json.dumps({"text": d.description, "categories": '%s__|__%s__|__%s' % (d.categories[0].theme, d.categories[0].subtheme, d.categories[0].groupname),
#                            "config": {"id": d.id, "what": "dataset", "taxonomy": d.taxonomy, "formats": fmts, "services": services, "tools": tools},
#                            "box": [float(b) for b in d.box], "lastupdate": d.dateadded.strftime('%d%m%D')[4:], "id": d.id, "gr": 0.0})
    elif version == 3:
        '''
        new format
        '''
        for d in datas:
            results.append(d.get_full_service_dict(base_url))
            #rsp = d.get_full_service_dict(base_url)
            #yield json.dumps(rsp)
            
    #yield "]}"

    #return stream_results()
    rsp.update({"results": results})
    return rsp


#TODO: finish this
#return fids for the features that match the params
#this is NOT the streamer (see views.features)
@view_config(route_name='search', match_param='resource=features')
def search_features(request):
    '''
    return a listing of fids that match the filters (for potentially some interface later or as an option to the streamer)
    '''
    #pagination
    limit = int(request.params.get('limit')) if 'limit' in request.params else 25
    offset = int(request.params.get('offset')) if 'offset' in request.params else 0

    #check for valid utc datetime
    start_valid = request.params.get('valid_start') if 'valid_start' in request.params else ''
    end_valid = request.params.get('valid_end') if 'valid_end' in request.params else ''

    #sort parameter
    #TODO: sort params for features - by param or dataset or what?
    sort = request.params.get('sort') if 'sort' in request.params else 'observed'
    if sort not in ['observed']:
        return HTTPNotFound('Bad sort parameter')

    #geometry type so just points, polygons or lines or something
    geomtype = request.params.get('geomtype', '')

    #sort direction
    sortdir = request.params.get('dir').upper() if 'dir' in request.params else 'DESC'
    direction = 0 if sortdir == 'DESC' else 1
    
    #sort geometry
    box = request.params.get('box') if 'box' in request.params else ''
    epsg = request.params.get('epsg') if 'epsg' in request.params else ''
    if box and epsg:
        #do stuff
        k = 0   

    #category search
    theme = request.params.get('theme') if 'theme' in request.params else ''
    subtheme = request.params.get('subtheme') if 'subtheme' in request.params else ''
    groupname = request.params.get('groupname') if 'groupname' in request.params else ''

    #parameter search
    #TODO: add the other bits to this and implement it
    param = request.params['param'] if 'param' in request.params else ''

    

    #so we'd want the intersect between the fids from the dataset queries and the fids from the mongo query
    #where we only run the mongo query if there's a valid dates? request so that we don't return them 
    #because they're in the dataset but outside the given date range
    if start_valid or end_valid:
        #load up the mongo request and intersect
        #this seems like a super bad idea
        #so let's go!
        pass

    return Response('searching the features')


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
