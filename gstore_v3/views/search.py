from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound

from sqlalchemy import desc, asc
from sqlalchemy.sql.expression import and_

import json

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    Category
    )

from ..models.vocabs import geolookups

'''
search
'''
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
    (what exactyl would be the point?)
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
#maybe not renderer - firefox open with? 
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
    sort (lastupdate | text |theme | subtheme | groupname)
    epsg
    box
    theme, subtheme, groupname - category
    query - keyword


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

    #sort parameter
    sort = request.params.get('sort') if 'sort' in request.params else 'lastupdate'
    if sort not in ['lastupdate', 'text', 'theme', 'subtheme', 'groupname']:
        return HTTPNotFound('Bad sort parameter')
    sort = 'dateadded' if sort == 'lastupdate' else sort
    sort = 'description' if sort == 'text' else sort

    #sort direction
    sortdir = request.params.get('dir').upper() if 'dir' in request.params else 'DESC'
    direction = 1 if sortdir == 'DESC' else 0

    #keyword search
    keyword = request.params.get('query') if 'query' in request.params else ''
    keyword = keyword.replace(' ', '%').replace('+', '%')

    #TODO: set up for the georelevance sorting
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

    fltr = ""
    if theme:
        fltr = "theme='%s'" % theme
    if subtheme:
        fltr += " and subtheme='%s'" % subtheme
    if groupname:
        fltr += " and groupname='%s'" % groupname

    datasets = DBSession.query(Dataset).join(Dataset.categories).filter(Category.theme==theme).filter(and_(Dataset.inactive==False, Dataset.is_available==True)).filter("'%s'=ANY(apps)" % (app))

    #.order_by(Dataset.dateadded.desc()).limit(limit).offset(offset)
    
    if not datasets:
        return {"total": 0, "results": []}
        
    #TODO: revise output format for v3
    #TODO: revise output format for is_available T/F (if F no downloads, no services)

    rsp = {"total": datasets.count()}
    results = []

    #and run the limit/offset/sort
    datasets = datasets.order_by(Dataset.dateadded.desc()).limit(limit).offset(offset)

    if version == 2:
        '''
        {"box": [-109.114059, 31.309483, -102.98925, 37.044096000000003], "lastupdate": "02/29/12", "gr": 0.0, "text": "NM Property Tax Rates - September 2011", "config": {"what": "dataset", "taxonomy": "vector", "formats": ["zip", "shp", "gml", "kml", "json", "csv", "xls"], "services": ["wms", "wfs"], "tools": [1, 1, 1, 1, 0, 0], "id": 130043}, "id": 130043, "categories": "Boundaries__|__General__|__New Mexico"}
        ''' 

        #TODO: deal with georelevance
        for d in datasets:
            

            #TODO: not this REVISE 
            tools = [0 for i in range(6)]
            if d.formats_cache:
                tools[0] = 1
            if d.taxonomy in ['vector', 'geoimage']:
                tools[1] = 1
                tools[2] = 1
                tools[3] = 1
            #if d.has_metadata: #NOT IN THE MODEL NOW AND REVISE
            tools[2] = 1

            #TODO: also not this
            services = ['wms', 'wfs'] if d.taxonomy == 'vector' else ['wms', 'wcs']
            services = [] if d.taxonomy == 'file' else services

            #TODO: and maybe not even this
            fmts = d.formats_cache.split(',')
                
            #let's build some json
            results.append({"text": d.description, "categories": '%s__|__%s__|__%s' % (d.categories[0].theme, d.categories[0].subtheme, d.categories[0].groupname),
                            "config": {"id": d.id, "what": "dataset", "taxonomy": d.taxonomy, "formats": fmts, "services": services, "tools": tools},
                            "box": [float(b) for b in d.box], "lastupdate": d.dateadded.strftime('%d%m%D')[4:], "id": d.id, "gr": 0.0})
        
    elif version == 3:
        '''
        new format
        '''
        for d in datasets:
            #let's build some json
            results.append({"id": d.id, "uuid": d.uuid, "dateadded": d.dateadded.strftime('%Y-%m-%d'), "description": d.description,
                            "apps": d.apps_cache, "categories": [{"theme": t.theme, "subtheme": t.subtheme, "groupname": t.groupname} for t in d.categories]})

    rsp.update({"results": results})
    return rsp


#return features (as fids)
@view_config(route_name='search', match_param='resource=features')
def search_features(request):
    #pagination
    limit = int(request.params.get('limit')) if 'limit' in request.params else 1000
    offset = int(request.params.get('offset')) if 'offset' in request.params else 0

    #check for valid utc datetime
    start_valid = request.params.get('valid_start') if 'valid_start' in request.params else ''
    end_valid = request.params.get('valid_end') if 'valid_end' in request.params else ''

    #sort parameter
    #TODO: sort params for features - by param or dataset or what?
    sort = request.params.get('sort') if 'sort' in request.params else 'observed'
    if sort not in ['observed']:
        return HTTPNotFound('Bad sort parameter')


    #sort direction
    sortdir = request.params.get('dir').upper() if 'dir' in request.params else 'DESC'
    direction = 1 if sortdir == 'DESC' else 0
    
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

    #get the epsg for the returned results
    epsg = request.params.get('epsg') if 'epsg' in request.params else ''

    #TODO: add the rest of the filtering
    geos = DBSession.query(geolookups).filter(geolookups.c.what==geolookup).filter(geolookups.c.description.ilike('%'+keyword+'%'))

    #dump the results
    #TODO: check for anything weird about the bbox (or deal with reprojection, etc)
    return {'results': [{'text': g.description, 'box': [float(b) for b in g.box]} for g in geos]}
