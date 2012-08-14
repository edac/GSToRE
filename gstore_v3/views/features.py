from pyramid.view import view_config
from pyramid.response import Response
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError

from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_
from sqlalchemy.sql import between

import json
from ..models import DBSession
from ..models.features import (
    Feature,
    )
from ..models.datasets import Dataset, Category

from ..lib.mongo import gMongo, gMongoUri
from ..lib.utils import normalize_params
from ..lib.spatial import *
from ..lib.database import get_dataset


'''
features
'''
#TODO: add formats? why would anyone want it as a shapefile though?
#TODO: update some indexes for better performance in both queries
@view_config(route_name='feature', renderer='json')
def feature(request):
    '''
    get a feature
    /apps/{app}/features/{id:[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}
    /apps/rgis/features/e74a1e0d-3e75-44dd-bb4c-3328a2425856.json


    now what did we forget? that uuids are not good indexes in mongo (too big)
    so ping shapes for the fid before pinging mongo (and then we already have our geom just in case)
    '''
    feature_id = request.matchdict['id']

    feature = DBSession.query(Feature).filter(Feature.uuid==feature_id).first()
    if not feature:
        return HTTPNotFound('Invalid feature request')

    #get the feature from mongo
    connstr = request.registry.settings['mongo_uri']
    collection = request.registry.settings['mongo_collection']
    mongo_uri = gMongoUri(connstr, collection)
    gm = gMongo(mongo_uri)
    vectors = gm.query({'f.id': feature.fid})

    if not vectors:
        return HTTPServerError('no cursor')

    vector = vectors[0]

    #add the check for the geometry (larger than 1.5mb not stored there - go to postgres)
    if not 'geom' in vector:        
        geom = feature.geom if feature else ''     
    else:
        geom = vector['geom']['g']

    #'observed': vector['obs'],

    #TODO: deal with observed values
    #rebuild some json from the other json (i like it)
    results = {'dataset': {'id': vector['d']['id'], 'uuid': vector['d']['u']}, 'feature': {'id': vector['f']['id'], 'uuid': vector['f']['u']}, 
                'attributes': vector['atts'], 'geometry': geom}

    return results

'''
the feature_streamer

so, neat trick, the current_registry is empty when called as part of an app_iter request
and get_current_registry is not supposed to be used at all even though it's all over the 
pyramid docs. yeah, so. we introduce many varieties of suck.
'''
@view_config(route_name='features')
def features(request):
    '''
    feature streamer

    PARAMS:
    app
    
    limit
    offset
    
    start_time
    end_time
    valid_start
    valid_end
    
    epsg
    box
    theme, subtheme, groupname - category
    query - keyword

    parameter (or parameters + units + frequency)

    dataset_id (as uuid)

    format (output format)
    geomtype !!! required

    
    '''

    #TODO: make this a post request with json chunk so that we can have better sorting (although getting the sort by values in attribute where attribute = x is unlikely from mongo)

    #TO START, GEOMETRY TYPE IS REQUIRED
    #id the datasets that match the filters
    app = request.matchdict['app']
    format = request.matchdict['ext'].lower()

    params = normalize_params(request.params)

    #pagination
    limit = int(params.get('limit')) if 'limit' in params else 25
    offset = int(params.get('offset')) if 'offset' in params else 0

    #check for valid utc datetime
    start_added = params.get('start_time') if 'start_time' in params else ''
    end_added = params.get('end_time') if 'end_time' in params else ''

    #check for valid utc datetime
    start_valid = params.get('valid_start') if 'valid_start' in params else ''
    end_valid = params.get('valid_end') if 'valid_end' in params else ''

    #check for OUTPUT format
    #format = params.get('format', 'json').lower()
    if format not in ['json', 'kml', 'csv', 'gml', 'geojson']:
        return HTTPNotFound()
    
    #check for geomtype
    geomtype = params.get('geomtype', '')

    #keyword search
    #TODO: chuck this. what would a keyword search be for data values (not searching every val obj, that's nuts)
    keyword = params.get('query') if 'query' in params else ''
    keyword = keyword.replace(' ', '%').replace('+', '%')

    #search geometry
    box = params.get('box') if 'box' in params else ''
    epsg = params.get('epsg') if 'epsg' in params else ''

    #category params
    theme = params.get('theme') if 'theme' in params else ''
    subtheme = params.get('subtheme') if 'subtheme' in params else ''
    groupname = params.get('groupname') if 'groupname' in params else ''

    #TODO: parameter search
    param = params.get('param', '')

    #sort (observed and ?)
    sort_flag = params.get('sortby', 'observed')
    sort_order = params.get('order', 'desc')
    if sort_flag not in ['observed', 'dataset']:
        return HTTPNotFound()
    sort_order = -1 if sort_order.lower() == 'desc' else 1

    #dataset_ids
    dataset_ids = params['datasets'].split(',') if 'datasets' in params else []
    #limit it to something?

    #set up the postgres checks
    dataset_clauses = [Dataset.inactive==False, "'%s'=ANY(apps_cache)" % (app)]
    
    if geomtype and geomtype.upper() in ['POLYGON', 'POINT', 'LINESTRING', 'MULTIPOLYGON', '3D POLYGON', '3D LINESTRING']:
        dataset_clauses.append(Dataset.geomtype==geomtype.upper())
    else:
        #let's not mix-and-match geometry types
        return HTTPNotFound()

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

    if param:
        #do that too
        pass

    #TODO: build the shapes query if there's a bbox
    #      except this is awkward - can't sort on what we probably
    #      want to sort by because it's not in the shapes table anymore
    #      but this is where limits would be handy but they are not possible
    #      without being able to sort here
    #      so we'd get an enormous list of fids to compare to the sorted mongo results
    #      and the performance hit from that would be... unpleasant

    #so, for now, a quick search on the datasets extent and hope for the best
    if box:
        srid = int(request.registry.settings['SRID'])
        epsg = int(epsg) if epsg else srid

        #convert the box to a bbox
        bbox = string_to_bbox(box)

        #and to a geom
        bbox_geom = bbox_to_geom(bbox, epsg)

        #and reproject to the srid if the epsg doesn't match the srid
        if epsg != srid:
            reproject_geom(bbox_geom, epsg, srid)

        #i don't remember why we're converting the geom back to wkt, probably the srid mismatch thing
        dataset_clauses.append(func.st_intersects(func.st_setsrid(Dataset.geom, srid), func.st_geometryfromtext(geom_to_wkt(bbox_geom, srid))))

    #and add the dataset id list
    if dataset_ids:
        dataset_clauses.append(Dataset.uuid.in_(dataset_ids))

    #get matching datasets
    query = DBSession.query(Dataset.id).filter(and_(*dataset_clauses))

    #and add the categories
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

    #get the dataset_ids that match our filters
    filtered_ids = [d.id for d in query]

    #TODO: QUERY SHAPES BY FID? limit to the number of fids in a list - check on that (cannot query for millions of things)

    #use the filters to build the mongo filters
    #dataset_ids, valid start and end
    #TODO: add operators for param search (val >=, <=, >, <, ==, !=, !NODATA)
    connstr = request.registry.settings['mongo_uri']
    collection = request.registry.settings['mongo_collection']
    mongo_uri = gMongoUri(connstr, collection)
    gm = gMongo(mongo_uri)

    #mongo_clauses = {'d.id': {'$in': filtered_ids}}
    mongo_clauses = []
    #TODO: check date format
    if start_valid and end_valid:
        mongo_clauses.append({'obs': {'$gte': start_valid, '$lte': end_valid}})
    elif start_valid and not end_valid:
        mongo_clauses.append({'obs': {'$gte': start_valid}})
    elif not start_valid and end_valid:
        mongo_clauses.append({'obs': {'$lte': end_valid}})

    #need to set up the AND
#    if len(mongo_clauses) > 1:
#        mongo_clauses = {'$and': mongo_clauses}

    #set up the sort
    sort_dict = {}
    if sort_flag == 'observed':
        sort_dict = {'obs': sort_order}
    else:
        sort_dict = {'d.id': sort_order, 'obs': sort_order}

    #so just going to lie about this
    #until we run map/reduce to group by dataset id
    sort_dict = {'obs': sort_order}   
    sort_dict = {}

    #export as the format somehow
    folder_head = ''
    folder_tail = ''
    delimiter = '\n'
    if format == 'geojson':
        content_type = 'application/json; charset=UTF-8'
        head = """{"type": "FeatureCollection", "features": ["""
        tail = "\n]}"
        delimiter = ','
    elif format == 'kml':
        content_type = 'application/vnd.google-earth.kml+xml; charset=UTF-8'
        head = """<?xml version="1.0" encoding="UTF-8"?>
                        <kml xmlns="http://earth.google.com/kml/2.2">
                        <Document>"""
        tail = """\n</Document>\n</kml>"""
        folder_head = "<Folder><name>%s</name>"
        folder_tail = "</Folder>"
    elif format == 'csv':
        content_type = 'text/csv; charset=UTF-8'
        head = '' 
        tail = ''
        #TODO: add some metadata/header info for each csv chunk 
    elif format == 'gml':
        content_type = 'application/xml; subtype="gml/3.1.1; charset=UTF-8"'
        head = """<?xml version="1.0" encoding="UTF-8"?>
                                <gml:FeatureCollection 
                                    xmlns:gml="http://www.opengis.net/gml" 
                                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                                    xmlns:xlink="http://www.w3.org/1999/xlink">
                                    xmlns:ogr="http://ogr.maptools.org/"
                                <gml:description>GSTORE API 3.0 Vector Stream</gml:description>\n""" 
        tail = """\n</gml:FeatureCollection>"""
    elif format == 'json':
        content_type = 'application/json; charset=UTF-8'
        head = """{"features": ["""
        tail = "]}"
        delimiter = ','
    else:
        #which it should have done by now anyway
        return HTTPNotFound()

    #total = vectors.count()
    #limit = total if total < limit else limit

    #running without limits FOR FUN
    #vectors = gm.query(mongo_clauses, {}, sort_dict)

    encode_as = 'utf-8'   
    epsg = int(request.registry.settings['SRID']) 
    
    def yield_results():
        #run through each dataset_id as a chunk of stuff (folder for kml, etc)
        dataset_cnt = 0
        
        for d in filtered_ids:
            result = ''
            
            mongo_clauses.append({'d.id': d})

            #and go get the attribute data for the dataset
            the_dataset = get_dataset(d)
            fields = the_dataset.attributes

            field_set = ''
            if format == 'kml':
                kml_flds = [{'type': ogr_to_kml_fieldtype(f.ogr_type), 'name': f.name} for f in fields]
                kml_flds.append({'type': 'string', 'name': 'observed'})
                field_set = """<Schema name="%(name)s" id="%(id)s">%(sfields)s</Schema>""" % {'name': str(the_dataset.uuid), 'id': str(the_dataset.uuid), 
                    'sfields': '\n'.join(["""<SimpleField type="%s" name="%s"><displayName>%s</displayName></SimpleField>""" % (k['type'], k['name'], k['name']) for k in kml_flds])
                }
            elif format == 'csv':
                #and add the dataset id, the fid and the observed datetime fields
                field_set = ','.join([f.name for f in fields]) + ',fid,dataset,observed\n'

            fhead = folder_head % (the_dataset.description) if format == 'kml' else ''
            
            vectors = gm.query({'$and': mongo_clauses}, {}, sort_dict)
            total = vectors.count()

            if dataset_cnt == 0:
                #we need to make sure we set up the file correctly
                result = head
                
            #so we'll yield each dataset instead (eek, that's a lot of stuff)
            cnt = 0
            for vector in vectors:
                #what we can't do is yield all the time - it stops at the first and then NOTHING else gets sent

                vector_result = convert_vector(vector, fields, format, the_dataset.basename, epsg)

                #if it's the first, add the HEAD and a DELIMITER
                #if it's the last, add the TAIL
                #if it's neither, add a DELIMITER
                if cnt == 0:
                    vector_result = fhead + field_set + vector_result + delimiter
                elif cnt == total - 1:
                    vector_result += folder_tail
                else:
                    vector_result += delimiter
                    
                #add it to everything
                result += vector_result
                cnt += 1
                #yield rst

            #last dataset, we're done
            if dataset_cnt == len(filtered_ids) - 1:
                result += tail

            dataset_cnt += 1
            yield result.encode(encode_as)


    def convert_vector(vector, fields, fmt, basename, epsg):
        #convert the mongo to a chunk of something based on the format
        fid = int(vector['f']['id'])
        did = int(vector['d']['id'])
        obs = vector['obs'] if 'obs' in vector else ''

        #get the geometry
        if not format == 'csv':
            wkb = vector['geom']['g'] if 'geom' in vector else ''
            if not wkb:
                #need to get it from shapes
                
                feature = DBSession.query(Feature).filter(Feature.fid==fid).first()
                wkb = feature.geom
                
            #and convert to geojson, kml, or gml
            geom_repr = wkb_to_output(wkb, epsg, fmt)
            if not geom_repr:
                return ''

        #deal with the attributes
        #where it's important, esp. for the csv, that the attributes are exported int he order of the fields
        atts = vector['atts']
        if format == 'kml': 
            vals = [(a['name'], a['val']) for a in atts]
            feature = "\n".join(["""<SimpleData name="%s">%s</SimpleData>""" % (v[0], v[1]) for v in vals])

            feature = """<Placemark id="%s">
                        <name>%s</name>
                        %s\n%s
                        <ExtendedData><SchemaData schemaUrl="%s">%s</SchemaData></ExtendedData>
                        <Style><LineStyle><color>ff0000ff</color></LineStyle><PolyStyle><fill>0</fill></PolyStyle></Style>
                        </Placemark>""" % (fid, fid, geom_repr, '', '', feature)
        elif format == 'gml':
            #going to match the gml from the dataset downloader
            #need a list of values as <ogr:{att name}>VAL</ogr:{att name}>
            vals = ''.join(['<ogr:%s>%s</ogr:%s>' % (a['name'], a['val'], a['name']) for a in atts])
            feature = """<gml:featureMember><ogr:%(basename)s><ogr:geometryProperty>%(geom)s</ogr:geometryProperty>%(values)s</ogr:%(basename)s></gml:featureMember>""" % {
                   'basename': basename, 'geom': geom_repr, 'values': vals} 
        elif format == 'geojson':
            #so we don't care about the facgt that the fields change?
            #or we do and we will deal with the consequences shortly
            vals = dict([(a['name'], a['val']) for a in atts])
            vals.update({'fid':fid, 'dataset_id': did, 'observed': obs})
            feature = json.dumps({"type": "Feature", "properties": vals, "geometry": json.loads(geom_repr)})
        elif format == 'csv':
            vals = []
            for f in fields:
                att = [a for a in atts if a['name'] == f.name]
                if not att:
                    continue
                vals.append(str(att[0]['val']))
            vals += [str(fid), str(did), obs]
            feature = ','.join(vals)
        elif format == 'json':
            feature = ''
        else:
            feature = ''
           
        return feature
    
    #let's yield stuff
    response = Response()
    response.content_type = content_type
    response.app_iter = yield_results()
    return response

@view_config(route_name='add_features', request_method='POST')
def add_feature(request):
    '''
    add features to a dataset
    '''
    dataset_id = request.matchdict['id']

    return Response('added new features')

@view_config(route_name='update_feature', request_method='PUT')
def update_feature(request):
    '''
    modify an existing feature - add qualty flag or something
    '''
    return Response('')
