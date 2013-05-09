from pyramid.view import view_config
from pyramid.response import Response
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError, HTTPBadRequest

from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_
from sqlalchemy.sql import between

import os, json, re
from xml.sax.saxutils import escape

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

#TODO: update some indexes for better performance in both queries (can't use the uuid as the index though.)
@view_config(route_name='feature')
def feature(request):
    '''
    get a feature
    /apps/{app}/features/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}.{ext}
    /apps/rgis/features/e74a1e0d-3e75-44dd-bb4c-3328a2425856.json


    now what did we forget? that uuids are not good indexes in mongo (too big)
    so ping shapes for the fid before pinging mongo (and then we already have our geom just in case)
    '''
    feature_id = request.matchdict['id']
    format = request.matchdict['ext']

    if format not in ['json', 'geojson', 'kml', 'gml']:
        return HTTPNotFound()

    try:
        i = int(feature_id)
        clause = Feature.fid==i
    except:
        clause = Feature.uuid==feature_id

    feature = DBSession.query(Feature).filter(clause).first()
    if not feature:
        return HTTPNotFound('Invalid feature request')

    #TODO: get the dataset for the feature and make sure it's not embargoed or inactive
    if feature.dataset.is_embargoed or feature.dataset.inactive or not feature.dataset.is_available:
        return HTTPNotFound('Unavailable')

    #get the feature from mongo
    connstr = request.registry.settings['mongo_uri']
    collection = request.registry.settings['mongo_collection']
    mongo_uri = gMongoUri(connstr, collection)
    gm = gMongo(mongo_uri)

    #TODO: update this? it returns multiple docs per feature id for time series data so this is probably not what we want (stripping off the first, anyway)
    vectors = gm.query({'f.id': feature.fid})

    if not vectors:
        return HTTPServerError()

    vector = vectors[0]

    #this is basically the same as the convert_vector method in the streamer
    #but the streamer fails if that method is outside of the request method
    #so it lives there and this is here and let's not speak of it again.
    fid = int(vector['f']['id'])
    did = int(vector['d']['id'])
    obs = vector['obs'] if 'obs' in vector else ''

    #get the geometry
    if not format in ['json']:
        wkb = vector['geom']['g'] if 'geom' in vector else ''
        if not wkb:
            #need to get it from shapes
            feat = DBSession.query(Feature).filter(Feature.fid==fid).first()
            wkb = feat.geom

        epsg = int(request.registry.settings['SRID'])
        
        #and convert to geojson, kml, or gml
        geom_repr = wkb_to_output(wkb, epsg, format)
        if not geom_repr:
            geom_repr = ''

    #deal with the attributes
    #where it's important, esp. for the csv, that the attributes are exported in the order of the fields
    atts = vector['atts']

    #there's some wackiness with a unicode char and mongo (and also a bad char in the data, see fid 6284858)
    #convert atts to name, value tuples so we only have to deal with the wackiness once
    atts = [(a['name'], unicode(a['val']).encode('ascii', 'xmlcharrefreplace')) for a in atts]
    
    if format == 'kml': 
        load_balancer = request.registry.settings['BALANCER_URL']
        base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

        schema_url = '%s%s/attributes.kml' % (base_url, vector['d']['u'])
    
        #make sure we've encoded the value string correctly for kml
        feature = "\n".join(["""<SimpleData name="%s">%s</SimpleData>""" % (v[0], re.sub(r'[^\x20-\x7E]', '', escape(str(v[1])))) for v in atts])

        #and no need for a schema url since it can be an internal schema linked by uuid here
        feature = """<Placemark id="%s">
                    <name>%s</name>
                    %s\n%s
                    <ExtendedData><SchemaData schemaUrl="%s">%s</SchemaData></ExtendedData>
                    <Style><LineStyle><color>ff0000ff</color></LineStyle><PolyStyle><fill>0</fill></PolyStyle></Style>
                    </Placemark>""" % (fid, fid, geom_repr, '', schema_url, feature)

        content_type = 'application/xml'
    elif format == 'gml':
        #going to match the gml from the dataset downloader
        #need a list of values as <ogr:{att name}>VAL</ogr:{att name}>
        vals = ''.join(['<ogr:%s>%s</ogr:%s>' % (a[0], re.sub(r'[^\x20-\x7E]', '', escape(str(a[1]))), a[0]) for a in atts])

        #and the dataset basename IS NOT used as the ogr id, instead it's g_{FID}
        feature = """<gml:featureMember><ogr:g_%(basename)s><ogr:geometryProperty>%(geom)s</ogr:geometryProperty>%(values)s</ogr:g_%(basename)s></gml:featureMember>""" % {
                'basename': fid, 'geom': geom_repr, 'values': vals} 

        content_type = 'application/xml'
    elif format == 'geojson':
        #TODO: qgis won't read a geojson from multiple datasets
        #so we don't care about the fact that the fields change?
        #or we do and we will deal with the consequences shortly
        vals = dict([(a[0], a[1]) for a in atts])
        vals.update({'fid':fid, 'dataset_id': did, 'observed': obs})
        feature = json.dumps({"type": "Feature", "properties": vals, "geometry": json.loads(geom_repr)})

        content_type = 'application/json'
    elif format == 'json':
        #no geometry, just attributes (good for timeseries requests)
        vals = dict([(a[0], a[1]) for a in atts])
        feature = json.dumps({'fid': fid, 'dataset_id': str(vector['d']['u']), 'properties': vals, 'observed': obs})
        content_type = 'application/json'
    else:
        feature = ''
        content_type = 'plain/text'


    return Response(feature, content_type=content_type)

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

    datasets (as uuid)

    format (output format)
    geomtype !!! required

    
    '''

    #TODO: make this a post request with json chunk so that we can have better sorting (although getting the sort by values in attribute where attribute = x is unlikely from mongo)

    #TODO: develop some heuristic for limiting the response (if polygon and lots of dataset and lots of records, limit by x; if point, limit by y)

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
        return HTTPNotFound('Invalid format')
    
    #check for geomtype
    geomtype = params.get('geomtype', '').replace('+', ' ')

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
    dataset_clauses = [Dataset.inactive==False, "'%s'=ANY(apps_cache)" % (app), Dataset.is_embargoed==False, Dataset.is_available==True]
                                                                                              
    if geomtype and geomtype.upper() in ['POLYGON', 'POINT', 'LINESTRING', 'MULTIPOLYGON', '3D POLYGON', '3D LINESTRING']:
        dataset_clauses.append(Dataset.geomtype==geomtype.upper())
#    else:
#        #let's not mix-and-match geometry types
#        return HTTPNotFound('bad geomtype')

    #add the dateadded
    if start_added or end_added:
        c = get_single_date_clause(Dataset.dateadded, start_added, end_added)
        if c is not None:
            dataset_clauses.append(c)

    #and the valid data range
    if start_valid or end_valid:
        c = get_overlap_date_clause(Dataset.begin_datetime, Dataset.end_datetime, start_valid, end_valid)
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
        delimiter = ',\n'
    elif format == 'kml':
        content_type = 'application/vnd.google-earth.kml+xml; charset=UTF-8'
        head = """<?xml version="1.0" encoding="UTF-8"?>
                        <kml xmlns="http://earth.google.com/kml/2.2">
                        <Document>"""
        tail = """\n</Document>\n</kml>"""
        folder_head = "<Folder><name>%s</name>"
        folder_tail = "</Folder>"


        load_balancer = request.registry.settings['BALANCER_URL']
        base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

        schema_base = base_url + '%s/attributes.kml'
        
    elif format == 'csv':
        content_type = 'text/csv; charset=UTF-8'
        head = '' 
        tail = ''
        delimiter = '\n'
        #some metadata to help people parse what is effectively a honking big text file of csv chunks
        '''
        dataset
        {description}
        {link to dataset metadata}
        {link to dataset description}
        '''
        load_balancer = request.registry.settings['BALANCER_URL']
        base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

        #template: dataset.description, dataset.uuid, dataset.uuid
        folder_head = '\n\nDATASET\n%s\nMetadata: '+base_url+'%s/metadata/fgdc.html\nServices: '+base_url+'%s/services.json\n'
    elif format == 'gml':
        content_type = 'application/xml; subtype="gml/3.1.1; charset=UTF-8"'
        head = """<?xml version="1.0" encoding="UTF-8"?>
                                <gml:FeatureCollection 
                                    xmlns:gml="http://www.opengis.net/gml" 
                                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                                    xmlns:xlink="http://www.w3.org/1999/xlink"
                                    xmlns:ogr="http://ogr.maptools.org/">
                                <gml:description>GSTORE API 3.0 Vector Stream</gml:description>\n""" 
        tail = """\n</gml:FeatureCollection>"""
    elif format == 'json':
        content_type = 'application/json; charset=UTF-8'
        head = """{"features": ["""
        tail = "]}"
        delimiter = ',\n'
    else:
        #which it should have done by now anyway
        return HTTPNotFound('failure')


    #TODO: once the param search is added, have split tracks for the response
    #      just return obs, value, fid, dataset id, qualifier
    #      so have the mongo query NOT return the geom (or make that an option? or make that json only response?)

    encode_as = 'utf-8'   
    epsg = int(request.registry.settings['SRID']) 
    
    def yield_results():
        #run through each dataset_id as a chunk of stuff (folder for kml, etc)
        dataset_cnt = 0

        yield head
       
        for d in filtered_ids:
            result = ''

            feature_clauses = [{'d.id': d}]
            if mongo_clauses:
                feature_clauses.append(mongo_clauses)

            #and go get the attribute data for the dataset
            the_dataset = get_dataset(d)
            fields = the_dataset.attributes

            schema_url = ''
            field_set = ''
            if format == 'kml':
                kml_flds = [{'type': ogr_to_kml_fieldtype(f.ogr_type), 'name': f.name} for f in fields]
                kml_flds.append({'type': 'string', 'name': 'observed'})
                field_set = """<Schema name="%(name)s" id="%(id)s">%(sfields)s</Schema>""" % {'name': str(the_dataset.uuid), 'id': str(the_dataset.uuid), 
                    'sfields': '\n'.join(["""<SimpleField type="%s" name="%s"><displayName>%s</displayName></SimpleField>""" % (k['type'], k['name'], k['name']) for k in kml_flds])
                }

#                field_set = ''
                schema_url = schema_base % (the_dataset.uuid)
            elif format == 'csv':
                #and add the dataset id, the fid and the observed datetime fields
                field_set = ','.join([f.name for f in fields]) + ',fid,dataset,observed\n'

            if format == 'kml':
                fhead = folder_head % (the_dataset.description)
            elif format == 'csv':
                fhead = folder_head % (the_dataset.description, the_dataset.uuid, the_dataset.uuid)
            else:
                fhead = folder_head

            if len(feature_clauses) > 1:
                feature_clauses = {'$and': feature_clauses}
            else:
                feature_clauses = feature_clauses[0]
            vectors = gm.query(feature_clauses, {}, sort_dict)
            total = vectors.count()
                
            #so we'll yield each vector instead (eek, that's a lot of stuff)
            cnt = 0

            #TODO: CHECK THIS FOR ONE FEATURE ONLY (WHEN WOULD THAT HAPPEN?)
            for vector in vectors:
                vector_result = convert_vector(vector, fields, format, the_dataset.basename, schema_url, epsg)

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
                #result += vector_result
                cnt += 1
                yield vector_result.encode(encode_as)

            dataset_cnt += 1
            #yield '\n\nDATASET_COUNT: %s (%s, %s)\n\n' % (dataset_cnt, total, json.dumps(feature_clauses))
        yield tail

    #build a feature chunk based on the given format (json (no geom), geojson, kml or gml)
    def convert_vector(vector, fields, fmt, basename, schema_url, epsg):
        #convert the mongo to a chunk of something based on the format
        fid = int(vector['f']['id'])
        did = int(vector['d']['id'])
        obs = vector['obs'] if 'obs' in vector else ''
        obs = obs.strftime('%Y-%m-%dT%H:%M:%S+00') if obs else ''

        #get the geometry
        if not fmt in ['csv', 'json']:
            wkb = vector['geom']['g'] if 'geom' in vector else ''
            if not wkb:
                #need to get it from shapes
                feat = DBSession.query(Feature).filter(Feature.fid==fid).first()
                wkb = feat.geom
                
            #and convert to geojson, kml, or gml
            geom_repr = wkb_to_output(wkb, epsg, fmt)
            if not geom_repr:
                return ''

        #deal with the attributes
        #where it's important, esp. for the csv, that the attributes are exported in the order of the fields
        atts = vector['atts']

        #there's some wackiness with a unicode char and mongo (and also a bad char in the data, see fid 6284858)
        #convert atts to name, value tuples so we only have to deal with the wackiness once
        atts = [(a['name'], unicode(a['val']).encode('ascii', 'xmlcharrefreplace')) for a in atts]

        #add the observed datetime for everything
        atts.append(('observed', obs))

        #TODO: revise to handle features where the field value is null (and so it doesn't have an attribute in mongo) for kml, gml, etc 
        #      (not csv that should be ok)
        if fmt == 'kml': 
            #make sure we've encoded the value string correctly for kml
            feature = "\n".join(["""<SimpleData name="%s">%s</SimpleData>""" % (v[0], re.sub(r'[^\x20-\x7E]', '', escape(str(v[1])))) for v in atts])

            #and no need for a schema url since it can be an internal schema linked by uuid here
            feature = """<Placemark id="%s">
                        <name>%s</name>
                        %s\n%s
                        <ExtendedData><SchemaData schemaUrl="%s">%s</SchemaData></ExtendedData>
                        <Style><LineStyle><color>ff0000ff</color></LineStyle><PolyStyle><fill>0</fill></PolyStyle></Style>
                        </Placemark>""" % (fid, fid, geom_repr, '', schema_url, feature)
        elif fmt == 'gml':
            #going to match the gml from the dataset downloader
            #need a list of values as <ogr:{att name}>VAL</ogr:{att name}>
            vals = ''.join(['<ogr:%s>%s</ogr:%s>' % (a[0], re.sub(r'[^\x20-\x7E]', '', escape(str(a[1]))), a[0]) for a in atts])
            feature = """<gml:featureMember><ogr:g_%(basename)s><ogr:geometryProperty>%(geom)s</ogr:geometryProperty>%(values)s</ogr:g_%(basename)s></gml:featureMember>""" % {
                    'basename': basename, 'geom': geom_repr, 'values': vals} 
        elif fmt == 'geojson':
            #TODO: qgis won't read a geojson from multiple datasets
            #so we don't care about the fact that the fields change?
            #or we do and we will deal with the consequences shortly
            vals = dict([(a[0], a[1]) for a in atts])
            vals.update({'fid':fid, 'dataset_id': did})
            feature = json.dumps({"type": "Feature", "properties": vals, "geometry": json.loads(geom_repr)})
        elif fmt == 'csv':
            #NOTE: still appending observed here - field order matters and observed is not a recognized feature_attribute for a dataset
            vals = []
            for f in fields:
                att = [a for a in atts if a[0] == f.name]
                v = att[0][1] if att else ""
                vals.append(v)
            vals += [str(fid), str(did), obs]
            feature = ','.join(vals)
        elif fmt == 'json':
            #no geometry, just attributes (good for timeseries requests)
            vals = dict([(a[0], a[1]) for a in atts])
            feature = json.dumps({'fid': fid, 'dataset_id': str(vector['d']['u']), 'properties': vals})
        else:
            feature = ''
            
        return feature
    
    #let's yield stuff
    response = Response()
    response.content_type = content_type
    #because it will be an issue, let's go for cors.
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.app_iter = yield_results()
    return response

@view_config(route_name='add_features', request_method='POST', renderer='json')
def add_feature(request):
    '''
    add features to a dataset

    where this is posting one geometry (wkb.hex) to shapes and returning the fid, uuid

    wkb should be reprojected (unprojected) to wgs84 BEFORE posting

    THIS IS NOT A COMPLETE VECTOR WITHIN GSTORE
    meaning this is only the geometry and the attribute data most be posted separately using the features.add_attributes method (see below)


    {
        gid:
        geom: 
        epsg:
    }
    or we could have geom + epsg and reproject here?

    or 
    {
        gid:
        x:
        y:
    }
    where x, y at lon, lat in wgs84 and we just make the geom here


    or FOR BULK INSERTS:
    {
        features: [{gid: , geom: , epsg: }, ...]
    }
    '''
    dataset_id = request.matchdict['id']
    the_dataset = get_dataset(dataset_id)
    if not the_dataset:
        return HTTPNotFound()

    post_data = request.json_body

    if 'gid' not in post_data and 'features' not in post_data:
        return HTTPBadRequest('invalid feature: no gid')

    if 'geom' not in post_data and ('x' not in post_data and 'y' not in post_data) and 'features' not in post_data:
        return HTTPBadRequest('invalid geometry')

    if 'features' in post_data:
        #bulk insert
        geoms_to_post = post_data['features']
    else:
        gid = int(post_data['gid'])
        geom = post_data['geom'] if 'geom' in post_data else ''
        x = post_data['x'] if 'x' in post_data else ''
        y = post_data['y'] if 'y' in post_data else ''

        if x and y:
            geom = wkt_to_geom('POINT (%s %s)' % (x, y), 4326)
            geom = geom_to_wkb(geom)

        geoms_to_post = [{"gid": gid, "geom": geom, "epsg": 4326}]

    #TODO: add something for the epsg reprojection, if we want that


    features_to_post = []
    for geom_to_post in geoms_to_post:
        feature = Feature(int(geom_to_post['gid']), geom_to_post['geom'], the_dataset.id)
        features_to_post.append(feature)

    try:
        DBSession.add_all(features_to_post)
        DBSession.commit()
        DBSession.flush()
    except Exception as err:
        DBSession.rollback()
        return HTTPServerError(err)    

    if len(features_to_post) > 1:
        output = []
        for feature in features_to_post:
            output.append({"fid": feature.fid, "uuid": feature.uuid, "gid": feature.gid, "dataset": feature.dataset_id})
        return {"features": output}
    else:
        return {"fid": feature.fid, "uuid": feature.uuid, "gid": feature.gid, "dataset": feature.dataset_id}
    #return {'fid': feature.fid, 'uuid': feature.uuid, 'gid': gid}

@view_config(route_name='add_feature_attributes', request_method='POST', renderer='json')
def add_attributes(request):
    '''
    add attributes to a feature in a dataset

    posting attribute data to mongo
    each item in the post must have an fid/uuid for an existing feature!
    but we fetch the wkb from shapes rather than from the post (which may not be great performance-wise, we should keep an eye on that)

    fyi: this can be a bulk insert so the fid/uuid info is in the post data and not the route

    {
        fids: []  #if this is separate, then we can assume (ha) that the fid:record is not 1:1 so we can use this to fetch the geoms once instead of pinging shapes every time
        records: [
            {
                fid:
                uuid:  # we do not want to have to retrieve this again
                observed: 
                atts: [
                    {
                        name:
                        u: 
                        val:
                        qual: #optional 
                    }
                ]
            },
        ]
    }
    '''
    dataset_id = request.matchdict['id']
    the_dataset = get_dataset(dataset_id)
    if not the_dataset:
        return HTTPNotFound()
    
    post_data = request.json_body

    fids = post_data['fids'] if 'fids' in post_data else []
    records = post_data['records'] if 'records' in post_data else []

    if not records:
        return HTTPServerError()

    #just get the dataset info once
    dataset_id = the_dataset.id
    dataset_uuid = the_dataset.uuid

    geoms = []
    if fids:
        #let's get the fids (fid, geom) from shapes and it's a tuple==(fid, geom)
        geoms = DBSession.query(Feature.fid, Feature.geom).filter(and_(Feature.dataset_id==dataset_id, Feature.fid.in_(fids)))

    #get the attributes for the dataset so we can do at least some check against the inputs
    fields = [f.name for f in the_dataset.attributes]

    #TODO: split up the inserts into sets for larger datasets

    bad_recs = []
    inserts = []
    for rec in records:
        fid = rec['fid']
        uid = rec['uuid']
        obs = rec['observed'] if 'observed' in rec else ''
        atts = rec['atts']

        invalid_fields = [a for a in atts if not a['name'] in fields]
        if invalid_fields:  
            r = rec
            r.update({"err": "bad field"})
            bad_recs.append(r)
            continue

        #fix the date and it needs to be utc already
        #yyyyMMddTHH:MM:ss
        fmt = '%Y%m%dT%H:%M:%S'
        if obs:
            try:
                obsd = datetime.strptime(obs, fmt)
            except:
                r = rec
                r.update({"err": "bad observed"})
                bad_recs.append(r)
                continue
        else:
            obsd = None

        #and get the geom
        geom = ''
        if geoms:
            #try the pre-fetched list first
            geom = [g[1] for g in geoms if g[0] == fid]
            geom = geom[0] if geom else ''
        if not geom:
            #otherwise, try the shapes table
            geom = DBSession.query(Feature.geom).filter(Feature.fid==fid).first()
            geom = geom[0] if geom else '' #for the tuple action
        if not geom:
            #we are really in trouble here
            r = rec
            r.update({"err": "bad geom"})
            bad_recs.append(r)
            continue

        #check the geometry size!
        geom_size = check_wkb_size(geom)

        #build the shard key from the dataset uuid and the feature uuid
        shardkey = dataset_uuid.split('-')[0] + uid.split('-')[0]

        #build the mongo object
        #we are including the year, month, day, hour, minute as separate items in case we want to 
        #do aggregation later (easier now than trying to update a gajillion docs)
        obj = {'key': shardkey, 'f': {'id': fid, 'u': uid}, 'd': {'id': dataset_id, 'u': dataset_uuid}, 'atts': atts}
        if geom_size < 3.0:
            #check the size of the wkb and if it's bigger than 3.0 mb, do not write it to mongo (insert will fail).
            #technically, our limit is 4mb but that's for the doc NOT just the one element
            obj.update({'geom': {'g': geom}})
        if obsd:
            obj.update({'obs': obsd, 'year': obsd.year, 'mon': obsd.month, 'day': obsd.day, 'hour': obsd.hour, 'mnt': obsd.minute})
        inserts.append(obj)

    #insert everything to mongo if there's stuff to insert
#    if len(inserts) != the_dataset.record_count:
#        return HTTPBadRequest('')

    failed_to_post = []    
    failed_errors = []
    if inserts:
        connstr = request.registry.settings['mongo_uri']
        collection = request.registry.settings['mongo_collection']

        #put it in the right collection
        if the_dataset.inactive:
            collection = request.registry.settings['mongo_inactive_collection']

        #embargo trumps all    
        if the_dataset.is_embargoed:
            collection = request.registry.settings['mongo_embargo_collection']
        
        mongo_uri = gMongoUri(connstr, collection)
        gm = gMongo(mongo_uri)

        #go for smaller bits - bulk inserts can fail with really big (either in terms of count or in terms of data size)
        #sets so we're running them in batches
        #but this limit is arbitrary (i just picked one based on not very much)
        limit = 5000
        
        for i in range(0, len(inserts), limit):
            j = i + limit if i + limit < len(inserts) else len(inserts)
            inserts_to_post = inserts[i:j]

            #hang on the to the fids? although it means nothing for the sensor data
            fids = [x['f']['id'] for x in inserts_to_post]
            
            try:
                fail = gm.insert(inserts_to_post)
                if fail:
                    #TODO: run a delete for the dataset id just in case it failed midstream
                    #return HTTPServerError(fail)
                    failed_to_post.append(fids)
                    failed_errors.append(fail)
            except Exception as err:
                #remove anything that got entered
                #pass
                failed_to_post.append(fids)
                failed_errors.append(err)
            finally:
                gm.close()    

        #deal with the insert list - pymongo updates the list with _id (objectid)
        #so that and the obs datetime cause json.dumps to fail. we don't care about the _id
        #but we want the datetime
        archives = []
        for i in inserts:
            del i['_id']
            
            if 'obs' in i:
                i['obs'] = i['obs'].strftime('%Y%m%dT%H:%M:%S') 
                
            archives.append(i)
        VECTOR_PATH = request.registry.settings['VECTOR_IMPORT_PATH']
        vector_file = os.path.join(VECTOR_PATH, '%s.json' % (dataset_uuid))
        with open(vector_file, 'w') as g:
            g.write('\n'.join([json.dumps(i) for i in archives]))

    output = {'features': len(inserts)}
    if bad_recs:
        output.update({'errors': bad_recs})
    if failed_to_post:
        output.update({'bulk insert errors': {'fids': failed_to_post, 'errors': failed_errors}})
    return output

@view_config(route_name='update_feature', request_method='PUT')
def update_feature(request):
    '''
    modify an existing feature - add qualty flag or something
    '''
    return Response('')







    
