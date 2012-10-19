#from pyramid.view import view_config
#from pyramid.response import Response, FileResponse
#from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError

#from pyramid.threadlocal import get_current_registry

#import json
#import pymongo
##from urlparse import urlparse

#from sqlalchemy import desc, asc, func
#from sqlalchemy.sql.expression import and_, or_
#from sqlalchemy.orm import defer

#from ..lib.mongo import *

##from the models init script
#from ..models import DBSession
##from the generic model loader (like meta from gstore v2)
#from ..models.datasets import (
#    Dataset,
#    )
#from ..models.features import Feature


#from ..lib.utils import *
#from ..lib.database import *
#from ..lib.mongo import gMongo

#from datetime import datetime

#import os 
#import Image
#import mapscript
#from cStringIO import StringIO



#def to_json(doc):
#    d = doc['d'] if 'd' in doc else {}

#    did = d['id']
#    duuid = d['u']

#    f = doc['f'] if 'f' in doc else {}

#    fid = f['id']
#    fuuid = f['u']

#    geom = doc['geom']['g']

#    props = doc['atts'] if 'atts' in doc else []
#    ps = []
#    for p in props:
#        name = p['name']
#        uuid = p['u']
#        value = p['val']
#        ps.append({'name': name, 'uuid': uuid, 'value': value})
#        
#    
#    return {'type': 'Feature', 'dataset': {'id': did, 'uuid': duuid}, 'feature': {'fid': fid, 'uuid': fuuid}, 'geom': geom,
#                    'properties': ps}

#def to_csv(doc):
#    d = doc['d'] if 'd' in doc else {}

#    did = str(d['id'])
#    duuid = str(d['u'])

#    f = doc['f'] if 'f' in doc else {}

#    fid = str(f['id'])
#    fuuid = str(f['u'])

#    geom = doc['geom']['g']

#    output = [did, duuid, fid, fuuid]
#    fields = ['dataset id', 'dataset uuid', 'fid', 'feature uuid']
#    
#    props = doc['atts'] if 'atts' in doc else []
#    ps = []
#    for p in props:
#        name = p['name']
#        uuid = p['u']
#        value = p['val']
#        ps.append({'name': name, 'uuid': uuid, 'value': value})

#        output.append(value) #need to escape this (or replace the commas, stupid people)
#        fields.append(name)


#    return fields, output


#@view_config(route_name='test_fidsearch', renderer='json')
#def fid_search(request):
#    #query postgres for something
#    ds = DBSession.query(Dataset).filter(Dataset.geomtype=='POINT')
#    ids = [s.id for s in ds]

#    #get the fids (and just the fids to help speed things up)
#    shp_fids = DBSession.query(Feature.fid).filter(Feature.dataset_id.in_(ids))    
#    shp_fids = [f.fid for f in shp_fids]

##    #query mongo for something else
##    connstr = get_current_registry().settings['mongo_uri']
##    collection = get_current_registry().settings['mongo_collection']
##    gm = gMongo(connstr, collection)
##    mongo_clauses = {'d.id': {'$in': ids}, 'atts.name': 'EASTING'}
##    #db.vectors.find({'d.id': {$in: [52208, 52209, 56282, 56350]}}, {'f.id': 1})
##    #run the query and just return the fids (we aren't interested in anything else here)
##    mongo_fids = gm.query(mongo_clauses, {'f.id': 1})
##    mongo_fids = [f['f']['id'] for f in mongo_fids]
##    #return {'total': mongo_fids.count()}

##    #intersect!
##    s = set(shp_fids)
##    fids = s.intersection(mongo_fids)

#    fids = set(shp_fids)
#    
#    return {'fids': list(fids)}

#@view_config(route_name='test_wcs')
#def test_ogc(request):
#    uuid = 'a427563f-3c7e-44a2-8b35-68ce2a78001a'
#    d = get_dataset(uuid) 

#    params = request.params
#    ogc_req = params.get('REQUEST', 'GetCapabilities')

#    #mapsrc, srcloc = d.get_mapsource()

#    #bbox = [float(b) for b in d.box]

#    #f = '/clusterdata/gstore/maps/a427563f-3c7e-44a2-8b35-68ce2a78001a.76744073-525b-4cd0-a786-e5f6c22f821b.map'
#    #f = '/clusterdata/gstore/maps/35107b42.map'
#    f = '/clusterdata/gstore/maps/8f11ec21-dfe1-437d-a7be-a85bbb9e4283.e45fcdeb-a487-4daa-aeba-a25e28377fa2.map'
#    #uncomment to test IF THE MAPFILE EXISTS
##    m = mapscript.mapObj(f)

##    #let's play with the wcs response
##    req = mapscript.OWSRequest()
##    req.setParameter('SERVICE', params.get('SERVICE', 'WMS'))
##    req.setParameter('VERSION', params.get('VERSION', '1.1.1'))
##    req.setParameter('REQUEST', ogc_req)

##    if ogc_req.lower() == 'describecoverage':
##        mapscript.msIO_installStdoutToBuffer()
##        m.OWSDispatch(req)
##        content_type = mapscript.msIO_stripStdoutBufferContentType()
##        content = mapscript.msIO_getStdoutBufferBytes()
##        return Response(content)
##    if ogc_req.lower() == 'getcapabilities':
##        mapscript.msIO_installStdoutToBuffer()
##        m.OWSDispatch(req)
##        content_type = mapscript.msIO_stripStdoutBufferContentType()
##        content = mapscript.msIO_getStdoutBufferBytes()

##        if 'xml' in content_type:
##            content_type = 'application/xml'

##        #TODO: double check all of this
##        
##        return Response(content, content_type=content_type)
#    
#    return Response('hi')

#@view_config(route_name='test_fmt')
#def format_tester(request):
#    #check if mapserver handles sids and ecws
#    fmt = request.matchdict['fmt']

#    #just run with getmap (since that's what we really care about anyway)
#    #test uuid for sid - c2e948ee-2ea9-4dc4-935b-b5e56ff8071c
#    #       /clusterdata/gstore/maps/c2e948ee-2ea9-4dc4-935b-b5e56ff8071c.bc0b2893-d5eb-4a75-83ad-74abc9749263.map
#    #YAY THE SID WORKS
#    #test uuid for ecw - 0f081ca5-a7bf-45e8-8f96-dd7fba5df4d4
#    #       /clusterdata/gstore/maps/0f081ca5-a7bf-45e8-8f96-dd7fba5df4d4.7c015173-50bd-4c47-b3e0-3bd399162387.map
#    #YAY THE ECW WORKS

#    #for the sid, note that i changed the filepath for this manually
#    #f = '/clusterdata/gstore/maps/c2e948ee-2ea9-4dc4-935b-b5e56ff8071c.bc0b2893-d5eb-4a75-83ad-74abc9749263.map'

#    f = '/clusterdata/gstore/maps/0f081ca5-a7bf-45e8-8f96-dd7fba5df4d4.7c015173-50bd-4c47-b3e0-3bd399162387.map'
#    m = mapscript.mapObj(f)
#    req = mapscript.OWSRequest()
#    req.setParameter('SERVICE', 'WMS')
#    req.setParameter('VERSION', '1.1.1')
#    req.setParameter('REQUEST', 'GetMap')
#    
#    req.setParameter('WIDTH', '256')
#    req.setParameter('HEIGHT', '256')

#    #for the sid
#    #eq.setParameter('BBOX', '-103.068,36.9349,-102.995,37.0026')

#    #for the ecw
#    req.setParameter('BBOX', '-108.038,35.4006,-107.899,35.5369')
#    req.setParameter('STYLES', '')

#    #dataset.basename
#    #layers = str(params['LAYERS']) if 'LAYERS' in params else ''
#    #req.setParameter('LAYERS', params.get('LAYERS', ''))
#    req.setParameter('LAYERS', 'RGIS_Dataset')
#    req.setParameter('FORMAT', 'image/png')
#    req.setParameter('SRS', 'EPSG:4326')

#    m.loadOWSParameters(req)
#    img = Image.open(StringIO(m.draw().getBytes()))
#    buffer = StringIO()
#    #TODO: change png to the specified output type
#    img.save(buffer, 'PNG')
#    buffer.seek(0)
#    #TODO: change content-type based on specified output type
#    return Response(buffer.read(), content_type='image/png')

#@view_config(route_name='test_url', renderer='json')
#def url_tester(request):
#    ext = request.matchdict['ext']
#    dataset_id = request.matchdict['id']
#    dataset_type = request.matchdict['type']
#    basename = request.matchdict['basename']

#    
#    return {'id': dataset_id, 'basename': basename, 'set': dataset_type, 'extension': ext}


##, renderer='dataone_logs.mako'
#@view_config(route_name='test')
#def tester(request):


#    def y():
#        yield request.registry.settings['TEMP_PATH']
#        #which stops it here so can't do what was in v2
#        for i in range(1,10):
#            yield x
#        yield 'bye'

#    response = Response()
#    response.content_type = 'plain/text'
#    response.app_iter = y()
#    return response
#    

##    dataset_id = request.matchdict['id']
##    ext = request.matchdict['ext']

##    '''
##    ids to test

##    52218: 32517de4-5301-4e45-aa4d-580591165cca
##    55355
##    55356
##    107352: a4e33c15-849c-4a02-a0ff-78d72b52daff
##    107353
##    107365: c329d0f6-ca5f-47bc-ad98-41cd52f614c6
##    107405

##    '''

##    #for the dataone log renderers
##    #2012-02-29T23:26:38.828+00:00
##    fmt = '%Y-%m-%dT%H:%M:%S+00:00'
##    post = {'total': 45, 'results': 3, 'offset': 0}
##    docs = [
##            {'id': 1, 'identifier': dataset_id, 'ip': '129.24.63.165', 'useragent': 'null', 'subject': 'CN=GStore,dc=informatics,dc=org', 'event':'read', 'dateLogged':datetime.utcnow().strftime(fmt), 'node': 'GSTORE'},
##            {'id': 2, 'identifier': dataset_id, 'ip': '129.24.63.55', 'useragent': 'null', 'subject': 'CN=GStore,dc=informatics,dc=org', 'event':'read', 'dateLogged':datetime.utcnow().strftime(fmt), 'node': 'GSTORE'},
##            {'id': 3, 'identifier': dataset_id, 'ip': '129.24.63.235', 'useragent': 'null', 'subject': 'CN=GStore,dc=informatics,dc=org', 'event':'read', 'dateLogged':datetime.utcnow().strftime(fmt), 'node': 'GSTORE'}
##        ]
##    post.update({'docs': docs})
##    return post

###    #for mongo
###    #go get the dataset
###    d = get_dataset(dataset_id)

###    if not d:
###        return HTTPNotFound('No results')

###    #make sure it's available
###    #TODO: replace this with the right status code
###    if d.is_available == False:
###        return HTTPNotFound('Temporarily unavailable')

###    #with the new class
###    connstr = get_current_registry().settings['mongo_uri']
###    gm = gMongo(connstr, 'vectors')

###    vectors = gm.query({'d.id': d.id})

###    #let's do stuff with stuff
###    atts = d.attributes

###    fields = []
###    res = []
###    for v in vectors:
###        if ext == 'json':
###            r = to_json(v)
###        elif ext == 'csv':
###            flds, r = to_csv(v)
###            if not fields:
###                fields = flds
###            r = ','.join(r)

###        res.append(r)

###    #close the mongo connection
###    gm.close()

###    if ext == 'json':
###        content_type = 'application/json'
###        rsp = json.dumps({'total': vectors.count(), 'results': res})
###    elif ext == 'csv':
###        content_type = 'text/csv; charset=UTF-8' #or application/csv
###        rsp = (','.join(fields) + '\n' + '\n'.join(res)).encode('utf8')


###    return Response(rsp, content_type=content_type)
###    #end mongo



#@view_config(route_name='test_insert', renderer='json')
#def insert(request):
#    '''
#    we are skipping the file upload - no one wanted to do that (or no one wanted it to post to ibrix)
#    so maybe add it again later if it comes up, but we're starting with the basic json post functionality

#    {
#        'description':
#        'basename':
#        'dates': {
#            'start': 
#            'end':
#        }
#        'uuid': 
#        'taxonomy': 
#        'spatial': {
#            'geomtype':
#            'epsg':
#            'bbox':
#            'geom': 
#            'features':
#            'records':
#        }
#        'metadata': {
#            'standard':
#            'file':
#        }
#        'apps': []
#        'formats': []
#        'services': []
#        'categories': [
#            {
#                'theme':
#                'subtheme':
#                'groupname':
#            }
#        ]
#        'sources': [
#            {
#                'set':
#                'extension':
#                'external':
#                'mimetype':
#                'identifier':
#                'identifier_type':
#                'files': []
#                
#            }
#        ]
#    }

#    '''

#    #get the data as json
#    post_data = request.json_body

#    

#    return post_data

#@view_config(route_name='test_bulkinsert', renderer='json')
#def mongo_bulkinsert(request):

#    amount = int(request.matchdict['amount'])
#    dataset = int(request.matchdict['id'])

#    #just need a basic doc with some data (fake) 
#    # a recognizable and very fake dataset id
#    #and a very fake shardkey

#    #1820;276610

#    docs = []
#    a = generate_uuid4()
#    b = generate_uuid4()
#    key = a.split('-')[0]+b.split('-')[0]

#    
#    
#    for i in range(amount):
#        basicdoc = {"observed": "20120801T07:00:00", "atts": [{"u": "a4e041a4-1915-42d4-87e8-e1a6bd7286f3", "name": "site_id", "val": 793}, {"u": "e3d4d4a5-89a6-44d3-86ce-59f2495bc736", "name": "date", "val": "2012-08-01"}, {"u": "db7d6208-45d0-4edf-b137-6475283a2511", "name": "time", "val": "00:00"}, {"u": "625aca36-25d1-4b04-9b56-d1f5a9152343", "name": "wteqi_1", "val": 0.0}, {"u": "26331fca-fafd-40b7-8d99-97b9d4a830c5", "name": "preci_1", "val": 13.8}, {"u": "c53c519d-4f61-4004-9e8b-18b85311df7c", "name": "tobsi_1", "val": 12.8}, {"u": "60c79f69-97ef-4d95-bb22-bb998f782378", "name": "snwdi_1", "val": 0.0}, {"u": "1b785a88-60b3-4acf-a6cc-e310f1b9a52d", "name": "batxh_1", "val": 13.74}, {"u": "444ccc79-e7eb-449a-9d30-790e97823edc", "name": "batxh_2", "val": 16.82}, {"u": "1fb6d08c-1af9-414c-b7b1-05bfe156afd4", "name": "batti_1", "val": 12.95}, {"u": "434efdd7-81d2-4c86-b9c1-107bc6cd4ec8", "name": "batti_2", "val": 13.34},{"u": "bcc3fc11-a3ea-4d44-8a3d-e293f0af46df", "name": "site_id", "val": 1162}, {"u": "352fab5e-d4cd-4b99-919a-14aa048385c2", "name": "date", "val": "2012-08-01"}, {"u": "f114504f-608b-4e29-8d39-aa567a23f0f0", "name": "time", "val": "00:00"}, {"u": "9ea307cc-e091-44a0-8a27-7ce6140b0bd2", "name": "wteqi_1", "val": 0.0}, {"u": "a798090a-9d59-4ea1-add1-11876900ad49", "name": "preci_1", "val": 16.4}, {"u": "23499b76-a3a3-4e5e-914b-26530b756101", "name": "tobsi_1", "val": 9.8}, {"u": "c9780853-f917-47a4-a28b-a6aa806ebb70", "name": "snwdi_1", "val": 0.0}, {"u": "ed41d8f1-ba10-4fda-ac14-40e4a5c8d8d3", "name": "smsi_1_2s", "val": 9.1}, {"u": "6242aa9a-e4d7-4ab7-ae13-ec52c7d24ba2", "name": "smsi_1_8s", "val": 11.3}, {"u": "21b0c54b-379c-40d5-b75f-535700973d5c", "name": "smsi_1_20s", "val": 13.8}, {"u": "9e686fc9-4ff7-4d56-bcf2-944f5426b3a8", "name": "stoi_1_2", "val": 12.5}, {"u": "afb0683d-7f47-41cb-9fa6-c6b2221c6dab", "name": "stoi_1_8", "val": 13.9}, {"u": "c4f6f080-c1ce-4cbb-8212-7463bf71a2a6", "name": "stoi_1_20", "val": 10.8}, {"u": "63e570fa-1fcf-4e00-a9b6-e17acf8f3105", "name": "sali_1_2", "val": 0.2}, {"u": "f8fe47f0-cd09-4bc7-a6be-6f741eb2faa2", "name": "sali_1_8", "val": 0.2}, {"u": "c7095cc9-2c55-4efe-8ebc-cfa6272a419b", "name": "sali_1_20", "val": 0.2}, {"u": "33643400-c445-4f12-883b-8fea9a238ec9", "name": "rdci_1_2", "val": 6.82}, {"u": "a35b5f15-90a2-427c-9f47-c2945310603f", "name": "rdci_1_8", "val": 7.64}, {"u": "884b425a-abf5-43cb-a515-6aeba2d5dde7", "name": "rdci_1_20", "val": 8.72},{"u": "bcc3fc11-a3ea-4d44-8a3d-e293f0af46df", "name": "site_id", "val": 1162}, {"u": "352fab5e-d4cd-4b99-919a-14aa048385c2", "name": "date", "val": "2012-08-01"}, {"u": "f114504f-608b-4e29-8d39-aa567a23f0f0", "name": "time", "val": "02:00"}, {"u": "9ea307cc-e091-44a0-8a27-7ce6140b0bd2", "name": "wteqi_1", "val": 0.0}, {"u": "a798090a-9d59-4ea1-add1-11876900ad49", "name": "preci_1", "val": 16.4}, {"u": "23499b76-a3a3-4e5e-914b-26530b756101", "name": "tobsi_1", "val": 6.6}, {"u": "c9780853-f917-47a4-a28b-a6aa806ebb70", "name": "snwdi_1", "val": 0.0}, {"u": "ed41d8f1-ba10-4fda-ac14-40e4a5c8d8d3", "name": "smsi_1_2s", "val": 8.6}, {"u": "6242aa9a-e4d7-4ab7-ae13-ec52c7d24ba2", "name": "smsi_1_8s", "val": 10.6}, {"u": "21b0c54b-379c-40d5-b75f-535700973d5c", "name": "smsi_1_20s", "val": 14.1}, {"u": "9e686fc9-4ff7-4d56-bcf2-944f5426b3a8", "name": "stoi_1_2", "val": 11.4}, {"u": "afb0683d-7f47-41cb-9fa6-c6b2221c6dab", "name": "stoi_1_8", "val": 13.5}, {"u": "c4f6f080-c1ce-4cbb-8212-7463bf71a2a6", "name": "stoi_1_20", "val": 10.8}, {"u": "63e570fa-1fcf-4e00-a9b6-e17acf8f3105", "name": "sali_1_2", "val": 0.2}, {"u": "f8fe47f0-cd09-4bc7-a6be-6f741eb2faa2", "name": "sali_1_8", "val": 0.2}, {"u": "c7095cc9-2c55-4efe-8ebc-cfa6272a419b", "name": "sali_1_20", "val": 0.2}, {"u": "33643400-c445-4f12-883b-8fea9a238ec9", "name": "rdci_1_2", "val": 6.67}, {"u": "a35b5f15-90a2-427c-9f47-c2945310603f", "name": "rdci_1_8", "val": 7.36}, {"u": "884b425a-abf5-43cb-a515-6aeba2d5dde7", "name": "rdci_1_20", "val": 8.85},{"u": "bbaf887b-93a3-4996-b66b-52afff88cb8c", "name": "site_id", "val": 1188}, {"u": "6dddd270-a0cd-4e81-aeda-f86274b1ae7f", "name": "date", "val": "2012-08-01"}, {"u": "79f73ad9-a812-46d6-85c5-3b649e3dbd0f", "name": "time", "val": "00:00"}, {"u": "fc5b08ed-b67a-4841-a124-92a139196547", "name": "wteqi_1", "val": 0.0}, {"u": "a1a27364-a094-4446-9a26-23fe1c4a9aa7", "name": "preci_1", "val": 18.0}, {"u": "e6b18f63-10b3-4c12-9252-0258328b555f", "name": "tobsi_1", "val": 6.5}, {"u": "19f22a8f-f40e-49a1-ba94-8d2b7737a2ea", "name": "snwdi_1", "val": 0.0}, {"u": "1d882249-7812-4627-93b4-55a3c5d44341", "name": "batxh_1", "val": 14.71}, {"u": "e5801d59-d1e0-4b3f-b84a-c1ebd5c39db8", "name": "batxh_2", "val": 14.22}, {"u": "17edabcd-51f3-471b-9fde-8749f708042f", "name": "batti_1", "val": 13.1}, {"u": "f6e0e76e-0f0d-4c60-835d-fdbda9b69efc", "name": "batti_2", "val": 13.33}, {"u": "bbaf887b-93a3-4996-b66b-52afff88cb8c", "name": "site_id", "val": 1188}, {"u": "6dddd270-a0cd-4e81-aeda-f86274b1ae7f", "name": "date", "val": "2012-08-01"}, {"u": "79f73ad9-a812-46d6-85c5-3b649e3dbd0f", "name": "time", "val": "01:00"}, {"u": "fc5b08ed-b67a-4841-a124-92a139196547", "name": "wteqi_1", "val": 0.0}, {"u": "a1a27364-a094-4446-9a26-23fe1c4a9aa7", "name": "preci_1", "val": 18.0}, {"u": "e6b18f63-10b3-4c12-9252-0258328b555f", "name": "tobsi_1", "val": 6.7}, {"u": "19f22a8f-f40e-49a1-ba94-8d2b7737a2ea", "name": "snwdi_1", "val": 0.0}, {"u": "1d882249-7812-4627-93b4-55a3c5d44341", "name": "batxh_1", "val": 14.71}, {"u": "e5801d59-d1e0-4b3f-b84a-c1ebd5c39db8", "name": "batxh_2", "val": 14.22}, {"u": "17edabcd-51f3-471b-9fde-8749f708042f", "name": "batti_1", "val": 13.06}, {"u": "f6e0e76e-0f0d-4c60-835d-fdbda9b69efc", "name": "batti_2", "val": 13.32},{"u": "bbaf887b-93a3-4996-b66b-52afff88cb8c", "name": "site_id", "val": 1188}, {"u": "6dddd270-a0cd-4e81-aeda-f86274b1ae7f", "name": "date", "val": "2012-08-01"}, {"u": "79f73ad9-a812-46d6-85c5-3b649e3dbd0f", "name": "time", "val": "01:00"}, {"u": "fc5b08ed-b67a-4841-a124-92a139196547", "name": "wteqi_1", "val": 0.0}, {"u": "a1a27364-a094-4446-9a26-23fe1c4a9aa7", "name": "preci_1", "val": 18.0}, {"u": "e6b18f63-10b3-4c12-9252-0258328b555f", "name": "tobsi_1", "val": 6.7}, {"u": "19f22a8f-f40e-49a1-ba94-8d2b7737a2ea", "name": "snwdi_1", "val": 0.0}, {"u": "1d882249-7812-4627-93b4-55a3c5d44341", "name": "batxh_1", "val": 14.71}, {"u": "e5801d59-d1e0-4b3f-b84a-c1ebd5c39db8", "name": "batxh_2", "val": 14.22}, {"u": "17edabcd-51f3-471b-9fde-8749f708042f", "name": "batti_1", "val": 13.06}, {"u": "f6e0e76e-0f0d-4c60-835d-fdbda9b69efc", "name": "batti_2", "val": 13.32}, {"u": "bbaf887b-93a3-4996-b66b-52afff88cb8c", "name": "site_id", "val": 1188}, {"u": "6dddd270-a0cd-4e81-aeda-f86274b1ae7f", "name": "date", "val": "2012-08-01"}, {"u": "79f73ad9-a812-46d6-85c5-3b649e3dbd0f", "name": "time", "val": "02:00"}, {"u": "fc5b08ed-b67a-4841-a124-92a139196547", "name": "wteqi_1", "val": 0.0}, {"u": "a1a27364-a094-4446-9a26-23fe1c4a9aa7", "name": "preci_1", "val": 18.0}, {"u": "e6b18f63-10b3-4c12-9252-0258328b555f", "name": "tobsi_1", "val": 5.8}, {"u": "19f22a8f-f40e-49a1-ba94-8d2b7737a2ea", "name": "snwdi_1", "val": 0.0}, {"u": "1d882249-7812-4627-93b4-55a3c5d44341", "name": "batxh_1", "val": 14.71}, {"u": "e5801d59-d1e0-4b3f-b84a-c1ebd5c39db8", "name": "batxh_2", "val": 14.22}, {"u": "17edabcd-51f3-471b-9fde-8749f708042f", "name": "batti_1", "val": 13.06}, {"u": "f6e0e76e-0f0d-4c60-835d-fdbda9b69efc", "name": "batti_2", "val": 13.31},{"u": "bbaf887b-93a3-4996-b66b-52afff88cb8c", "name": "site_id", "val": 1188}, {"u": "6dddd270-a0cd-4e81-aeda-f86274b1ae7f", "name": "date", "val": "2012-08-01"}, {"u": "79f73ad9-a812-46d6-85c5-3b649e3dbd0f", "name": "time", "val": "04:00"}, {"u": "fc5b08ed-b67a-4841-a124-92a139196547", "name": "wteqi_1", "val": 0.0}, {"u": "a1a27364-a094-4446-9a26-23fe1c4a9aa7", "name": "preci_1", "val": 18.0}, {"u": "e6b18f63-10b3-4c12-9252-0258328b555f", "name": "tobsi_1", "val": 5.4}, {"u": "19f22a8f-f40e-49a1-ba94-8d2b7737a2ea", "name": "snwdi_1", "val": -1.0}, {"u": "1d882249-7812-4627-93b4-55a3c5d44341", "name": "batxh_1", "val": 14.71}, {"u": "e5801d59-d1e0-4b3f-b84a-c1ebd5c39db8", "name": "batxh_2", "val": 14.22}, {"u": "17edabcd-51f3-471b-9fde-8749f708042f", "name": "batti_1", "val": 13.05}, {"u": "f6e0e76e-0f0d-4c60-835d-fdbda9b69efc", "name": "batti_2", "val": 13.3}, {"u": "bbaf887b-93a3-4996-b66b-52afff88cb8c", "name": "site_id", "val": 1188}, {"u": "6dddd270-a0cd-4e81-aeda-f86274b1ae7f", "name": "date", "val": "2012-08-01"}, {"u": "79f73ad9-a812-46d6-85c5-3b649e3dbd0f", "name": "time", "val": "05:00"}, {"u": "fc5b08ed-b67a-4841-a124-92a139196547", "name": "wteqi_1", "val": 0.0}, {"u": "a1a27364-a094-4446-9a26-23fe1c4a9aa7", "name": "preci_1", "val": 18.0}, {"u": "e6b18f63-10b3-4c12-9252-0258328b555f", "name": "tobsi_1", "val": 5.8}, {"u": "19f22a8f-f40e-49a1-ba94-8d2b7737a2ea", "name": "snwdi_1", "val": -1.0}, {"u": "1d882249-7812-4627-93b4-55a3c5d44341", "name": "batxh_1", "val": 14.71}, {"u": "e5801d59-d1e0-4b3f-b84a-c1ebd5c39db8", "name": "batxh_2", "val": 14.22}, {"u": "17edabcd-51f3-471b-9fde-8749f708042f", "name": "batti_1", "val": 13.02}, {"u": "f6e0e76e-0f0d-4c60-835d-fdbda9b69efc", "name": "batti_2", "val": 13.29},{"u": "bbaf887b-93a3-4996-b66b-52afff88cb8c", "name": "site_id", "val": 1188}, {"u": "6dddd270-a0cd-4e81-aeda-f86274b1ae7f", "name": "date", "val": "2012-08-01"}, {"u": "79f73ad9-a812-46d6-85c5-3b649e3dbd0f", "name": "time", "val": "04:00"}, {"u": "fc5b08ed-b67a-4841-a124-92a139196547", "name": "wteqi_1", "val": 0.0}, {"u": "a1a27364-a094-4446-9a26-23fe1c4a9aa7", "name": "preci_1", "val": 18.0}, {"u": "e6b18f63-10b3-4c12-9252-0258328b555f", "name": "tobsi_1", "val": 5.4}, {"u": "19f22a8f-f40e-49a1-ba94-8d2b7737a2ea", "name": "snwdi_1", "val": -1.0}, {"u": "1d882249-7812-4627-93b4-55a3c5d44341", "name": "batxh_1", "val": 14.71}, {"u": "e5801d59-d1e0-4b3f-b84a-c1ebd5c39db8", "name": "batxh_2", "val": 14.22}, {"u": "17edabcd-51f3-471b-9fde-8749f708042f", "name": "batti_1", "val": 13.05}, {"u": "f6e0e76e-0f0d-4c60-835d-fdbda9b69efc", "name": "batti_2", "val": 13.3}, {"u": "bbaf887b-93a3-4996-b66b-52afff88cb8c", "name": "site_id", "val": 1188}, {"u": "6dddd270-a0cd-4e81-aeda-f86274b1ae7f", "name": "date", "val": "2012-08-01"}, {"u": "79f73ad9-a812-46d6-85c5-3b649e3dbd0f", "name": "time", "val": "05:00"}, {"u": "fc5b08ed-b67a-4841-a124-92a139196547", "name": "wteqi_1", "val": 0.0}, {"u": "a1a27364-a094-4446-9a26-23fe1c4a9aa7", "name": "preci_1", "val": 18.0}, {"u": "e6b18f63-10b3-4c12-9252-0258328b555f", "name": "tobsi_1", "val": 5.8}, {"u": "19f22a8f-f40e-49a1-ba94-8d2b7737a2ea", "name": "snwdi_1", "val": -1.0}, {"u": "1d882249-7812-4627-93b4-55a3c5d44341", "name": "batxh_1", "val": 14.71}, {"u": "e5801d59-d1e0-4b3f-b84a-c1ebd5c39db8", "name": "batxh_2", "val": 14.22}, {"u": "17edabcd-51f3-471b-9fde-8749f708042f", "name": "batti_1", "val": 13.02}, {"u": "f6e0e76e-0f0d-4c60-835d-fdbda9b69efc", "name": "batti_2", "val": 13.29}], 'd': {'id': dataset}, 'f': {'id': 30000000}, "geom": {"g": "0103000000010000000D00000007EB8D5A611E5BC03159A31EA2BD4040C0AF1F62831E5BC0B19BE09BA6BD4040ACAB90F2931E5BC019FD2FD7A2BD40401D5646239F1E5BC0949F1B9AB2BD40405EF7E461A11E5BC03D41295AB9BD4040BB633F8BA51E5BC01CAE9CBD33BE4040B611DDB3AE1E5BC0B9F73768AFBE4040139E5E29CB1E5BC0C474E8F4BCBF40408AB1DAFCBF1E5BC05F0795B88EBF40404B211E89971E5BC0EFA62215C6BE4040DD7D1CCD911E5BC01BD099B4A9BE4040983028D3681E5BC03A8EE6C8CABD404007EB8D5A611E5BC03159A31EA2BD4040"}, 'key': key}
#        docs.append(basicdoc)

#   # return docs

#    connstr = request.registry.settings['mongo_uri']
#    collection = request.registry.settings['mongo_collection']
#    mongo_uri = gMongoUri(connstr, collection)
#    gm = gMongo(mongo_uri)    

##    try:
##        fail = gm.insert(docs)
##        if fail:
##            #TODO: run a delete for the dataset id just in case it failed midstream
##            return HTTPServerError(fail)
##    except Exception as err:
##        return HTTPServerError(err)
##    finally:
##        gm.close()
#    
#    return {'key': key, 'id': dataset, 'docs': len(docs)}













#    
