from pyramid.view import view_config
from pyramid.response import Response, FileResponse
from pyramid.httpexceptions import HTTPNotFound, HTTPFound

from pyramid.threadlocal import get_current_registry

import json
import pymongo
#from urlparse import urlparse

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )


from ..lib.utils import *
from ..lib.database import *
from ..lib.mongo import gMongo

from datetime import datetime

import os 
import Image
import mapscript
from cStringIO import StringIO



def to_json(doc):
    d = doc['d'] if 'd' in doc else {}

    did = d['id']
    duuid = d['u']

    f = doc['f'] if 'f' in doc else {}

    fid = f['id']
    fuuid = f['u']

    geom = doc['geom']['g']

    props = doc['atts'] if 'atts' in doc else []
    ps = []
    for p in props:
        name = p['name']
        uuid = p['u']
        value = p['val']
        ps.append({'name': name, 'uuid': uuid, 'value': value})
        
    
    return {'type': 'Feature', 'dataset': {'id': did, 'uuid': duuid}, 'feature': {'fid': fid, 'uuid': fuuid}, 'geom': geom,
                    'properties': ps}

def to_csv(doc):
    d = doc['d'] if 'd' in doc else {}

    did = str(d['id'])
    duuid = str(d['u'])

    f = doc['f'] if 'f' in doc else {}

    fid = str(f['id'])
    fuuid = str(f['u'])

    geom = doc['geom']['g']

    output = [did, duuid, fid, fuuid]
    fields = ['dataset id', 'dataset uuid', 'fid', 'feature uuid']
    
    props = doc['atts'] if 'atts' in doc else []
    ps = []
    for p in props:
        name = p['name']
        uuid = p['u']
        value = p['val']
        ps.append({'name': name, 'uuid': uuid, 'value': value})

        output.append(value) #need to escape this (or replace the commas, stupid people)
        fields.append(name)


    return fields, output

@view_config(route_name='test_wcs')
def test_ogc(request):
    uuid = 'a427563f-3c7e-44a2-8b35-68ce2a78001a'
    d = get_dataset(uuid) 

    params = request.params
    ogc_req = params.get('REQUEST', 'GetCapabilities')

    #mapsrc, srcloc = d.get_mapsource()

    #bbox = [float(b) for b in d.box]

    #f = '/clusterdata/gstore/maps/a427563f-3c7e-44a2-8b35-68ce2a78001a.76744073-525b-4cd0-a786-e5f6c22f821b.map'
    #f = '/clusterdata/gstore/maps/35107b42.map'
    f = '/clusterdata/gstore/maps/8f11ec21-dfe1-437d-a7be-a85bbb9e4283.e45fcdeb-a487-4daa-aeba-a25e28377fa2.map'
    m = mapscript.mapObj(f)

    #let's play with the wcs response
    req = mapscript.OWSRequest()
    req.setParameter('SERVICE', params.get('SERVICE', 'WMS'))
    req.setParameter('VERSION', params.get('VERSION', '1.1.1'))
    req.setParameter('REQUEST', ogc_req)

    if ogc_req.lower() == 'describecoverage':
        mapscript.msIO_installStdoutToBuffer()
        m.OWSDispatch(req)
        content_type = mapscript.msIO_stripStdoutBufferContentType()
        content = mapscript.msIO_getStdoutBufferBytes()
        return Response(content)
    if ogc_req.lower() == 'getcapabilities':
        mapscript.msIO_installStdoutToBuffer()
        m.OWSDispatch(req)
        content_type = mapscript.msIO_stripStdoutBufferContentType()
        content = mapscript.msIO_getStdoutBufferBytes()

        if 'xml' in content_type:
            content_type = 'application/xml'

        #TODO: double check all of this
        
        return Response(content, content_type=content_type)
    
    return Response('hi')

@view_config(route_name='test_fmt')
def format_tester(request):
    #check if mapserver handles sids and ecws
    fmt = request.matchdict['fmt']

    #just run with getmap (since that's what we really care about anyway)
    #test uuid for sid - c2e948ee-2ea9-4dc4-935b-b5e56ff8071c
    #       /clusterdata/gstore/maps/c2e948ee-2ea9-4dc4-935b-b5e56ff8071c.bc0b2893-d5eb-4a75-83ad-74abc9749263.map
    #YAY THE SID WORKS
    #test uuid for ecw - 0f081ca5-a7bf-45e8-8f96-dd7fba5df4d4
    #       /clusterdata/gstore/maps/0f081ca5-a7bf-45e8-8f96-dd7fba5df4d4.7c015173-50bd-4c47-b3e0-3bd399162387.map
    #YAY THE ECW WORKS

    #for the sid, note that i changed the filepath for this manually
    #f = '/clusterdata/gstore/maps/c2e948ee-2ea9-4dc4-935b-b5e56ff8071c.bc0b2893-d5eb-4a75-83ad-74abc9749263.map'

    f = '/clusterdata/gstore/maps/0f081ca5-a7bf-45e8-8f96-dd7fba5df4d4.7c015173-50bd-4c47-b3e0-3bd399162387.map'
    m = mapscript.mapObj(f)
    req = mapscript.OWSRequest()
    req.setParameter('SERVICE', 'WMS')
    req.setParameter('VERSION', '1.1.1')
    req.setParameter('REQUEST', 'GetMap')
    
    req.setParameter('WIDTH', '256')
    req.setParameter('HEIGHT', '256')

    #for the sid
    #eq.setParameter('BBOX', '-103.068,36.9349,-102.995,37.0026')

    #for the ecw
    req.setParameter('BBOX', '-108.038,35.4006,-107.899,35.5369')
    req.setParameter('STYLES', '')

    #dataset.basename
    #layers = str(params['LAYERS']) if 'LAYERS' in params else ''
    #req.setParameter('LAYERS', params.get('LAYERS', ''))
    req.setParameter('LAYERS', 'RGIS_Dataset')
    req.setParameter('FORMAT', 'image/png')
    req.setParameter('SRS', 'EPSG:4326')

    m.loadOWSParameters(req)
    img = Image.open(StringIO(m.draw().getBytes()))
    buffer = StringIO()
    #TODO: change png to the specified output type
    img.save(buffer, 'PNG')
    buffer.seek(0)
    #TODO: change content-type based on specified output type
    return Response(buffer.read(), content_type='image/png')

@view_config(route_name='test_url', renderer='json')
def url_tester(request):
    ext = request.matchdict['ext']
    dataset_id = request.matchdict['id']
    dataset_type = request.matchdict['type']
    basename = request.matchdict['basename']

    
    return {'id': dataset_id, 'basename': basename, 'set': dataset_type, 'extension': ext}

@view_config(route_name='test', renderer='dataone_logs.mako')
def tester(request):
    dataset_id = request.matchdict['id']
    ext = request.matchdict['ext']

    '''
    ids to test

    52218: 32517de4-5301-4e45-aa4d-580591165cca
    55355
    55356
    107352: a4e33c15-849c-4a02-a0ff-78d72b52daff
    107353
    107365: c329d0f6-ca5f-47bc-ad98-41cd52f614c6
    107405

    '''

    #for the dataone log renderers
    #2012-02-29T23:26:38.828+00:00
    fmt = '%Y-%m-%dT%H:%M:%S+00:00'
    post = {'total': 45, 'results': 3, 'offset': 0}
    docs = [
            {'id': 1, 'identifier': dataset_id, 'ip': '129.24.63.165', 'useragent': 'null', 'subject': 'CN=GStore,dc=informatics,dc=org', 'event':'read', 'dateLogged':datetime.utcnow().strftime(fmt), 'node': 'GSTORE'},
            {'id': 2, 'identifier': dataset_id, 'ip': '129.24.63.55', 'useragent': 'null', 'subject': 'CN=GStore,dc=informatics,dc=org', 'event':'read', 'dateLogged':datetime.utcnow().strftime(fmt), 'node': 'GSTORE'},
            {'id': 3, 'identifier': dataset_id, 'ip': '129.24.63.235', 'useragent': 'null', 'subject': 'CN=GStore,dc=informatics,dc=org', 'event':'read', 'dateLogged':datetime.utcnow().strftime(fmt), 'node': 'GSTORE'}
        ]
    post.update({'docs': docs})
    return post

#    #for mongo
#    #go get the dataset
#    d = get_dataset(dataset_id)

#    if not d:
#        return HTTPNotFound('No results')

#    #make sure it's available
#    #TODO: replace this with the right status code
#    if d.is_available == False:
#        return HTTPNotFound('Temporarily unavailable')

#    #with the new class
#    connstr = get_current_registry().settings['mongo_uri']
#    gm = gMongo(connstr, 'vectors')

#    vectors = gm.query({'d.id': d.id})

#    #let's do stuff with stuff
#    atts = d.attributes

#    fields = []
#    res = []
#    for v in vectors:
#        if ext == 'json':
#            r = to_json(v)
#        elif ext == 'csv':
#            flds, r = to_csv(v)
#            if not fields:
#                fields = flds
#            r = ','.join(r)

#        res.append(r)

#    #close the mongo connection
#    gm.close()

#    if ext == 'json':
#        content_type = 'application/json'
#        rsp = json.dumps({'total': vectors.count(), 'results': res})
#    elif ext == 'csv':
#        content_type = 'text/csv; charset=UTF-8' #or application/csv
#        rsp = (','.join(fields) + '\n' + '\n'.join(res)).encode('utf8')


#    return Response(rsp, content_type=content_type)
#    #end mongo




















    
