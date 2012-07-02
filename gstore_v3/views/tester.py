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

@view_config(route_name='test')
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

    #go get the dataset
    d = get_dataset(dataset_id)

    if not d:
        return HTTPNotFound('No results')

    #make sure it's available
    #TODO: replace this with the right status code
    if d.is_available == False:
        return HTTPNotFound('Temporarily unavailable')

    #with the new class
    connstr = get_current_registry().settings['mongo_uri']
    gm = gMongo(connstr, 'vectors')

    vectors = gm.query({'d.id': d.id})

    #let's do stuff with stuff
    atts = d.attributes

    fields = []
    res = []
    for v in vectors:
        if ext == 'json':
            r = to_json(v)
        elif ext == 'csv':
            flds, r = to_csv(v)
            if not fields:
                fields = flds
            r = ','.join(r)

        res.append(r)

    #close the mongo connection
    gm.close()

    if ext == 'json':
        content_type = 'application/json'
        rsp = json.dumps({'total': vectors.count(), 'results': res})
    elif ext == 'csv':
        content_type = 'text/csv; charset=UTF-8' #or application/csv
        rsp = (','.join(fields) + '\n' + '\n'.join(res)).encode('utf8')


    return Response(rsp, content_type=content_type)





















    
