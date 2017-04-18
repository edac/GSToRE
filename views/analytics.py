#from ..lib.mongo import gMongo
from pyramid.view import view_config
from pyramid.response import Response
from ..lib.mongo import gMongo
from ..lib.mongo import gMongoUri
from ..lib.database import *
from StringIO import StringIO
from ..lib.utils import *
import datetime
import csv
import urllib2   
#Soooon!
import pandas as pd
#import numpy as np


@view_config(route_name='analyticsdata')
def analyticsdata(request):
    #get params if there are any
    params = normalize_params(request.params)
    attrs = params.get('attributes', '')

    app = request.matchdict['app']
    dataset_id = request.matchdict['id']
    format = request.matchdict['ext']
    #get the dataset from id
    d = get_dataset(dataset_id)
    mconn = request.registry.settings['mongo_uri']
    mcoll = request.registry.settings['mongo_collection']
    mongo_uri = gMongoUri(mconn, mcoll)
    line=""
    label=""
    gm = gMongo(mongo_uri)
    response = Response()



    if format=="csv":
        response.content_type = 'text'
        response.text=MakeResponse(d,gm,normalize_params(request.params),format)

    elif format=="json":
        response.content_type = 'application/json'
        response.body=MakeResponse(d,gm,normalize_params(request.params),format)

    elif format=="html":
        response.content_type = 'text/html'
        response.text=MakeResponse(d,gm,normalize_params(request.params),format)

    response.headers['Access-Control-Allow-Origin'] = '*'
    return response


def MakeResponse(d,gm,params,format):
    line=""
    label=""

    fields = d.attributes

    labelformat = params['labelformat'] if 'labelformat' in params else "name"
    limit = int(params['limit']) if 'limit' in params else d.record_count + 10000000
    offset = int(params['offset']) if 'offset' in params else 0
    sort = []
    if 'sort' in params and 'order' in params:
        #NOTE: this is only going to work for observed AS OBS right now
        sort = [(params['sort'].lower(), 1 if params['order'] == 'asc' else -1)]

    #add the basic parameters (limit, offset, sort)
    vectors = gm.query({'d.id': d.id}, None, sort, limit, offset)
    firstrecord = gm.query({'d.id': d.id },limit=1)
    for record1 in firstrecord:
        geom_repr=""
        record_id = record1['f']['id']
        observed = record1['obs'].strftime('%Y-%m-%dT%H:%M:%S+00') if 'obs' in record1 else ''
        atts = record1['atts']
        datavalues = [(a['name'], convert_by_ogrtype(a['val'], ogr.OFTString, "csv") if isinstance(a['val'], str) or isinstance(a['val'], unicode) else a['val']) for a in atts]
        datavalues.insert(0,(u'observed', observed))
        delimiter = '\n'
        for tuple in datavalues:
            if tuple[0]!="date":
                if tuple[0]!="time":
                    if tuple[0]!="site_id":

                            label=label + str(tuple[0])+","
        label=label[:-1] + delimiter
        if labelformat=="original_name":
            for a in fields:
                label=label.replace(a.name, a.orig_name)
        if labelformat=="description":
            for a in fields:
                label=label.replace(a.name, a.description)

    is_spatial = False
    allvals=""
    for vector in vectors:
        geom_repr=""
        record_id = vector['f']['id']
        observed = vector['obs'].strftime('%Y-%m-%dT%H:%M:%S+00') if 'obs' in vector else ''
        atts = vector['atts']
        datavalues = [(a['name'], convert_by_ogrtype(a['val'], ogr.OFTString, "csv") if isinstance(a['val'], str) or isinstance(a['val'], unicode) else a['val']) for a in atts]
        observed = datetime.datetime.strptime(observed, '%Y-%m-%dT%H:%M:%S+00').strftime('%Y/%m/%d %H:%M:%S')
        datavalues.insert(0,(u'observed', observed))
        delimiter = '\n'
        for tuple in datavalues:
            if tuple[0]!="date":
                if tuple[0]!="time":
                    if tuple[0]!="site_id":
                            line=line + str(tuple[1])+","
        line=line[:-1] + delimiter
    response = label + line
    response = response.replace("-99.9","NaN")
    response = response.replace("observed","Date")
 
    if format=="csv":
        return response
    elif format=="json":
        DaDATA=StringIO(response)
        df = pd.read_csv(DaDATA, sep=",", index_col = ["Date"]) 
        jsonresponse=df.describe().to_json()
        return jsonresponse
    elif format=="html":
        DaDATA=StringIO(response)
        df = pd.read_csv(DaDATA, sep=",", index_col = ["Date"])
        jsonresponse=df.describe().transpose().to_html(classes="table table-hover text-centered")
        return jsonresponse

