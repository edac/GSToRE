import requests
import json
from pyramid.view import view_config
from pyramid.response import Response
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPBadRequest, HTTPServerError
from owslib.wms import WebMapService
from owslib.wfs import WebFeatureService
from owslib.wcs import WebCoverageService
import os
from pwd import getpwuid 
from ..lib.mongo import gMongoUri
from ..models import DBSession
from ..models.apps import GstoreApp
from ..lib.es_searcher import *
import requests


def checkperms(directory, perm):
    body=""
    if (os.access(directory, os.F_OK)):
        body=body+'<div class="row"><div class="col-md-4"></div><div class="col-md-4">'
        body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> Directory '+directory+' exists.</p>'
        body=body+'</div><div class="col-md-4"></div></div>'
    else:
        body=body+'<div class="row"><div class="col-md-4"></div><div class="col-md-4">'
        body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> Directory '+directory+' exists.</p>'
        body=body+'</div><div class="col-md-4"></div></div>'
    if (perm=='r'):
        if (os.access(directory, os.R_OK)):
            body=body+'<div class="row"><div class="col-md-4"></div><div class="col-md-4">'
            body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> Directory '+directory+' is readable.</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
        else:
            body=body+'<div class="row"><div class="col-md-4"></div><div class="col-md-4">'
            body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> Directory '+directory+' is NOT readable.</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
    elif (perm=='rw'):
        if (os.access(directory, os.R_OK)):
            body=body+'<div class="row"><div class="col-md-4"></div><div class="col-md-4">'
            body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> Directory '+directory+' is readable.</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
        else:
            body=body+'<div class="row"><div class="col-md-4"></div><div class="col-md-4">'
            body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> Directory '+directory+' is NOT readable.</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
        if (os.access(directory, os.W_OK)):
            body=body+'<div class="row"><div class="col-md-4"></div><div class="col-md-4">'
            body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> Directory '+directory+' is writeable.</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
        else:
            body=body+'<div class="row"><div class="col-md-4"></div><div class="col-md-4">'
            body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> Directory '+directory+' is NOT writable.</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
    return body


@view_config(route_name='test')
def test(request):
    body='<!DOCTYPE html><html><head><link rel="stylesheet" href="javascript/bootstrap/css/bootstrap.css"><script src="javascript/bootstrap/js/bootstrap.js"></script></head><body><h1>GSToRE Tests</h1>'





    r = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3')
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    body=body+'</div><div class="col-md-4">'
    body=body+'<h3 style="text-align: left; margin-top: 0px;" >Search Tests</h3>'
    
    if r.status_code==200:
        statusString="Basic Search status code is: "+str(r.status_code)
        totalstr="Total results: "+str(r.json()["total"])
        body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span>  '+statusString+'</p>'
        body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span>  '+totalstr+'</p>'
        body=body+'</div><div class="col-md-4"></div></div>'
    else:
               
        statusString="Basic Search status code is: "+str(r.status_code)
       
        body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span>  '+statusString+'</p>'

        body=body+'</div><div class="col-md-4"></div></div>'

    r = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3&author="fake"')
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    # body=body+'<h3 style="text-align: center; margin-top: 0px;" >Search Tests</h3>'
    body=body+'</div><div class="col-md-4">'
    if r.status_code==200 and r.json()["total"]==0:
        statusString="Author Search status code is: "+str(r.status_code)
        body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span>  '+statusString+'</p>'
        body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> Author search was sucsessfull</p>'
        body=body+'</div><div class="col-md-4"></div></div>'
    else:
               
        statusString="Basic Search status code is: "+str(r.status_code)
       
        body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span>  '+statusString+'</p>'
        body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> Author search failed.</p>'
        body=body+'</div><div class="col-md-4"></div></div>'

    r = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3&start_time=20110901&limit=1')
    nothingr = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3&start_time=30110901&limit=1')
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    body=body+'</div><div class="col-md-4">'
    if r.json()["total"]>=1 and nothingr.json()["total"]==0:
        body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> Temporal search was sucsessfull</p>'
        body=body+'</div><div class="col-md-4"></div></div>'
    else:
               
        statusString="Basic Search status code is: "+str(r.status_code)
       
        body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> Temporal search failed.</p>'
        body=body+'</div><div class="col-md-4"></div></div>'

        
    r = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3&box=-111.289044%2C31.082063%2C-101.217075%2C37.197485')
    nothingr = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3&box=-111.729662%2C35.695988%2C-111.371736%2C36.04996')
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    body=body+'</div><div class="col-md-4">'
    if r.json()["total"]>=1 and nothingr.json()["total"]==0:
        body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> Spatial search was sucsessfull</p>'
        body=body+'</div><div class="col-md-4"></div></div>'
    else:      
        statusString="Basic Search status code is: "+str(r.status_code)
        body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> Spatial search failed.</p>'
        body=body+'</div><div class="col-md-4"></div></div>'
    body=body+'<br></br>'



    r = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3&limit=1&taxonomy=vector')
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    body=body+'</div><div class="col-md-4">'
    body=body+'<h3 style="text-align: left; margin-top: 0px;" >Download Tests</h3>'

    if r.status_code==200:
        csvurl=r.json()['results'][0]['downloads'][0]['csv']
        print csvurl
        csv = requests.get(csvurl)
        if csv.status_code==200:
            statusString="Basic CSV transform download status code is: "+str(r.status_code)
            totalstr="Total results: "+str(r.json()["total"])
            body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span>  '+statusString+'</p>'
            body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span>  '+totalstr+'</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
        else:
            statusString="Basic CSV Download status code is: "+str(r.status_code)       
            body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span>  '+statusString+'</p>'
            body=body+'</div><div class="col-md-4"></div></div>'    
    else:
               
        statusString="Basic Download status code is: "+str(r.status_code)
       
        body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span>  '+statusString+'</p>'

        body=body+'</div><div class="col-md-4"></div></div>'


#WMS
    body=body+'<br></br>'
    r = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3&service=wms')
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    body=body+'</div><div class="col-md-4">'
    body=body+'<h3 style="text-align: left; margin-top: 0px;" >Service Tests</h3>'

    if r.status_code==200:
        wmsurl=r.json()['results'][0]['services'][0]['wms']
        wms = WebMapService(wmsurl, version='1.1.1')
        try:
            wms
        except NameError:
            body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span>WMS Service Connection FAILED!</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
        else:
            title=wms.identification.title
            body=body+'<p>Testing WMS for layer '+ title +'</p>'
            body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> WMS Service Success</p>'
            body=body+'</div><div class="col-md-4"></div></div>'

#WFS
    r = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3&service=wfs')
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    body=body+'</div><div class="col-md-4">'
    if r.status_code==200:
        wfsurl=r.json()['results'][0]['services'][1]['wfs']
        wfs = WebFeatureService(url=wfsurl, version='1.0.0')
        try:
            wfs
        except NameError:
            body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span>WFS Service Connection FAILED!</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
        else:        
            title=wfs.identification.title
            body=body+'<p>Testing WFS for layer '+ title +'</p>'
            body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> WFS Service Success</p>'
            body=body+'</div><div class="col-md-4"></div></div>'

#WCS
    r = requests.get('http://localhost/gstore_v3/apps/gstore/search/datasets.json?version=3&service=wcs')
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    body=body+'</div><div class="col-md-4">'
    if r.status_code==200:
        wcsurl=r.json()['results'][0]['services'][1]['wcs']
        
        wcs = WebCoverageService(url=wcsurl, version='1.0.0')
        try:
            wcs
        except NameError:
            body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span>WCS Service Connection FAILED!</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
        else:        
            title=wcs.identification.title
            body=body+'<p>Testing WCS for layer '+ title +'</p>'
            body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> WCS Service Success</p>'
            body=body+'</div><div class="col-md-4"></div></div>'

#OS
    body=body+'<br>'
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    body=body+'</div><div class="col-md-4">'
    body=body+'<h3 style="text-align: left; margin-top: 0px;" >Folder Permissions</h3>'

    body=body+'</div><div class="col-md-4"></div></div>'
    body=body+checkperms('/clusterdata',"rw")
    body=body+checkperms('/clusterdata/gstore',"rw")
    body=body+checkperms('/clusterdata/gstore/dataone',"r")
    body=body+checkperms('/clusterdata/gstore/formats',"rw")
    body=body+checkperms('/geodata',"r")

#DB
    body=body+'<br>'
    body=body+'<div class="row">'
    body=body+'<div class="col-md-4">'
    body=body+'</div><div class="col-md-4">'
    body=body+'<h3 style="text-align: left; margin-top: 0px;" >Database Connectivity</h3>'

    body=body+'</div><div class="col-md-4"></div></div>'
        #set up the mongo connection
    mconn = request.registry.settings['mongo_uri']
    mcoll = request.registry.settings['mongo_collection']
    mongo_uri = gMongoUri(mconn, mcoll)
    print mongo_uri
    try:
            mongo_uri
    except NameError:
            body=body+'<div class="row">'
            body=body+'<div class="col-md-4">'
            body=body+'</div><div class="col-md-4">'
            body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> MongoDB Connection FAILED!</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
    else:        
            
            body=body+'<div class="row">'
            body=body+'<div class="col-md-4">'
            body=body+'</div><div class="col-md-4">'
            body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> MongoDB Connection Success</p>'
            body=body+'</div><div class="col-md-4"></div></div>'
    

    try:
        req_app = DBSession.query(GstoreApp).first()
    except NameError:
        body=body+'<div class="row">'
        body=body+'<div class="col-md-4">'
        body=body+'</div><div class="col-md-4">'
        body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> PostgreSQL Connection FAILED!</p>'
        body=body+'</div><div class="col-md-4"></div></div>'
    else:                
        body=body+'<div class="row">'
        body=body+'<div class="col-md-4">'
        body=body+'</div><div class="col-md-4">'
        body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> PostgreSQL Connection Success</p>'
        body=body+'</div><div class="col-md-4"></div></div>'




    searcher = EsSearcher(
        {
            "host": request.registry.settings['es_root'], 
            "index": request.registry.settings['es_dataset_index'], 
            "type": 'datasets', 
            "user": request.registry.settings['es_user'].split(':')[0], 
            "password": request.registry.settings['es_user'].split(':')[-1]
        }
    )
    try:
        searcher.parse_basic_query('gstore', {'limit': '1'})
    except NameError:
        body=body+'<div class="row">'
        body=body+'<div class="col-md-4">'
        body=body+'</div><div class="col-md-4">'
        body=body+'<p><span style="color:red;" class="glyphicon glyphicon-remove-sign"</span> Elasticsearch Connection FAILED!</p>'
        body=body+'</div><div class="col-md-4"></div></div>'
    else:
        body=body+'<div class="row">'
        body=body+'<div class="col-md-4">'
        body=body+'</div><div class="col-md-4">'
        body=body+'<p><span style="color:green;" class="glyphicon glyphicon-ok"</span> Elasticsearch Connection Success</p>'
        body=body+'</div><div class="col-md-4"></div></div>'


################################


    body = body+'</body></html>'
    bodybytes=str.encode(body)
    resp=Response()
    resp.content_type = 'text/html'
    resp.body=bodybytes
    return resp