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

@view_config(route_name='analyticspage', renderer='../templates/analytics.pt')
def analyticspage(request):

  #  params = normalize_params(request.params)
 #   attrs = params.get('attributes', '')

    app = request.matchdict['app']
    dataset_id = request.matchdict['id']


    aurl = 'http://129.24.63.66/gstore_v3/apps/'+app+'/datasets/'+dataset_id+'/analytics.csv'
# + attrs    
    params=""
    items = ""
    truefalse = ""
    response = urllib2.urlopen(aurl)
    cr = csv.reader(response)
    for i in range(1):
       params=cr.next()
    print params 
    for param in params:
#one change
      if param != "Date":
          n=params.index(param)-1
      
          items = items + '      <input type=checkbox id="'+ str(n) +'" onClick="change(this)">'
          items = items + '<label for="'+ str(n) +'">' + param + '</label><br/>'
          truefalse = truefalse + "false,"
#   for row in cr:
 #       if len(row) <= 1: continue
  #      print row
    
    print aurl
    htmltruefalse = truefalse.rstrip(',')
    return dict(
        url = aurl,
        htmlitems = items,
        truefalse = htmltruefalse
        )


#def fixparams(params):
    

@view_config(route_name='analyticsdata')
def analyticsdata(request):
    #get params if there are any
    params = normalize_params(request.params)
    attrs = params.get('attributes', '')

    app = request.matchdict['app']
    dataset_id = request.matchdict['id']
    format = request.matchdict['ext']
 #   basename = request.matchdict['basename']
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
	response.body=getCSV(d,gm,attrs)
#    TESTDATA=StringIO(response.body)
#    df = pd.read_csv(TESTDATA, sep=",", index_col = ["date","time"])

    elif format=="tsv":
        response.body = '''Broken'''
    elif format=="json":
        response.body = '''Broken'''
        response.content_type = 'application/json'
        for name in list(df.columns.values):
            print name


    return response


def getJSstuff(d,gm):
    line=""
    label=""
    vectors = gm.query({'d.id': d.id })
    firstrecord = gm.query({'d.id': d.id },limit=1)
    for record1 in firstrecord:
        geom_repr=""
        record_id = record1['f']['id']
        observed = record1['obs'].strftime('%Y-%m-%dT%H:%M:%S+00') if 'obs' in record1 else ''
        atts = record1['atts']
        datavalues = [(a['name'], convert_by_ogrtype(a['val'], ogr.OFTString, "csv") if isinstance(a['val'], str) or isinstance(a['val'], unicode) else a['val']) for a in atts]
        datavalues.append((u'observed', observed))
        delimiter = '\n'
        for tuple in datavalues:
            if tuple[0]!="date":
                if tuple[0]!="time":
                            label=label + str(tuple[0])+","
        label=label[:-1] + delimiter

    is_spatial = False
    allvals=""
    for vector in vectors:
        geom_repr=""
        record_id = vector['f']['id']
        observed = vector['obs'].strftime('%Y-%m-%dT%H:%M:%S+00') if 'obs' in vector else ''
        atts = vector['atts']
        datavalues = [(a['name'], convert_by_ogrtype(a['val'], ogr.OFTString, "csv") if isinstance(a['val'], str) or isinstance(a['val'], unicode) else a['val']) for a in atts]
        datavalues.append((u'observed', observed))
        delimiter = '\n'
        for tuple in datavalues:
            if tuple[0]!="date":
                if tuple[0]!="time":
                            line=line + str(tuple[1])+","
        line=line[:-1] + delimiter
    responsenull = label + line
    response = responsenull.replace("-99.9","0")
    labellist = label.split(",")
 #   for lab in labellist:
  #       if lab !='observed\n':
   #         annstring= lab + ":" + str(df[lab].mean())
    #        print annstring
    return response




#    labels: ['2014-03-01T08:00:00+00', '2014-03-01T09:00:00+00', '2014-03-01T10:00:00+00', '2014-03-01T11:00:00+00', '2014-03-01T11:0$
 #   datasets: [{
  #    label: 'tobsi',
   #   data: [9.4, 9.6, 9.6, 11.4, 9.2, 3, 7],
#      backgroundColor: "rgba(153,255,51,0.6)"
 #   }, {
  #    label: 'stoi_1_20,',
   #   data: [19, 27, 22, 14, 16, 13, 10],
    #  backgroundColor: "rgba(255,153,0,0.6)"
 #   }]
 # }



def getCSV(d,gm,attrs):
#    gm = gMongo(mongo_uri)
    print attrs
    line=""
    label=""
    vectors = gm.query({'d.id': d.id })
    firstrecord = gm.query({'d.id': d.id },limit=1)
    for record1 in firstrecord:
        geom_repr=""
        record_id = record1['f']['id']
        observed = record1['obs'].strftime('%Y-%m-%dT%H:%M:%S+00') if 'obs' in record1 else ''
        atts = record1['atts']
        datavalues = [(a['name'], convert_by_ogrtype(a['val'], ogr.OFTString, "csv") if isinstance(a['val'], str) or isinstance(a['val'], unicode) else a['val']) for a in atts]
        datavalues.insert(0,(u'observed', observed))
        delimiter = '\n'
#    content_type = 'text/csv; charset=UTF-8'
        for tuple in datavalues:
            if tuple[0]!="date":
                if tuple[0]!="time":
                    if tuple[0]!="site_id":

                            label=label + str(tuple[0])+","
        label=label[:-1] + delimiter

    is_spatial = False
    allvals=""
    for vector in vectors:
#        str="smsi_1_40l"
        geom_repr=""
        record_id = vector['f']['id']
        observed = vector['obs'].strftime('%Y-%m-%dT%H:%M:%S+00') if 'obs' in vector else ''
        atts = vector['atts']
        datavalues = [(a['name'], convert_by_ogrtype(a['val'], ogr.OFTString, "csv") if isinstance(a['val'], str) or isinstance(a['val'], unicode) else a['val']) for a in atts]
 #       print observed
        observed = datetime.datetime.strptime(observed, '%Y-%m-%dT%H:%M:%S+00').strftime('%Y/%m/%d %H:%M:%S')
#        print observed
#        datavalues.append((u'observed', observed))
        datavalues.insert(0,(u'observed', observed))
#        print datavalues
        delimiter = '\n'
#    content_type = 'text/csv; charset=UTF-8'
        for tuple in datavalues:
            if tuple[0]!="date":
                if tuple[0]!="time":
                    if tuple[0]!="site_id":
                            line=line + str(tuple[1])+","
        line=line[:-1] + delimiter
  #  print label
    #response = Response()
    #response.content_type = 'text/html'
    responsenull = label + line
    response = responsenull.replace("-99.9","0")
    response = response.replace("observed","Date")
    DaDATA=StringIO(response)

    df = pd.read_csv(DaDATA, sep=",", index_col = ["Date"])


#    print df
#    annstring=""
#    print label
 #   labellist = label.split(",")
  #  for lab in labellist:
   #      print lab
   #      if lab !='observed\n':
    #        annstring= lab + ":" + str(df[lab].mean())
     #       print annstring
    return response
 #   print "REsponse; " + response
#    DaDATA=StringIO(response.body)

#    df = pd.read_csv(DaDATA, sep=",", index_col = ["observed"])

