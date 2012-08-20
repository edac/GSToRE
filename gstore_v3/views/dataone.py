from pyramid.view import view_config
from pyramid.response import Response, FileResponse

from pyramid.httpexceptions import HTTPNotFound, HTTPServerError

from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_
from sqlalchemy.sql import between

from datetime import datetime
import urllib2

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )
from ..models.dataone import *

from ..lib.utils import *
from ..lib.database import *
from ..lib.mongo import gMongo, gMongoUri

import os

'''
make sure that the general errors are being posted correctly (404, 500, etc)
at the app level

see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html
'''

'''
some presets
'''
#TODO: move to config?
NODE = 'urn:node:GSTORE'
SUBJECT = 'CN=GStore,DC=dataone,DC=org'
RIGHTSHOLDER = 'CN=GStore,DC=dataone,DC=org'
CONTACTSUBJECT = 'CN=GStore,DC=dataone,DC=org'
IDENTIFIER = ''
NAME = ''
DESCRIPTION = ''


#convert to the d1 format
def datetime_to_dataone(dt):
    #TODO: deal with timezone-y issues
    fmt = '%Y-%m-%dT%H:%M:%S.0Z'
    return dt.strftime(fmt)
    
def datetime_to_http(dt):
    #TODO: utc to gmt
    #Wed, 16 Dec 2009 13:58:34 GMT
    fmt = '%a, %d %b %Y %H:%M:%S GMT'
    return dt.strftime(fmt)

def dataone_to_datetime(dt):
    #TODO: deal with more datetime issues (could be gmt or utc, with or without milliseconds)
    fmt = '%Y-%m-%dT%H:%M:%S'
    dt = dt.replace('+00:00', '') if '+00:00' in dt else dt
    return datetime.strptime(dt, fmt)


#some generic error handling 
#that would be nicer if dataone was consistent in their error handling (or their documentation was consistent, i don't know which)
def return_error(error_type, detail_code, error_code, error_message='', pid=''):  
    if error_code == 404 and error_type == 'object':
        xml = '<?xml version="1.0" encoding="UTF-8"?><error detailCode="%s" errorCode="404" name="NotFound"><description>No system metadata could be found for given PID: DOESNTEXIST</description></error>' % (detail_code, error_code)
        return Response(xml, content_type='text/xml; charset=UTF-8', status='404')

    elif error_code == 404 and error_type == 'metadata':
        xml = '<?xml version="1.0" encoding="UTF-8"?><error detailCode="%s" errorCode="404"><description>No system metadata could be found for given PID: %s</description></error>' % (detail_code, pid)
        return Response(xml, content_type='text/xml; charset=UTF-8', status='404')

    return Response()
'''
dataone logging in mongodb
'''
def log_entry(identifier, ip, event, mongo_uri, useragent=None):
    gm = gMongo(mongo_uri)

    #TODO: possible that identifier is just the pid?
    gm.insert({"identifier": "" + identifier, "ip": ip, "useragent": useragent, "subject": SUBJECT, "date": datetime.utcnow(), "event": event, "node": NODE})

    gm.close()

def query_log(mongo_uri, querydict, limit=0, offset=0):
    gm = gMongo(mongo_uri)
    if limit:
        logs = gm.query(querydict, {}, {}, limit, offset)
    else:
        logs = gm.query(querydict)
    gm.close()
    return logs

#TODO: modify the cache settings
@view_config(route_name='dataone_ping', match_param='app=dataone', http_cache=3600)
def ping(request):
    #curl -v  http://129.24.63.66/gstore_v3/apps/dataone/monitor/ping
    
    #raise notimplemented
    #raise servicefailure
    #raise insufficientresources

    #run a quick test to make sure the db connection is active
    try:
        d = get_dataset('61edaf94-2339-4096-9cc0-4bfb79a9c848')
    except:
        return HTTPServerError()
    if not d:
        return HTTPServerError()

    #TODO: add check for insufficient resources (except we don't seem to know that)
    #      (413)

    return Response()
	
@view_config(route_name='dataone', match_param='app=dataone', renderer='../templates/dataone_node.pt')
@view_config(route_name='dataone_node', match_param='app=dataone', renderer='../templates/dataone_node.pt')
def dataone(request):
    '''
    system metadata about the member node

    <?xml version="1.0" encoding="UTF-8"?>
    <d1:node xmlns:d1="http://ns.dataone.org/service/types/v1" replicate="true" synchronize="true" type="mn" state="up">
      <identifier>urn:node:DEMO2</identifier>
      <name>DEMO2 Metacat Node</name>
      <description>A DataONE member node implemented in Metacat.</description>
      <baseURL>https://demo2.test.dataone.org:443/knb/d1/mn</baseURL>
      <services>
        <service name="MNRead" version="v1" available="true"/>
        <service name="MNCore" version="v1" available="true"/>
      </services>
      <synchronization>
        <schedule hour="*" mday="*" min="0/3" mon="*" sec="10" wday="?" year="*"/>
        <lastHarvested>2012-03-06T14:57:39.851+00:00</lastHarvested>
        <lastCompleteHarvest>2012-03-06T14:57:39.851+00:00</lastCompleteHarvest>
      </synchronization>
      <ping success="true"/>
      <subject>CN=urn:node:DEMO2, DC=dataone, DC=org</subject>
      <contactSubject>CN=METACAT1, DC=dataone, DC=org</contactSubject>
    </d1:node>
    '''

    #TODO: check the synchronization part 
    #TODO: check all the other parts, i.e. subject and contact subject and identifier


    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/apps/dataone/' % (load_balancer)

    #set up the dict
    rsp = {'name': 'GSTORE Node',
           'description': 'DATAONE member node for GSTORE',
           'baseUrl': base_url,
           'subject': SUBJECT,
           'contactsubject': CONTACTSUBJECT
        }
    request.response.content_type='text/xml'
    return rsp

@view_config(route_name='dataone_log', match_param='app=dataone', renderer='dataone_logs.mako')
def log(request):
    '''
    <?xml version="1.0" encoding="UTF-8"?>
    <d1:log xmlns:d1="http://ns.dataone.org/service/types/v1" count="3" start="0" total="1273">
      <logEntry>
        <entryId>1</entryId>
        <identifier>MNodeTierTests.201260152556757.</identifier>
        <ipAddress>129.24.0.17</ipAddress>
        <userAgent>null</userAgent>
        <subject>CN=testSubmitter,DC=dataone,DC=org</subject>
        <event>create</event>
        <dateLogged>2012-02-29T23:25:58.104+00:00</dateLogged>
        <nodeIdentifier>urn:node:DEMO2</nodeIdentifier>
      </logEntry>
      <logEntry>
        <entryId>2</entryId>
        <identifier>TierTesting:testObject:RightsHolder_Person.4</identifier>
        <ipAddress>129.24.0.17</ipAddress>
        <userAgent>null</userAgent>
        <subject>CN=testSubmitter,DC=dataone,DC=org</subject>
        <event>create</event>
        <dateLogged>2012-02-29T23:26:38.828+00:00</dateLogged>
        <nodeIdentifier>urn:node:DEMO2</nodeIdentifier>
      </logEntry>
    </d1:log>

    '''

    params = normalize_params(request.params)

    offset = int(request.params.get('start')) if 'start' in request.params else 0
    limit = int(request.params.get('count')) if 'count' in request.params else 1000

    fromDate = request.params.get('fromDate') if 'fromDate' in request.params else ''
    toDate = request.params.get('toDate') if 'toDate' in request.params else ''

    #return objects with pid that start with this string
    pid_init = request.params.get('pidFilter') if 'pidFilter' in request.params else ''

    event = request.params.get('event') if 'event' in request.params else ''

    querydict = {}

    connstr = request.registry.settings['dataone_mongo_uri']
    collection = request.registry.settings['dataone_mongo_collection']
    mongo_uri = gMongoUri(connstr, collection)
    logs = query_log(mongo_uri, querydict, limit, offset)

    results = logs.count()
    #TODO: what is total? the number of all log entries? or what?
    total = 0

    '''
    #2012-02-29T23:26:38.828+00:00
    fmt = '%Y-%m-%dT%H:%M:%S+00:00'
    post = {'total': 45, 'results': 3, 'offset': 0}
    docs = [
            {'id': 1, 'identifier': dataset_id, 'ip': '129.24.63.165', 'useragent': 'null', 'subject': 'CN=GStore,dc=informatics,dc=org', 'event':'read', 'dateLogged':datetime.utcnow().strftime(fmt), 'node': 'GSTORE'},
            {'id': 2, 'identifier': dataset_id, 'ip': '129.24.63.55', 'useragent': 'null', 'subject': 'CN=GStore,dc=informatics,dc=org', 'event':'read', 'dateLogged':datetime.utcnow().strftime(fmt), 'node': 'GSTORE'},
            {'id': 3, 'identifier': dataset_id, 'ip': '129.24.63.235', 'useragent': 'null', 'subject': 'CN=GStore,dc=informatics,dc=org', 'event':'read', 'dateLogged':datetime.utcnow().strftime(fmt), 'node': 'GSTORE'}
        ]
    post.update({'docs': docs})
    '''

    rsp = {'total': total, 'results': results, 'offset': offset}
    docs = []
    for g in logs:
        logged = g['date']
        
        docs.append({'id': str(g['_id']), 'identifier': g['identifier'], 'ip': g['ip'], 'useragent': 'null', 'subject': g['subject'], 'event': g['event'], 'dateLogged': '', 'node': g['node']})

    rsp.update({'docs': docs})
    return rsp
	
@view_config(route_name='dataone_search', match_param='app=dataone', renderer='dataone_search.mako')
def search(request):
    '''
    <?xml version="1.0"?>
    <ns1:objectList xmlns:ns1="http://ns.dataone.org/service/types/v1" count="5" start="0" total="12">
      <objectInfo>
        <identifier>AnserMatrix.htm</identifier>
        <formatId>eml://ecoinformatics.org/eml-2.0.0</formatId>
        <checksum algorithm="MD5">0e25cf59d7bd4d57154cc83e0aa32b34</checksum>
        <dateSysMetadataModified>1970-05-27T06:12:49</dateSysMetadataModified>
        <size>11048</size>
      </objectInfo>

      ...

      <objectInfo>
        <identifier>hdl:10255/dryad.218/mets.xml</identifier>
        <formatId>eml://ecoinformatics.org/eml-2.0.0</formatId>
        <checksum algorithm="MD5">65c4e0a9c4ccf37c1e3ecaaa2541e9d5</checksum>
        <dateSysMetadataModified>1987-01-14T07:09:09</dateSysMetadataModified>
        <size>2796</size>
      </objectInfo>
    </ns1:objectList>

    '''

    params = normalize_params(request.params)

    offset = int(params.get('start')) if 'start' in params else 0
    limit = int(params.get('count')) if 'count' in params else 1000

    fromDate = params.get('fromdate', '') 
    toDate = request.params.get('todate', '') 

    formatId = params.get('formatid', '')


    #TODO: add replica status somewhere (but we're not replicating stuff yet)
    replicaStatus = params.get('replicastatus', '')

    #set up the clauses
    #AND we're going for dataone_uuids NOT obsolete_uuids
    #so that we're only querying for the most recent obsolete_uuid (we don't care about previous ones here)
    #and we can return the correct uuid from the core objects anyway

    #this returns a tuple that is... awkward to use
    #query = DBSession.query(DataoneObsolete.dataone_uuid, func.max(DataoneObsolete.date_changed).group_by(DataoneObsolete.dataone_uuid)

    '''
>>> from gstore_v3.models import *
>>> from sqlalchemy.sql.expression import and_
>>> from datetime import datetime
>>> d = datetime(2012, 8, 14)
>>> search_clauses = [dataone.DataoneSearch.the_date>d, dataone.DataoneSearch.format=='FGDC-STD-001-1998']
    
 query = DBSession.query(dataone.DataoneSearch, dataone.DataoneCore).join(dataone.DataoneCore, dataone.DataoneCore.dataone_uuid==dataone.DataoneSearch.the_uuid).filter(and_(*search_clauses)).all()


    '''

    search_clauses = []

    if fromDate or toDate:
        #make them dates
        #build the clauses
        if fromDate and not toDate:
            #greater than from
            fd = dataone_to_datetime(fromDate)
            search_clauses.append(DataoneSearch.the_date >= fd)
        elif not fromDate and toDate:
            #less than to
            ed = dataone_to_datetime(toDate)
            search_clauses.append(DataoneSearch.the_date < ed)
        else:
            #between
            fd = dataone_to_datetime(fromDate)
            ed = dataone_to_datetime(toDate)
            search_clauses.append(between(DataoneSearch.the_date, fd, ed))

    if formatId:
        #join the formats so we can query that
        #TODO: deal with http escaping from the request
        formatId = urllib2.unquote(formatId)
        search_clauses.append(DataoneSearch.format==formatId)
        
    #and add the limit/offset for fun
    query = DBSession.query(DataoneSearch, DataoneCore).join(DataoneCore, DataoneCore.dataone_uuid==DataoneSearch.the_uuid).filter(and_(*search_clauses))
    total = query.count()

    objects = query.limit(limit).offset(offset).all()

    #now go do stuff with our tuple of search, core
    #where we really kinda just care about the core (because we want the hash, the size and the most recent uuid)

    dataone_path = request.registry.settings['DATAONE_PATH']

    #convert to the dict needed for the template
    cnt = total if total < limit else limit
    
    docs = []
    for obj in objects:
        #get the core obj
        core = obj[1]
        algo = 'md5'
        #f = core.get_object(dataone_path)
        h = core.get_hash(algo, dataone_path)
        #h = 500
        size = core.get_size(dataone_path)
        #size = 30

        #get the current id
        current = core.get_current()
        
        docs.append({'identifier': current, 'format': core.format.format, 'algo': algo, 'checksum': h, 'date': datetime_to_dataone(obj[0].the_date), 'size': size})

    return {'total': total, 'count': cnt, 'start': offset, 'docs': docs}
	
@view_config(route_name='dataone_object', request_method='GET', match_param='app=dataone')
def show(request):
    '''
    return the file object for this uuid

    error = 
    <?xml version="1.0" encoding="UTF-8"?>
    <error detailCode="1800" errorCode="404" name="NotFound">
       <description>No system metadata could be found for given PID: DOESNTEXIST</description>
    </error>
    '''
    pid = request.matchdict['pid']
    
    #go check in the obsolete table
    obsolete = DBSession.query(DataoneObsolete).filter(DataoneObsolete.obsolete_uuid==pid).first()
    if not obsolete:
        #emit that xml
        return_error('object', 1800, 404)

    #get the object path 
    core_object = obsolete.core

    dataone_path = request.registry.settings['DATAONE_PATH']
    obj_path = core_object.get_object(dataone_path)

    if not obj_path:    
        return_error('object', 1800, 404)

    #should be xml or zip only
    #TODO: check on the RDF mimetype if it's not xml
    mimetype = 'application/xml'
    if core_object.object_type in ['source', 'vector']:
        mimetype = 'application/x-zip-compressed'

    fr = FileResponse(obj_path, content_type=mimetype)
    #make the download filename be the obsolete_uuid that was requested just to be consistent
    ext = obj_path.split('.')[-1]
    fr.content_disposition = 'attachment; filename=%s.%s' % (pid, ext)
    return fr

@view_config(route_name='dataone_object', request_method='HEAD', match_param='app=dataone')
def head(request):
    '''
    d1.method = describe

    curl -I http://129.24.63.66/gstore_v3/apps/dataone/object/b45bbf88-0b81-441c-bf9a-590c8ac5f0bf
    HTTP/1.1 200 OK
    Last-Modified: Wed, 16 Dec 2009 13:58:34 GMT
    Content-Length: 10400
    Content-Type: application/octet-stream
    DataONE-ObjectFormat: eml://ecoinformatics.org/eml-2.0.1
    DataONE-Checksum: SHA-1,2e01e17467891f7c933dbaa00e1459d23db3fe4f
    DataONE-SerialVersion: 1234

    curl -I http://129.24.63.66/gstore_v3/apps/dataone/object/b45bbf88-0b81-441c-bf9a-590c8ac5f09f
    error = 
    HTTP/1.1 404 Not Found
    Last-Modified: Wed, 16 Dec 2009 13:58:34 GMT
    Content-Length: 1182
    Content-Type: text/xml
    DataONE-Exception-Name: NotFound
    DataONE-Exception-DetailCode: 1380
    DataONE-Exception-Description: The specified object does not exist on this node.
    DataONE-Exception-PID: IDONTEXIST
    '''

    pid = request.matchdict['pid']

    #go check in the obsolete table
    obsolete = DBSession.query(DataoneObsolete).filter(DataoneObsolete.obsolete_uuid==pid).first()
    
    if not obsolete:
        lst = [('Content-Type', 'text/xml'), 
               ('DataONE-Exception-Name', 'NotFound'), 
               ('DataONE-Exception-DetailCode', '1380'), 
               ('DataONE-Exception-Description', 'The specified object does not exist on this node.'),
               ('DataONE-Exception-PID', str(pid))]
        rsp = Response()
        rsp.status = 404
        rsp.headerlist = lst
        return rsp

    #get the object path 
    core_object = obsolete.core

    dataone_path = request.registry.settings['DATAONE_PATH']
    #obj_path = core_object.get_object(dataone_path)

    #get the file info
    file_hashtype = 'md5'
    file_hash = core_object.get_hash(file_hashtype, dataone_path)
    file_size = core_object.get_size(dataone_path)

    #convert to the expectged d1 datetime
    lastmodified = obsolete.date_changed

    content_type = 'application/xml'
    if core_object.object_type in ['source', 'vector']:
        content_type = 'application/x-zip-compressed' 

    #get the d1 object identifier value
    obj_format = core_object.format.format

    #TODO: deal with all the strings (can't be unicode from postgres)
    lst = [('Last-Modified','%s' % (str(datetime_to_http(lastmodified)))), ('Content-Type', content_type), ('Content-Length','%s' % (int(file_size)))]
    lst.append(('DataONE-ObjectFormat', str(obj_format)))
    lst.append(('DataONE-Checksum', '%s,%s' % (str(file_hashtype), str(file_hash))))
    #TODO: change this to something
    lst.append(('DataONE-SerialType', '1234'))

    rsp = Response()
    rsp.headerlist = lst
    return rsp

@view_config(route_name='dataone_meta', match_param='app=dataone', renderer='dataone_metadata.mako')
def metadata(request):
    '''
    <?xml version="1.0" encoding="UTF-8"?>
    <d1:systemMetadata xmlns:d1="http://ns.dataone.org/service/types/v1">
      <serialVersion>1</serialVersion>
      <identifier>XYZ332</identifier>
      <formatId>eml://ecoinformatics.org/eml-2.1.0</formatId>
      <size>20875</size>
      <checksum algorithm="MD5">e7451c1775461b13987d7539319ee41f</checksum>
      <submitter>uid=mbauer,o=NCEAS,dc=ecoinformatics,dc=org</submitter>
      <rightsHolder>uid=mbauer,o=NCEAS,dc=ecoinformatics,dc=org</rightsHolder>
      <accessPolicy>
        <allow>
          <subject>uid=jdoe,o=NCEAS,dc=ecoinformatics,dc=org</subject>
          <permission>read</permission>
          <permission>write</permission>
          <permission>changePermission</permission>
        </allow>
        <allow>
          <subject>public</subject>
          <permission>read</permission>
        </allow>
        <allow>
          <subject>uid=nceasadmin,o=NCEAS,dc=ecoinformatics,dc=org</subject>
          <permission>read</permission>
          <permission>write</permission>
          <permission>changePermission</permission>
        </allow>
      </accessPolicy>
      <replicationPolicy replicationAllowed="false"/>
      <obsoletes>XYZ331</obsoletes>
      <obsoletedBy>XYZ333</obsoletedBy>
      <archived>true</archived>
      <dateUploaded>2008-04-01T23:00:00.000+00:00</dateUploaded>
      <dateSysMetadataModified>2012-06-26T03:51:25.058+00:00</dateSysMetadataModified>
      <originMemberNode>urn:node:TEST</originMemberNode>
      <authoritativeMemberNode>urn:node:TEST</authoritativeMemberNode>
    </d1:systemMetadata>

    error = 
    <?xml version="1.0" encoding="UTF-8"?>
    <error detailCode="1800" errorCode="404" name="NotFound">
      <description>No system metadata could be found for given PID: SomeObjectID</description>
    </error>
    '''
    pid = request.matchdict['pid']
    
    #go check in the obsolete table
    obsolete = DBSession.query(DataoneObsolete).filter(DataoneObsolete.obsolete_uuid==pid).first()
    
    if not obsolete:
        return return_error('metadata', 1800, 404, '', pid)

    #get the object path 
    core_object = obsolete.core

    dataone_path = request.registry.settings['DATAONE_PATH']

    #get the file info
    file_hashtype = 'md5'
    file_hash = core_object.get_hash(file_hashtype, dataone_path)
    file_size = core_object.get_size(dataone_path)

    #convert to the expectged d1 datetime
    lastmodified = obsolete.date_changed

    #get the d1 object identifier value
    obj_format = core_object.format.format

    #need the list of any obsoleted objects
    obsoletes = core_object.get_obsoletes(obsolete.obsolete_uuid)

    #and the latest and greatest
    obsoletedby = core_object.get_current()

    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/apps/dataone/' % (load_balancer)

    #TODO: fix the date formats, i think
    rsp = {'pid': pid, 'dateadded': datetime_to_dataone(core_object.date_added), 'obj_format': obj_format, 'file_size': file_size, 
           'uid': 'GSTORE', 'o': 'EDAC', 'dc': 'everything', 'org': 'EDAC', 'hash_type': file_hashtype,
           'hash': file_hash, 'metadata_modified': datetime_to_dataone(obsolete.date_changed), 'mn': base_url, 'obsoletes': obsoletes, 'obsoletedby': obsoletedby}

    request.response.content_type = 'text/xml; charset=UTF-8'
    return rsp

@view_config(route_name='dataone_checksum', match_param='app=dataone')
def checksum(request):
    '''
    <checksum algorithm="SHA-1">2e01e17467891f7c933dbaa00e1459d23db3fe4f</checksum>
    '''

    pid = request.matchdict['pid']

    algo = request.params.get('checksumAlgorithm', '')
    if not algo:
        return HTTPNotFound()

    obsolete = DBSession.query(DataoneObsolete).filter(DataoneObsolete.obsolete_uuid==pid).first()
    if not obsolete:
        #emit that xml
        return return_error('object', 1800, 404)

    #get the object path 
    core_object = obsolete.core

    dataone_path = request.registry.settings['DATAONE_PATH']

    h = core_object.get_hash(algo, dataone_path)
    
    #TODO: double check output
    #TODO: double-check list of hash algorithm terms
    return Response('<checksum algorithm="%s">%s</checksum>' % (algo, h), content_type='application/xml')

@view_config(route_name='dataone_error', request_method='POST', match_param='app=dataone')
def error(request):
    return Response('dataone error from controlling node')

@view_config(route_name='dataone_replica', match_param='app=dataone')
def replica(request):
    '''
    log as replica request not just GET

    return file
    '''
    pid = request.matchdict['pid']
    return Response('dataone replica')


'''
dataone management methods

- create dataone vector object
- create dataone package object
- create dataone core object (after vector made if it's for a vector and after package made if it's a package widget)
- add new obsolete record for a dataone core object
'''
#TODO: add methods
