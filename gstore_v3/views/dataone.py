from pyramid.view import view_config
from pyramid.response import Response, FileResponse

from pyramid.httpexceptions import HTTPNotFound, HTTPServerError, HTTPBadRequest, HTTPNotImplemented

from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_
from sqlalchemy.sql import between

from datetime import datetime
import urllib2
import json
import os, shutil

#from the models init script (BOTH connections)
from ..models import DBSession, DataoneSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )
from ..models.dataone import *
from ..models.dataone_logs import DataoneLog

from ..lib.utils import *
from ..lib.database import *
from ..lib.mongo import gMongo, gMongoUri



'''
make sure that the general errors are being posted correctly (404, 500, etc)
at the app level

see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html
'''

'''
some presets
'''
#TODO: move to config?
NODE = 'urn:node:EDAC-GSTORE'
SUBJECT = 'CN=EDAC-GSTORE,DC=dataone,DC=org'
RIGHTSHOLDER = 'CN=EDAC-GSTORE,DC=dataone,DC=org'
CONTACTSUBJECT = 'CN=EDAC-GSTORE,DC=dataone,DC=org'
NAME = ''
DESCRIPTION = ''

#TODO: what else needs to be logged other than object/pid (read)?


#convert to the d1 format
def datetime_to_dataone(dt):
    fmt = '%Y-%m-%dT%H:%M:%S.0Z'
    return dt.strftime(fmt)
    
def datetime_to_http(dt):
    #TODO: utc to gmt
    #Wed, 16 Dec 2009 13:58:34 GMT
    fmt = '%a, %d %b %Y %H:%M:%S GMT'
    return dt.strftime(fmt)

def dataone_to_datetime(dt):
    #TODO: deal with more datetime issues (could be gmt or utc, with or without milliseconds)
    '''
    YYYY-MM-DDTHH:MM:SS.mmm
    YYYY-MM-DDTHH:MM:SS.mmm+00:00
    '''
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
def log_entry(identifier, ip, event, useragent='public'):
#    gm = gMongo(mongo_uri)
#    gm.insert({"identifier": identifier, "ip": ip, "useragent": useragent, "subject": SUBJECT, "date": datetime.utcnow(), "event": event, "node": NODE})
#    gm.close()
    
    dlog = DataoneLog(identifier, ip, SUBJECT, event, NODE, useragent)
    try:
        DataoneSession.add(dlog)
        DataoneSession.commit()
    except:
        DataoneSession.rollback()
        raise


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
           'description': 'DATAONE member node for GSTORE (EDAC)',
           'baseUrl': base_url,
           'subject': SUBJECT,
           'contactsubject': CONTACTSUBJECT
        }
    request.response.content_type='text/xml'
    return rsp

#, renderer='dataone_logs.mako'
@view_config(route_name='dataone_log', match_param='app=dataone')
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

    #TODO: check opn filters again (pidFilter not working??)


    params = normalize_params(request.params)

    offset = int(request.params.get('start')) if 'start' in request.params else 0
    limit = int(request.params.get('count')) if 'count' in request.params else 1000

    fromDate = request.params.get('fromdate') if 'fromdate' in request.params else ''
    toDate = request.params.get('todate') if 'todate' in request.params else ''

    #return objects with pid that start with this string
    pid_init = request.params.get('pidfilter') if 'pidfilter' in request.params else ''

    event = request.params.get('event') if 'event' in request.params else ''

    #TODO: add the session request

    clauses = []
    if pid_init:
        clauses.append(DataoneLog.identifier.like(pid_init + '%'))

    if event:
        clauses.append(DataoneLog.event.like(event))

    if fromDate and not toDate:
        from_date = dataone_to_datetime(fromDate)
        clauses.append(DataoneLog.logged>=from_date)
    elif not fromDate and toDate:
        to_date = dataone_to_datetime(toDate)
        clauses.append(DataoneLog.logged<=to_date)
    elif fromDate and toDate:
        from_date = dataone_to_datetime(fromDate)
        to_date = dataone_to_datetime(toDate)
        clauses.append(between(DataoneLog.logged, fromDate, toDate))
           

    query = DataoneSession.query(DataoneLog).filter(and_(*clauses))
    total = query.count()
    query = query.limit(limit).offset(offset).all()

    fmt = '%Y-%m-%dT%H:%M:%S+00:00'
    
#    rsp = {'total': total, 'results': query.count(), 'offset': offset}
#    docs = []
    
#    for q in query:
#        docs.append({'id': q.id, 'identifier': q.identifier, 'ip': q.ip_address, 'useragent': q.useragent, 'subject': q.subject, 'event': q.event, 'dateLogged': q.logged.strftime(fmt), 'node': q.node})

    entries = []
    for q in query:
        entries.append(q.get_log_entry())
    rsp = '<?xml version="1.0" encoding="UTF-8"?><d1:log xmlns:d1="http://ns.dataone.org/service/types/v1" count="%s" start="%s" total="%s">%s</d1:log>' % (len(query), offset, total, ''.join(entries))

    return Response(rsp, content_type='application/xml')    
#    rsp.update({'docs': docs})
#    return rsp
	
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
>>> query = DBSession.query(dataone.DataoneSearch, dataone.DataoneCore).join(dataone.DataoneCore, dataone.DataoneCore.dataone_uuid==dataone.DataoneSearch.the_uuid).filter(and_(*search_clauses)).all()
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
        formatId = urllib2.unquote(formatId)
        search_clauses.append(DataoneSearch.format==formatId)
        
    #join to the core so we don't have to query for those later
    query = DBSession.query(DataoneSearch, DataoneCore).join(DataoneCore, DataoneCore.dataone_uuid==DataoneSearch.the_uuid).filter(and_(*search_clauses))
    total = query.count()

    #and add the limit/offset for fun but do it after we get a complete count for the total attribute
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
        h = core.get_hash(algo, dataone_path)
        size = core.get_size(dataone_path)

        #get the current id
        current = core.get_current()

        #this date comes from mongo so it should by utc already
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
    mimetype = 'application/xml'
    if core_object.object_type in ['source', 'vector']:
        mimetype = 'application/x-zip-compressed'

    #TODO: add the session info or something    
    log_entry(pid, request.client_addr, 'read')

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

    obsoletedby = '' if obsoletedby == obsolete.obsolete_uuid else obsoletedby

    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/apps/dataone/' % (load_balancer)

    #dates should be from postgres, i.e. in utc
    rsp = {'pid': pid, 'dateadded': datetime_to_dataone(core_object.date_added), 'obj_format': obj_format, 'file_size': file_size, 
           'uid': 'EDAC-GSTORE', 'o': 'EDAC', 'dc': 'everything', 'org': 'EDAC', 'hash_type': file_hashtype,
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

    #TODO: parse a multipart post (without examples) and log to mongo as an error
    #return HTTPServerError('', status=501)
    return HTTPNotImplemented()

@view_config(route_name='dataone_replica', match_param='app=dataone')
def replica(request):
    '''
    log as replica request not just GET

    return file
    '''
    return HTTPNotImplemented()
    
    pid = request.matchdict['pid']
    return Response('dataone replica')


'''
dataone management methods

- create dataone vector object
- create dataone package object
- create dataone core object (after vector made if it's for a vector and after package made if it's a package widget)
- add new obsolete record for a dataone core object

adding vector, source, metadata and package objects does not register a dataone object. you must still add those objects
as dataone core objects using the object uuid and type. and then push the core object uuid to obsoletes. so the process is:

    1. create object (mostly adding the zip/xml/rdf to the dataone cache)
    2. register the object in core
    3. register the core object in obsolete

that means three posts per object right now. the basic object posts always return the object uuid to register, the object type
and the utc datetime it was posted.
'''
@view_config(route_name='dataone_addcore', request_method='POST')
def add_dataone_core(request):
    '''
    {
        object_uuid
        object_type: vector | source | package | metadata
        format 
    }

    NOTE: if you are posting metadata, the uuid is from the metadata (DatasetMetadata) table not datasets OR original_metadata
    '''
    post_data = request.json_body
    if 'object_uuid' not in post_data and 'object_type' not in post_data and 'format' not in post_data:
        return HTTPBadRequest()

    format = post_data['format']
    object_uuid = post_data['object_uuid']
    object_type = post_data['object_type'].lower()
    if object_type not in ['vector', 'source', 'metadata', 'package']:
        return HTTPBadRequest()

    format_id = DBSession.query(DataoneFormat).filter(DataoneFormat.format==format).first()
    if not format_id:
        return HTTPBadRequest()
    format_id = format_id.id

    core_obj = DBSession.query(DataoneCore).filter(and_(DataoneCore.object_uuid==object_uuid, DataoneCore.object_type==object_type)).first()
    if core_obj:
        return HTTPBadRequest('An object with this uuid already exists.')

    core_obj = DataoneCore(object_uuid, object_type, format_id)
    try:
        DBSession.add(core_obj)
        DBSession.commit()
        DBSession.flush()
        DBSession.refresh(core_obj)
    except:
        return HTTPServerError()
    
    return Response(json.dumps({"core_uuid": core_obj.dataone_uuid, "date_added": core_obj.date_added.strftime('%Y-%m-%dT%H:%M:%S'), "object_uuid": core_obj.object_uuid, "object_type": core_obj.object_type}))
    
@view_config(route_name='dataone_addmetadata', request_method='POST')
def add_dataone_metadata(request):
    '''
    {
        dataset_uuid
        standard (once we have multiple standards)
    }

    this is a metadata reference to the metadata table (datasetmetadata model) that can be used to get the original metadata
    or later, the iso metadata

    but that table does not currently reference datasets so we go in a circle that is bad
    '''
    post_data = request.json_body

    if 'dataset_uuid' not in post_data and 'standard' not in post_data:
        return HTTPBadRequest()

    dataset_uuid = post_data['dataset_uuid']
    standard = post_data['standard'].lower()
    if standard not in ['fgdc']:
        return HTTPBadRequest()

    d = get_dataset(dataset_uuid)
    if not d:
        return HTTPBadRequest()

    if not d.has_metadata_cache or not d.original_metadata or not d.original_metadata[0].original_xml:
        return HTTPBadRequest()

    #TODO: UPDATE THIS FOR THE METADATA SCHEMA CHANGES SOMEDAY
    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

    #get the xml and add the online linkages
    orig_metadata = d.original_metadata[0]
    xml = orig_metadata.append_onlink(base_url)
    meta_id = orig_metadata.id

    #get the metadata object that we want
    meta_obj = DBSession.query(DatasetMetadata).filter(DatasetMetadata.original_id==meta_id).first()
    if not meta_obj:
        return HTTPBadRequest('Invalid metadata object')
    meta_uuid = meta_obj.uuid

    DATAONE_PATH = request.registry.settings['DATAONE_PATH']
    dataone_path = os.path.join(DATAONE_PATH, 'metadata', '%s.xml' % (meta_uuid))

    if os.path.isfile(dataone_path):
        return HTTPBadRequest('Metadata xml already exists for this dataset')

    with open(dataone_path, 'w') as f:
        f.write(xml)

    return Response(json.dumps({'object_uuid': meta_uuid, 'object_type': 'metadata', 'date_added': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')}))

@view_config(route_name='dataone_addvector', request_method='POST')
def add_dataone_vector(request):
    '''
    {
        dataset_uuid
        format
    }
    '''
    post_data = request.json_body
    if 'dataset_uuid' not in post_data or 'format' not in post_data:
        return HTTPBadRequest()

    dataset_uuid = post_data['dataset_uuid']
    format = post_data['format'].lower()

    d = get_dataset(dataset_uuid)
    if not d:
        return HTTPBadRequest()

    fmts = d.get_formats(request)
    if format not in fmts:
        return HTTPBadRequest()

    #check for an existing vector with this uuid and format
    vector = DBSession.query(DataoneVector).filter(and_(DataoneVector.dataset_uuid==dataset_uuid, DataoneVector.format==format)).first()
    if vector:
        return HTTPBadRequest('A vector object already exists for this dataset')
    
    #add a new dataone vector object
    vector = DataoneVector(dataset_uuid, format)
    try:
        DBSession.add(vector)
        DBSession.commit()
        DBSession.flush()
        DBSession.refresh(vector)
    except:
        return HTTPServerError()


    vector_uuid = vector.vector_uuid

    #see if the vector exists as a zip in formats
    FORMATS_PATH = request.registry.settings['FORMATS_PATH']
    DATAONE_PATH = request.registry.settings['DATAONE_PATH']
    
    dataone_path = os.path.join(DATAONE_PATH, 'datasets')
    datapath = os.path.join(dataone_path, '%s.zip' % (vector_uuid))


#    #TODO: may need to update this and the _source method for the original v derived + format setup (in case there's original+shp and derived+shp or something)
    try:
        source = [s for s in d.sources if s.extension == format and s.active and not s.is_external] if d.sources else None
        if source:
            #make a copy 
            source = source[0]
#            xslt_path = request.registry.settings['XSLT_PATH']
#            output = source.pack_source(dataone_path, '%s.zip' % (vector_uuid), xslt_path)
        else:   

            outpath = os.path.join(FORMATS_PATH, d.uuid, format, '%s.%s.zip' % (d.uuid, format))
            if os.path.isfile(outpath):
                #copy the file to dataone and rename
                shutil.copyfile(outpath, datapath)
            else:   
                #if not, go make it and copy the zip to dataone
                cachepath = os.path.join(FORMATS_PATH, d.uuid, format)
                if not os.path.isdir(cachepath):
                    #make a new one and this is stupid
                    if not os.path.isdir(os.path.join(FORMATS_PATH, str(d.uuid))):
                        os.mkdir(os.path.join(FORMATS_PATH, str(d.uuid)))
                    os.mkdir(os.path.join(FORMATS_PATH, str(d.uuid), format))
                
                mconn = request.registry.settings['mongo_uri']
                mcoll = request.registry.settings['mongo_collection']
                mongo_uri = gMongoUri(mconn, mcoll)
                srid = int(request.registry.settings['SRID'])
                success = d.build_vector(format, cachepath, mongo_uri, srid)
                if success[0] != 0:
                    return HTTPServerError(success[1])
                #and copy to dataone
                shutil.copyfile(outpath, datapath)
    except Exception as err:
        return HTTPServerError(err)
    return Response(json.dumps({'object_uuid': vector.vector_uuid, 'object_type': 'vector', 'date_added': vector.date_added.strftime('%Y-%m-%dT%H:%M:%S')}))

@view_config(route_name='dataone_addsource', request_method='POST')
def add_dataone_source(request):
    '''
    {
        dataset_uuid
        format
    }
    '''
    post_data = request.json_body
    if 'dataset_uuid' not in post_data or 'format' not in post_data:
        return HTTPBadRequest()

    dataset_uuid = post_data['dataset_uuid']
    format = post_data['format'].lower()

    d = get_dataset(dataset_uuid)
    if not d:
        return HTTPBadRequest()

    fmts = d.get_formats(request)
    if format not in fmts:
        return HTTPBadRequest()

    #get the source object for the dataset + format
    source = [s for s in d.sources if s.extension == format and s.active and not s.is_external]
    if not source:
        return HTTPBadRequest()
    source = source[0]

    core_obj = DBSession.query(DataoneCore).filter(and_(DataoneCore.object_uuid==source.uuid, DataoneCore.object_type=='source')).first()
    if core_obj:
        return HTTPBadRequest()

    #check for the zipped source file in the dataone cache
    DATAONE_PATH = request.registry.settings['DATAONE_PATH']
    
    dataone_path = os.path.join(DATAONE_PATH, 'datasets')
    outfile = os.path.join(dataone_path, '%s.zip' % (source.uuid))
    if os.path.isfile(outfile):
        return HTTPBadRequest()

    #pack up the data
    xslt_path = request.registry.settings['XSLT_PATH']
    output = source.pack_source(dataone_path, '%s.zip' % (source.uuid), xslt_path)
    
    return Response(json.dumps({'object_uuid': source.uuid, 'object_type': 'source', 'date_added': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')}))

#TODO: modify this to create packages for 1+ datasets and 1 metadata (all versions of vector (shp, kml, csv, etc) use same metadata). 
#      should just be a matter of magically finding all vector objects for a dataset uuid and doing that. but the build_package
#      method will also have to be updated. and what to do about source objects? 
@view_config(route_name='dataone_addpackage', request_method='POST')
def add_dataone_package(request):
    '''
    {
        metadata_uuid (core_uuid for this metadata object)
        dataobject_uuid (core_uuid for this metadata object)
        dataobject_type 
    }

    fyi: core_uuid == dataone_uuid

    NOTE: this does NOT check to make sure that the metadata object is related in any way to the data object. so be careful.
    '''

    post_data = request.json_body
    if 'metadata_uuid' not in post_data and 'dataobject_uuid' not in post_data and 'dataobject_type' not in post_data:
        return HTTPBadRequest()

    dataobject_uuid = post_data['dataobject_uuid']
    dataobject_type = post_data['dataobject_type']
    metadata_uuid = post_data['metadata_uuid']

    #the object type is not 100% necessary but we like specificity here. and if not
    #TODO: just do an or (or in) for the two uuids and count the result set
    data_obj = DBSession.query(DataoneCore).filter(and_(DataoneCore.dataone_uuid==dataobject_uuid, DataoneCore.object_type==dataobject_type)).first()
    meta_obj = DBSession.query(DataoneCore).filter(and_(DataoneCore.dataone_uuid==metadata_uuid, DataoneCore.object_type=='metadata')).first()

    if not data_obj or not meta_obj:
        return HTTPBadRequest()

    #check for a package with these objects as well
    package_obj = DBSession.query(DataonePackage).filter(and_(DataonePackage.dataset_object==dataobject_uuid, DataonePackage.metadata_object==metadata_uuid)).first()
    if package_obj:
        return HTTPBadRequest('This data package already exists (%s).' % (package_obj.package_uuid))

    #add the new package
    package_obj = DataonePackage(dataobject_uuid, metadata_uuid)
    try:
        DBSession.add(package_obj)
        DBSession.commit()
        DBSession.flush()
        DBSession.refresh(package_obj)
    except:
        DBSession.rollback()
        return HTTPServerError()

    #add it to the core table
    format_id = DBSession.query(DataoneFormat).filter(DataoneFormat.format=='http://www.w3.org/TR/rdf-syntax-grammar').first()
    if not format_id:
        return HTTPBadRequest()
    format_id = format_id.id

    core_obj = DBSession.query(DataoneCore).filter(and_(DataoneCore.object_uuid==package_obj.package_uuid, DataoneCore.object_type=='package')).first()
    if core_obj:
        return HTTPBadRequest('An object with this uuid already exists.')

    core_obj = DataoneCore(package_obj.package_uuid, 'package', format_id)
    try:
        DBSession.add(core_obj)
        DBSession.commit()
        DBSession.flush()
        DBSession.refresh(core_obj)
    except:
        return HTTPServerError('Failed to add core')

    #add it to the obsoletes
    obsolete = DataoneObsolete(core_obj.dataone_uuid)
    try:
        DBSession.add(obsolete)
        DBSession.commit()
        DBSession.flush()
        DBSession.refresh(obsolete)
    except:
        DBSession.rollback()
        return HTTPServerError()

    #build the rdf
    LOAD_BALANCER = request.registry.settings['BALANCER_URL']
    DATAONE_PATH = request.registry.settings['DATAONE_PATH']
    base_url = '%s/apps/dataone' % LOAD_BALANCER
    rdfpath = os.path.join(DATAONE_PATH, 'packages')
    success = package_obj.build_rdf(rdfpath, base_url)
    if success != 'success':
        return HTTPServerError(success)
    
    return Response(json.dumps({'obsolete_uuid': obsolete.obsolete_uuid, 'core_uuid': core_obj.dataone_uuid, 'object_uuid': package_obj.package_uuid, 'object_type': 'package', 'date_added': package_obj.date_added.strftime('%Y-%m-%dT%H:%M:%S')}))

@view_config(route_name='dataone_addobsolete', request_method='POST')
def add_dataone_obsolete(request):
    '''
    {
        core_uuid
    }
    '''

    post_data = request.json_body
    if 'core_uuid' not in post_data:
        return HTTPBadRequest()

    #let's make sure it's a valid dataone_core uuid
    core_uuid = post_data['core_uuid']
    core_obj = DBSession.query(DataoneCore).filter(DataoneCore.dataone_uuid==core_uuid).first()
    if not core_obj:
        return HTTPBadRequest()

    obsolete = DataoneObsolete(core_obj.dataone_uuid)
    try:
        DBSession.add(obsolete)
        DBSession.commit()
        DBSession.flush()
        DBSession.refresh(obsolete)
    except:
        DBSession.rollback()
        return HTTPServerError()
     
    return Response(json.dumps({'obsolete_uuid': obsolete.obsolete_uuid, 'core_uuid': core_uuid, 'date_added': obsolete.date_changed.strftime('%Y-%m-%dT%H:%M:%S')}))

@view_config(route_name='dataone_updatepackage', request_method='POST')
def update_dataone_package(request):
    '''
    {
        package_uuid
        metadata_uuid (core_uuid for this metadata object IF MODIFIED)
        dataobject_uuid (core_uuid for this metadata object IF MODIFIED)
    }

    if metadata object or dataset object has been modified, go get the core object uuid
    and add a new obsolete uuid for that core obj

    then get core obj for package uuid and add a new obsolete uuid for that as well

    finally overwrite the data package rdf (make sure that has the obsoleted by in it?)
    
    '''

    
    
    return Response()



