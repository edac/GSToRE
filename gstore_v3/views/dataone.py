from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPServerError
from pyramid.threadlocal import get_current_registry

import logging
from datetime import datetime

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )


from ..lib.utils import *
from ..lib.database import *

#log = logging.getLogger(__name__)

'''
make sure that the general errors are being posted correctly (404, 500, etc)
at the app level

see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html
'''

#convert to the d1 format
def datetime_to_dataone(dt):
    #TODO: deal with timezone-y issues
    fmt = '%Y-%m-%dT%H:%M:%S.0Z'
    return dt.strftime(fmt)

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

    #TODO: why isn't the logger writing to the stupid file?
    logging.getLogger('dataone').debug('ping dataone')

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

    #set up the dict
    rsp = {'name': 'GSTORE Node',
           'description': 'DATAONE member node for GSTORE',
           'baseUrl': '%s/%s/apps/dataone/' % (request.host_url, request.script_name[1:]),
           'subject': 'CN=urn:node:DEMO2, DC=dataone, DC=org',
           'contactsubject': 'CN=METACAT1, DC=dataone, DC=org'
        }
    request.response.content_type='text/xml'
    return rsp

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

    offset = int(request.params.get('start')) if 'start' in request.params else 0
    limit = int(request.params.get('count')) if 'count' in request.params else 1000

    fromDate = request.params.get('fromDate') if 'fromDate' in request.params else ''
    toDate = request.params.get('toDate') if 'toDate' in request.params else ''

    #return objects with pid that start with this string
    pid_init = request.params.get('pidFilter') if 'pidFilter' in request.params else ''

    event = request.params.get('event') if 'event' in request.params else ''

    return Response('dateone log')
	
@view_config(route_name='dataone_search', match_param='app=dataone')
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

    offset = int(request.params.get('start')) if 'start' in request.params else 0
    limit = int(request.params.get('count')) if 'count' in request.params else 1000

    fromDate = request.params.get('fromDate') if 'fromDate' in request.params else ''
    toDate = request.params.get('toDate') if 'toDate' in request.params else ''

    #the pid
    formatId = request.params.get('formatId') if 'formatId' in request.params else ''

    replicaStatus = request.params.get('replicaStatus') if 'replicaStatus' in request.params else ''


    return Response('dataone search')
	
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

    #get the dataset
    d = get_dataset(pid)

    if not d:
        #emit that xml
        request.response.content_type = 'text/xml; charset=UTF-8'
        request.response.status = 404
        return '<?xml version="1.0" encoding="UTF-8"?><error detailCode="1800" errorCode="404" name="NotFound"><description>No system metadata could be found for given PID: DOESNTEXIST</description></error>'

    #temporary we're not serving vector stuff data check
    if d.taxonomy not in ['file', 'geoimage']:
        #emit that xml
        request.response.content_type = 'text/xml; charset=UTF-8'
        request.response.status = 404
        return '<?xml version="1.0" encoding="UTF-8"?><error detailCode="1800" errorCode="404" name="NotFound"><description>No system metadata could be found for given PID: DOESNTEXIST</description></error>'

    #get the file we want to serve as the dataset
    #for file + geomimage, should be set==original
    #NOPE - just sources with a hash right now
    src = [s for s in d.sources if s.file_hash != None]
    if not src:
        #emit that xml
        request.response.content_type = 'text/xml; charset=UTF-8'
        request.response.status = 404
        return '<?xml version="1.0" encoding="UTF-8"?><error detailCode="1800" errorCode="404" name="NotFound"><description>No system metadata could be found for given PID: DOESNTEXIST</description></error>'
        #this just gets worse

    srcfiles = src[0].src_files
    file_size = src[0].file_size_mb
    file_hash = src[0].file_hash

    #TODO: add some header stuff and check that we need to add some header stuff
    #dump the file
    return FileResponse(srcfiles[0], content_type='application/octet-stream')

@view_config(route_name='dataone_object', request_method='HEAD', match_param='app=dataone')
def head(request):
    '''
    HTTP/1.1 200 OK
    Last-Modified: Wed, 16 Dec 2009 13:58:34 GMT
    Content-Length: 10400
    Content-Type: application/octet-stream
    DataONE-ObjectFormat: eml://ecoinformatics.org/eml-2.0.1
    DataONE-Checksum: SHA-1,2e01e17467891f7c933dbaa00e1459d23db3fe4f
    DataONE-SerialVersion: 1234

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
    return Response('dataone object header')

@view_config(route_name='dataone_meta', match_param='app=dataone', renderer='templates/dataeone_metadata.pt')
def metadata(request):
    '''
    <?xml version="1.0" encoding="UTF-8"?>
    <d1:systemMetadata xmlns:d1="http://dataone.org/coordinating_node_sysmeta_0.1"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="http://dataone.org/coordinating_node_sysmeta_0.1 https://repository.dataone.org/software/cicore/trunk/schemas/coordinating_node_sysmeta.xsd">
        <!-- This instance document was auto generated by oXygen XML for testing purposes.
             It contains no useful information.
        -->
        <identifier>Identifier0</identifier>
        <objectFormat>eml://ecoinformatics.org/eml-2.0.1</objectFormat>
        <size>0</size>
        <submitter>uid=jones,o=NCEAS,dc=ecoinformatics,dc=org</submitter>
        <rightsHolder>uid=jones,o=NCEAS,dc=ecoinformatics,dc=org</rightsHolder>
        <describes>XYZ333</describes>
        <checksum algorithm="SHA-1">2e01e17467891f7c933dbaa00e1459d23db3fe4f</checksum>
        <embargoExpires>2006-05-04T18:13:51.0Z</embargoExpires>
        <accessRule rule="allow" service="read" principal="Principal0"/>
        <accessRule rule="allow" service="read" principal="Principal1"/>
        <replicationPolicy replicationAllowed="true" numberReplicas="2">
            <preferredMemberNode>MemberNode12</preferredMemberNode>
            <preferredMemberNode>MemberNode13</preferredMemberNode>
            <blockedMemberNode>MemberNode6</blockedMemberNode>
            <blockedMemberNode>MemberNode7</blockedMemberNode>
        </replicationPolicy>
        <dateUploaded>2006-05-04T18:13:51.0Z</dateUploaded>
        <dateSysMetadataModified>2009-05-04T18:13:51.0Z</dateSysMetadataModified>
        <originMemberNode>mn1.dataone.org/</originMemberNode>
        <authoritativeMemberNode>mn1.dataone.org/</authoritativeMemberNode>
    </d1:systemMetadata>

    error = 
    <error errorCode='404' detailCode='4060'>
      <description>The specified object does not exist on this node.</description>
      <traceInformation>
        <value key='identifier'>SomeObjectID</value>
        <value key='method'>cn.getSystemMetadat</value>
        <value key='hint'>http://cn.dataone.org/cn/resolve/SomeObjectID</value>
      </traceInformation>
    </error>
    '''
    pid = request.matchdict['pid']
    d = get_dataset(pid)

    if not pid:
        request.response.content_type = 'text/xml; charset=UTF-8'
        request.response.status = 404
        return '<?xml version="1.0" encoding="UTF-8"?><error detailCode="1800" errorCode="404" name="NotFound"><description>No system metadata could be found for given PID: DOESNTEXIST</description></error>'

    src = [s for s in d.sources if s.file_hash != None]
    if not src:
        #emit that xml
        request.response.content_type = 'text/xml; charset=UTF-8'
        request.response.status = 404
        return '<?xml version="1.0" encoding="UTF-8"?><error detailCode="1800" errorCode="404" name="NotFound"><description>No system metadata could be found for given PID: DOESNTEXIST</description></error>'
        #this just gets worse

    srcfiles = src[0].src_files
    file_size = src[0].file_size_mb
    file_hash = src[0].file_hash
    file_hash_type = src[0].file_hash_type

    host = request.host_url
    g_app = request.script_name[1:]
    base_url = '%s/%s/apps/dataone/' % (host, g_app)

    #TODO: fix all of this
    rsp = {'pid': pid, 'dateadded': datetime_to_dataone(d.dateadded), 'obj_format': src[0].extension, 'file_size': file_size, 'uid': 'GSTORE', 'o': 'EDAC', 'dc': 'everything', 'org': 'EDAC', 'hash_type': file_hash_type,
           'hash': file_hash, 'embargo': '', 'metadata_modified': datetime_to_dataone(datetime.now()), 'mn': host}

    return rsp

@view_config(route_name='dataone_checksum', match_param='app=dataone')
def checksum(request):
    '''
    <checksum algorithm="SHA-1">2e01e17467891f7c933dbaa00e1459d23db3fe4f</checksum>
    '''

    pid = request.matchdict['pid']

    d = get_dataset(pid)
    if not d:
        return HTTPNotFound('no dataset')
    if d.is_available == False:
        return HTTPNotFound('not available')

    #need to get the source that has a hash (original + zip)
    #TODO: what happens when more sources have hashes?

    src = [s for s in d.sources if s.extension == 'zip' and s.set == 'original' and s.active and s.file_hash is not None]
    if not src:
        return HTTPNotFound('not available')

    h = src[0].file_hash
    htype = src[0].file_hash_type

    #TODO: double check output
    #TODO: double-check list of hash algorithm terms
    return Response('<checksum algorithm="%s">%s</checksum>' % (htype, h), content_type='application/xml')

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
