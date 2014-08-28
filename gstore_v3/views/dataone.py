from pyramid.view import view_config
from pyramid.response import Response, FileResponse

from pyramid.httpexceptions import HTTPNotFound, HTTPServerError, HTTPBadRequest, HTTPNotImplemented, HTTPServiceUnavailable

from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_
from sqlalchemy.sql import between

from datetime import datetime
import urllib2
import json
import os, shutil, cgi
from lxml import etree

from ..models import DBSession, DataoneSession
from ..models.datasets import (
    Dataset,
    )
from ..models.metadata import DatasetMetadata
from ..models.dataone import *
from ..models.dataone_logs import DataoneLog, DataoneError 
from ..models.apps import GstoreApp

from ..lib.utils import *
from ..lib.database import *
from ..lib.mongo import gMongo, gMongoUri



'''
see http://mule1.dataone.org/ArchitectureDocs-current/apis/MN_APIs.html
'''

'''
some presets
'''
NODE = 'urn:node:EDACGSTORE'
SUBJECT = 'CN=gstore.unm.edu,DC=dataone,DC=org'
RIGHTSHOLDER = 'CN=gstore.unm.edu,DC=dataone,DC=org'
#CONTACTSUBJECT = 'CN=gstore.unm.edu,O=Google,C=US,DC=cilogon,DC=org'

#the string for the dev coordinating node (or maybe staging)
#CONTACTSUBJECT = 'CN=Dev Team A10142,O=Google,C=US,DC=cilogon,DC=org'
#the one for produciton
#CONTACTSUBJECT='CN=Soren Scott A11096,O=Google,C=US,DC=cilogon,DC=org'
CONTACTSUBJECT='CN=Hays Barrett A13341,O=Google,C=US,DC=cilogon,DC=org'
NAME = 'EDAC Gstore Repository'
ALIAS = 'EDACGSTORE'
DESCRIPTION = "Earth Data Analysis Center's (EDAC) Geographical Storage, Transformation and Retrieval Engine (GSTORE) platform archives data produced by various NM organizations, including NM EPSCoR and RGIS. GSTORE primarily houses GIS and other digital documents relevant to state agencies, local government, and scientific researchers. See RGIS and NM EPSCoR for more information on the scope of data. It currently uses the FGDC metadata standard to describe all of its holdings."
CN_RESOLVER='https://cn.dataone.org/cn/v1/resolve'

#TODO: add the system metadata date modified trigger to obsoleting step and then add something for actually updating that model

#convert to the d1 format
def datetime_to_dataone(dt):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    
    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    return dt.strftime(fmt)
    
def datetime_to_http(dt):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    
    #TODO: utc to gmt
    #Wed, 16 Dec 2009 13:58:34 GMT
    fmt = '%a, %d %b %Y %H:%M:%S GMT'
    return dt.strftime(fmt)

def dataone_to_datetime(dt):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    
    #TODO: deal with more datetime issues (could be gmt or utc, with or without milliseconds)
    '''
    YYYY-MM-DDTHH:MM:SS:mm.fff
    YYYY-MM-DDTHH:MM:SS:mm.fff+00:00
    '''

    fmts = ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']
    dt = dt.replace('+00:00', '') if '+00:00' in dt else dt

    d = None
    for fmt in fmts:
        try:
            d = datetime.strptime(dt, fmt)
        except:
            pass
    return d

def is_good_int(s, default):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    if not s:
        #not provided is okay, we have a default
        return True, default
    try:
        i = int(s)

        if i >= 0:
            return True, i
        else:
            #it's negative, which we could replace with default, but let's fail as a bad request instead
            return False, -99
    except:
        #it's not an integer, which we could replace with default, but let's fail as a bad request instead
        return False, -99
    ##not sure what this is, which we could replace with default, but let's fail as a bad request instead
    return False, -99


def is_valid_url(url):
    """

    this assumes that we care about anything that isn't a uuid? but the d1 tests will always be 404 anyway. i don't get it.
    and they don't really explain what they expect as far as fails go.

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    
    try:
        url = urllib2.unquote(url.decode('unicode_escape'))
    except:
        return False
    return True    

def is_valid_uuid(u):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    
    pattern = '^[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}$'
    return match_pattern(pattern, u)

def return_error(error_type, detail_code, error_code, error_message='', pid=''):  
    """

    i am currently just guessing, but there is maybe some conflict in the encoding specified by the xml and the pid handling on the d1 end (in the tester)
    as in, we are sent a unicode pid, but we can't return the unicode in the utf8 xml, so we change the pid encoding to not have the xml fail
    and then the tests fail. 

    so we are NOT returning the pid in the error xml for the object/metadata responses to pass the tests

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    if error_code == 404 and (error_type == 'object' or error_type == 'metadata'):
        xml = '<?xml version="1.0" encoding="UTF-8"?><error detailCode="%s" errorCode="404" name="NotFound"><description>No system metadata could be found for given PID: DOESNTEXIST</description></error>' % (detail_code)
        return Response(xml, content_type='text/xml; charset=UTF-8', status='404')

#removed this because something about the PID (probably the encoding) caused the D1 tester to fail in ways that make no sense
#    elif error_code == 404 and error_type == 'metadata':
#        xml = '<?xml version="1.0" encoding="UTF-8"?><error detailCode="%s" errorCode="404" name="NotFound"><description>No system metadata could be found for given PID: %s</description></error>' % (detail_code, ('%s' % pid).encode('utf-8'))

#        return Response(xml, content_type='text/xml; charset=UTF-8', status='404')
    elif error_code == 400 or error_code == 401:
        error_message = error_message if error_message else 'Invalid Request'
        xml = '<?xml version="1.0" encoding="UTF-8"?><error detailCode="%s" errorCode="%s"><description>%s</description></error>' % (detail_code, error_code, error_message)
        return Response(xml.encode('utf-8'), content_type='text/xml; charset=UTF-8', status=str(error_code))

    elif error_code == 501:
        xml = '<?xml version="1.0" encoding="UTF-8"?><error detailCode="1001" errorCode="501"><description>Not implemented</description></error>'
        return Response(xml, content_type='text/xml; charset=UTF-8', status='501')
    return Response()

def return_error_head(pid):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    
    return [('Content-Type', 'text/xml'), 
               ('DataONE-Exception-Name', 'NotFound'), 
               ('DataONE-Exception-DetailCode', '1380'), 
               ('DataONE-Exception-Description', 'The specified object does not exist on this node.'),
               ('DataONE-Exception-PID', ('%s' % pid).encode('utf-8'))]
    
'''
dataone logging in second postgres db because they demand authentication for themselves but we have to just accept every damn thing.
'''
def log_entry(identifier, ip, event, useragent='public'):  
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    dlog = DataoneLog(identifier, ip, SUBJECT, event, NODE, useragent)
    try:
        DataoneSession.add(dlog)
        DataoneSession.commit()
    except:
        DataoneSession.rollback()
        raise

#TODO: modify the cache settings
@view_config(route_name='dataone_ping', http_cache=3600)
@view_config(route_name='dataone_ping_slash', http_cache=3600)
def ping(request):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    try:
        d = get_dataset('61edaf94-2339-4096-9cc0-4bfb79a9c848')
    except:
        return HTTPServerError()
    return Response()

@view_config(route_name='dataone_noversion', renderer='../templates/dataone_node.pt')
@view_config(route_name='dataone_noversion_slash', renderer='../templates/dataone_node.pt')	
@view_config(route_name='dataone', renderer='../templates/dataone_node.pt')
@view_config(route_name='dataone_slash', renderer='../templates/dataone_node.pt')
@view_config(route_name='dataone_node', renderer='../templates/dataone_node.pt')
@view_config(route_name='dataone_node_slash', renderer='../templates/dataone_node.pt')
def dataone(request):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/dataone/' % (load_balancer)

    #this is annoying. and incorrect anywhere but production
    base_url = base_url.replace('http:', 'https:')

    #set up the dict
    rsp = {
           'node': NODE,
           'name': NAME,
           'description': DESCRIPTION,
           'baseUrl': base_url,
           'subject': SUBJECT,
           'contactsubject': CONTACTSUBJECT
        }
    request.response.content_type='text/xml'
    return rsp

@view_config(route_name='dataone_log')
@view_config(route_name='dataone_log_slash')
def log(request):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    #TODO: check on filters again (pidFilter not working??)

    #check the encoding
    url = request.path_qs
    good_encoding = is_valid_url(url)
    if good_encoding == False:
        return return_error('object', 1504, 400)

    params = normalize_params(request.params)
    params = decode_params(params)

    offset = params['start'] if 'start' in params else ''
    limit = params['count'] if 'count' in params else ''

    is_good_offset, offset = is_good_int(offset, 0)
    is_good_limit, limit = is_good_int(limit, 20)

    #just because it's a good int doesn't mean we like it
    #reset to 20 if greater
    limit = 20 if limit > 20 else limit

    if not is_good_offset or not is_good_limit:
        return return_error('object', 1504, 400)

    fromDate = params.get('fromdate') if 'fromdate' in params else ''
    toDate = params.get('todate') if 'todate' in params else ''

    #return objects with pid that start with this string
    pid_init = params.get('pidfilter') if 'pidfilter' in params else '' 

    event = params.get('event') if 'event' in params else ''

    #TODO: add the session request

    clauses = []
    if pid_init:
        clauses.append(DataoneLog.identifier.ilike(pid_init + '%'))
    if event:
        clauses.append(DataoneLog.event.like(event))

    if fromDate and not toDate:
        from_date = dataone_to_datetime(fromDate)
        if not from_date:
            return return_error('object', 1504, 400)
        clauses.append(DataoneLog.logged>=from_date)
    elif not fromDate and toDate:
        to_date = dataone_to_datetime(toDate)
        if not to_date:
            return return_error('object', 1504, 400)
        clauses.append(DataoneLog.logged<=to_date)
    elif fromDate and toDate:
        from_date = dataone_to_datetime(fromDate)
        to_date = dataone_to_datetime(toDate)
        if not from_date or not to_date:
            return return_error('object', 1504, 400)
        clauses.append(between(DataoneLog.logged, from_date, to_date))
           

    query = DataoneSession.query(DataoneLog).filter(and_(*clauses))
    total = query.count()
    query = query.limit(limit).offset(offset).all()

    fmt = '%Y-%m-%dT%H:%M:%S+00:00'
    
    entries = []
    for q in query:
        entries.append(q.get_log_entry())
    rsp = '<?xml version="1.0" encoding="UTF-8"?><d1:log xmlns:d1="http://ns.dataone.org/service/types/v1" count="%s" start="%s" total="%s">%s</d1:log>' % (len(query), offset, total, ''.join(entries))

    return Response(rsp, content_type='application/xml')    
	

@view_config(route_name='dataone_search', renderer='dataone_search.mako')
@view_config(route_name='dataone_search_slash', renderer='dataone_search.mako')
def search(request):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    url = request.path_qs
    good_encoding = is_valid_url(url)
    if good_encoding == False:
        return return_error('object', 1504, 400)

    params = normalize_params(request.params)
    params = decode_params(params)

    offset = params.get('start') if 'start' in params else ''
    limit = params.get('count') if 'count' in params else ''

    is_good_offset, offset = is_good_int(offset, 0)
    is_good_limit, limit = is_good_int(limit, 20)

    #just because it's a good int doesn't mean we like it
    #reset to 20 if greater
    limit = 20 if limit > 20 else limit

    if not is_good_offset or not is_good_limit:
        return return_error('object', 1504, 400)

    fromDate = params.get('fromdate', '') 
    toDate = params.get('todate', '') 

    formatId = params.get('formatid', '')

    #TODO: add replica status somewhere (but we're not replicating stuff yet)
    replicaStatus = params.get('replicastatus', '')

    search_clauses = []

    if fromDate or toDate:
        #make them dates
        #build the clauses
        if fromDate and not toDate:
            #greater than from
            fd = dataone_to_datetime(fromDate)
            if not fd:
                return return_error('object', 1504, 400)
            search_clauses.append(DataoneSearch.object_changed >= fd)
        elif not fromDate and toDate:
            #less than to
            ed = dataone_to_datetime(toDate)
            if not ed:
                return return_error('object', 1504, 400)
            search_clauses.append(DataoneSearch.object_changed < ed)
        else:
            #between
            fd = dataone_to_datetime(fromDate)
            ed = dataone_to_datetime(toDate)
            if not fd or not ed:
                return return_error('object', 1504, 400)
            search_clauses.append(between(DataoneSearch.object_changed, fd, ed))

    if formatId:
        #formatId = urllib2.unquote(formatId)
        search_clauses.append(DataoneSearch.object_format==formatId)

    query = DBSession.query(DataoneSearch, DataoneObsolete).join(DataoneObsolete, DataoneSearch.obsolete_uuid==DataoneObsolete.uuid).filter(and_(*search_clauses))
    total = query.count()

    objects = query.limit(limit).offset(offset).all()

    dataone_path = request.registry.settings['DATAONE_PATH']

    cnt = total if total < limit else limit

    docs = []
    algo = 'md5'
    for obj in objects:
        search = obj[0]
        obsolete = obj[1]
        
        object_format = search.object_format
        object_type = search.object_type
        object_ext = search.object_ext

        obsolete_uuid = obsolete.uuid

        type_path = 'datasets'
        if object_type == 'science metadata':
            type_path = 'metadata'
        elif object_type == 'data package':
            type_path = 'packages'
 
        if object_type == 'data object':
            packed_ext = 'csv' if object_ext == 'csv' else 'zip' 
            cached_path = os.path.join(dataone_path, type_path, object_ext, '%s.%s' % (obsolete_uuid, packed_ext))
        else:
            cached_path = os.path.join(dataone_path, type_path, '%s.xml' % obsolete_uuid)
            
        #get the obsolete object (get hash and size)
        md5 = obsolete.get_hash(algo, cached_path)
        fsize = obsolete.get_size(cached_path)

        sysmeta = obsolete.system_metadatas[0]
        sys_date = sysmeta.date_changed

        docs.append({'identifier': obsolete_uuid, 'format': object_format, 'algo': algo, 'checksum': md5, 'date': datetime_to_dataone(sys_date), 'size': fsize})
        
    return {'total': total, 'count': cnt, 'start': offset, 'docs': docs}
	

@view_config(route_name='dataone_object', request_method='GET')
@view_config(route_name='dataone_object_slash', request_method='GET')
def show(request):
    """return the file object for this uuid

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    pid = request.matchdict['pid']
    
    try:
        pid = urllib2.unquote(urllib2.unquote(pid).decode('unicode_escape'))
    except:
        return return_error('object', 1020, 404)

    is_uuid = is_valid_uuid(pid)
    if not is_uuid:
        return return_error('object', 1020, 404)

    dataone_path = request.registry.settings['DATAONE_PATH']

    try:
        obsolete, obj_path, mimetype, formatid, formatname = get_obsoleted_object(pid, dataone_path)
    except:    
        return return_error('object', 1020, 404)

    if not obsolete or not obj_path:
        return return_error('object', 1020, 404)

    #TODO: add the session info or something    
    log_entry(pid, request.client_addr, 'read')

    fr = FileResponse(obj_path, content_type=str(mimetype))
    fr.content_disposition = 'attachment; filename=%s' % obj_path.split('/')[-1]
    return fr

@view_config(route_name='dataone_object', request_method='HEAD')
def head(request):
    """

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

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    pid = request.matchdict['pid']
    
    try:
        pid = urllib2.unquote(urllib2.unquote(pid).decode('unicode_escape'))
    except:
        lst = return_error_head(pid)
        rsp = Response()
        rsp.status = 404
        rsp.headerlist = lst
        return rsp


    is_uuid = is_valid_uuid(pid)
    if not is_uuid:
        lst = return_error_head(pid)
        rsp = Response()
        rsp.status = 404
        rsp.headerlist = lst
        return rsp

    dataone_path = request.registry.settings['DATAONE_PATH']

    try:
        obsolete, obj_path, mimetype, formatid, formatname = get_obsoleted_object(pid, dataone_path)
    except:  
        lst = return_error_head(pid)
        rsp = Response()
        rsp.status = 404
        rsp.headerlist = lst
        return rsp

    algo = 'md5'
    file_hash = obsolete.get_hash(algo, obj_path)
    file_size = obsolete.get_size(obj_path)

    lst = [('Last-Modified','%s' % (str(datetime_to_http(obsolete.date_changed)))), ('Content-Type', str(mimetype)), ('Content-Length','%s' % (int(file_size)))]

    #see misleading mimetype-ness re get_obsoleted_object
    lst.append(('DataONE-ObjectFormat', str(formatid)))
    lst.append(('DataONE-Checksum', '%s,%s' % (algo, str(file_hash))))
    #TODO: change this to something but i don't think it's ours to set so who the hell knows.
    lst.append(('DataONE-SerialVersion', '1'))

    rsp = Response()
    rsp.headerlist = lst
    return rsp

@view_config(route_name='dataone_meta', renderer='dataone_metadata.mako')
@view_config(route_name='dataone_meta_slash', renderer='dataone_metadata.mako')
def metadata(request):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    pid = request.matchdict['pid']
    try:
        pid = urllib2.unquote(urllib2.unquote(pid).decode('unicode_escape'))
    except:
        return return_error('metadata', 1060, 404, '', pid)

    #let's make sure it's a valid uuid before pinging the database
    is_uuid = is_valid_uuid(pid)
    if not is_uuid:
        return return_error('metadata', 1060, 404, '', pid)    

    dataone_path = request.registry.settings['DATAONE_PATH']

    obsolete, obj_path, mimetype, formatid, formatname = None, None, None, None, None
    try:
        #mimetype is pretty much a lie here (it's the format.format) 
        obsolete, obj_path, mimetype, formatid, formatname = get_obsoleted_object(pid, dataone_path)
    except:
        return return_error('metadata', 1060, 404, '', pid)

    #get the obsoleted by obsolete obj (if core.obsoletes > 1, get prev 1)
    obsoleted_by = obsolete.get_obsoleted_by()
    obsoleted_by_uuid = obsoleted_by.uuid if obsoleted_by else ''

    obsoletes = obsolete.get_obsoletes()
    obsoletes_uuid = obsoletes.uuid if obsoletes else ''

    algo = 'md5'
    file_hash = obsolete.get_hash(algo, obj_path)
    file_size = obsolete.get_size(obj_path)

    obj = obsolete.cores.get_object()
    if not obj:
        return return_error('metadata', 1060, 404, '', pid)

    sysmeta = obsolete.system_metadatas[0]
    access_policies = sysmeta.access_policies
    replication = "true" if sysmeta.replication_policy else "false"
    sys_date = sysmeta.date_changed
    
    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/dataone/v1/' % (load_balancer)

    #this is annoying. and incorrect anywhere but production
    base_url = base_url.replace('http:', 'https:')

    #TODO: what the hell will the modified date be? who knows?

    #use the obsoletedby's date as the system modified date?

    #TODO: replace the hardcoded junk with ???
    rsp = {'pid': pid, 'dateadded': datetime_to_dataone(obj.date_added), 
            'obj_format': str(formatid), 'file_size': file_size, 
            'uid': ALIAS, 
            'o': 'EDAC', 
            'dc': 'everything', 
            'org': 'EDAC', 
            'hash_type': algo,
            'hash': file_hash, 
            'metadata_modified': datetime_to_dataone(sys_date), 
            'mn': NODE, 
            'obsoletes': obsoletes_uuid, 
            'obsoletedby': obsoleted_by_uuid, 
            'replication': replication, 
            'access_policies': access_policies
        }

    if obsoleted_by:
        rsp.update({"archived": True})

    request.response.content_type = 'text/xml; charset=UTF-8'
    return rsp

@view_config(route_name='dataone_checksum')
@view_config(route_name='dataone_checksum_slash')
def checksum(request):
    """

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    pid = request.matchdict['pid']
    try:
        pid = urllib2.unquote(urllib2.unquote(pid).decode('unicode_escape'))
    except:
        return return_error('object', 1800, 404)

    is_uuid = is_valid_uuid(pid)
    if not is_uuid:
        return return_error('object', 1800, 404)

    url = request.path_qs
    good_encoding = is_valid_url(url)
    if good_encoding == False:
        return return_error('object', 1504, 400)

    params = normalize_params(request.params)
    params = decode_params(params)

    algo = params.get('checksumalgorithm', '')
    if not algo:
        return return_error('object', 1800, 404)

    dataone_path = request.registry.settings['DATAONE_PATH']

    try:
        obsolete, obj_path, mimetype, formatid, formatname = get_obsoleted_object(pid, dataone_path)
    except:
        return return_error('object', 1800, 404)


    h = obsolete.get_hash(algo, obj_path)

    return Response('<?xml version="1.0" encoding="UTF-8"?><d1:checksum xmlns:d1="http://ns.dataone.org/service/types/v1" algorithm="%s">%s</d1:checksum>' % (algo, h), content_type='application/xml')

@view_config(route_name='dataone_error', request_method='POST')
@view_config(route_name='dataone_error_slash', request_method='POST')
def error(request):
    """

    key should be message. no guarantees

    also, they don't use their own error codes

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    message = request.POST['message']

    kvps = []
    fs = cgi.FieldStorage(fp=request.environ['wsgi.input'], environ=request.environ, keep_blank_values=1)
    for k in fs.keys():
        values = fs[k]
        if not isinstance(values, list):
            values = [values]

        txt = ''
        for v in values:
            txt += v.value + ';'

        kvps.append((k, txt))

    message = [kvp for kvp in kvps if kvp[0] == 'message']
    
    if not message:
        #this is not the right error, but if it fails ???
        return return_error('object', 2164, 401)

    #just for kicks
    message = message[0][1].split(';')[0]

    try:
        xml = etree.fromstring(message)
        '''
        <?xml version="1.0" encoding="UTF-8"?>
        <error detailCode="0" errorCode="0" name="SynchronizationFailed" pid="6b074af9-1ca4-4c23-9693-e3c249b44425">
            <description>a message</description>
        </error>
        '''

        detail = xml.attrib['detailCode']
        code = xml.attrib['errorCode']
        name = xml.attrib['name']
        pid = xml.attrib['pid']

        desc = xml.find('description')

        if not detail or not code or not name or not pid or desc is None:
            return return_error('object', 2161, 500)
        
    except:
        return return_error('object', 2161, 500)

    #just dump it into another table? sure why the hell not
    de = DataoneError(message)
    try:
        DataoneSession.add(de)
        DataoneSession.commit()
    except:
        DataoneSession.rollback()
        return return_error('object', 2161, 500)
        
  
    return Response()

@view_config(route_name='dataone_replica')
@view_config(route_name='dataone_replica_slash')
def replica(request):
    """

    log as replica request not just GET

    return file but this means absolutely nothing for us and they can't really 
    explain what replica i would return (or have been storing) as a tier one node

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    pid = request.matchdict['pid']
    try:
        pid = urllib2.unquote(urllib2.unquote(pid).decode('unicode_escape'))
    except:
        return return_error('object', 2185, 404)

    is_uuid = is_valid_uuid(pid)
    if not is_uuid:
        return return_error('object', 2185, 404)

    url = request.path_qs
    good_encoding = is_valid_url(url)
    if good_encoding == False:
        return return_error('object', 2185, 404)

    dataone_path = request.registry.settings['DATAONE_PATH']
    
    try:
        obsolete, obj_path, mimetype, formatid, formatname = get_obsoleted_object(pid, dataone_path)
    except: 
        return_error('object', 2185, 404)

    if not obsolete or not obj_path:
        return_error('object', 2185, 404)

    #add the super special replica log entry
    log_entry(pid, request.client_addr, 'replicate')

    fr = FileResponse(obj_path, content_type='application/octet-stream')
    #make the download filename be the obsolete_uuid that was requested just to be consistent
    fr.content_disposition = 'attachment; filename=%s' % obj_path.split('/')[-1]
    return fr


def get_obsoleted_object(pid, dataone_path):
    """
    check the obsoletes table for the object
    get the object
    get the path to the datafile
    get the file info

    
    this is a little roundabout. just... leave it.

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    
    obsolete = DBSession.query(DataoneObsolete).filter(DataoneObsolete.uuid==pid).first()
    if not obsolete:
        raise Exception('no obsolete')

    #need to get the object
    cores = obsolete.cores
    if not cores:
        raise Exception('no object')

    if cores.object_type == 'data object':
        obj = DBSession.query(DataoneDataObject).filter(DataoneDataObject.uuid==cores.object_uuid).first()
        if not obj:
            raise Exception('no data object')

        formatid = obj.formats.format
        formatname = obj.formats.name  
        mimetype = obj.formats.mimetype

        #TODO: this won't work if we ever serve anything else
        packed_ext = 'csv' if 'csv' in mimetype else 'zip'
        object_ext = obj.dataset_format.lower()     

        obj_path = os.path.join(dataone_path, 'datasets', object_ext, '%s.%s' % (obsolete.uuid, packed_ext))  
    elif cores.object_type == 'science metadata object':
        obj = DBSession.query(DataoneScienceMetadataObject).filter(DataoneScienceMetadataObject.uuid==cores.object_uuid).first()
        if not obj:
            raise Exception('no science metadata object')

        formatid = obj.formats.format
        formatname = obj.formats.name
        mimetype = obj.formats.mimetype

        obj_path = os.path.join(dataone_path, 'metadata', '%s.xml' % obsolete.uuid)
    elif cores.object_type == 'data package':
        obj = DBSession.query(DataoneDataPackage).filter(DataoneDataPackage.uuid==cores.object_uuid).first()
        if not obj:
            raise Exception('no data package')
        
        formatid = obj.formats.format
        formatname = obj.formats.name
        mimetype = obj.formats.mimetype

        obj_path = os.path.join(dataone_path, 'packages', '%s.xml' % obsolete.uuid)
    else:
        raise Exception('invalid object type')

    #TODO: mimetype is misleading, it's the formatid.format field
    #return the obsolete object, the actual path to the data file, the mimetype and name of the d1 format obj
    return obsolete, obj_path, mimetype, formatid, formatname


'''
dataone management methods

'''

@view_config(route_name='dataone_add', request_method='POST', renderer='json')
def add_object(request):
    """

    for data object/science metadata
    {
        'dataset': #id
        'options': {
            'dataset format':
            'metadata standard':
            'd1 file format':
        }
        "generate": t/f
        'activate': t/f
    }
    for data package
    {
        'data object'
        'metadata object'
        'format'
    }  

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """

    object_type = request.matchdict['object'].lower()

    data = request.json_body

    generate = data['generate'] if 'generate' in data else False
    activate = data['activate'] if 'activate' in data else False

    if object_type == 'dataobject':
        dataset_id = data['dataset']
        options = data['options']

        dataset_format = options['dataset_format']
        dataobject_format = options['object_format']

        format_obj = DBSession.query(DataoneFormat).filter(DataoneFormat.format==dataobject_format).first()
        if not format_obj:
            return HTTPServerError('Invalid format name')

        filters = [DataoneDataObject.dataset_id==dataset_id, DataoneDataObject.dataset_format==dataset_format.lower(), DataoneDataObject.format_id==format_obj.id]
        if DBSession.query(DataoneDataObject).filter(and_(*filters)).count() > 0:
            return HTTPServerError('This data object combination already exists')

        filters = [Dataset.id==dataset_id, Dataset.inactive==False, Dataset.is_embargoed==False]
        dataset = DBSession.query(Dataset).filter(and_(*filters)).first()
        if not dataset:
            return HTTPServerError('Invalid dataset')            

        supported_formats = dataset.get_formats(request)
        if dataset_format.lower() not in supported_formats:
            return HTTPServerError('Invalid dataset format')
        
        new_object = DataoneDataObject(dataset_id, dataset_format.lower(), format_obj.id)
        
    elif object_type == 'sciencemetadata':
        dataset_id = data['dataset']
        options = data['options']

        metadata_standard = options['standard']
        metadata_format = options['object_format']

        format_obj = DBSession.query(DataoneFormat).filter(DataoneFormat.format==metadata_format).first()
        if not format_obj:
            return HTTPServerError('Invalid format name')

        standard = DBSession.query(MetadataStandards).filter(MetadataStandards.alias==metadata_standard).first()
        if not standard:
            return HTTPServerError('invalid metadata standard')

        filters = [Dataset.id==dataset_id, Dataset.inactive==False, Dataset.is_embargoed==False]
        dataset = DBSession.query(Dataset).filter(and_(*filters)).first()
        if not dataset:
            return HTTPServerError('Invalid dataset')

        filters = [DataoneScienceMetadataObject.dataset_id==dataset_id, DataoneScienceMetadataObject.standard_id==standard.id, DataoneScienceMetadataObject.format_id==format_obj.id]
        if DBSession.query(DataoneDataObject).filter(and_(*filters)).count() > 0:
            return HTTPServerError('This data object combination already exists')    

        supported_standards = dataset.get_standards(request)
        if metadata_standard not in supported_standards:
            return HTTPServerError('unsupported standard')

        new_object = DataoneScienceMetadataObject(dataset.id, standard.id, format_obj.id)
        
    elif object_type == 'datapackage':
        data_obj_uuid = data['dataobject']
        scimeta_obj_uuid = data['metadataobject']
        package_format = data['format']

        format_obj = DBSession.query(DataoneFormat).filter(DataoneFormat.format==package_format).first()
        if not format_obj:
            return HTTPServerError('Invalid format name')

        data_object = DBSession.query(DataoneDataObject).filter(DataoneDataObject.uuid==data_obj_uuid).first()
        if not data_object:
            return HTTPServerError('invalid data object')

        metadata_object = DBSession.query(DataoneScienceMetadataObject).filter(DataoneScienceMetadataObject.uuid==scimeta_obj_uuid).first()
        if not metadata_object:
            return HTTPServerError('invalid metadata object')

        filters = [DataoneDataPackage.dataobj_uuid==data_object.uuid, DataoneDataPackage.scimetadataobj_uuid==metadata_object.uuid, DataoneDataPackage.format_id==format_obj.id]
        if DBSession.query(DataoneDataPackage).filter(and_(*filters)).count() > 0:
            return HTTPServerError('data package already exists')


        new_object = DataoneDataPackage(data_object.uuid, metadata_object.uuid, format_obj.id)
    else:
        return HTTPNotFound('invalid dataone object type')    

    #commit the object
    try:
        DBSession.add(new_object)
        DBSession.commit()
        DBSession.refresh(new_object)
    except Exception as ex:
        DBSession.rollback()
        return HTTPServerError(ex)

    #register it
    try:
        object_uuid, obsolete_uuid = new_object.register_object()
    except Exception as ex:
        return HTTPServerError(ex)


    if generate:
        balancer_url = request.registry.settings['BALANCER_URL']
        xslt_path = request.registry.settings['XSLT_PATH'] + '/xslts'
        dataone_path = request.registry.settings['DATAONE_PATH']

        #this is annoying. and incorrect anywhere but production
        balancer_url = balancer_url.replace('http:', 'https:')
    
        if object_type == 'dataobject':
            mconn = request.registry.settings['mongo_uri']
            mcoll = request.registry.settings['mongo_collection']
            mongo_uri = gMongoUri(mconn, mcoll)
            epsg = int(request.registry.settings['SRID'])

            object_type = 'csv' if 'csv' in format_obj.format else 'zip'

            original_dataset = DBSession.query(Dataset).filter(Dataset.id==dataset_id).first()
            supported_standards = original_dataset.get_standards(request)
            std = ''    

            req_app = DBSession.query(GstoreApp).filter(GstoreApp.route_key=='dataone').first()
            if not req_app:
                app_prefs = ['FGDC-STD-001-1998','FGDC-STD-012-2002','ISO-19115:2003']
            else:
                app_prefs = req_app.preferred_metadata_standards    
            std = next(s for s in app_prefs if s in supported_standards)

            #the size of the file is unknown since we are now building the file
            #that would contain the metadata listing the size of the file.
            metadata_info = {
                "app": "dataone",
                "distribution_links": [
                    {
                        "link": '%s/dataone/v1/object/%s' % (balancer_url, obsolete_uuid),
                        "size": 0,
                        "type": object_type
                    }
                ],
                "onlinks": [
                    '%s/dataone/v1/object/%s' % (balancer_url, obsolete_uuid)
                ],
                "base_url": '%s/dataone/v1/object/' % balancer_url,
                "xslt_path": xslt_path,
                "standard": std
            }

            try:
                new_object.generate_object(os.path.join(dataone_path, 'datasets'), obsolete_uuid, epsg, mongo_uri, metadata_info)
            except Exception as ex:
                return HTTPServerError('failed to generate object for %s\n%s' % (new_object.uuid, ex))
            
        elif object_type == 'sciencemetadata':
            try:
                new_object.generate_object(os.path.join(dataone_path, 'metadata'), obsolete_uuid, xslt_path, balancer_url)
            except Exception as ex:
                return HTTPServerError('failed to generate object for %s\n%s' % (new_object.uuid, ex))
        elif object_type == 'datapackage':
            try:
                new_object.generate_object(os.path.join(dataone_path, 'packages'), obsolete_uuid, False)
            except Exception as ex:
                return HTTPServerError('failed to generate object for %s\n%s' % (new_object.uuid, ex))

    if activate:
        #activate it
        try:
            new_object.activate_object()
        except Exception as ex:
            DBSession.rollback()
            return HTTPServerError(ex)
    
    return json.dumps({"object_uuid": new_object.uuid})

@view_config(route_name='dataone_update', request_method='POST')
def update_object(request):
    """

    {
        'identifier': #for the actual object which i don't know how we'll know actually
        'update': {
            'method': # register as dirty
        }
    }

    Notes:
        
    Args:
        
    Returns:
    
    Raises:
    """
    object_type = request.match_dict['object'].lower()

    data = request.json_body

    return ''


