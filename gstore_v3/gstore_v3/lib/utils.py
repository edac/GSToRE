import os, re, zipfile
import hashlib

from sqlalchemy.sql.expression import and_
from sqlalchemy.sql import between

from datetime import datetime

import uuid
import urllib2

import subprocess


'''
image mimetypes (mostly for mapserver)
'''
_IMAGE_MIMETYPES = {
        'PNG': 'image/png',
        'JPEG': 'image/jpeg',
        'GIF': 'image/gif',
        'TIFF': 'image/tiff'
    }
def get_image_mimetype(s):
    #check the value and the key for fun
    m = [(k, v) for k, v in _IMAGE_MIMETYPES.iteritems() if v.lower() == s.lower() or k.upper() == s.upper()]
    m = m[0] if m else None
    return m

'''
file utils
'''

def create_zip(fullname, files):
    zipf = zipfile.ZipFile(fullname, mode='w', compression=zipfile.ZIP_STORED)
    for f in files: 
        fname = f.split('/')[-1]
        zipf.write(f, fname)
    zipf.close()
    return fullname

'''
xslt/xml subprocess calls

'''
def transform_xml(xml, xslt_path, params):
    '''
    use saxonb-xslt for the transform (lxml only supports xslt 1, our xslts are in 2.0)

    note: xml is the xml as string
    '''
    #where params is a dict {"key": "value"}
    param_str = convert_to_xslt_params(params)

    #want the input xml to come from stdin (so -s:-)
    cmd = 'saxonb-xslt -s:- -xsl:%s %s' % (xslt_path, param_str)
    
    #shell must be true for saxonb to handle stdout here (otherwise it's a file not found error)
    s = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    output = s.communicate(input=xml.encode('utf-8'))[0]
    ret = s.wait()
    return output

def transform_xml_file(xml_path, xslt_path, params):
    '''
    use saxonb-xslt for the transform (lxml only supports xslt 1, our xslts are in 2.0)

    note: xml is the xml as string
    '''
    #where params is a dict {"key": "value"}
    param_str = convert_to_xslt_params(params)

    #want the input xml to come from stdin (so -s:-)
    cmd = 'saxonb-xslt -s:%s -xsl:%s %s' % (xml_path, xslt_path, param_str)
    
    #shell must be true for saxonb to handle stdout here (otherwise it's a file not found error)
    s = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    output = s.communicate()[0]
    ret = s.wait()
    return output

    return '' 

def validate_xml(xml):
    '''
    use pparse (stdinparse, but same thing really) to validate xml
    based on the schema defined in the xml (xsi:schemaLocation, etc)

    using stdinparse because we don't want to write the input to disk first

    note: can't provide an external (to the file) schema here
    note: xml is the xml as string
    '''
    
    cmd = 'StdInParse -v=always -n -s -f'
    s = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    #we need stderr from pparse/stdinparse to actually catch the errors
    stdout, stderr = s.communicate(input=xml)
    ret = s.wait()
    return stderr

def convert_to_xslt_params(params):
    return ' '.join(['%s=%s' % (p, params[p] if ' ' not in params[p] else '"%s"' % params[p]) for p in params])


'''
hashes
(see dataone api)
'''

#return the hash as sha1 or md5 for a file (so zip it somewhere first if it's a bunch of things)
def generate_hash(zipfile, algo):
    #TODO: change this if dataone rolls out other hash types or we want something else
    m = hashlib.md5() if algo.lower() == 'md5' else hashlib.sha1()
    zf = open(zipfile, 'rb')
    #turn this on if the files are too big for memory
    while True:
        data = zf.read(2**20)
        if not data:
            break
        m.update(data)
    zf.close()
    return m.hexdigest()

'''
uuid4 generator
'''
def generate_uuid4():
    return str(uuid.uuid4())

'''
regex

'''

#just do a check (for like the uuids in the urls)
def match_pattern(pattern, test):
    p = re.compile(pattern)
    results = p.match(test)

    return results is not None

'''
get the default format lists - vector, raster, file
update: all one list now
'''
def get_all_formats(req):
    fmts = req.registry.settings['DEFAULT_FORMATS']
    if not fmts:
        return []
    return fmts.split(',')
    
'''
get default services
'''
def get_all_services(req):
    svcs =  req.registry.settings['DEFAULT_SERVICES']
    if not svcs:
        return []
    return svcs.split(',')

'''
get default repositories
'''
def get_all_repositories(req):
    repos =  req.registry.settings['DEFAULT_REPOSITORIES']
    if not repos: 
        return []
    return repos.split(',')

'''
get the default standards
except gstore so that it doesn't turn up in the "public" links but also isn't excluded in the database
'''
def get_all_standards(req):
    stds = req.registry.settings['DEFAULT_STANDARDS']
    if not stds:
        return []
    return [s for s in stds.split(',') if s != 'GSTORE']



'''
build standard route urls (ogc services, metadata services, dataset downloads)

for things like the tile index or dataset or collection ogc service, the structure is the same

and for those that care, the route_path/route_url method for request is problematic with the load balancer (balancer/apps v appnode/gstore_v3/apps)
where the app node doesn't know about the balancer or the balancer url in any useful way. and the proxy widget in pyramid only works with traversal? 
and not url dispatch. so. 
'''
def build_ogc_url(app, data_type, uuid, service, version):
    '''
    /apps/{app}/{type}/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/services/{service_type}/{service}

    and the structure of the getcapabilities:
    ?SERVICE={service}&REQUEST=GetCapabilities&VERSION={version}

    '''
    return '/apps/%s/%s/%s/services/%s/%s?SERVICE=%s&REQUEST=GetCapabilities&VERSION=%s' % (app, data_type, uuid, 'ogc', service, service, version) 


def build_metadata_url(app, data_type, uuid, standard, extension):
    '''
    /apps/{app}/{datatype}/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/metadata/{standard}.{ext}
    '''
    return '/apps/%s/%s/%s/metadata/%s.%s' % (app, data_type, uuid, standard, extension)

def build_dataset_url(app, uuid, basename, aset, extension):
    '''
    /apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/{basename}.{type}.{ext}
    '''
    return '/apps/%s/datasets/%s/%s.%s.%s' % (app, uuid, basename, aset, extension)

def build_service_url(app, data_type, uuid):
    '''
    /apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/services.json
    '''
    return '/apps/%s/%s/%s/services.json' % (app, data_type, uuid)
    
def build_mapper_url(app, uuid):
    '''
    /apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/mapper
    '''

    return '/apps/%s/datasets/%s/mapper' % (app, uuid)

def build_prov_trace_url(app, uuid, ontology, format):
    '''
    /apps/{app}/datasets/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/prov/{ontology}.{ext}
    '''
    return '/apps/%s/datasets/%s/prov/%s.%s' % (app, uuid, ontology, format)
    

'''
convert the multidict request parameters to lowercase keys
'''

def normalize_params(params):
    new_params = {}
    for k in params.keys():
        new_params[k.lower()] = params[k]
    return new_params 

'''
decode a params dict
for dataone? see dataone view for all the ways they baffle me
'''
def decode_params(params):
    new_params = {}
    for k in params.keys():
        new_params[urllib2.unquote(urllib2.unquote(k.lower()).decode('unicode_escape'))] = urllib2.unquote(urllib2.unquote(params[k]).decode('unicode_escape')) 
    return new_params


'''
datetime utils
mostly for the sqla queries

dates as yyyyMMdd{THHMMss} (date with time optional)
and UTC time - interfaces should do the conversion
'''
def convert_timestamp(in_timestamp):
    sfmt = '%Y%m%dT%H:%M:%S'
    if not in_timestamp:
        return None
    try:
        if 'T' not in in_timestamp:
            in_timestamp += 'T00:00:00'
        out_timestamp = datetime.strptime(in_timestamp, sfmt)
        return out_timestamp
    except:
        return None
        
#to compare a date (single column) with a search range
def get_single_date_clause(column, start_range, end_range):
    start_range = convert_timestamp(start_range)
    end_range = convert_timestamp(end_range)

    if start_range and not end_range:
        clause = column >= start_range
    elif not start_range and end_range:
        clause = column < end_range
    elif start_range and end_range:
        clause = between(column, start_range, end_range)
    else:
        clause = None
    return clause
#to compare two sets of date ranges, one in table and one from search
def get_overlap_date_clause(start_column, end_column, start_range, end_range):
    start_range = convert_timestamp(start_range)
    end_range = convert_timestamp(end_range)

    if start_range and not end_range:
        clause = start_column >= start_range
    elif not start_range and end_range:
        clause = end_column < end_range
    elif start_range and end_range:
        clause = and_(start_column <= end_range, end_column >= start_range)
    else:
        clause = None
    return clause
    


def validate_uuid4(uuid_string):
     try:
         val = uuid.UUID(uuid_string, version=4)
     except ValueError:
         return False
     return val.hex == uuid_string.replace('-', '')

