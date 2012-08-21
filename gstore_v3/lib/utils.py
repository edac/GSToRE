import os, re, zipfile
import hashlib

from sqlalchemy.sql.expression import and_
from sqlalchemy.sql import between

from datetime import datetime

import uuid


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
    #m = [v for k, v in _IMAGE_MIMETYPES.iteritems() if v.lower() == s.lower() or k.upper() == s.upper()]
    m = [(k, v) for k, v in _IMAGE_MIMETYPES.iteritems() if v.lower() == s.lower() or k.upper() == s.upper()]
    m = m[0] if m else None
    return m

'''
file utils
'''

def createZip(fullname, files):
    zipf = zipfile.ZipFile(fullname, mode='w', compression=zipfile.ZIP_STORED)
    for f in files: 
        fname = f.split('/')[-1]
        zipf.write(f, fname)
    zipf.close()

    #which is silly except as a success indicator
    #which is silly
    return fullname

'''
hashes
(see dataone api)
'''

#return the hash as sha1 or md5 for a file (so zip it somewhere first if it's a bunch of things)
def getHash(zipfile, algo):
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
def matchPattern(pattern, test):
    p = re.compile(pattern)
    results = p.match(test)

    return results is not None

'''
get the default format lists - vector, raster, file
update: all one list now
'''
def getFormats(req):
    fmts = req.registry.settings['DEFAULT_FORMATS']
    if not fmts:
        return []
    return fmts.split(',')
    
'''
get default services
'''
def getServices(req):
    svcs =  req.registry.settings['DEFAULT_SERVICES']
    if not svcs:
        return []
    return svcs.split(',')


'''
convert the multidict request parameters to lowercase keys
'''
#convert all of the request parameter keys to lower case just to not have to deal with camelcase, uppercase, lowercase, all the cases!
def normalize_params(params):
    new_params = {}
    for k in params.keys():
        new_params[k.lower()] = params[k]
    return new_params 


'''
datetime utils
mostly for the sqla queries

dates as yyyyMMdd{THHMMss} (date with time optional)
and UTC time - interfaces should do the conversion
'''
def convertTimestamp(in_timestamp):
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
def getSingleDateClause(column, start_range, end_range):
    start_range = convertTimestamp(start_range)
    end_range = convertTimestamp(end_range)

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
def getOverlapDateClause(start_column, end_column, start_range, end_range):
    start_range = convertTimestamp(start_range)
    end_range = convertTimestamp(end_range)

    if start_range and not end_range:
        clause = start_column >= start_range
    elif not start_range and end_range:
        clause = end_column < end_range
    elif start_range and end_range:
        clause = and_(start_column <= end_range, end_column >= start_range)
    else:
        clause = None
    return clause
    
