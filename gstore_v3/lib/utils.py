import os, re, zipfile
import hashlib

from pyramid.threadlocal import get_current_registry


'''
image mimetypes (mostly for mapserver)
'''
_IMAGE_MIMETYPES = {
        'PNG': 'image/png',
        'JPEG': 'image/jepg',
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
def getFormats():
    fmts = get_current_registry().settings['DEFAULT_FORMATS']
    if not fmts:
        return []
    return fmts.split(',')
    
#def getFormats(taxonomy):
#    key = 'FILE_FORMATS' if taxonomy == 'file' else 'RASTER_FORMATS'
#    key = 'VECTOR_FORMATS' if taxonomy == 'vector' else key
#    fmts = get_current_registry().settings[key]

#    if not fmts:
#        return []

#    return fmts.split(',')

'''
get default services
'''
def getServices():
    svcs = get_current_registry().settings['DEFAULT_SERVICES']
    if not svcs:
        return []
    return svcs.split(',')


    
