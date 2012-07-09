import os, re, zipfile

from pyramid.threadlocal import get_current_registry

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
