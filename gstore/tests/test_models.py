from gstore.model.shapes import VectorDataset
from gstore.model import meta
from gstore.model.geobase import Dataset

import zipfile
import StringIO
import httplib2
import socket

datasets = meta.Session.query(Dataset).filter(Dataset.taxonomy == 'vector').all()

h = httplib2.Http()

for D in datasets:
    if D.sources_ref and 'http' == D.sources_ref[0].location[0:4]:
        continue
    tem = 'http://'+socket.gethostbyname(socket.gethostname()) +':5000/apps/rgis/datasets/%s' % D.id
    tem = tem + '.%s'
    s = StringIO.StringIO()
    response, content = h.request(tem % 'shp')
    if response.get('status') == '500':
       print 'Internal server error', tem
       continue
    s.write(content)
    s.seek(0)
    if zipfile.ZipFile(s).testzip():
       print 'Error with zipfile', tem
    for fmt in D.formats.split(','):
       if fmt == 'shp': 
           continue
       response, content = h.request(tem % fmt)
        
