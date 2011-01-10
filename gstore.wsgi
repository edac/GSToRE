import os, sys
sys.path.append('/var/gstore')
#os.environ['PYTHON_EGG_CACHE'] = '/usr/local/pylons/python-eggs'

from paste.deploy import loadapp

application = loadapp('config:/var/gstore/production.ini')
