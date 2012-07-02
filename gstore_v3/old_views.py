from pyramid.response import Response
from pyramid.view import view_config

from sqlalchemy.exc import DBAPIError

#from the models init script
from .models import DBSession
#from the generic model loader (like meta from gstore v2)
# from .models.models import (
#    # MyModel,
#     )

from .models.datasets import *

#TODO: set up a better default - like the api docs currently
@view_config(route_name='home', renderer='templates/mytemplate.pt')
def my_view(request):
    try:
        #one = DBSession.query(MyModel).filter(MyModel.name=='one').first()
        #one = DBSession.query(MyModel).filter(MyModel.name=='one').first()
        
        one = DBSession.query(Dataset).filter(Dataset.id==143533).first()
        one = ''
    except DBAPIError as e:
        return Response(str(e), content_type='text/plain', status_int=500)
    return {'one':one, 'project':'gstore_v3'}

conn_err_msg = """\
Pyramid is having a problem using your SQL database.  The problem
might be caused by one of the following things:

1.  You may need to run the "initialize_gstore_v3_db" script
    to initialize your database tables.  Check your virtual 
    environment's "bin" directory for this script and try to run it.

2.  Your database server may not be running.  Check that the
    database server referred to by the "sqlalchemy.url" setting in
    your "development.ini" file is running.

After you fix the problem, please restart the Pyramid application to
try it again.
"""

