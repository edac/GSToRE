import json
from pyramid.response import Response
from pyramid.view import view_config
from ..models import DBSession
from ..models.datasets import Dataset

@view_config(route_name='authors')
def authors(request):
    app = request.matchdict['app']
    d = DBSession.query(Dataset.author).filter(*["apps_cache @> ARRAY['%s']" % (app)]).distinct()
    resp = {}
    rslts = []
    rslts = [{'author': r.author} for r in d]
    resp.update({"results": rslts})
    response = Response(json.dumps(resp))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.content_type="application/json"
    return response

