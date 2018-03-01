import json
from pyramid.response import Response
from pyramid.view import view_config
from ..models import DBSession
from ..models.datasets import Dataset
from ..models.categories import Category
from sqlalchemy import func

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

@view_config(route_name='datasetsreport')
def datasetsreport(request):
    after=request.params.get('after') if 'after' in request.params else '1050-01-01'
    before=request.params.get('before') if 'before' in request.params else '9850-01-01'
    excludedtheme=request.params.get('excludetheme') if 'excludetheme' in request.params else 'lol'
    app = request.matchdict['app']
    d = DBSession.query(Dataset.author, Category.theme, func.count().label('c')).filter(Category.theme!=excludedtheme,Dataset.is_available==True,Dataset.dateadded>=after,Dataset.dateadded<=before,*["apps_cache @> ARRAY['%s']" % (app)]).group_by(Dataset.author, Category.theme).distinct().join(Dataset.categories).order_by('c DESC')
    resp = {}
    rslts = []
    rslts = [{'author': r.author,'count':r.c, 'theme':r.theme} for r in d]
    resp.update({"results": rslts})
    response = Response(json.dumps(resp))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.content_type="application/json"
    return response

