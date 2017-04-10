import json
from pyramid.response import Response
from pyramid.view import view_config
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import Dataset

from ..models.tileindexes import *
from ..models.collections import Collection
from ..models.repositories import Repository
from ..models.apps import GstoreApp
from ..models.provenance import ProvOntology


@view_config(route_name='authors')
def authors(request):
    app = request.matchdict['app']
    d = DBSession.query(Dataset.author).filter(and_(*[Dataset.inactive==False, "apps_cache @> ARRAY['%s']" % (app), Dataset.is_embargoed==False])).distinct()
#    d = DBSession.query(Dataset.author).filter(app=ANY(apps_cache)).distinct()
#DBSession.query(DataoneFormat).filter(DataoneFormat.format==package_format).first()
#dataset_clauses = [Dataset.inactive==False, "'%s'=ANY(apps_cache)" % (app)]
    resp = {}
    rslts = []
    rslts = [{'author': r.author} for r in d]
    resp.update({"results": rslts})
    response = Response(json.dumps(resp))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.content_type="application/json"
    return response

