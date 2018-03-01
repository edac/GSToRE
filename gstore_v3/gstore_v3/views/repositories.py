from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPServerError, HTTPBadRequest

import json
from datetime import datetime

#from the models init script
from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.repositories import (
    Repository
    )

from ..lib.spatial import *
from ..lib.utils import *
from ..lib.database import *
from ..lib.es_searcher import RepositorySearcher


'''
views to request the datasets/collections/whatever that belong in a repo
so "give me all of the datasets that should be registered with data.gov"

'''
_DATE_FORMAT = '%Y%m%d'

@view_config(route_name='repository', renderer='json')
def show_repo(request):
    '''
    return info about repo:
        name
        description
        url
        apps that call it
        supports metadata standards? OR has metadata standards?
    '''

    repo_name = request.matchdict['repo']
    repo = get_repository(repo_name)
    if not repo:
        return HTTPNotFound()

    return {"uuid": str(repo.uuid), "name": repo.name, "description": repo.description, "url": repo.url, "standards": repo.get_standards(request)}

@view_config(route_name='repositories', renderer='json')
def show_repos(request):
    '''
    return all repos supported for an app and the standards supported by the repo in the app
    '''
    #TODO: need to change the structure to get the repos per app without polling the whole app + repo + dataset table

    app_name = request.matchdict['app']

    app = get_app(app_name)
    if not app:
        return HTTPNotFound()

    output = app.get_repositories(request)

    response = Response(json.dumps(output))
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.content_type = 'application/json'
    return response

@view_config(route_name='search_repo')
def search_repo(request):
    '''
    return gstore object metadata links
    /apps/{app}/repository/{repo}/{doctypes}/{standard}.{ext}

    params:
        limit/offset
        version?
        basic vs complete?

        changed={before|after}:{yyyyMMdd}
        added={before|after}:{yyyyMMdd}
        
    doctypes = datasets, collections
    ext = json
    
    '''

    app = request.matchdict['app']
    repo = request.matchdict['repo']
    doctypes = request.matchdict['doctypes']
    standard = request.matchdict['standard']
    ext = request.matchdict['ext']

    if ext != 'json':
        return HTTPNotFound()

    params = normalize_params(request.params)

    limit = int(params['limit']) if 'limit' in params else 20
    offset = int(params['offset']) if 'offset' in params else 0

    #if both of these, it is effectivel changedAndAdded
    changed_param = params['changed'] if 'changed' in params else ''
    added_param = params['added'] if 'added' in params else ''

    #if one of these, ignore changed and added and just use one. if both of these, bail because YOU MUST DECIDE SOMETHING.
    changedOrAdded_param = params['changedoradded'] if 'changedoradded' in params else ''
    changedAndAdded_param = params['changedandadded'] if 'changedandadded' in params else ''

    optional_params = {}

    if changedOrAdded_param and changedAndAdded_param:
        return HTTPBadRequest('pick a date query type')

    date_query = {}
    if changedOrAdded_param or changedAndAdded_param:
        date_query['combination'] = 'or' if changedOrAdded_param else 'and'
        parts = changedOrAdded_param.split(':') if changedOrAdded_param else changedAndAdded_param.split(':')

        if 'before' not in parts[0] and 'after' not in parts[0]:
            return HTTPBadRequest()

        try:
            the_date = datetime.strptime(parts[1], _DATE_FORMAT)
        except:
            return HTTPBadRequest()

        date_query['changed'] = {"order": parts[0], "date": the_date}
        date_query['added'] = {"order": parts[0], "date": the_date}

    if changed_param and not(changedOrAdded_param or changedAndAdded_param):
        parts = changed_param.split(':')
        if 'before' not in parts[0] and 'after' not in parts[0]:
            return HTTPBadRequest()

        try:
            change_date = datetime.strptime(parts[1], _DATE_FORMAT)
        except:
            return HTTPBadRequest()

        date_query['changed'] = {"order": parts[0], "date": change_date}    
    if added_param and not(changedOrAdded_param or changedAndAdded_param):
        parts = added_param.split(':')
        if 'before' not in parts[0] and 'after' not in parts[0]:
            return HTTPBadRequest()

        try:
            add_date = datetime.strptime(parts[1], _DATE_FORMAT)
        except:
            return HTTPBadRequest()

        date_query['added'] = {"order": parts[0], "date": add_date}    

    if changed_param and added_param:
        date_query['combination'] = 'and'

    if date_query:
        optional_params['metadata_date'] = date_query    

    #check that we support the repo
    supported_repos = get_all_repositories(request)

    if repo not in [s for s in supported_repos]:
        return HTTPNotFound('no repo: %s (%s)' % (repo, ','.join(supported_repos)))

    repository = get_repository(repo)
    if not repository:
        return HTTPNotFound('failed repo')

    #and check that the standard id supported for the repo
    supported_standards = repository.get_standards(request)

    if standard not in supported_standards:
        return HTTPNotFound('no standard')

    doctypes = ','.join([dt[:-1] for dt in doctypes.split(',')])

    #do a search for DOCTYPES where app == app AND repo == repo AND standard = standard, RETURN metadata links
    searcher = RepositorySearcher(
        {
            "host": request.registry.settings['es_root'], 
            "index": request.registry.settings['es_dataset_index'], 
            "type": doctypes, 
            "user": request.registry.settings['es_user'].split(':')[0], 
            "password": request.registry.settings['es_user'].split(':')[-1]
        }
    )

    searcher.default_limit = limit
    searcher.default_offset = offset

    try:
        searcher.build_basic_search(app, repo, standard, optional_params)
    except Exception as ex:
        return HTTPServerError(ex.message)

    if 'check' in params:
        #for testing - get the elasticsearch json request
        return Response(json.dumps({"search": searcher.get_query(), "url": searcher.es_url}), content_type = 'application/json')

    try:
        searcher.search()
    except Exception as ex:
        return HTTPServerError(ex.message)

    total = searcher.get_result_total()

    if total < 1:
        return Response('{"total": 0}')
    
    search_objects = searcher.get_result_ids()

    subtotal = len(search_objects)

    if subtotal < 1:
        return Response('{"total": 0}')

    
    limit = subtotal if subtotal < limit else limit

    base_url = request.registry.settings['BALANCER_URL']

    def yield_objects(object_tuples, total):
        yield '{"total": %s, "results": [' % total

        cnt = 0
        for object_tuple in object_tuples:
            to_yield = ''

            if object_tuple[1] == 'collection':
                output = {}
            elif object_tuple[1] == 'dataset':
                o = get_dataset(object_tuple[0])

                #add a thing for uuid, metadata link for the standard if it's supported
                #TODO: fix the 19119 for wxs
                if standard in o.get_standards(request):
                    output = {
                        "dataset": str(o.uuid), 
                        "url": base_url + build_metadata_url(app, "datasets", o.uuid, standard, "xml"), 
                        "date_modified": o.gstore_metadata[0].date_modified.strftime(_DATE_FORMAT),
                        "date_added": o.date_published.strftime(_DATE_FORMAT) if o.date_published else o.dateadded.strftime(_DATE_FORMAT)
                    }
                else:
                    output = {}

            to_yield = json.dumps(output) if cnt == limit - 1 else json.dumps(output) + ','          
                        
            cnt += 1
            yield to_yield

        yield ']}'

    response = Response()
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.content_type = 'application/json'
    response.app_iter = yield_objects(search_objects, total)
    return response








    
