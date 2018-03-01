from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import Dataset
from ..models.tileindexes import *
from ..models.collections import Collection
from ..models.repositories import Repository
from ..models.apps import GstoreApp
from ..models.provenance import ProvOntology
from ..lib.utils import *
from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPServerError, HTTPBadRequest, HTTPServiceUnavailable

'''
some methods for database bits

'''

#get the dataset by id or uuid
def get_dataset(dataset_id):
    try:
        dataset_id = int(dataset_id)
        clause = Dataset.id==dataset_id
    except:
        if validate_uuid4(dataset_id):
            clause = Dataset.uuid==dataset_id
        else:
            return HTTPBadRequest("Unknown ID or UUID.")

    d = DBSession.query(Dataset).filter(clause).first()
    return d

#get the tile index by id or uuid
def get_tileindex(tile_id):
    try:
        tile_id = int(tile_id)
        clause = TileIndex.id==tile_id
    except:
        clause = TileIndex.uuid==tile_id

    tile = DBSession.query(TileIndex).filter(clause).first()
    return tile

def get_collection(collection_id):
    try:
        collection_id = int(collection_id)
        clause = Collection.id==collection_id
    except:
        clause = Collection.uuid==collection_id

    collection = DBSession.query(Collection).filter(clause).first()
    return collection
#and the repo
def get_repository(repo_name):
    clause = Repository.name.ilike(repo_name)
    repo = DBSession.query(Repository).filter(clause).first()
    return repo   

def get_app(app_key):    
    clause = GstoreApp.route_key==app_key
    app = DBSession.query(GstoreApp).filter(clause).first()
    return app
        
