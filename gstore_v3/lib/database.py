from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import Dataset

from ..models.tileindexes import *

'''
some methods for database bits

'''    

#get the dataset by id or uuid
def get_dataset(dataset_id):
    #should probably do some horrible regex thing to see if it's a uuid
    #since it's passing a string
    try:
        dataset_id = int(dataset_id)
        clause = Dataset.id==dataset_id
    except:
        clause = Dataset.uuid==dataset_id
   
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
    
