from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )

'''
some methods for database bits

'''    

def get_dataset(dataset_id):
    #should probably do some horrible regex thing to see if it's a uuid
    #since it's passing a string
    try:
        dataset_id = int(dataset_id)
    except:
        pass

    #TODO: change this to the sql strings for multiple and different filters
    #get the dataset by the filter 
    if isinstance(dataset_id, (int, long)): 
        dataset_id = int(dataset_id)
        d = DBSession.query(Dataset).filter(Dataset.id==dataset_id).first()
    else:
        #it's the uuid        
        d = DBSession.query(Dataset).filter(Dataset.uuid==dataset_id).first()   

    return d
