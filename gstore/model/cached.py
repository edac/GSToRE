from pylons import cache
from gstore.model import meta, params_from_query, FromCallable
from gstore.model.shapes import *
from gstore.model.rasters import *

@cache.region('short_term', 'datasetsbyid')
def load_dataset(dataset_id):
    """Load dataset by id, caching the result in Beaker."""
    dataset = meta.Session.query(Dataset).get(dataset_id)
    if dataset.taxonomy == 'vector':
        assert(len(dataset.attributes_ref) > 0)
    meta.Session.close()
    return dataset

class CachedDataset(FromCallable):
    """ A MapperOption that will pull user datasets from Beaker.
        
        We build a subclass of FromCallable with a __call__ method
        so that the option itself is pickleable.
        Use it like:
        a1, a2, a3 = meta.Session.query(Dataset_Attributes).options(CachedDataset(Dataset.attributes_ref)).all()
    """
    def __call__(self, q):  
        return [load_dataset(*params_from_query(q))]
