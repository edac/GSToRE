"""The application's model objects"""
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.orm.query import Query, _generative
from sqlalchemy.orm.interfaces import MapperOption
from sqlalchemy.sql import visitors


from gstore.model import meta

# Adapted from http://www.sqlalchemy.org/trac/browser/sqlalchemy/branches/rel_0_5/examples/query_caching/per_relation.py?rev=6623
class CachingQuery(Query):
    """override __iter__ to pull results from a callable 
       that might have been attached to the Query.
        
    """

    @_generative()
    def with_cache_key(self, cachekey):
        self.cachekey = cachekey

    def __iter__(self):
        if hasattr(self, 'cachekey'):
            try:
                ret = _cache[self.cachekey]
            except KeyError:
                ret = list(Query.__iter__(self))
                for x in ret:
                    self.session.expunge(x)
                _cache[self.cachekey] = ret
  

class FromCallable(MapperOption):
    """A MapperOption that associates a callable with particular 'path' load.
    
    When a lazyload occurs, the Query has a "path" which is a tuple of
    (mapper, key, mapper, key) indicating the path along relations from
    the original mapper to the endpoint mapper.
    
    """
    
    propagate_to_loaders = True
    
    def __init__(self, key):
        self.cls_ = key.property.parent.class_
        self.propname = key.property.key
    
    def __call__(self, q):
        raise NotImplementedError()
        
    def process_query(self, query):
        if query._current_path:
            mapper, key = query._current_path[-2:]
            if mapper.class_ is self.cls_ and key == self.propname:
                query.cache_callable = self

def params_from_query(query):
    """Pull the bind parameter values from a query.
    
    This takes into account any scalar attribute bindparam set up.
    
    E.g. params_from_query(query.filter(Cls.foo==5).filter(Cls.bar==7)))
    would return [5, 7].
    """
    
    v = []
    def visit_bindparam(bind):
        value = query._params.get(bind.key, bind.value)
        v.append(value)
    visitors.traverse(query._criterion, {}, {'bindparam':visit_bindparam})
    return v
# end of query_caching sa example code



def init_model(engine):
    """Call me before using any of the tables or classes in the model"""
    sm = orm.sessionmaker(bind=engine, autoflush = True, autocommit = False)
    meta.engine = engine
    meta.Session = orm.scoped_session(sm)
