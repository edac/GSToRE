# Adapted from
# - http://sgillies.net/blog/531/the-shapely-alchemist/
# - http://www.sqlalchemy.org/trac/wiki/05Migration#SchemaTypes
# - http://svn.sqlalchemy.org/sqlalchemy/trunk/examples/postgis/postgis.py

from sqlalchemy.orm.interfaces import AttributeExtension

from sqlalchemy import types, literal
from sqlalchemy.sql import func, expression

from osgeo import ogr as ogr
from osgeo import osr as osr

import re


def _to_postgis(value):
    """Interpret a value as a GIS-compatible construct."""
    
    if hasattr(value, '__clause_element__'):
        return value.__clause_element__()
    elif isinstance(value, (expression.ClauseElement, GisElement)):
        return value
    elif isinstance(value, ogr.Geometry):
        return value.ExportToWkb()
    elif value is None:
        return None
    else:
        raise Exception("Invalid type")

class PostgisGeomSetter(AttributeExtension):
    def set(self, state, value, oldvalue, initiator):
        return _to_postgis(value)
    
class Geometry(types.TypeDecorator):
    impl = types.String

    def __init__(self, srid, dims=2):
        super(Geometry, self).__init__()
        if not srid:
            srid = 4326
        self.set_spatial_reference(srid)
        self.dims = dims
   
    def get_spatial_reference(self):
        return self.spatial_reference

    def set_spatial_reference(self, srid):
        self.srid = srid
        s = osr.SpatialReference()
        s.ImportFromEPSG(int(srid))
        self.spatial_reference = s
         
    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        else:
            return func.setsrid(func.geomfromewkb(func.encode(value, 'hex'), self.srid))
        
    def process_result_value(self, value, dialect):
        if value is None:
            return None
        else:
            return ogr.CreateGeometryFromWkb(str(value), self.get_spatial_reference())
    
    def copy(self):
        return Geometry(self.impl.length)
    


