"""The application's model objects"""
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.orm.query import Query, _generative
from sqlalchemy.orm.interfaces import MapperOption
from sqlalchemy.sql import visitors

from gstore.model import meta

from datasets import *
from shapes import * 
from files import *

def init_model(engine):
    """Call me before using any of the tables or classes in the model"""
    meta.Session.configure(bind = engine)

orm.mapper(Bundle, bundles_table)
orm.mapper(GeoLookup, geolookups_table)
orm.mapper(SpatialReference, spatial_ref_sys)
orm.mapper(MapfileTemplate, mapfile_templates_table)
orm.mapper(ShapesAttribute, shapes_attributes)

orm.mapper(Dataset, datasets_table, properties={
    'formats': datasets_table.c.formats_cache,
    'mapfile_template': orm.relation(
        MapfileTemplate,
        backref = 'datasets'
    ), 
    'attributes_ref': orm.relation(
        ShapesAttribute, 
        backref = 'dataset',
        lazy = True
    ),
    'sources_ref': orm.relation(
        Resource,
        secondary = datasets_sources_table, 
        lazy = False, 
        uselist = True,
        backref = 'datasets'
    )
})

orm.mapper(
    ShapesVector, 
    shapes_table, 
    properties = {
        'dataset': 
            orm.relation(
                Dataset,
                primaryjoin = shapes_table.c.dataset_id == datasets_table.c.id,
                foreign_keys = [shapes_table.c.dataset_id],
                backref = 'shapes',
                lazy = True
            )
        ,
        'properties': 
            orm.relation(
                ShapesAttribute, 
                primaryjoin = shapes_table.c.dataset_id == shapes_attributes.c.dataset_id,
                foreign_keys = [shapes_table.c.dataset_id],
                uselist = True,
                lazy = True
            )
    }
)


orm.mapper(
    Resource,
    sources_table,
    properties = {
        'location': orm.synonym('_url', map_column = True), 
    } 
)
