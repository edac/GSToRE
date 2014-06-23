from gstore_v3.models import Base, DBSession
from sqlalchemy import MetaData, Table, ForeignKey, Column, String, Integer, Boolean, FetchedValue, TIMESTAMP, Numeric
from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from zope.sqlalchemy import ZopeTransactionExtension

from sqlalchemy import func

from sqlalchemy.dialects.postgresql import UUID, ARRAY

from ..models.metadata import MetadataStandards

from ..lib.spatial import *
from ..lib.utils import *

'''
gstoredata.collections and join table
'''
collections_datasets = Table('collections_datasets', Base.metadata,
    Column('collection_id', Integer, ForeignKey('gstoredata.collections.id')),
    Column('dataset_id', Integer, ForeignKey('gstoredata.datasets.id')),
    schema='gstoredata'
)

class Collection(Base):
    """

    Note:

    Attributes:
        
    """
    __table__ = Table('collections', Base.metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String(50)),
        Column('description', String(200)),
        Column('apps', ARRAY(String)),
        Column('valid_start', TIMESTAMP),
        Column('valid_end', TIMESTAMP),
        Column('bbox', ARRAY(Numeric)),
        Column('bbox_geom', String),
        Column('geom', String),  #the footprint geometry
        Column('uuid', UUID, FetchedValue()),
        Column('date_added', TIMESTAMP, FetchedValue()),
        Column('is_available', Boolean),
        Column('is_embargoed', Boolean),
        Column('is_active', Boolean),
        Column('has_tileindex', Boolean),
        Column('tileindex_taxonomies', ARRAY(String)), #this is the subset of dataset_taxonomies that should be incorporated into the tile index
        Column('is_spatial', Boolean),
        Column('dataset_taxonomies', ARRAY(String)), #just cache the dataset taxonomies instead of doing a DISTINCT while compiling search results
        Column('excluded_standards', ARRAY(String)), #for the collection-specific metadata standard support. nothing to do with what the datasets support
        schema='gstoredata'
    )

    #relate with datasets (see Dataset)

    #relate with categories with the backref
    categories = relationship('Category',
                    secondary='gstoredata.categories_collections',
                    backref='collections')

    #to the gstore (and main) metadata
    gstore_metadata = relationship('CollectionMetadata', backref='collections')

    def __init__(self, name, apps):  
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """

        
        self.name = name
        self.apps = apps

    def __repr__(self):
        return '<Collection (%s, %s, %s)>' % (self.id, self.name, self.uuid)


    def update_geometries(self):
        """regenerate the bbox, bbox geom and footprint based on the datasets

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        try:
            DBSession.execute(func.gstoredata.generate_collection_geometries(self.id))
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(self)
        except Exception as err:
            DBSession.rollback()
            raise err


    def update_date_range(self):
        """
        update the valid date range based on the min/max dataset valid start/ends

        this doesn't need to be a postgres function really, but it is

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        try:
            DBSession.execute(func.gstoredata.generate_collection_daterange(self.id))
            DBSession.commit()
            DBSession.flush()
            DBSession.refresh(self)
        except Exception as err:
            DBSession.rollback()
            raise err

    def get_standards(self, req=None):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
    
        if not self.is_available:
            return []

        if not req:
            lst = get_current_registry().settings['DEFAULT_STANDARDS']
        else:
            lst = req.registry.settings['DEFAULT_STANDARDS']

        if not lst:
            return None
        lst = lst.split(',')

        exc_lst = self.excluded_standards
        return [i for i in lst if i not in exc_lst]

    def get_onlinks(self, base_url, req, app):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        '''
        get the links to the collection metadata, and the tile index service?, and the plain service (replace with html)
        '''

        #TODO: expand this to add the service links and whatever else
        
        return [base_url + build_service_url(app, 'collections', self.uuid)]

    def get_dataset_links(self, base_url, req, app):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        '''
        get the info links for each dataset in the collection
        '''
        #TODO: revise this for the not hardcoding ISO
        #TODO: and for datasets that don't have ISO (add some baby MI?)
        standard = 'ISO-19115:2003'
        extension = 'xml'
        return [base_url + build_metadata_url(app, 'datasets', d.uuid, standard, extension) for d in self.datasets if d.inactive == False and d.is_embargoed == False]

    def get_full_service_dict(self, base_url, req, app):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        '''
        generate the service json blob
        '''
        results = {
            "type": "collection",
            "uuid": str(self.uuid),
            "name": self.name,
            "description": self.description,
            "date_added": self.date_added.strftime('%Y%m%d'),
            "categories": [{"theme": t.theme, "subtheme": t.subtheme, "groupname": t.groupname} for t in self.categories],
            "valid_dates": {
                "start": self.valid_start.strftime('%Y%m%d') if self.valid_start else "",
                "end": self.valid_end.strftime('%Y%m%d') if self.valid_end else ""
            }
        }

        if self.bbox:
            results.update({"spatial": {"bbox": string_to_bbox(self.bbox)}})

        #add the info about the taxonomies found in the collection (tables, vectors, rasters, etc)
        results.update({"taxonomies": self.dataset_taxonomies})

        #add the metadata representation FOR THE COLLECTION 
        supported_standards = self.get_standards(req)

        mt = []
        for supported_standard in supported_standards:
            std = DBSession.query(MetadataStandards).filter(MetadataStandards.alias==supported_standard).first()
            if not std:
                continue

            mt += std.get_urls(app, 'collections', base_url, self.uuid, [])

        results.update({"metadata": mt})

        #add the link for the preview wms service (footprint for now?)


        #add the link for the tile index service if there is one


        return results
    
        
    def get_basic_service_dict(self, base_url, req):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        '''
        generate the tiniest blob o' collection data

        uuid, name, part of abstract?, pointer to metadata, pointer to full service dict, point to ?
        '''

        return {}        



    def get_footprint(self, epsg, format):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        '''
        return the footprint of the collection as format and projected (or unprojected) as epsg
        '''
        output_geometry = wkb_to_output(self.geom, epsg, format)
        return output_geometry

    def generate_tileindex(self):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        '''
        build the shapefile index for the spatial-only datasets
        '''
        pass

    def write_metadata(self, output_location, out_standard, out_format, metadata_info={}):
        """

        Notes:
            
        Args:
            
        Returns:
        
        Raises:
        """
        
        '''
        build the iso ds record? for a file?
        '''
        pass
        

#and the collection-to-category join
collections_categories = Table('categories_collections', Base.metadata,
    Column('collection_id', Integer, ForeignKey('gstoredata.collections.id')),
    Column('category_id', Integer, ForeignKey('gstoredata.categories.id')),
    schema='gstoredata'
)


