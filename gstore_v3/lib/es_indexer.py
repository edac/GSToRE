import requests
from ..lib.spatial import *

from gstore_v3.models import Base, DBSession
from sqlalchemy import func

import json

from requests.auth import HTTPBasicAuth


#TODO: POST MAPPING TO PRODUCTION
'''
info about the gstore doctypes

dataset mapping:
{
    "dataset": {
        "properties": {
            "active": {
                "type": "boolean"
            },
            "embargo": {
                "type": "boolean"
            },
            "available": {
                "type": "boolean"
            },
            "abstract": {
                "type": "string"
            },
            "aliases": {
                "type": "string",
                "index_name": "alias"
            },
            "applications": {
                "type": "string",
                "index_name": "application"
            },
            "area": {
                "type": "double"
            },
            "category_facets": {
                "type": "nested",
                "properties": {
                    "groupname": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "subtheme": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "theme": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "apps": {
                        "type": "string"
                    }
                }
            },
            "category": {
                "properties": {
                    "groupname": {
                        "type": "string"
                    },
                    "subtheme": {
                        "type": "string"
                    },
                    "theme": {
                        "type": "string"
                    },
                    "apps": {
                        "type": "string"
                    }
                }
            },
            "date_added": {
                "type": "date",
                "format": "YYYY-MM-dd"
            },
            "formats": {
                "type": "string"
            },
            "isotopic": {
                "type": "string"
            },
            "location": {
                "properties": {
                    "bbox": {
                        "type": "geo_shape",
                        "tree": "quadtree",
                        "tree_levels": 40
                    }
                }
            },
            "services": {
                "type": "string"
            },
            "standards": {
                "type": "string",
                "index": "not_analyzed"
            },
            "taxonomy": {
                "type": "string"
            },
            "geomtype": {
                "type": "string"
            },
            "title": {
                "type": "string"
            },
            "title_search": {
                "type": "string",
                "index": "not_analyzed"
            },
            "collections": {
                "type": "string"
            },
            "keywords": {
                "type": "string"
            },
            "valid_start": {
                "properties": {
                    "date": {
                        "type": "date",
                        "format": "YYYY-MM-dd"
                    },
                    "year": {"type": "integer"},
                    "month": {"type": "integer"},
                    "day": {"type": "integer"}
                }
            },
            "valid_end": {
                "properties": {
                    "date": {
                        "type": "date",
                        "format": "YYYY-MM-dd"
                    },
                    "year": {"type": "integer"},
                    "month": {"type": "integer"},
                    "day": {"type": "integer"}
                }
            },
            "date_published": {
                "properties": {
                    "date": {
                        "type": "date",
                        "format": "YYYY-MM-dd"
                    },
                    "year": {"type": "integer"},
                    "month": {"type": "integer"},
                    "day": {"type": "integer"}
                }
            },
            "supported_repositories": {
                "type": "nested",
                "properties": {
                    "app": {"type": "string"},
                    "repos": {"type": "string", "index": "not_analyzed"}
                }
            },
            "gstore_metadata": {
                "properties": {
                    "date": {
                        "type": "date",
                        "format": "YYYY-MM-dd"
                    },
                    "year": {"type": "integer"},
                    "month": {"type": "integer"},
                    "day": {"type": "integer"}
                }
            }
        }
    }
}

for the categories, the not_analyzed is necesary to get the right facet structure (the entire phrase, not each word in the phrase).
for location searches, the shape needs to be a polygon in the document and a polygon in the search request.
aliases, services, formats, applications are all arrays.
area is the area of the dataset bbox without a projection (or decent units).

category is a singleton, category_facets is a list of category obj.
at some point they will diverge more - the category structure will change between rgis/epscor so the apps list for each category obj could be different.

'''


class EsIndexer:
    '''
    take:
        a gstore object
        a request (to get the formats/services/etc)
        a list of things to intersect for the facet lists

    build the json request for an es doc

    post the doc

    update the doc (mostly dataset status updates)

    #for testing:
    >>> from gstore_v3.models import *
    >>> from gstore_v3.lib import es_indexer
    >>> d = DBSession.query(datasets.Dataset).filter(datasets.Dataset.uuid=='14900f2f-0da4-4811-b612-310cc6ab1ac0').first()
    >>> ed = {"host": "http://es.sandbox.edac.bldg16:9200/", "index": "gstore", "type": "dataset"}
    >>> es = es_indexer.EsIndexer(ed, d, request)
    >>> es.es_url
    'http://129.24.63.18:9200/gstore_a/dataset'
    >>> fti = ["counties"]
    >>> es.build_document(fti)
    '''

    def __init__(self, es_description, gstore_object, req):
        self.es_description = es_description

        self.es_url = es_description['host'] + es_description['index'] + '/' + es_description['type']
        self.user = es_description['user']
        self.password = es_description['password']

        self.gstore_object = gstore_object
        self.request = req

        self.document = ''
        self.partial = {}
        self.uuid = gstore_object.uuid

    def __repr__(self):
        return '<EsIndexer (%s, %s)>' % (self.es_url, self.uuid)

    def put_document(self):
        '''
        take the generated document
        set up the requests PUT
        and put
        '''

        if not self.document or not self.uuid:
            return

        #it's like rocket science over here
        #but need to append the dataset uuid (as the es _id) to the route
        es_url = self.es_url + '/' + self.uuid
        result = requests.put(es_url, data=json.dumps(self.document), auth=(self.user, self.password))

        if result.status_code != 201:
            raise Exception('Failed to create document')

    def update_document(self, elements_to_update):
        '''
        just post some changes to the document?

        esp. embargo | active | available

        POST:

        http://129.24.63.18:9200/{index}/{type}/{id}/_update

        {
	        "script": "ctx._source.dataset.status = 'inactive'; next thing", 
	        "params": {
                "param1": value
	        }
        }

        indexed:
            embargo: true/false
            active: true/false
            available: true/false
        '''
        updates = []
        params = {}

        cnt = 1
        for k,v in elements_to_update.iteritems():
            #ctx is es required apparently; fyi: if you don't include the doc type, it will post to the root.
            #update = 'ctx._source.' + self.es_type + '.' + k + ' = ' + str(v).lower()
            update = 'ctx._source.' + self.es_description['type'] + '.' + k + ' = param_%s' % cnt
            updates.append(update)
            params.update({"param_%s" % cnt: v})
            cnt += 1

        es_url = self.es_url + '/' + self.uuid + '/_update'
        #concat multiple script bits with ';'

        #return es_url, json.dumps({"script": ';'.join(updates), "params": params})
        
        result = requests.post(es_url, data=json.dumps({"script": ';'.join(updates), "params": params}), auth=(self.user, self.password))

        if result.status_code != 200:
            raise Exception('Failed to update document')

        '''
        remove an element:
        {
            "script": "ctx._source.dataset.remove(\"repositories\")"
        }
        '''

    def update_partial(self):
        '''
        to update a partial document (instead of using the script option)
        better for complex elements
        
        POST:

        http://129.24.63.18:9200/{index}/{type}/{id}/_update

        {
	        "doc": {
                "DOCTYPE": {
                    key: value,
                    key: value
                }
	        }
        }

        the key/value pairs need to be in the same structure as they would be in the INSERT doc json
        and should come from the child object's build_partial method

        this just wraps them in the doc>DOCTYPE dict for posting
        
        '''   
        if not self.partial:
            raise Exception('No update document')

        data = {"doc": {self.es_description['type']: self.partial}}

        es_url = self.es_url + '/' + self.uuid + '/_update'
        
        result = requests.post(es_url, data=json.dumps(data), auth=(self.user, self.password))

        if result.status_code != 200:
            raise Exception(result.content)
            

    def build_location(self, box):
        '''
        generate the location element for the index (bbox builder, area, etc)

        this is the same for any doctype with a spatial component

        really should have put the area in loc, but whatever
        '''

        loc = {}

        bbox = string_to_bbox(box)

        #TODO: deal with the srid
        geom = bbox_to_geom(bbox, 4326)

        area = geom.GetArea()

        if area == 0.:
            #just represent the points as actual points
            loc = {"bbox": {"type": "Point", "coordinates": [bbox[0], bbox[1]]}}
        else:
            #otherwise it's a polygon (not thrilled with how es/lucene handles envelopes - it doesn't seem to do it very well)
            #note also that it's sort of a geojson structure
            loc = {"bbox": {"type": "Polygon", "coordinates": [[[bbox[0], bbox[1]], [bbox[2], bbox[1]], [bbox[2], bbox[3]], [bbox[0], bbox[3]], [bbox[0], bbox[1]]]]}}
        
        return area, loc

    def build_date_element(self, key, datetime_obj):
        return {key: {"date": datetime_obj.strftime('%Y-%m-%d'), "year": datetime_obj.year, "month": datetime_obj.month, "day": datetime_obj.day}}

class DatasetIndexer(EsIndexer):
    '''
    document builder for the dataset    
    '''
    def build_document(self, facets_to_include):
        '''
        take the dataset

        get the title
        get the bbox
        get the dates
        get the apps
        get the collections
        get the projects
        get the categories
        get the services
        get the formats
        get the taxonomy ( + geomtype)
        get the aliases

        get the statuses 
            embargo: t/f 
            active: t/f
            available: t/f

        get the metadata
            get the abstract
            get the keywords?
            get isotopic

        get the facets to include
            for each facet
                intersect the geometries in geolookups (+what)
                append list (if no list, punt to us?)

        things to add for full faceted search:
            projects
            attributes
            parameters
            
        '''
        doc = {}

        doc.update({"title": self.gstore_object.description, "title_search": self.gstore_object.description,
                "date_added": self.gstore_object.dateadded.strftime('%Y-%m-%d')
            })

        doc.update(self.build_date_element("date_published", self.gstore_object.date_published))     

        if self.gstore_object.begin_datetime:
            doc.update(self.build_date_element("valid_start", self.gstore_object.begin_datetime))
        if self.gstore_object.end_datetime:
            doc.update(self.build_date_element("valid_end", self.gstore_object.end_datetime))

        formats = self.gstore_object.get_formats(self.request)
        services = self.gstore_object.get_services(self.request)
        standards = self.gstore_object.get_standards(self.request)
        repos = self.gstore_object.get_repositories()
        #TODO: metadata standards?

        doc.update({
                "applications": self.gstore_object.apps_cache, 
                "formats": formats, 
                "services": services,
                "standards": standards
            })

        #repack the repos
        doc.update({"supported_repositories": [{"app": k, "repos": v} for k,v in repos.iteritems()]})
        
        isotopic = ''
        abstract = ''
        terms = []
        if self.gstore_object.gstore_metadata:
            isotopic = self.gstore_object.gstore_metadata[0].get_isotopic()
            abstract = self.gstore_object.gstore_metadata[0].get_abstract()
            terms = self.gstore_object.gstore_metadata[0].get_keywords()
            
            #only add the metadata timestamp if it's gstore? too many of the repos rely on iso so this is the current safest bet
            metadata_date = self.gstore_object.gstore_metadata[0].date_modified
            if metadata_date:
                doc.update(self.build_date_element("gstore_metadata", metadata_date))
            
        doc.update({"isotopic": isotopic, "abstract": abstract, "keywords": terms})

        doc.update({"aliases": self.gstore_object.aliases if self.gstore_object.aliases else []})

        doc.update({"embargo": self.gstore_object.is_embargoed, "active": not self.gstore_object.inactive, "available": self.gstore_object.is_available})

        doc.update({"taxonomy": self.gstore_object.taxonomy})
        if self.gstore_object.geomtype and self.gstore_object.taxonomy == 'vector':
            doc.update({"geomtype": self.gstore_object.geomtype.lower()})

        #nested category structure
        categories = []
        for category in self.gstore_object.categories:
            categories.append({"theme": str(category.theme), "subtheme": str(category.subtheme), "groupname": str(category.groupname), "apps": self.gstore_object.apps_cache})
        doc.update({"category_facets": categories})

        #and the original structure just in case
        cat = self.gstore_object.categories[0]
        doc.update({"category": {"theme": str(cat.theme), "subtheme": str(cat.subtheme), "groupname": str(cat.groupname), "apps": self.gstore_object.apps_cache}})

        if self.gstore_object.taxonomy not in ['table']:
            area, loc = self.build_location(self.gstore_object.box)
            doc.update({"location": loc})
            doc.update({"area": area})

        #TODO: ADD THE COLLECTION UUIDs SO THAT WE CAN SEARCH WITHIN COLLECTION
        if self.gstore_object.collections:
            doc.update({"collections": [str(c.uuid) for c in self.gstore_object.collections]})

        self.document = {self.es_description['type']: doc}

    def build_partial(self, keys_to_update):
        '''
        build a dict for data elements to update/add for a partial doc update (see esindexer.update_partial)

        from gstore_v3.models import *
        from gstore_v3.lib import es_indexer
        g = DBSession.query(datasets.Dataset).filter(datasets.Dataset.uuid=='0005c365-a09a-475e-a501-8a001882381f').first()
        ed = {"host": "http://es.sandbox.edac.bldg16:9200/", "index": "gstore", "type": "dataset", "user": "", "password": ""}
        es = es_indexer.DatasetIndexer(ed, g, request)
        keys=["keywords", "valid_start", "valid_end", "date_published", "services", "supported_repositories"]
        new_doc = es.build_partial(keys)
        '''

        data_to_update = {}

        for key in keys_to_update:
            if key == 'title':
                #the description as tokens for the keyword search and as complete string for sorting
                data_to_update.update({"title": self.gstore_object.description})
                data_to_update.update({"title_search": self.gstore_object.description})
                
            elif key == 'date_added':
                data_to_update.update({"date_added": self.gstore_object.dateadded.strftime('%Y-%m-%d')})
                
            elif key == 'date_published':
                data_to_update.update(self.build_date_element("date_published", self.gstore_object.date_published))
               
            elif key == 'valid_start':
                if self.gstore_object.begin_datetime:
                    data_to_update.update(self.build_date_element("valid_start", self.gstore_object.begin_datetime))
                    
            elif key == 'valid_end':
                if self.gstore_object.end_datetime:
                     data_to_update.update(self.build_date_element("valid_end", self.gstore_object.end_datetime))

            elif key == 'metadata_date':   
                if self.gstore_object.gstore_metadata:
                    metadata_date = self.gstore_object.gstore_metadata[0].date_modified
                    if metadata_date:
                        data_to_update.update(self.build_date_element("gstore_metadata", metadata_date))  
            elif key == 'formats':
                formats = self.gstore_object.get_formats(self.request)
                data_to_update.update({"formats": formats})
                
            elif key == 'services':
                services = self.gstore_object.get_services(self.request)
                data_to_update.update({"services": services})
                
            elif key == 'standards':
                standards = self.gstore_object.get_standards(self.request)
                data_to_update.update({"standards": standards})
                
            elif key == 'supported_repositories':
                repos = self.gstore_object.get_repositories()
                data_to_update.update({"supported_repositories": [{"app": k, "repos": v} for k,v in repos.iteritems()]})
                
            elif key == 'applications':
                data_to_update.update({"applications": self.gstore_object.apps_cache})
                
            elif key == 'isotopic':
                isotopic = self.gstore_object.gstore_metadata[0].get_isotopic() if self.gstore_object.gstore_metadata else ''
                data_to_update.update({"isotopic": isotopic})
                
            elif key == 'abstract':
                abstract = self.gstore_object.gstore_metadata[0].get_abstract() if self.gstore_object.gstore_metadata else ''
                data_to_update.update({"abstract": abstract})
                
            elif key == 'keywords':
                keywords = self.gstore_object.gstore_metadata[0].get_keywords() if self.gstore_object.gstore_metadata else ''
                data_to_update.update({"keywords": keywords})
                
            elif key == 'aliases':
                data_to_update.update({"aliases": self.gstore_object.aliases if self.gstore_object.aliases else []})
                
            elif key == 'embargo':
                data_to_update.update({"embargo": self.gstore_object.is_embargoed})
                
            elif key == 'active':
                data_to_update.update({"active": not self.gstore_object.inactive})
                
            elif key == 'available':
                data_to_update.update({"available": self.gstore_object.is_available})
                
            elif key == 'taxonomy':
                data_to_update.update({"taxonomy": self.gstore_object.taxonomy})
                if self.gstore_object.geomtype and self.gstore_object.taxonomy == 'vector':
                    data_to_update.update({"geomtype": self.gstore_object.geomtype.lower()})
                    
            elif key == 'categories':
                #nested category structure
                categories = []
                for category in self.gstore_object.categories:
                    categories.append({"theme": str(category.theme), "subtheme": str(category.subtheme), "groupname": str(category.groupname), "apps": self.gstore_object.apps_cache})
                data_to_update.update({"category_facets": categories})

                #and the original structure just in case
                cat = self.gstore_object.categories[0]
                data_to_update.update({"category": {"theme": str(cat.theme), "subtheme": str(cat.subtheme), "groupname": str(cat.groupname), "apps": self.gstore_object.apps_cache}})
                
            elif key == 'category_hierarchy':
                #for the 1..3 structure not in place
                pass
            elif key == 'location':
                if self.gstore_object.taxonomy not in ['table']:
                    area, loc = self.build_location(self.gstore_object.box)
                    data_to_update.update({"location": loc})
                    data_to_update.update({"area": area})
                
            elif key == 'collections':
                data_to_update.update({"collections": [str(c.uuid) for c in self.gstore_object.collections]})   
                         
            else:
                pass

        self.partial = data_to_update

class CollectionIndexer(EsIndexer):
    '''
    and one for the collection
    '''
    def build_document(self):

        doc = {}

        doc.update({"title": self.gstore_object.name, "date_added": self.gstore_object.date_added.strftime('%Y-%m-%d')})
        doc.update({"applications": self.gstore_object.apps})

        #TODO: deal with the services (and add to mapping)

        doc.update({"embargo": self.gstore_object.is_embargoed, "active": not self.gstore_object.is_active, "available": self.gstore_object.is_available})

        #TODO: this is a list in collections BUT not in datasets so check on the cross-doctype searching against it
        doc.update({"taxonomy": self.gstore_object.taxonomy})

        isotopic = ''
        abstract = ''
        if self.gstore_object.gstore_metadata:
            isotopic = self.gstore_object.gstore_metadata[0].get_isotopic()
            abstract = self.gstore_object.gstore_metadata[0].get_abstract()
        doc.update({"isotopic": isotopic, "abstract": abstract})

        #nested category structure
        categories = []
        for category in self.gstore_object.categories:
            categories.append({"theme": str(category.theme), "subtheme": str(category.subtheme), "groupname": str(category.groupname), "apps": self.gstore_object.apps})
        doc.update({"category_facets": categories})

        #and the original structure just in case
        cat = self.gstore_object.categories[0]
        doc.update({"category": {"theme": str(cat.theme), "subtheme": str(cat.subtheme), "groupname": str(cat.groupname), "apps": self.gstore_object.apps}})

        if self.gstore_object.is_spatial:
            #add a box because it is special
            area, loc = self.build_location(self.gstore_object.bbox)
            doc.update({"location": loc})
            doc.update({"area": area})
    
        self.document = {self.es_description['type']: doc}










'''
update doc in place with something more than ==

{
    "doc": {
        "dataset": {
            "valid_start": {
                "date": "2013-11-12",
                "year": 2013,
                "month": 11,
                "day": 12
            },
            "valid_end": {
                "date": "2013-12-12",
                "year": 2013,
                "month": 12,
                "day": 12
            },
            "keywords": ["tree", "playa", "usgs"],
            "date_published": {
                "date": "2013-03-02",
                "year": 2013,
                "month": 3,
                "day": 2
            },
            "supported_repositories": [{"app": "rgis", "repos": ["GEOSS", "DATA.gov"]}]
        }
    }
}    
'''











        
