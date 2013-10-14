import requests
from ..lib.spatial import *

from gstore_v3.models import Base, DBSession
from sqlalchemy import func

import json

from requests.auth import HTTPBasicAuth

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
                    },
                    "counties": {
                        "type": "string",
                        "index_name": "county"
                    },
                    "quads": {
                        "type": "string",
                        "index_name": "quad"
                    }
                }
            },
            "services": {
                "type": "string"
            },
            "taxonomy": {
                "type": "string"
            },
            "geomtype": {
                "type": "string"
            },
            "title": {
                "type": "string"
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
    >>> ed = {"host": "http://129.24.63.18:9200/", "index": "gstore_a", "type": "dataset"}
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
            collections
            projects
            attributes
            parameters
            
        '''
        doc = {}

        doc.update({"title": self.gstore_object.description, "date_added": self.gstore_object.dateadded.strftime('%Y-%m-%d')})

        formats = self.gstore_object.get_formats(self.request)
        services = self.gstore_object.get_services(self.request)
        #TODO: metadata standards?

        doc.update({"applications": self.gstore_object.apps_cache, "formats": formats, "services": services})
        
        isotopic = ''
        abstract = ''
        if self.gstore_object.gstore_metadata:
            isotopic = self.gstore_object.gstore_metadata[0].get_isotopic()
            abstract = self.gstore_object.gstore_metadata[0].get_abstract()
        doc.update({"isotopic": isotopic, "abstract": abstract})

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

        loc = {}
        bbox = string_to_bbox(self.gstore_object.box)
        minx = bbox[0]
        miny = bbox[1]
        maxx = bbox[2]
        maxy = bbox[3]

        #TODO: change the epsg to the SRID default
        geom = bbox_to_geom(bbox, 4326)
        area = geom.GetArea()
        doc.update({"area": area})

        if area == 0.:
            #just represent the points as acutal points
            loc.update({"bbox": {"type": "Point", "coordinates": [minx, miny]}})
        else:
            #otherwise it's a polygon (not thrilled with how es/lucene handles envelopes - it doesn't seem to do it very well)
            #note also that it's sort of a geojson structure
            loc.update({"bbox": {"type": "Polygon", "coordinates": [[[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]]}})


        for facet_to_include in facets_to_include:
            ''' 
            so a list of geolookups.what options, without the nm_
            '''

            geolookup = 'nm_' + facet_to_include.lower()
            
            #TODO: revise to deal with new mexico vs. u.s. coverage to not have enormous lists

            intersects = DBSession.query(func.gstoredata.intersect_geolookup(self.gstore_object.id, geolookup)).first()
            intersects = intersects[0] if intersects else []
            loc.update({facet_to_include: intersects})

        doc.update({"location": loc})

        self.document = {self.es_description['type']: doc}

class CollectionIndexer(EsIndexer):
    '''
    and one for the collection
    '''
    def build_document(self):
        pass
        
