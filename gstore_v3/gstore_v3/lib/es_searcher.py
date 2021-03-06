import requests
from ..lib.spatial import *
from ..lib.utils import convert_timestamp

from gstore_v3.models import Base, DBSession
from sqlalchemy import func

import json
import datetime
import re

from requests.auth import HTTPBasicAuth

class EsSearcher():
    """ElasticSearch search api access

    Note:
        See EsIndexer for indexing access.
    Attributes:
        query_data (dict): search filters for the ES POST request
        results (dict): request response
        dfmt (str): basic date format
        srid (int): default spatial ref for the bbox filter
        default_limit (int): defaults to 14
        default_offset (int): defaults to 0
        default_fields (list): list of field names to include in the response (defaults to _id, our uuids, only)
    """
    query_data = {} 
    results = {} 

    dfmt = '%Y-%m-%d'
    srid = 4326
    default_limit = 15
    default_offset = 0
    default_fields = ["_id"] 

    def __init__(self, es_description):
        """set up the elasticsearch POST url

        Notes:
            "type" in the es_description can be a comma-delimited list of objects types (datasets,collections)

        Args:
            es_description (dict): host, index, type, user, password
        """
        self.es_description = es_description 

        self.es_url = es_description['host'] + es_description['index'] + '/' + es_description['type'] + '/_search'
        self.user = es_description['user']
        self.password = es_description['password']

    def __repr__(self):
        return '<EsSearcher (%s)>' % (self.es_url)

    def get_query(self):
        """return the search filters 

        Notes:
            For testing purposes
        Returns:
            (dict): the search filters
        """
        return self.query_data

    def get_result_total(self):
        """return the number of objects returned by the filter
        Returns:
            (int): the total number of objects found for the filter(s)
        """
        return self.results['hits']['total'] if 'hits' in self.results else 0

    def get_result_ids(self):
        """return the _id and object type for the result set

        Notes:
            Used to generate the output from the object models (services description)
        Returns:
            (list): list of tuples (_id, _type)
        """
        if not self.results:
            return []

        return [(i['_id'], i['_type']) for i in self.results['hits']['hits']]

    def search(self):
        """execute the es request
        Returns:
            (json): the json response from elasticsearch
        Raises:
            Exception: returns the es error if the status code isn't 200
        """
        results = requests.post(self.es_url, data=json.dumps(self.query_data), auth=(self.user, self.password))
        if results.status_code != 200:
            self.results = {}
            raise Exception(results.text)

        self.results = results.json()

        return self.results

    def parse_basic_query(self, app, query_params, exclude_fields=[]):

        """build the search filter dict 
        Args:
            app (str): the app key alias
            query_params (dict): the query params from the gstore search request
            exclude_fields (list, optional): list of fields to use for MISSING query (see collections)
        """
        #pagination
        limit = int(query_params['limit']) if 'limit' in query_params else self.default_limit
        offset = int(query_params['offset']) if 'offset' in query_params else self.default_offset

        #category
        theme, subtheme, groupname = self.extract_category(query_params)

	excludetheme, excludesubtheme, excludegroupname = self.extract_category_exclude(query_params)

        #keywords
        keyword = query_params.get('query', '').replace('+', '')

        #date added
        start_added = query_params.get('start_time', '')
        start_added_date = convert_timestamp(start_added)
        end_added = query_params.get('end_time', '')
        end_added_date = convert_timestamp(end_added)

        #valid dates
        start_valid = query_params.get('valid_start', '')
        start_valid_date = convert_timestamp(start_valid)
        end_valid = query_params.get('valid_end', '')
        end_valid_date = convert_timestamp(end_valid)

	#dataOne
	dataone_archive=query_params.get('dataone_archive','')
	releasedsince=query_params.get('releasedsince','')

	#author
	author=query_params.get('author','')

        #uuid
        uuid=query_params.get('uuid','')
        #formats/services/data type
        format = query_params.get('format', '')
        taxonomy = query_params.get('taxonomy', '')
        geomtype = query_params.get('geomtype', '').replace('+', ' ')
        service = query_params.get('service', '')

        #spatial search
        box = query_params.get('box', '')
        epsg = query_params.get('epsg', '')

        #sorting
        sort = query_params.get('sort', 'lastupdate') #sets lastupdate to default
        if sort not in ['lastupdate', 'description', 'geo_relevance']:
            raise Exception('Bad sort')
        sort = 'date_added' if sort == 'lastupdate' else ('title_search' if sort == 'description' else sort)

        order = query_params.get('dir', 'desc').lower()
        if order not in ['asc', 'desc']:
            raise Exception('bad order')

        #set up the es sorting
        #use title_search (not_analyzed) field for sorting. otherwise, it will parse the string
        # and sort on something that is not what we intend (ie. sort on 2013 at the end of the string rather
        # than the starting from the first word of the string)
        sort_arr = [{sort: {"order": order.lower()}}]
        if sort != 'title_search':
            #add a secondary sort for the title
            sort_arr.append({"title_search": {"order": "asc"}})

        sorts = {"sort": sort_arr}

        #build the json data
        query_request = {"size": limit, "from": offset, "fields": self.default_fields}

        # the main query block
        filtered = {}

        if author:
		filtered.update({"query":{"query_string":{"query": author,"fields": ["author"]}}})


        #all of the filters
        ands = [
            {"term": {"applications": app.lower()}},
            {"term": {"active": True}}
        ]

        spatial_search = False


        if theme or subtheme or groupname:
            ands.append(self.build_category_filter(app.lower(), theme, subtheme, groupname))

        if excludetheme or excludesubtheme or excludegroupname:
            ands.append(self.build_exclude_category_filter(app.lower(), excludetheme, excludesubtheme, excludegroupname))

	if (dataone_archive and dataone_archive.lower()=='true'):
	    ands.append({"term": {"dataOne_archive":True}})
	elif (dataone_archive and dataone_archive.lower()=='false'):
	    ands.append({"term": {"dataOne_archive":False}})

        if format:
            ands.append({"query": {"term": {"formats": format.lower()}}})
        if service:
            ands.append({"query": {"term": {"services": service.lower()}}})
        if taxonomy:
            ands.append({"query": {"term": {"taxonomy": taxonomy.lower()}}})
        if uuid:
            ands.append({"query": {"term": {"_id": uuid.lower()}}})

            #NOTE: geomtype is not currently in the indexed docs
            if geomtype and geomtype.upper() in ['POLYGON', 'POINT', 'LINESTRING', 'MULTIPOLYGON', '3D POLYGON', '3D LINESTRING']:
                ands.append({"query": {"term": {"geomtype": geomtype.lower()}}})
        if keyword:
            keyword_query = self.build_keyword_filter(keyword, ['aliases', 'title', 'keywords', 'author'])
            if keyword_query:
                ands.append(keyword_query)
        if box:
            geo_shape, search_area = self.build_geoshape_filter(box, epsg)

            ands.append(geo_shape)

            #override the default sort query
            sorts = {"sort": [{"_score": order.lower()}]}
            spatial_search = True

        if start_added_date or end_added_date:
            range_query = self.build_simple_date_filter('date_added', start_added_date, end_added_date)
            if range_query:
                ands.append(range_query)

        if start_valid_date or end_valid_date:
            range_query = self.build_date_filter('valid_start', start_valid_date, 'valid_end', end_valid_date)
            if range_query:
                ands += range_query

	#Build in releaseDate to filter out datasets that have been embargoed and haven't reached their release dates
	release_date_query=self.build_releasedate_filter()
	if release_date_query:
		ands.append(release_date_query)

	releasedsince_query=self.build_releasedsince_filter(releasedsince)
	if releasedsince_query:
		ands.append(releasedsince_query)

        if exclude_fields:
            ands += [{"missing": {"field": e}} for e in exclude_fields]

        if ands:
            filtered.update({"filter": {"and": ands}})

        if spatial_search:
            rescorer = {
                "custom_score": {
                    "query": {
                        "filtered": filtered
                    },
                    "params": {
                        "search_area": search_area
                    },
                    "script": "doc['area'].value / search_area"
                }
            }

            query_request.update({"query": rescorer})
        else:
            query_request.update({"query": {"filtered": filtered}})

        #and add the sort element back in
        query_request.update(sorts)


        #should have a nice es search
        self.query_data = query_request

    '''
    parse helpers
    '''

    #TODO: change this to handle the new hierarchy
    def extract_category(self, query_params):
        """parse the category triplet for the search 
        Args:
            query_params (dict): the query params from the gstore search request
        Returns:
            theme (str): the theme
            subtheme (str): the subtheme
            groupname (str): the groupname
        """
        theme = query_params['theme'].replace('+', ' ') if 'theme' in query_params else ''
        subtheme = query_params['subtheme'].replace('+', ' ') if 'subtheme' in query_params else ''
        groupname = query_params['groupname'].replace('+', ' ') if 'groupname' in query_params else ''

        return theme, subtheme, groupname

    '''
    build helpers
    '''
    def extract_category_exclude(self, query_params):
        """parse the category triplet for the search 
        Args:
            query_params (dict): the query params from the gstore search request

        Returns:
            theme (str): the theme
            subtheme (str): the subtheme
            groupname (str): the groupname
        """
        excludetheme = query_params['excludetheme'].replace('+', ' ') if 'excludetheme' in query_params else ''
        excludesubtheme = query_params['excludesubtheme'].replace('+', ' ') if 'excludesubtheme' in query_params else ''
        excludegroupname = query_params['excludegroupname'].replace('+', ' ') if 'excludegroupname' in query_params else ''

        return excludetheme, excludesubtheme, excludegroupname

    '''
    build helpers
    '''


    def build_category_filter(self, app, theme, subtheme, groupname):
        '''
        using the category_facet set (multiple categories per doctype),
        build a nested query filter widget using theme + subtheme + groupname + app

        Example:
        {
            "query": {
                "nested": {
                    "path": "category_facets",
                    "query": {
                        "filtered": {
                            "filter": {
                                "and": [
                                    {"term": {"category_facets.apps": "energize"}},
                                    {"query": {"match": {"category_facets.theme":{ "query": "Climate Change Impacts (RIII)", "operator": "and"}}}},
                                    {"query": {"match": {"category_facets.subtheme":{ "query": "Ecosystem", "operator": "and"}}}}
                                ]
                            }
                        }
                    }
                }
            }
        }

        Args:
            app:
            theme:
            subtheme:
            groupname:

        Returns:
        '''
        ands = [{"term": {"category_facets.apps": app}}]

        if theme:
            ands.append({"query": {"match": {"category_facets.theme":{"query": theme, "operator": "and"}}}})
        if subtheme:
            ands.append({"query": {"match": {"category_facets.subtheme":{"query": subtheme, "operator": "and"}}}})
        if groupname:
            ands.append({"query": {"match": {"category_facets.groupname":{"query": groupname, "operator": "and"}}}})
        return {
            "query": {
                "nested": {
                    "path": "category_facets",
                    "query": {
                        "filtered": {
                            "filter": {
                                "and": ands
                            }
                        }
                    }
                }
            }
        }
    def build_exclude_category_filter(self, app, excludetheme, excludesubtheme, excludegroupname):
        ands = [{"term": {"category_facets.apps": app}}]
        if excludetheme:
            excludethemelist=re.split(r',', excludetheme)
            for excludethemeitem in excludethemelist:
                ands.append({"not": {"query": {"match": {"category_facets.theme":{"query": excludethemeitem, "operator": "and"}}}}})
        if excludesubtheme:
            excludesubthemelist=re.split(r',', excludesubtheme)
            for excludesubthemeitem in excludesubthemelist:
                ands.append({"not": {"query": {"match": {"category_facets.subtheme":{"query": excludesubthemeitem, "operator": "and"}}}}})
        if excludegroupname:
            excludegroupnamelist=re.split(r',', excludegroupname)
            for excludegroupnameitem in excludegroupnamelist:
                ands.append({"not": {"query": {"match": {"category_facets.groupname":{"query": excludegroupnameitem, "operator": "and"}}}}})
        return {
            "query": {
                "nested": {
                    "path": "category_facets",
                    "query": {
                        "filtered": {
                            "filter": {
                                "and": ands
                            }
                        }
                    }
                }
            }
        }

    def build_releasedate_filter(self):
        today=datetime.datetime.now().strftime(self.dfmt)
        ands = {"lte":today}
	"""
	{
	    "sort": [
	        {"date_added": {"order": "u'desc'"}},
	        {"title_search": {"order": "asc"}}
	    ],
	    "fields": ["_id"],
	    "query": {"filtered": {"filter": {"and": [
	        {"term": {"applications": "u'energize'"}},
	        {"term": {"embargo": "false"}},
	        {"term": {"active": "true"}},
	        {"term": {"dataone_archive":"true"}},
	        {"range": {"releaseDate": {"lte": "2017-03-01"}}},
	        {"range": {"releaseDate": {"gte": "2016-03-01"}}}]
	        }}},
	    "from": 0,
	    "size": 15
	}

	"""
	range_query={"range": {"releaseDate": ands}}
        return range_query


    def build_releasedsince_filter(self,releasedsince):
	if(releasedsince):
		pass
	else:
		releasedsince="1970-01-01"
        ands = {"gte":releasedsince}
        range_query= {"range": {"releaseDate": ands}}
	return range_query


    def build_simple_date_filter(self, element, start_date, end_date):
        """build a date filter for an element (single date element unparsed only)

        Notes:
            greater than equal, less than equal, between
        Args:
            element (str): name of the element for the date range filter
            start_date (str): date string a yyyy-MM-dd
            end_date (str): date string a yyyy-MM-dd
        Returns:
            (dict): a range filter element
        """
        if not start_date and not end_date:
            return {}

        range_query = {}
        if start_date and not end_date:
            range_query = {"gte": start_date.strftime(self.dfmt)}
        if not start_date and end_date:
            range_query = {"lte": end_date.strftime(self.dfmt)}
        if start_date and end_date:
            range_query = {"gte": start_date.strftime(self.dfmt), "lte": end_date.strftime(self.dfmt)}
        return {"range": {element: range_query}}

    def build_date_filter(self, start_element, start_date, end_element, end_date):
        '''build the more complete, cross-element date filter
        '''
        if not start_date and not end_date:
            return {}

        range_query = []
        if start_date and not end_date:
            range_query.append({"range": {"%s.date" % start_element: {"gte": start_date.strftime(self.dfmt)}}})
        if not start_date and end_date:
            range_query.append({"range": {"%s.date" % end_element: {"lte": end_date.strftime(self.dfmt)}}})
        if start_date and end_date:
            range_query = [
                {"range": {"%s.date" % start_element: {"gte": start_date.strftime(self.dfmt)}}},
                {"range": {"%s.date" % end_element: {"lte": end_date.strftime(self.dfmt)}}}
            ]
        return range_query

    def build_geoshape_filter(self, box, epsg):
        """build a geometry filter for the location element 

        Notes:
            the geo relevance scoring is handling in the main search parser
        Args:
            box (str): bbox as minx,miny,maxx,maxy string
            epsg (str): epsg code
        Returns:
            geo_shape (dict): a geoshape filter element
            search_area (float): area of the geometry (for the rescorer)
        """

        epsg = int(epsg) if epsg else self.srid

        bbox = string_to_bbox(box)
        bbox_geom = bbox_to_geom(bbox, epsg)

        if epsg != self.srid:
            reproject_geom(bbox_geom, epsg, self.srid)

        search_area = bbox_geom.GetArea()
        coords = [[[bbox[0], bbox[1]],[bbox[0],bbox[3]],[bbox[2],bbox[3]],[bbox[2],bbox[1]],[bbox[0],bbox[1]]]]

        geo_shape = {
            "geo_shape": 
            {
                "location.bbox" : {
                    "shape": {
                        "type": "Polygon",
                        "coordinates": coords
                    }
                }
            }
        }

        return geo_shape, search_area

    def build_keyword_filter(self, keywords, elements):
        """build a keyword filter for ORs across one or more elements 
        Args:
            keywords (str): the keyword string (phrase, etc)
            elements (list): list of elements to include in the search
        Returns:
            (dict): an OR filter element
        """
        ors = [{"query": {"match": {element: {"query": keywords, "operator": "or"}}}} for element in elements]

        #TODO: add the wildcard search:
        '''
        {
            "sort": [{"title_search": {"order": "desc"}}],
            "fields": ["_id"],
            "query": {"filtered": {"filter": {"and": [
                {"term": {"applications": "rgis"}},
                {"term": {"embargo": false}},
                {"term": {"active": true}},
                {"query": {"wildcard": {"title_search":"Surfa*"}}}
            ]}}},
            "from": 0,
            "size": 15
        }
        '''
        return {
            "query": {
                "filtered": {
                    "filter": {
                        "or": ors
                    }
                }
            }
        }


class CollectionWithinSearcher(EsSearcher):
    """Additional ES search capability for "search within collection" 

    Note:
        Use the dataset doctype when instantiating.
        Don't include the exclude_fields (or leave out collections)

        Execute update_collection_filter between parsing the query and executing the search
    Attributes:
    """
    def update_collection_filter(self, collection_uuid):
        """add a filter to include the collection uuid to search within 

        Notes:
        Args:
            collection_uuid (str): the collection uuid to search within
        """

        if not self.query_data:
            return ''

        '''
        as query.filtered.filter.and

        OR

        query.custom_score.query.filtered.filter.and

        '''

        #so we don't want any of the missing fields (def not missing collections, we need that now)

        uuid_filter = {"query": {"match": {"collections": collection_uuid}}}

        if 'custom_score' in self.query_data['query']:
            self.query_data['query']['custom_score']['query']['filtered']['filter']['and'].append(uuid_filter)
        else:
            self.query_data['query']['filtered']['filter']['and'].append(uuid_filter)


#for the repository search (object in repository?)
class RepositorySearcher(EsSearcher):
    """Additional ES search capability for repository listings 

    """
    query_data = {}
    results = {}

    dfmt = '%Y-%m-%d'
    srid = 4326
    default_limit = 20
    default_offset = 0
    default_fields = ["_id"]
    def __init__(self, es_description):
        self.es_description = es_description

        self.es_url = es_description['host'] + es_description['index'] + '/' + es_description['type'] + '/_search'
        self.user = es_description['user']
        self.password = es_description['password']

    def __repr__(self):
        return '<RepositorySearcher (%s)>' % (self.es_url)

    def get_query(self):
        """return the search filters 

        Notes:
            For testing purposes
        Returns:
            (dict): the search filters
        """
        return self.query_data

    def get_result_total(self):
        """return the number of objects returned by the filter
        Returns:
            (int): the total number of objects found for the filter(s)
        """
        return self.results['hits']['total'] if 'hits' in self.results else 0

    def get_result_ids(self):
        """return the _id and object type for the result set

        Notes:
            Used to generate the output from the object models (services description)
        Returns:
            (list): list of tuples (_id, _type)
        """
        if not self.results:
            return []
        return [(i['_id'], i['_type']) for i in self.results['hits']['hits']]

    def search(self):
        """execute the es request

        Returns:
            (json): the json response from elasticsearch
        Raises:
            Exception: returns the es error if the status code isn't 200
        """

        results = requests.post(self.es_url, data=json.dumps(self.query_data), auth=(self.user, self.password))
        if results.status_code != 200:
            self.results = {}
            raise Exception(results.text)

        self.results = results.json()
        #return the json
        return self.results

    def build_basic_search(self, app, repo, standard, query_params={}):
        """build the search filter dict 

        Notes:
            the query_params is not the straight request params obj. it's repacked to be clear about the date search
        Args:
            app (str): the app key alias
            repo (str): the repository alias
            standard (str): the documentation standard
            query_params (dict): the query params from the gstore search request
        """

        query_request = {"size": self.default_limit, "from": self.default_offset, "fields": self.default_fields}

        ands = []

        repo_query = {
            "query": {
                "filtered": {
                    "filter": {
                        "nested": {
                            "path": "supported_repositories",
                            "filter": {
                                "and": [
                                    {"term": {"repos": repo}},
                                    {"term": {"app": app}}
                                ]
                            }
                        }
                    }
                }
            }
        }
        ands.append(repo_query)

        app_query = {
            "query": {
                "match": {"applications": app}
            }
        }
        ands.append(app_query)

        #TODO: change this to standard once the doc has been updated for it
        standards_query = {
            "query": {
                "match": {"standards": standard}
            }
        }
        ands.append(standards_query)

        #now the optional query params (basically the date searches)
        if query_params:

            metadata_query = query_params['metadata_date'] if 'metadata_date' in query_params else {}
            if metadata_query:
                '''
                changed: {order: date:}
                added: {order: date:}
                combination: and | or
                '''
                changed = metadata_query['changed'] if 'changed' in metadata_query else {}
                added = metadata_query['added'] if 'added' in metadata_query else {}

                mq = {}
                if changed and not added:
                    #basic range filter on metadata data
                    range_order = changed['order']
                    range_date = changed['date']
                    mq = self.build_date_filter('gstore_metadata.date', None, range_date) if range_order == 'before' else self.build_date_filter('gstore_metadata.date', range_date, None)
                elif not changed and added:
                    #basic range filter on date added or published?
                    range_order = added['order']
                    range_date = added['date']
                    mq = self.build_date_filter('date_added', None, range_date) if range_order == 'before' else self.build_date_filter('date_added', range_date, None)
                elif changed and added:
                    #combined range filter either AND or OR
                    combos = []
                    range_order = changed['order']
                    range_date = changed['date']
                    mq = self.build_date_filter('gstore_metadata.date', None, range_date) if range_order == 'before' else self.build_date_filter('gstore_metadata.date', range_date, None)
                    combos.append(mq)

                    range_order = added['order']
                    range_date = added['date']
                    mq = self.build_date_filter('date_added', None, range_date) if range_order == 'before' else self.build_date_filter('date_added', range_date, None)
                    combos.append(mq)

                    key = metadata_query['combination'].lower()

                    mq = {
                        "query": {
                            "filtered": {
                                "filter": {
                                    key: combos
                                }
                            }
                        }
                    }

                if mq:
                    ands.append(mq) 


        query_request.update({"query": {"filtered": {"filter": {"and": ands}}}})

        self.query_data = query_request


    #TODO: this
    def build_search(self):
        '''search within a repo
        '''
        pass

