import pymongo
from urlparse import urlparse


'''
mongodb object for the vector data

pretty basic - connect, query, and insert

note: not in init - possible remote mongo instances on
      a dataset-by-dataset need (i.e. dataset x is in a different db than dataset y)
      so no one connection

      and diff db/collection for dataone logging
'''
class gMongo:
#    def __init__(self, connstr, collection=''):
#        #set up the connection
#        #required: mongodb://edacdb1:27017/gstore_test (host:port/database)
#        conn_url = urlparse(connstr)
#        self.conn = pymongo.Connection(host=conn_url.hostname, port=conn_url.port)

#        #TODO: add any authentication information in
#        self.db = self.conn[conn_url.path[1:]]

#        if collection:
#            self.collection = self.db[collection]

    #use the mongo connection widget instead
    def __init__(self, mongo_uri):
        self.conn = pymongo.Connection(host=mongo_uri.hostname, port=mongo_uri.port)
        self.db = self.conn[mongo_uri.db]
        if mongo_uri.user and mongo_uri.password:
            #TODO: add some error handling here?
            self.db.authenticate(mongo_uri.user, mongo_uri.password)
        self.collection = self.db[mongo_uri.collection_name]

    def set_collection(self, coll):
        self.collection = self.db[coll]

    def close(self):
        self.conn.close()

    #TODO: something about the possibly unknown collection info
    def query(self, querydict, fielddict={}, sortdict = {}, limit=None, offset=None):
        #what to do if there's no defined collection?
        '''
        generally for gstore we want all of the query results without paging
        but for the dataone logging, we need to run limit/offset for the api

        use the fielddict to just return some elements (i.e. fids for the feature search that isn't the streamer)
        '''

        if fielddict:
            q = self.collection.find(querydict, fielddict)
        else:
            #do not use an empty fielddict -> it will only return the _id values
            q = self.collection.find(querydict)

        #1 = asc, -1 = desc
        if sortdict:
            q = q.sort(sortdict)

        if limit:
            offset = offset if offset else 0
            q = q.limit(limit).skip(offset)

        return q

    #TODO: this.
    def insert(self, docs):
        '''
        docs can be one json doc {'thing': 'data'} or a list of docs [{'thing': 'data'}, {'thing2': 'data'}]
        '''
        try:
            self.collection.insert(docs)
        except Exception as err:
            return err
        return ''
        

    #TODO: add some insert doc validation?
    #      but now we have mutliple mongo deals running so maybe not here

#meh. just do all the parsing and have one passable widget from view to model 
class gMongoUri:
    def __init__(self, connstr, collstr):
        connection_uri = urlparse(connstr)
        self.collection_name = collstr

        #TODO: add the authentication part someday
        self.hostname = connection_uri.hostname
        self.port = connection_uri.port
        self.db = connection_uri.path[1:]
        self.user = connection_uri.username if connection_uri.username else ''
        self.password = connection_uri.password if connection_uri.password else ''

    
