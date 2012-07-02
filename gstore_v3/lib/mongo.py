import pymongo
from urlparse import urlparse


'''
mongodb object

pretty basic - connect, query, and insert

note: not in init - possible remote mongo instances on
      a dataset-by-dataset need (i.e. dataset x is in a different db than dataset y)
      so no one connection

'''
class gMongo:
    def __init__(self, connstr, collection=''):
        #set up the connection
        #required: mongodb://edacdb1:27017/gstore_test (host:port/database)
        conn_url = urlparse(connstr)
        self.conn = pymongo.Connection(host=conn_url.hostname, port=conn_url.port)

        #TODO: add any authentication information in
        self.db = self.conn[conn_url.path[1:]]

        if collection:
            self.collection = self.db[collection]

    def close(self):
        self.conn.close()

    def query(self, querydict):
        #what to do if there's no defined collection?
        q = self.collection.find(querydict)
        return q

    def insert(self, data):
        pass
