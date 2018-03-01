from sqlalchemy import text
from pyramid.view import view_config
from pyramid.response import Response
from sqlalchemy.dialects import postgresql
from pyramid.httpexceptions import HTTPNotFound, HTTPServerError, HTTPBadRequest
import sqlalchemy
from sqlalchemy import desc, asc, func
from sqlalchemy.sql.expression import and_, or_, cast
from sqlalchemy.sql import between
import xml.etree.ElementTree as ET
from datetime import datetime
from time import strftime
import requests
from ..models.metadata import DatasetMetadata
from ..models import DBSession
from ..models.datasets import Dataset
from ..models.categories import Category
from ..lib.utils import *
from ..lib.database import get_dataset, get_collection
from ..lib.es_searcher import *
from  search import generate_search_response
import logging
import sys
import xlwt
handler = logging.StreamHandler(stream=sys.stderr)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(handler)


def object_as_dict(obj):
    return {c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs}
@view_config(route_name='digitalcommons')
def search_digitalcommons(request):

    ext = request.matchdict['ext'] #grab extension
    app = request.matchdict['app'] #get app from url
    if ext not in ('csv', 'xls'):
	return Response("This route does not understand \""+ext+"\".")

    
    book = xlwt.Workbook(encoding="utf-8")
    sheet1 = book.add_sheet("8898655")
#    sheet1.write(0, 0, "title")
#    sheet1.write(0, 1, "fulltext_url")
    row_num = 0
    columns = ['title', 'fulltext_url', 'keywords', 'abstract', 'author1_fname', 'author1_mname', 'author1_lname', 'author1_suffix', 'author1_email', 'author1_institution', 'author1_is_corporate', 'author2_fname', 'author2_mname', 'author2_lname', 'author2_suffix', 'author2_email', 'author2_institution', 'author2_is_corporate', 'author3_fname', 'author3_mname', 'author3_lname', 'author3_suffix', 'author3_email', 'author3_institution', 'author3_is_corporate', 'author4_fname', 'author4_mname', 'author4_lname', 'author4_suffix', 'author4_email', 'author4_institution', 'author4_is_corporate', 'associated_publications', 'award_number', 'disciplines', 'centroid', 'comments', 'component', 'custom_citation', 'document_type', 'geolocate', 'latitude', 'longitude', 'permanent_url', 'polygon', 'project', 'publication_date', 'season', 'sponsor']
    for col_num in range(len(columns)):
            sheet1.write(row_num, col_num, columns[col_num])     



    doctypes = request.matchdict['doctypes'] 

    #reset doctypes from the route-required plural to the doctype-required singular
    doctypes = ','.join([dt[:-1] for dt in doctypes.split(',')])

    params = normalize_params(request.params)

    #get version (not for querying, just for the output) 
    version = int(params.get('version')) if 'version' in params else 3

    #and if you don't limit we give you everything.
    limit = int(params['limit']) if 'limit' in params else 1#0000000

    #set up the elasticsearch search object
    searcher = EsSearcher(
        {
            "host": request.registry.settings['es_root'],
            "index": request.registry.settings['es_dataset_index'],
            "type": doctypes,
            "user": request.registry.settings['es_user'].split(':')[0],
            "password": request.registry.settings['es_user'].split(':')[-1]
        }
    )
    try:
        searcher.parse_basic_query(app, params)
    except Exception as ex:
        return HTTPBadRequest(json.dumps({"query": searcher.query_data, "msg": ex.message}))

    if 'check' in params:
        #for testing - get the elasticsearch json request
        return Response(json.dumps({"search": searcher.get_query(), "url": searcher.es_url}), content_type = 'application/json')

    try:
        searcher.search()
    except Exception as ex:
        return HTTPServerError(ex.message)

    base_url = request.registry.settings['BALANCER_URL']

    csvstring="title,fulltext_url,keywords,abstract,author1_fname,author1_mname,author1_lname,author1_suffix,author1_email,author1_institution,author1_is_corporate,author2_fname,author2_mname,author2_lname,author2_suffix,author2_email,author2_institution,author2_is_corporate,author3_fname,author3_mname,author3_lname,author3_suffix,author3_email,author3_institution,author3_is_corporate,author4_fname,author4_mname,author4_lname,author4_suffix,author4_email,author4_institution,author4_is_corporate,associated_publications,award_number,disciplines,centroid,comments,component,custom_citation,document_type,geolocate,latitude,longitude,permanent_url,polygon,project,publication_date,season,sponsor\n"
    for object_tuple in searcher.get_result_ids():
        if object_tuple[1] == 'dataset':
            uuid = str(object_tuple[0])

            query=text("SELECT gstoredata.datasets.date_published, gstoredata.datasets.author, gstoredata.datasets.description, gstoredata.metadata.gstore_xml, gstoredata.datasets.basename, gstoredata.categories.theme, gstoredata.sources.extension, gstoredata.datasets.box FROM gstoredata.datasets, gstoredata.metadata, gstoredata.categories, gstoredata.sources, gstoredata.categories_datasets WHERE categories_datasets.dataset_id = datasets.id AND categories_datasets.category_id = categories.id AND gstoredata.sources.dataset_id = gstoredata.datasets.id AND gstoredata.metadata.dataset_id = gstoredata.datasets.id AND datasets.uuid=\'" +uuid + "\';")
            s = DBSession()
            result = s.execute(query)
	    abstract=""
	    keywords=""
	    email=""
	    institution=""
	    latitude=""
	    longitude=""
	    polygon=""
	    geolocate=""
	    a2fname=""
            a3fname=""
            a4fname=""
            a2lname=""
            a3lname=""
            a4lname=""
	    a2email=""
            a3email=""
            a4email=""
            a2iscorp=""
            a3iscorp=""
            a4iscorp=""
            a2inst=""
            a3inst=""
            a4inst=""
	    datarow=0
	    pubdate=""
            for row in result:
                pubdate=row[0].strftime("%m/%d/%Y %H:%M:%S")

		if row[7] is not None:
			box=row[7]
			if box[0]==box[2] and box[1]==box[3]:
				latitude=str(box[1])
				longitude=(box[0])
			if box[0]!=box[2] or box[1]!=box[3]:
			 #create polygon
				log.info(box)

#		if latitude and longitude:
#			geolocate=""
#		unicode(string_data.encode('utf-8'))
#                xmlroot = ET.fromstring(row[3])
                xmlroot = ET.fromstring(row[3].encode('utf-8'))
                ab = xmlroot.findall("./identification[1]/abstract[1]")
		for i in ab:
  			abstract=i.text

                kw = xmlroot.findall("./identification[1]/themes[1]/theme/term")
                for i in kw:
			if i.text is not None:
                            keywords=keywords+i.text +", "

                em = xmlroot.findall("./contacts[1]/contact[1]/email[1]")
                for i in em:
                        email=i.text

		inst = xmlroot.findall("./contacts[1]/contact[1]/organization[1]/name[1]")
                for i in inst:
                        institution=i.text
		#Secondary authors
                a2 = xmlroot.findall("./contacts[1]/contact[2]/person[1]/name[1]")
                for i in a2:
			if i.text!="Earth Data Analysis Center":
                        	a2name=i.text
				if a2name:
					a2name=a2name.split()
					a2fname=a2name[0].replace(',', '')
                	                a2lname=a2name[1].replace(',', '')
					a2iscorp="FALSE"
                                        a2inst=institution
                a2em = xmlroot.findall("./contacts[1]/contact[2]/email[1]")
                for i in a2em:
                        if i.text!="clearinghouse@edac.unm.edu":
                                a2email=i.text
                a3 = xmlroot.findall("./contacts[1]/contact[3]/person[1]/name[1]")
                for i in a3:
                        if i.text!="Earth Data Analysis Center":
                                a3name=i.text
				if a3name:
					a3name=a3name.split()
                                	a3fname=a3name[0]
                                	a3lname=a3name[1]
					a3iscorp="FALSE"
                                        a3inst=institution
                a3em = xmlroot.findall("./contacts[1]/contact[3]/email[1]")
                for i in a3em:
                        if i.text!="clearinghouse@edac.unm.edu":
                                a3email=i.text
                a4 = xmlroot.findall("./contacts[1]/contact[4]/person[1]/name[1]")
                for i in a4:
                        if i.text!="Earth Data Analysis Center":
                                a4name=i.text
                                if a4name:
					a4name=a4name.split()
                                	a4fname=a4name[0]
                                	a4lname=a4name[1]
					a4iscorp="FALSE"
					a4inst=institution
                a4em = xmlroot.findall("./contacts[1]/contact[4]/email[1]")
                for i in a4em:
                        if i.text!="clearinghouse@edac.unm.edu":
                                a4email=i.text
		keywords = keywords.rstrip(', ')
		name=row[1].split()
	        gstoreurl="http://gstore.unm.edu/apps/"+app+"/datasets/"+uuid+"/"+row[4]+".original."+row[6]

#Populate the data
	    	if ext=="csv":
                	rowstring="\""+str(row[2])+"\","+str(gstoreurl)+",\""+str(keywords)+"\",\""+abstract+"\","+str(name[0])+",,"+str(name[1])+",,"+str(email)+",\""+institution+"\",FALSE,"+a2fname+",,"+a2lname+",,"+a2email+","+a2inst+","+a2iscorp+","+a3fname+",,"+a3lname+",,"+a3email+","+a3inst+","+a3iscorp+","+a4fname+",,"+a4lname+",,"+a4email+","+a4inst+","+a4iscorp+",,IIA-1301346,,,,"+str(row[5])+",,dataset,"+geolocate+","+str(latitude)+","+str(longitude)+",,,Energize New Mexico,"+pubdate+",,National Science Foundation"
			csvstring=csvstring+rowstring+"\n"
		row_num=row_num+1
		if ext=="xls":
                        columns = [str(row[2]), str(gstoreurl), str(keywords), abstract, str(name[0]), '', str(name[1]), '', str(email), institution, 'FALSE', a2fname, '', a2lname, '', a2email, a2inst, a2iscorp, a3fname, '', a3lname, '', a3email, a3inst, a3iscorp, a4fname, '', a4lname, '', a4email, a4inst, a4iscorp, '', 'IIA-1301346', '', '', '', str(row[5]), '', 'Dataset', geolocate, str(latitude), str(longitude), '', '', 'Energize New Mexico', str(pubdate), '', 'National Science Foundation']
    			for col_num in range(len(columns)):
            			sheet1.write(row_num, col_num, columns[col_num])

			

    if ext=="csv":
	return Response(csvstring, content_type='text/csv')
    
    if ext=="xls":
        response = Response(content_type='application/vnd.ms-excel',content_disposition='attachment; filename="dataset.xls"')
	book.save(response)
	return response

