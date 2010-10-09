from rgis.lib.base import *
from rgis.model import meta
from rgis.model.shapes import ShapesVector
from tg import expose, request, response
from tg.decorators import postpone_commits

from sqlalchemy.sql import func

from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon
from shapely.wkt import loads


from mapfish.lib.filters import *
from mapfish.lib.protocol import Protocol, dumps, FeatureCollection, Spatial

from mapfish_rgis import *

from gstore.model.geotables import load_dataset

class ShapesController(BaseController):
	readonly = True # if set to True, only GET is supported
	noisy = False 
	
	def __init__(self):
		self.protocol = RGISProtocol(meta.Session, ShapesVector, self.readonly)

	@expose('json')
	def index(self, dataset_id, format='json', **kw):
		"""GET /: return all features."""
		#
		# If you need your own filter with application-specific params 
		# taken into acount, create your own filter and pass it to the
		# protocol index method.
		#
		# E.g.
		#
		# default_filter = create_default_filter(
		#	  request,
		#	  Dummy.primary_key_column(),
		#	  Dummy.geometry_column()
		# )
		# compare_filter = comparison.Comparison(
		#	  comparison.Comparison.ILIKE,
		#	  Dummy.mycolumnname,
		#	  value=myvalue
		# )
		# filter = logical.Logical(logical.Logical.AND, [default_filter, compare_filter])
		# return self.protocol.index(request, response, format=format, filter=filter)
		#
	
		# There is nothing we can show at the index level since this is a collection of datasets
		filter = getRGISFilter(request, ShapesVector.primary_key_column(), ShapesVector.geometry_column())
		
		return self.protocol.index(request, response, dataset_id, format=format, filter = filter)

	@expose()
	def show(self, dataset_id, gid, format='json', **kw):
		"""GET /id: Show a specific feature."""
		return self.protocol.show(request, response, dataset_id, gid, format=format)

	@expose()
	def feature_info(self, id):
		metadata_html = ''
		if id is not None:
			if id.isdigit():
				dataset = load_dataset(id)
				if dataset:
					met_txt = dataset.metadata_ref.metadata_txt
					filetmp = '/tmp/met_'+id
					met_tmp = open(filetmp,'w')
					met_tmp.write(met_txt)
					met_tmp.close()
					metadata_html = commands.getoutput('xsltproc %s %s' % fgdc_xslt, filetmp)
					commands.getoutput('rm %s' % filetmp)
		return "<style> * { font-size: 8pt }</style> " + metadata_html


	@expose()
	@postpone_commits
	def create(self, **kw):
		"""POST /: Create a new feature."""
		return self.protocol.create(request, response)

	@expose()
	@postpone_commits
	def update(self, id, **kw):
		"""PUT /id: Update an existing feature."""
		return self.protocol.update(request, response, id)

	@expose()
	@postpone_commits
	def delete(self, id, **kw):
		"""DELETE /id: Delete an existing feature."""
		return self.protocol.delete(request, response, id)
