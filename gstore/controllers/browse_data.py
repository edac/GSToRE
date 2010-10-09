#from gstore.lib.base import BaseController
from gstore.lib.base import BaseController
from gstore.model import meta
from gstore.model.geobase import Dataset

from pylons import request, response
#from gstore.model.request_log import LogEntry

from simplejson import dumps
from pylons import cache, config

DBSession = meta.Session

class BrowseDataController(BaseController):
	readonly = False # if set to True, only GET is supported

	def json(self, arg1, arg2):
		args = (arg1, arg2)
		kw = request.params
		res = []
		if args[0] == 'tree' and args[1] == 'themes':
			theme = ''
			subtheme = ''
			groupname = ''
			filter = ''
			limit = 25
			offset = 0

			if kw.has_key('filter'):
				filter = kw['filter'].replace(' ','%')
			
			if kw.has_key('limit'):
				limit = kw['limit']

			if kw.has_key('offset'):
				offset = kw['offset']
	
			if kw.has_key('node'):
				node = kw['node'].split('_|_')
				if len(node) == 1:
					theme = kw['node']
				elif len(node) == 2:
					theme = node[0]
					subtheme = node[1]
				elif len(node) == 3:
					theme = node[0]
					subtheme = node[1]
					groupname = node[2]
				#else:
				#	return False
		
				if groupname:
					#self.insert_logentry()
					(total, results) = Dataset.category_search(groupname = groupname, theme = theme, subtheme = subtheme, filter = filter, limit = limit, offset = offset)
					for dataset in results:
						d = { 
							'text': dataset.description, 
							'allowDrag': True,
							'allowDrop': True,
							'config': { 
								'id' : dataset.id,
								'what' : 'dataset',
								'taxonomy': dataset.taxonomy,
								'formats' : dataset.formats.split(','),
								'services' : Dataset.get_services(dataset),
                                'metadata' : Dataset.get_metadata(dataset),
								'tools'	: Dataset.get_tools(dataset)
							},
							'lastupdate': dataset.dateadded.strftime('%d%m%D')[4:],
							'id': dataset.id 
						}
						res.append(d)
					return dumps(dict(results = res, total = total))
			
				elif subtheme:
					results = Dataset.category_search(subtheme = subtheme, theme = theme, filter = filter)
				elif theme and node != 'root': # theme is not empty
					results = Dataset.category_search(theme = theme, filter = filter)
					id = theme
				else:
					results = Dataset.category_search(filter = filter)
					id = None
			
				for category in results:
					d = { 
						'text' : category.text, 
						'leaf' : False,
						'allowDrag': False, 
						'options': '-',
						'allowDrop': False 
					}
					if subtheme:
						d['leaf'] = True		
						d['id'] = '_|_'.join([theme,subtheme, category.text])
					elif theme and theme != 'root':
						d['id'] = '_|_'.join([theme, category.text])
					else:
						d['id'] = category.text

					res.append(d)
		else:
			if kw.has_key('filter'):
				results = Dataset.category_search(filter = filter)	
 
		if args[0] == 'tree' and args[1] == 'bundles':
			d = { 'text': 'New Mexico All Boundaries (Sample Bundle)', 
				 'allowDrag': True,
				'allowDrop': True,
				'options': [ 86, True, False ],
				'lastupdate': '05/01/09',
				'leaf' : True,
				'what' : 'bundle',
				'id': 86 }
			res.append(d)

		if args[0] == 'combo' and args[1] == 'themes':
			query = DBSession.query(
				Dataset.id, 
				Dataset.description, 
				Dataset.theme,
				Dataset.subtheme,
				Dataset.groupname
			)
		
			if kw.has_key('theme'):
				query = query.filter(Dataset.theme.ilike(kw['theme']+'%')).cache()
			if kw.has_key('subtheme'):
				query = query.filter(Dataset.subtheme.ilike(kw['subtheme']+'%')).cache()
			if kw.has_key('groupname'):
				query = query.filter(Dataset.groupname.ilike(kw['groupname']+'%')).cache()

		return dumps(res)

