
"""
http://docs.pylonsproject.org/projects/pylons_framework/dev/advanced_pylons/paster_commands.html
Run it this way:
renzo@app-dev:/var/gstore$ paster --plugin=gstore ingest-datasets /home/renzo/bber/bber_may_2011.json

Where newdatasets.json is a file of the form:

[ 
    {
        "basename": "acs0509coseconsum", 
        "box": "-109.05017700000001,31.332173999999998,-103.00206900000001,37.000292999999999", 
        "description": "Economic Summaries (2005-2009)", 
        "file_url": "http://bber.unm.edu/fusion/acs0509counties/acs0509coseconsum.zip", 
        "formats": "zip", 
        "groupname": "2005-2009 5-Yr. Est. for All Counties in NM", 
        "has_metadata": true, 
        "mapfile_template_id": 1, 
        "metadata_xml": "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\r\n<metadata>\r\n<idinfo>\r\n<citation>\r\n<citeinfo>\r\n<origin>U.S. Census Bureau</origin>\r\n<pubdate>20101214</pubdate>\r\n<title>American Community Survey (
        ...
        </metadata>\r\n",
        "orig_epsg": 4326, 
        "subtheme": "American Community Survey (ACS) - Tabular", 
        "taxonomy": "file", 
        "theme": "Socioeconomic Data"
    },
    {

        ...

    },
    ...
]
"""

from paste.script.command import Command

import json
from paste.deploy import appconfig
from pylons import config
from gstore.config.environment import load_environment
from sqlalchemy.sql import func, and_

conf = appconfig('config:/var/gstore/development.ini')
load_environment(conf.global_conf, conf.local_conf)

from gstore.model import *
from gstore.model.geoutils import *

class PromoteVectorDatasets(Command):
    summary = "Command line tool for promoting datasets into vector datasets. JSON file should query unique datasets."
    usage = "inputfile (json)"
    group_name = "gstore"
    parser =  Command.standard_parser(verbose=False)

    def command(self):
        print "Promoting vector datasets..."
        ds = json.loads(open(self.args[0], 'r').read())
        for d in ds:
            kw = {
                'taxonomy': d['taxonomy'],
                'formats': d['formats'],
                'theme': d['theme'],
                'basename': d['basename'],
                'subtheme': d['subtheme'],
                'groupname': d['groupname'],
                'description': d['description'],
                'orig_epsg': d['orig_epsg'],
                'metadata_xml': d['metadata_xml'],
                'mapfile_template_id': d['mapfile_template_id'],
                'inactive': d['inactive']
            }
            dataset = meta.Session.query(Dataset).filter(Dataset.basename == d['basename']).filter(Dataset.theme == d['theme']).\
                filter(Dataset.subtheme == d['subtheme']).filter(Dataset.groupname == d['groupname']).filter(Dataset.description == d['description']).one()
            if d.has_key('shapeloc') and dataset is not None:
                #PromoteVectorDatasetFromShapefile(d['shapeloc'], dataset, meta.Session, load_data = False, **kw)
                dat = VectorDataset(dataset, meta.Session, config)
                meta.Session.execute(dat.get_index())

class DumpVectorDatasets(Command):
    """
    Usage: paster --plugin=gstore dump-vector-datasets /home/renzo/rgis_jobs/census_tiger_bber_jun2011/census2010_june_2011.json   /home/renzo/rgis_jobs/census_tiger_bber_jun2011/sql_dumps
    """
    summary = "Command line tool to dump a vector dataset into PG dump sql format"
    usage = "inputfile (json) dest_file"
    group_name = "gstore"
    parser = Command.standard_parser(verbose=False)
    
    def command(self):
        ds = json.loads(open(self.args[0], 'r').read())
        output_dir = self.args[1]
        for d in ds:
            dataset = meta.Session.query(Dataset).filter(Dataset.basename == d['basename']).filter(Dataset.theme == d['theme']).\
                filter(Dataset.subtheme == d['subtheme']).filter(Dataset.groupname == d['groupname']).filter(Dataset.description == d['description']).one()
            if d['taxonomy'] == 'vector' and dataset is not None:
                # Here shapeloc is not necessary to be listed in the json file. The shp2sql actually unzips the shape file location stored in the Sources table.
                dat = VectorDataset(dataset, meta.Session, config)
                destination_filename = os.path.join(output_dir, '%s.sql' % dataset.id)
                dat.shp2sql(destination_filename, source = 'source', dump = True, encoding = 'latin-1')
 
    
class IngestDatasets(Command):
    summary = "Command line tool for Dataset ingestion from JSON schema files"
    usage = "inputfile (json) commit (any value to commit to database or null for dry run)"
    group_name = "gstore"
    parser = Command.standard_parser(verbose=False)
    
    def command(self):
        print "Loading datasets..."

        ds = json.loads(open(self.args[0], 'r').read())
        if len(self.args)>1:
            commit = True 
        else:
            commit = False
         
        for d in ds:
            dataset = Dataset(d['basename'], d['metadata_xml'])
            dataset.taxonomy = d['taxonomy']
            dataset.theme = d['theme']  
            dataset.subtheme = d['subtheme']
            dataset.groupname = d['groupname']
            dataset.description = d['description']
            dataset.box = map(float, d['box'].split(','))
            dataset.sources_ref.append(Resource(d['file_url']))
            dataset.formats = d['formats']
            dataset.mapfile_template = meta.Session.query(MapfileTemplate).get(d['mapfile_template_id'])
            dataset.orig_epsg = d['orig_epsg']
            if d.has_key('geom'):
                dataset.geom = d['geom']
            else:
                # Find the wkb representation of the geometry based on the box. 
                dataset.geom = bbox_to_polygon(dataset.box).ExportToWkb().encode('hex')                

            if d.has_key('inactive'):
                dataset.inactive = d['inactive']
        
            # Save dataset in list for second pass after commit
            d['dataset'] = dataset
            
        if commit:
            print "Commiting changes to database..."
            meta.Session.commit()


    
            

