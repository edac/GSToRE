import json, re
from xml.sax.saxutils import escape

from ..lib.spatial import *
from ..lib.utils import *

class SpatialStreamer():
    """Basis for the spatial streamers

    Example usage:
        >>> from gstore_v3.lib import spatial_streamer
        >>> fields = [{'type': 4, 'name': 'name'}, {'type': 0, 'name': 'measure'}, {'type': 0, 'name': 'id'}]
        >>> records = [{"geom": "", "fid": 1, "datavalues": [("measure", 3), ("name", "bob")]},{"geom": "", \
                "fid": 2, "datavalues": [("measure", 2), ("name", "nope")]},{"geom": "", "fid": 3, \
                "datavalues": [("measure", 73), ("name", "hi")]}]
        >>> csv_streamer = spatial_streamer.CsvStreamer(fields)
        >>> for c in csv_streamer.yield_set(records):
        >>>     print c


    Note: 
        See the streamer for a specific format for default
        attribute values.

    Attributes:
        encode_as (str): string encoding for the output
        head (str): default structure for the start of the document
        tail (str): default structure for the end of the document
        delimiter (str): default delimiter BETWEEN elements
        content_type (str): content type for the streamer output
        folder_head (str): default structure for the start of any folder or subgroup
        folder_tail (str): default structure for the end of any folder or subgroup
        field_definitions (str): field definition structure (kml, gml, etc)
    """
    encode_as = 'utf-8' 

    head = ''
    tail = ''
    delimiter = ''
    content_type = ''
    folder_head = ''
    folder_tail = ''

    field_definitions = ''

    count = ''

    def __init__(self, fields):
        """initialize the streamer

        Notes:
            If the streamer type requires a certain field definition structure (kml,
            gml, etc), it is also initialized.
            
        Args:
            fields (list): list of dicts describing the fields. keys: type, name, len
            
        Returns:
        
        Raises:
        """
        self.fields = fields
        self.field_definitions = self.generate_field_definitions(fields)

    def yield_set(self, records):
        """main controller for the streamer

        Notes:
            
        Args:
            records (dict): as {'geom': repr, 'fid/rid': INT, 'datavalues': [(field, value), (field, value)]}
            
        Returns:
        
        Raises:
        """

        if self.head:    
            yield self.head.encode(self.encode_as) 
        if self.folder_head:
            yield self.folder_head.encode(self.encode_as) 
        if self.field_definitions:
            yield self.field_definitions.encode(self.encode_as) 

        cnt = 0
        
        for record in records:
            output = self.yield_item(record, cnt)

            if (cnt < len(records) - 1 and len(records) > 1):
                output += self.delimiter

            cnt += 1
            yield output.encode(self.encode_as)

        if self.folder_tail:
            yield self.folder_tail.encode(self.encode_as) + self.delimiter
        if self.tail:
            yield self.tail.encode(self.encode_as)
        

    def yield_head(self):
        """yield the all of the header info (anything before the actual record set)

        Notes:
            just in case we need to run the generator from the record list 
            without building it and passing it along (as in, it is too big
            so let's just iterate the list in place instead of iterate 
            to build and then iterate to output)
            
        Args:
                        
        Returns:
        
        Raises:
        """
        if self.head:    
            yield self.head.encode(self.encode_as)
        if self.folder_head:
            yield self.folder_head.encode(self.encode_as)
        if self.field_definitions:
            yield self.field_definitions.encode(self.encode_as)

    def yield_tail(self):
        """yield the all of the tail info (anything after the actual record set)

        Notes:
            just in case we need to run the generator from the record list 
            without building it and passing it along (as in, it is too big
            so let's just iterate the list in place instead of iterate 
            to build and then iterate to output)
            
        Args:
                        
        Returns:
        
        Raises:
        """
        if self.folder_tail:
            yield self.folder_tail.encode(self.encode_as) + self.delimiter
        if self.tail:
            yield self.tail.encode(self.encode_as)

    def generate_field_definitions(self, fields):
        pass

    def yield_item(self, record, index):
        pass

    def return_record_id(self, record, default_id):
        """check the record for a proper id value

        Notes:
            It may not have one (tabular data) so defaults to
            the specified default which is basically the record index.
            
        Args:
            record (dict): the record dict to check
            default_id (int): fallback option for an id
                    
        Returns:
            (int): the ID value to use
        
        Raises:
        """
        return record['id'] if 'id' in record else default_id
    

class GeoJsonStreamer(SpatialStreamer):
    """GeoJSON streamer

    Note:
        
    Attributes:
        
    """

    head = '{"type": "FeatureCollection", "features": ['
    tail = '\n]}'
    delimiter = ',\n'
    content_type = 'application/json; charset=UTF-8'

    folder_head = ""
    folder_tail = ""

    def generate_field_definitions(self, fields):
        """generate field definitions

        Notes:
            GeoJSON has none.
            
        Args:    
        Returns:
        Raises:
        """
        return ''

    def yield_item(self, record, index):
        """generate the geojson item to yield

        Notes:
            
        Args:
            record (dict): the record to convert
            index (int): integer index of the record
                    
        Returns:
            (str): serialized JSON to yield
        
        Raises:
        """
        geom = record['geom'] if 'geom' in record else ''
        if not geom:
            return ''

        idval = self.return_record_id(record, index)
        vals = dict(record['datavalues'] + [('id', idval)])

        return json.dumps({"type": "Feature", "properties": vals, "geometry": json.loads(geom)})
                    

class JsonStreamer(SpatialStreamer):
    """JSON streamer

    Note:
        NON SPATIAL output
        
    Attributes:
        
    """
    head = '{"features": ['
    tail = ']}'
    delimiter = ',\n'
    content_type = 'application/json; charset=UTF-8'

    folder_head = ""
    folder_tail = ""

    def generate_field_definitions(self, fields):
        """generate field definitions

        Notes:
            JSON has none.
            
        Args:    
        Returns:
        Raises:
        """
        return ''

    def yield_item(self, record, index):
        """generate the json item to yield

        Notes:
            the geom element can be empty or a an empty string here.
            we don't really care.
            
        Args:
            record (dict): the record to convert
            index (int): integer index of the record
                    
        Returns:
            (str): serialized JSON to yield
        
        Raises:
        """
        idval = self.return_record_id(record, index)
        vals = dict(record['datavalues'])

        return json.dumps({"id": idval, "properties": vals})
        
    def update_description(self, count, countsub):
        """update the head to include the count

        Notes:

        Args:
            description (str): appropriately escaped description
        Returns:
        Raises:
        """
        self.head = '{"total":'+count+',"subtotal":'+countsub+',"features": ['

class GmlStreamer(SpatialStreamer):
    """GML streamer

    Note:
        
    Attributes:
        
    """
    head = """<?xml version="1.0" encoding="UTF-8"?>
                                <gml:FeatureCollection 
                                    xmlns:gml="http://www.opengis.net/gml" 
                                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                                    xmlns:xlink="http://www.w3.org/1999/xlink"
                                    xmlns:ogr="http://ogr.maptools.org/">
                                <gml:description>%s</gml:description>\n"""
    tail = """\n</gml:FeatureCollection>"""
    delimiter = '\n'
    content_type = 'application/xml; subtype="gml/3.1.1; charset=UTF-8"'

    folder_head = ""
    folder_tail = ""

    namespace = ""

    def generate_field_definitions(self, fields):
        """generate field definitions

        Notes:
            GML has none.
            
        Args:    
        Returns:
        Raises:
        """
        return ''
        
    def yield_item(self, record, index):
        """generate the gml item to yield

        Notes:
            
        Args:
            record (dict): the record to convert
            index (int): integer index of the record
                    
        Returns:
            (str): serialized GML to yield
        
        Raises:
        """
        geom = record['geom'] if 'geom' in record else ''
        if not geom:
            return ''

        #build the attributes
        idval = self.return_record_id(record, index)
        datavalues = [('id', idval)] + record['datavalues']
        values = ''.join(['<ogr:%(field)s>%(value)s</ogr:%(field)s>' % {'field': dv[0], 'value': dv[1]} for dv in datavalues])

        #and the feature
        return '<gml:featureMember><ogr:g_%(namespace)s><ogr:geometryProperty>%(geometry)s</ogr:geometryProperty>%(values)s</ogr:g_%(namespace)s></gml:featureMember>' % {'namespace': self.namespace, 'geometry': geom, 'values': values}   


    '''
    gml-specific methods
    '''
    def update_description(self, description):
        """update the head to include the title/description

        Notes:
            
        Args: 
            description (str): appropriately escaped description   
        Returns:
        Raises:
        """
        self.head = self.head % description

    def update_namespace(self, namespace):
        """set the namespace

        Notes:
            assume a valid basename (doesn't start with a digit)
            
        Args: 
            namespace (str): valid namespace   
        Returns:
        Raises:
        """
        self.namespace = namespace
    
class KmlStreamer(SpatialStreamer):
    """KML streamer

    Note:
        
    Attributes:
        
    """

    head = """<?xml version="1.0" encoding="UTF-8"?>
                        <kml xmlns="http://earth.google.com/kml/2.2">
                        <Document>"""
    tail = """\n</Document>\n</kml>"""
    delimiter = '\n'
    content_type = 'application/vnd.google-earth.kml+xml; charset=UTF-8'

    folder_head = "<Folder><name>%s</name>"
    folder_tail = "</Folder>"

    def generate_field_definitions(self, fields):
        """generate field definitions for the kml schema definition

        Notes:
            
        Args:    
        Returns:
        Raises:
        """
        kml_fields = self.delimiter.join(['<SimpleField type="%(type)s" name="%(name)s"><displayName>%(name)s</displayName></SimpleField>' % {"type": ogr_to_kml_fieldtype(f['type']), "name": f['name']} for f in self.fields])
        
        return '<Schema name="attributes">%(fields)s</Schema>' % {"fields": kml_fields}

    def yield_item(self, record, index):
        """generate the kml item to yield

        Notes:
            
        Args:
            record (dict): the record to convert
            index (int): integer index of the record
                    
        Returns:
            (str): serialized KML to yield
        
        Raises:
        """
        geom = record['geom'] if 'geom' in record else ''
        if not geom:
            return ''

        idval = self.return_record_id(record, index)
        datavalues = record['datavalues']

        vals = self.delimiter.join(['<SimpleData name="%(name)s">%(value)s</SimpleData>' % {"name": dv[0], "value": dv[1]} for dv in datavalues])

        return '<Placemark id="%(id)s"><name>%(id)s</name>%(geometry)s<ExtendedData><SchemaData schemaUrl="#attributes">%(values)s</SchemaData></ExtendedData><Style><LineStyle><color>ff0000ff</color></LineStyle><PolyStyle><fill>0</fill></PolyStyle></Style></Placemark>' % {"id": idval, "geometry": geom, "values": vals}  


    '''
    kml-specific methods
    '''
    def update_description(self, folder_name):
        """update the folder head to include a name

        Notes:
            
        Args: 
            folder_name (str): appropriately escaped folder name
        Returns:
        Raises:
        """
        self.folder_head = self.folder_head % folder_name  

class CsvStreamer(SpatialStreamer):
    """CSV streamer

    Note:
        NON-SPATIAL output
        
    Attributes:
        
    """
    
    head = ""
    tail = ""
    delimiter = '\n'
    content_type = 'text/csv; charset=UTF-8'

    folder_head = ""
    folder_tail = ""

    def generate_field_definitions(self, fields):
        """generate field definitions for the kml schema definition

        Notes:
            Include the delimiter to properly end the column header row.
            This is the only streamer to require this.
            
        Args:    
        Returns:
        Raises:
        """
        return ','.join([f['name'] for f in fields]) + self.delimiter

    def yield_item(self, record, index):
        """generate the cml item to yield

        Notes:
            Must be built in the order of the fields in the header (obv.)
            
        Args:
            record (dict): the record to convert
            index (int): integer index of the record
                    
        Returns:
            (str): serialized CSV to yield
        
        Raises:
        """
        idval = self.return_record_id(record, index)
        datavalues = [('id', idval)] + record['datavalues']
        vals = []
        print self.fields
        for f in self.fields:   
            dv = [d for d in datavalues if str(d[0]) == f['name']]
            v = '%s' % dv[0][1] if dv else ''
            vals.append(v)

        return ','.join(vals)  
    
