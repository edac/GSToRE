import json, re
from xml.sax.saxutils import escape

from ..lib.spatial import *
from ..lib.utils import *

class SpatialStreamer():
    '''
    basic setup for streaming vector data (for text-based formats only: geojson, json, csv, kml, gml)

    vector:
    fields: [field, field, field, ...]
    records: [{geom: wkb, fid/rid: INT, datavalues: [(field,value), (field,value), ...]}, ..{}]
        where geom is the representation of the wkb for the given format
    

    '''
    encode_as = 'utf-8' 

    head = ''
    tail = ''
    delimiter = ''
    content_type = ''
    folder_head = ''
    folder_tail = ''

    field_definitions = ''

    def __init__(self, fields):
        self.fields = fields #as [{'type':, 'name':, 'len': }, ]

        self.field_definitions = self.generate_field_definitions(fields)


    def yield_set(self, records):
        '''
        records = {'geom': repr, 'fid/rid': INT, 'datavalues': [(field, value), (field, value)]}
        '''

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
        '''
        just in case we need to run the generator from the record list 
        without building it and passing it along (as in, it is too big
        so let's just iterate the list in place instead of iterate 
        to build and then iterate to output)
        '''
        if self.head:    
            yield self.head.encode(self.encode_as)
        if self.folder_head:
            yield self.folder_head.encode(self.encode_as)
        if self.field_definitions:
            yield self.field_definitions.encode(self.encode_as)

    def yield_tail(self):
        '''
        same again
        '''
        if self.folder_tail:
            yield self.folder_tail.encode(self.encode_as) + self.delimiter
        if self.tail:
            yield self.tail.encode(self.encode_as)

    def generate_field_definitions(self, fields):
        pass

    def yield_item(self, record, index):
        pass

    def return_record_id(self, record, default_id):
        return record['id'] if 'id' in record else default_id
    

class GeoJsonStreamer(SpatialStreamer):

    head = '{"type": "FeatureCollection", "features": ['
    tail = '\n]}'
    delimiter = ',\n'
    content_type = 'application/json; charset=UTF-8'

    folder_head = ""
    folder_tail = ""

    def generate_field_definitions(self, fields):
        '''
        no field info
        '''
        return ''

    def yield_item(self, record, index):
        #deal with the geometry
        geom = record['geom'] if 'geom' in record else ''
        if not geom:
            return ''

        idval = self.return_record_id(record, index)
        vals = dict(record['datavalues'] + [('id', idval)])

        return json.dumps({"type": "Feature", "properties": vals, "geometry": json.loads(geom)})
                    

class JsonStreamer(SpatialStreamer):

    head = '{"features": ['
    tail = ']}'
    delimiter = ',\n'
    content_type = 'application/json; charset=UTF-8'

    folder_head = ""
    folder_tail = ""

    def generate_field_definitions(self, fields):
        '''
        no field info outside of the key so this is empty
        '''
        return ''

    def yield_item(self, record, index):
        '''
        record = {'geom': , 'fid/rid': , 'datavalues': [(field, value)]}

        and we don't really care about the geom here, so it's okay if it's an empty string
        '''    

        idval = self.return_record_id(record, index)
        vals = dict(record['datavalues'])

        return json.dumps({"id": idval, "properties": vals})
        
        

class GmlStreamer(SpatialStreamer):

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
        '''
        no field info
        '''
        return ''
        
    def yield_item(self, record, index):
        #deal with the geometry
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
        self.head = self.head % description

    def update_namespace(self, namespace):
        '''
        for a dataset, the namespace is generally the basename
        '''
        self.namespace = namespace
    
class KmlStreamer(SpatialStreamer):

    head = """<?xml version="1.0" encoding="UTF-8"?>
                        <kml xmlns="http://earth.google.com/kml/2.2">
                        <Document>"""
    tail = """\n</Document>\n</kml>"""
    delimiter = '\n'
    content_type = 'application/vnd.google-earth.kml+xml; charset=UTF-8'

    folder_head = "<Folder><name>%s</name>"
    folder_tail = "</Folder>"

    def generate_field_definitions(self, fields):
        '''
        
        '''
        kml_fields = self.delimiter.join(['<SimpleField type="%(type)s" name="%(name)s"><displayName>%(name)s</displayName></SimpleField>' % {"type": ogr_to_kml_fieldtype(f['type']), "name": f['name']} for f in self.fields])
        
        return '<Schema name="attributes">%(fields)s</Schema>' % {"fields": kml_fields}

    def yield_item(self, record, index):
        #deal with the geometry
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
        self.folder_head = self.folder_head % folder_name  

class CsvStreamer(SpatialStreamer):

    head = ""
    tail = ""
    delimiter = '\n'
    content_type = 'text/csv; charset=UTF-8'

    folder_head = ""
    folder_tail = ""

    #include the delimiter here so the headers + first data row is correct
    def generate_field_definitions(self, fields):
        return ','.join([f['name'] for f in fields]) + self.delimiter

    def yield_item(self, record, index):
        '''
        build the row in the order of the fields

        '''
        idval = self.return_record_id(record, index)
        datavalues = [('id', idval)] + record['datavalues']
        vals = []
        for f in self.fields:   
            dv = [d for d in datavalues if str(d[0]) == f['name']]
            v = '%s' % dv[0][1] if dv else ''
            vals.append(v)

        return ','.join(vals)  




'''
>>> from gstore_v3.lib import spatial_streamer
>>> fields = [{'type': 4, 'name': 'name'}, {'type': 0, 'name': 'measure'}, {'type': 0, 'name': 'id'}]
>>> records = [{"geom": "", "fid": 1, "datavalues": [("measure", 3), ("name", "bob")]},{"geom": "", "fid": 2, "datavalues": [("measure", 2), ("name", "nope")]},{"geom": "", "fid": 3, "datavalues": [("measure", 73), ("name", "hi")]}]
>>> csv_streamer = spatial_streamer.CsvStreamer(fields)
>>> for c in csv_streamer.yield_set(records):
>>>     print c


>>> kml_streamer = spatial_streamer.KmlStreamer(fields)
>>> kml_streamer.update_description('New Name')



records = [{"geom": "010200008026000000DE22864D14115AC0DFAAD7D9DE6C4240000000000068BF4045898EEA14115AC0F1B71209E06C4240000000000068BF400CE8D40D15115AC04495154DE06C4240000000000068BF40AE81092015115AC00FA20770E06C4240000000000068BF40EF294F2215115AC0151BCA73E06C4240000000000068BF40E87EE23F15115AC0B1F406B5E06C4240000000000068BF40DA55E67915115AC0BD6A9F35E16C4240000000000068BF40850FCF2616115AC0378B87B5E26C4240000000000068BF40C41DAC8116115AC0A377EC74E36C4240000000000068BF40FFDBF1E016115AC0E740C233E46C4240000000000068BF40BEBC4EA817115AC0E5894FB0E56C4240000000000068BF407DC9941718115AC098C2BA6EE66C4240000000000068BF405F71729218115AC0C78F4A2DE76C4240000000000068BF4004FE5C9F19115AC0374CB3AAE86C4240000000000068BF40E3B5B3C51A115AC0B8B8E7CEE96C4240000000000068BF4051F4EB8B1C115AC025119C58EB6C4240000000000068BF402E00205821115AC04EC60337EF6C4240000000000068BF4065A3C7AD25115AC0CB163EEBF06C4240000000000068BF406CF6FCF22B115AC0A55D7D64F26C4240000000000068BF4017D1835C3C115AC04B6DFCE0F46C4240000000000068BF407A64C6E441115AC00CF8682BF66C4240000000000068BF40AF8E87C044115AC0A0C30782F76C4240000000000068BF409383061F45115AC0D3EEAC47FA6C4240000000000068BF405052CD2F45115AC0C5FC12B2FB6C4240000000000068BF4059701B2245115AC0BC830C24FD6C4240000000000068BF408A40C6C944115AC012822617006D4240000000000068BF408E2AA6AE44115AC0F71CE551016D4240000000000068BF40729A90A444115AC07554D54D026D4240000000000068BF4051897AB244115AC010FD18C8036D4240000000000068BF404A6427B744115AC0BD695946046D4240000000000068BF4089228CB944115AC0DA6EB885046D4240000000000068BF406465C5B944115AC002AAB386046D4240000000000068BF40CBC9EFB944115AC05E843989046D4240000000000068BF402AF127BA44115AC0889BC78D046D4240000000000068BF40DCC5B3BA44115AC06E43F49A046D4240000000000068BF40E40688BD44115AC0683609DC046D4240000000000068BF40B19EEAC244115AC064C89C57056D4240000000000068BF40B96BBDDA44115AC0195BD379076D4240000000000068BF40", "fid": 1, "datavalues": [("measure", 3), ("name", "bob")]},{"geom": "010200008083000000B4C6100104115AC0D28139D1566742400000000000DCB940260C7EB103115AC0CBF85184566742400000000000DCB9404EFDAD7603115AC0FDF97845566742400000000000DCB940F41E941B03115AC01D7E69E1556742400000000000DCB940C0CACC2402115AC0A883DDCE546742400000000000DCB940E57189B601115AC0E4838257546742400000000000DCB9407A67665501115AC0B28412F2536742400000000000DCB9400FEF60AD00115AC09387084B536742400000000000DCB9400B5B184E00115AC0E67C23DE526742400000000000DCB9409EEF89E3FF105AC03166DE57526742400000000000DCB9405A70E1F7FE105AC03E1B9418516742400000000000DCB9407B2FA034FE105AC044C73F45506742400000000000DCB94096F3F123FD105AC0CE3A3C3E4F6742400000000000DCB94090B1BB67FA105AC02070D6C84C6742400000000000DCB9408EA2C009F9105AC02CB0632B4B6742400000000000DCB940ACA4E5ABF7105AC0A125312B496742400000000000DCB9404B0B70F0F4105AC0DA5B4C65446742400000000000DCB9400EB07836F3105AC0A0390E59426742400000000000DCB9400BB44420F1105AC0014A84A3406742400000000000DCB94023A5633BEC105AC0331CD8E53D6742400000000000DCB94090D7152BE9105AC0DA4B604C3D6742400000000000DCB9404786EA7CE5105AC0D39246783D6742400000000000DCB9406735D8E4DC105AC0F922CD5A3F6742400000000000DCB940F7C55A32DA105AC0070189133F6742400000000000DCB9400E606919D9105AC0D3F4BE933D6742400000000000DCB9403C689E1ADA105AC00AB61F23386742400000000000DCB940CE260F4BD9105AC086BEFE61346742400000000000DCB940309C562BD7105AC0213C0C982F6742400000000000DCB940B252954BD0105AC0F23D82F2236742400000000000DCB940D6541E6CCC105AC0C523FBE11F6742400000000000DCB940FD15101DC8105AC019FBB1931D6742400000000000DCB940B038C49FBE105AC0F143997B1C6742400000000000DCB940119CAEDEBA105AC00E9553CF1C6742400000000000DCB940BF01291BB8105AC01013D6021E6742400000000000DCB940246D3D8FB4105AC07F4A6C29226742400000000000DCB940876910D2B3105AC0C991D13B226742400000000000DCB940BE63AC1DB4105AC0AE48514D206742400000000000DCB94000A575C6B6105AC07C50856E186742400000000000DCB9407098ED6FB7105AC031B438A1146742400000000000DCB9403136796EB7105AC07DA005F6106742400000000000DCB9406347B815B6105AC02684D2E3096742400000000000DCB9401D127F39B4105AC0D0AD683F086742400000000000DCB9408CF16C2DB1105AC06C21AE7F086742400000000000DCB940FD7395B5A8105AC058E994C90C6742400000000000DCB9405A16ECDBA4105AC005CFA56B0B6742400000000000DCB94030578664A1105AC04023D58A066742400000000000DCB940DFFF443A9B105AC0BA9070C3F56642400000000000DCB940FA44E2A197105AC03618DF2FF26642400000000000DCB940C49E3B8693105AC0B45F6E6CF36642400000000000DCB94034D961488A105AC09404CC85FF6642400000000000DCB940DB7BAB0D87105AC0306DDEAB026742400000000000DCB940BD6F2D3785105AC02D1A56EB026742400000000000DCB9400EEAA25284105AC0F806119DFD6642400000000000DCB9407CDBC3F382105AC089CBA92EFC6642400000000000DCB94056F34AA880105AC0F69EFDF8FB6642400000000000DCB940BBDF24387A105AC0A7D219FFFD6642400000000000DCB94014774FAC78105AC064D3A7B1FD6642400000000000DCB940C9E1B7CC78105AC0FC9AB613FC6642400000000000DCB9406AA403667C105AC04C64D536F66642400000000000DCB940280F5FC67D105AC0B6F42AE4F26642400000000000DCB940473770BA7E105AC00EEC462DEF6642400000000000DCB9405A13FEC97F105AC0A7CB0BF7E66642400000000000DCB940A82D14C680105AC08625D4A7E46642400000000000DCB9407A92793682105AC036578224E46642400000000000DCB940D470E3FF85105AC016ECA9B5E66642400000000000DCB940D8EBC4EA87105AC028DC1210E66642400000000000DCB940DC97D2DB89105AC0150C517CE36642400000000000DCB940704145CA8D105AC0E5997778DA6642400000000000DCB94023E0806F8E105AC0C6CFE0A5D66642400000000000DCB940BD71BFC28D105AC03947A082D36642400000000000DCB940B75043C589105AC0031ECB9ACE6642400000000000DCB940954234C787105AC0667E3308CD6642400000000000DCB94040F3D3C985105AC072F5EE56CC6642400000000000DCB940FB8770D081105AC004CC0BB7CC6642400000000000DCB9407A9C4F9D80105AC0DCEDA6BECB6642400000000000DCB9402789BF3380105AC01D19CF9DC96642400000000000DCB940E1D0C0F380105AC0F69D390BC36642400000000000DCB940AB78D9A880105AC02E9D243FC06642400000000000DCB940AA4F0AB37F105AC0155245F0BD6642400000000000DCB94071AF9C717C105AC086B9F14CBA6642400000000000DCB940BAC3B5087B105AC0F6F94E67B76642400000000000DCB940A8B99ED779105AC01E6DB36DB36642400000000000DCB940FAB710E577105AC0D5C08A52A96642400000000000DCB9407DC8E3D777105AC08F46B290A36642400000000000DCB9403560D0B678105AC0C0AF951A9D6642400000000000DCB94025E9DA4C7C105AC03FAED3C58E6642400000000000DCB940FBCB46BC7D105AC003EC0B55866642400000000000DCB940A5B919D07E105AC064A0DD9D7C6642400000000000DCB940CC898D4080105AC01E04B4A2666642400000000000DCB9401713440B7F105AC0D6238438606642400000000000DCB940600478E87B105AC07FCEB8615E6642400000000000DCB9409A5AD8C771105AC05805E7DA636642400000000000DCB9402ECD086D6D105AC0E5693AEC646642400000000000DCB940FA80BAC769105AC04D9A4B52646642400000000000DCB94016A421E863105AC0800FEAC75F6642400000000000DCB940844E47D561105AC066414B795D6642400000000000DCB940E3BD5E9F60105AC0A90F3F215B6642400000000000DCB9403DE170ED5F105AC0BA4C4C5E566642400000000000DCB940DBFF64A75E105AC0138BF019556642400000000000DCB940F22144745C105AC0BA2DB2F2546642400000000000DCB94034ECD73356105AC0E9BD6EDE566642400000000000DCB940DA3A6EA252105AC095AFE5CA566642400000000000DCB9408A51D19F4E105AC09D4FF5AD556642400000000000DCB9400B4C32B845105AC0D0E54361516642400000000000DCB940269818BB42105AC0595687004F6642400000000000DCB9401838B43441105AC05F7668654C6642400000000000DCB940D594551541105AC0E9F766BA466642400000000000DCB940EFC5E86840105AC00F37C663456642400000000000DCB9409F75BE1F3F105AC00C60058C456642400000000000DCB940C558EE533B105AC03AF342DA486642400000000000DCB940C33764CC39105AC01C03C02C496642400000000000DCB940B64338A338105AC018979B2A486642400000000000DCB940F1FB9C0D37105AC021E90F7D436642400000000000DCB940D30871E135105AC0CFDFE627426642400000000000DCB9403EADE65334105AC082925AD4416642400000000000DCB9405FDB147630105AC000F17A30436642400000000000DCB940D80101FB2E105AC0150638F8426642400000000000DCB9407D58C2F32D105AC08344A2D9416642400000000000DCB9406185EFCC2C105AC0493ED1CF3D6642400000000000DCB9401ECA321B2B105AC09FA59A213D6642400000000000DCB9406C98224B28105AC0EB9715CA3D6642400000000000DCB94080305A6E20105AC0063C6CC8416642400000000000DCB9408925EB4D1D105AC029BA7D7F456642400000000000DCB940EB4471FB1A105AC00B6A76EE4A6642400000000000DCB940874F67F217105AC096BA363C596642400000000000DCB94021ACCF3A16105AC076D614FE5C6642400000000000DCB9406C11265014105AC075F1F05A5D6642400000000000DCB940AF44B01410105AC0A8B8A44A576642400000000000DCB940D769C5810D105AC0F581312C556642400000000000DCB9400F55AA790A105AC0F41971F7536642400000000000DCB940F914C80100105AC09C004516536642400000000000DCB940", "fid": 2, "datavalues": [("measure", 2), ("name", "nope")]},{"geom": "01020000800D00000034A77A209D125AC0ABCC0CCE986C4240000000000024BD409DD3B7EB99125AC02FE1DB8C976C4240000000000024BD4028BD0C9998125AC054AEDAC6976C4240000000000024BD40A1DD178E97125AC00AE770DB986C4240000000000024BD4095609A0796125AC00442CCB99C6C4240000000000024BD40AEAB170096125AC0C4B35F449E6C4240000000000024BD403A2151B496125AC0EAF0586A9F6C4240000000000024BD4031873C9499125AC005AA16EDA06C4240000000000024BD40B8EEC3D89A125AC06D7804BEA06C4240000000000024BD4019ECDCF19B125AC0E954819E9F6C4240000000000024BD40C5D631CD9D125AC0532C997E9B6C4240000000000024BD40DE35D8E29D125AC08D0119E9996C4240000000000024BD4034A77A209D125AC0ABCC0CCE986C4240000000000024BD40", "fid": 3, "datavalues": [("measure", 73), ("name", "hi")]}]
'''










           
