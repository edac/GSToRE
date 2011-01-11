# Copyright (c) 2010 University of New Mexico - Earth Data Analysis Center
# Author: Renzo Sanchez-Silva renzo@edac.unm.edu
# See LICENSE.txt for details.

import re
import commands
import os
import urllib

class OGC:
    def __init__(self, app_id, app_config, layers):
        """
        list : of objects with the following string attributes:
               Object.taxonomy
               Object.geomtype
               Object.basename
        """
        self.app_config = app_config
        #self.app_config['BASE_URL'] = 'http://gstore.unm.edu/apps/%s' % app_id
        #self.app_config['BASE_URL'] = 'http://gstore.unm.edu/apps/%s' % app_id
        self.source = None
        self.app_id = app_id

        if layers == 'base':
            self.basename = 'base'
            return None

        if len(layers) == 0:
            raise Exception('layers must have at least one layer not ' + str(len(layers)))
        # Any bundle (or just 1 dataset) must have same taxonomy
        self.taxonomy = layers[0].dataset.taxonomy
        self.extent = layers[0].dataset.box
        self.id = layers[0].dataset.id
        self.map_output_format = """ 
            """

        if len(layers) > 1:
            self.title = ', '.join([ layer.dataset.basename for layer in layers ])
        else:
            self.title = layers[0].dataset.basename 
            self.basename = layers[0].dataset.basename

        if self.taxonomy == 'geoimage':
            ds = layers[0]
            self.basename = ds.dataset.basename
            self.source = ds.filename
            self.projection = ds.dataset.orig_epsg
            self.theme = ds.dataset.theme
            self.subtheme = ds.dataset.subtheme
            self.groupname = ds.dataset.groupname
            self.dateadded = ds.dataset.dateadded
            ds = None

        
            if not self.source and self.taxonomy == 'geoimage':
                raise Exception('geoimage seems to be missing tif file')

        self.layers = layers
        self.mapfile = """
MAP
  EXTENT %(EXTENT)s
  IMAGECOLOR 255 255 255
  IMAGETYPE png24
  SHAPEPATH "%(SHAPES_PATH)s"
  SIZE 600 600
  STATUS ON
  UNITS dd
  NAME "RGISMap"
    OUTPUTFORMAT
     NAME "png24"
     MIMETYPE "image/png"
     DRIVER "GD/PNG"
     EXTENSION "png"
     IMAGEMODE "RGBA"
     TRANSPARENT TRUE
     FORMATOPTION "QUALITY=70"
    END
    OUTPUTFORMAT
      NAME gif
      DRIVER "GD/GIF"
      MIMETYPE "image/gif"
      IMAGEMODE PC256
      EXTENSION "gif"
      TRANSPARENT TRUE
    END
    OUTPUTFORMAT
      NAME jpeg
      DRIVER "GD/JPEG"
      MIMETYPE "image/jpeg"
      IMAGEMODE RGB
      EXTENSION "jpg"
    END
  SYMBOL
    NAME "line"
    TYPE ELLIPSE
    FILLED TRUE
    POINTS
      1 1
    END
  END

  SYMBOL
    NAME "default-circle"
    TYPE ELLIPSE
    POINTS
      1 1
    END
  END

  SYMBOL
    NAME "square"
    TYPE VECTOR
    FILLED TRUE
    POINTS
      0 0
      0 4
      4 4
      4 0
      0 0
    END
  END

  PROJECTION
    "init=epsg:4326"
  END
  LEGEND
    IMAGECOLOR 255 255 255
    KEYSIZE 22 12
    KEYSPACING 2 7
    LABEL
      ANGLE 0.000000
      ANTIALIAS TRUE
      FONT "Arial-Normal"
      MAXSIZE 256
      MINSIZE 4
      SIZE 1
      TYPE TRUETYPE
      BACKGROUNDCOLOR 255 255 255
      BACKGROUNDSHADOWSIZE 2 2
      BUFFER 0
      COLOR 0 0 0
      FORCE FALSE
      MINDISTANCE -1
      MINFEATURESIZE -1
      OFFSET 0 0
      PARTIALS TRUE
    END
    POSITION LL
    POSTLABELCACHE TRUE
    STATUS ON
  END

  QUERYMAP
    COLOR 255 255 0
    SIZE -1 -1
    STATUS ON
    STYLE HILITE
  END

  SCALEBAR
    BACKGROUNDCOLOR 255 255 255
    COLOR 0 0 0
    IMAGECOLOR 255 255 255
    INTERVALS 4
    LABEL
      SIZE TINY
      TYPE BITMAP
      BUFFER 0
      COLOR 0 0 0
      FORCE FALSE
      MINDISTANCE -1
      MINFEATURESIZE -1
      OFFSET 0 0
      PARTIALS TRUE
    END
    OUTLINECOLOR 0 0 0
    POSITION LR
    POSTLABELCACHE TRUE
    SIZE 200 4
    STATUS ON
    STYLE 0
    UNITS METERS
  END

  WEB
    IMAGEPATH "/clusterdata/rgis2/tmp/"
    IMAGEURL "/clusterdata/rgis2/tmp/"
    TEMPLATE "/clusterdata/rgis2/tmp/client.html"
    METADATA
      "wms_srs" "EPSG:4326 EPSG:4269 EPSG:4267 EPSG:26913 EPSG:26912 EPSG:26914 EPSG:26713 EPSG:26712 EPSG:26714"
      "ows_contactposition" "Analyst/Programmer"
      "WFS_ONLINERESOURCE"  "%(BASE_URL)s/apps/%(app_id)s/datasets/%(id)s/services/ogc/wfs"
      "WMS_ONLINERESOURCE"  "%(BASE_URL)s/apps/%(app_id)s/datasets/%(id)s/services/ogc/wms"
      "WCS_ONLINERESOURCE"  "%(BASE_URL)s/apps/%(app_id)s/datasets/%(id)s/services/ogc/wcs" #*#
      "WMS_ABSTRACT"    "WMS service for RGIS Dataset(s) %(TITLE)s"
      "ows_stateorprovince" "NM"
      "ows_contactvoicetelephone"   "(505) 277-3622 x239"
      "ows_contactorganization" "Earth Data Analysis Center, University of New Mexico"
      "ows_contactperson"   "Renzo Sanchez-Silva"
      "ows_address" "Earth Data Analysis Center, MSC01 1110, 1 University of New Mexico"
      "ows_contactfacsimiletelephone"   "(505) 277-3614"
      "wms_name"    "imagery_wms_%(basename)s"
      "wcs_name"    "imagery_wcs_%(basename)s"
      "ows_contactelectronicmailaddress"    "renzo@edac.unm.edu"
      "ows_country" "US"
      "ows_fees"    "none"
      "ows_postcode"    "87131"
      "wms_formatlist"  "image/png,image/gif,image/jpeg"
      "wms_format"  "image/jpeg"
      "ows_keywordlist" "RGIS, New Mexico"
      "ows_ACCESSCONSTRAINTS"   "none"
      "ows_addresstype" "Mailing address"
      "wms_server_version"  "1.1.1"
      "ows_city"    "Albuquerque"
      "ows_title"   "RGIS Dataset"
      "wcs_label" "imagery_wcs_%(basename)s" #*#
    END
    QUERYFORMAT text/html
    LEGENDFORMAT text/html
    BROWSEFORMAT text/html
  END

  %(LAYERS)s

END
"""

        self.layerTemplate = """
  LAYER
    NAME "%(basename)s" #*#
    PROJECTION
      "init=epsg:4326"
    END
    METADATA
      "wms_srs" "epsg:4326"
      "layer_title" "%(basename)s"
      "base_layer"  "no"
      "legend_display"  "yes"
      "wms_encoding"    "UTF-8"
      "wms_title"   "%(basename)s"
      "gml_include_items" "all"
    END
    STATUS ON
    DATA %(shapefile)s 
    DUMP TRUE
    TRANSPARENCY 50
    EXTENT %(extent)s
    TYPE %(geomtype)s
    UNITS dd
    CLASS
      NAME "everything"
      STYLE
        
        SYMBOL %(symbol)s
        COLOR %(color)s
        SIZE %(size)s
        OUTLINECOLOR 0 0 0
        
      END
      TITLE "everything"
    END
  END
    """
         
    def addLayers(self):
        LAYERS = ""

        if self.taxonomy == "vector":
            for layer in self.layers:
                if layer.geomtype == "MULTIPOLYGON":
                    geomtype = 'POLYGON'
                    size = '1'
                    color = '180 223 238'   
                    symbol = '0' 

                elif layer.geomtype == "POLYGON":
                    geomtype = 'POLYGON'
                    size = '1'
                    color = '180 223 238'   
                    symbol = '0' 

                elif layer.geomtype == "LINESTRING":
                    geomtype = 'LINE'
                    size = '2'
                    color = '0 0 0' 
                    symbol = '0' 

                elif layer.geomtype == "POINT": 
                    geomtype = 'POINT'
                    size = '3'
                    color = '0 0 0' 
                    symbol = '0' 
                
                else:
                    geomtype = 'POINT'
                    size = '3'
                    color = '0 0 0'
                    symbol = '0'
    
                LAYERS = LAYERS + self.layerTemplate % dict( color = color, 
                                                             size = size, 
                                                             symbol = symbol, 
                                                             extent =  ' '.join([str(b) for b in self.extent]),
                                                             basename = layer.basename, 
                                                             shapefile = layer.shapefile,
                                                             geomtype = geomtype 
                ) 
            self.mapfile = self.mapfile % { 
                'LAYERS' : LAYERS, 
                'TITLE' : self.title,
                'app_id': self.app_id, 
                'MAP_EPSG' : self.app_config.get('MAP_EPSG'),
                'SHAPES_PATH' : self.app_config.get('SHAPES_PATH'),
                'EXTENT' : ' '.join([str(b) for b in self.extent]),
                'id' : self.id,
                'basename': self.basename,
                'BASE_URL' : self.app_config.get('BASE_URL')
            }
            self.layers = None
        if self.taxonomy == "geoimage": 
            # TODO
            # Ugly class addition to handle DEM 1-band tiffs. Fix this!
            if self.basename[-5:] == '__DEM':
                CLASS = """
                    CLASS
                        STYLE
                            COLORRANGE 0 0 0 255 255 255
                            DATARANGE -100 3000
                        END
                    END
                    """ 
            else:
                CLASS = "" 
            LAYERS = """
            LAYER                                                                                    
                NAME "%(basename)s"
                PROJECTION
                    "init=epsg:%(epsg)s"
                END # PROJECTION  
                METADATA 
                    ows_abstract "%(description)s" #wms_abstract
                    ows_keywordlist "%(theme)s %(subtheme)s %(groupname)s" #wms_keywordlist
                    wms_opaque "1" #wms_opaque
                    wms_title "%(title)s" #wms_title
                    "wcs_label" "imagery_wcs_%(basename)s" #*#
                    "wcs_formats" "GTiff,PNG,JPEG,GIF"
                    "wcs_rangeset_name" "image_rangeset_name" #*#
                    "wcs_rangeset_label" "image_rangeset_label" #*#
                    layer_title "%(title)s"
                    legend_display "yes"
                    annotation_name "%(basename)s: %(dateadded)s"
                    queryable "no"
                    background "no"
                    static "no"   
                    checked "unchecked" 
                    raster_selected "yes"
                    time_sensitive "no"
                    wms_srs   "EPSG:%(epsg)s"
                END # METADATA 
                DATA "%(location)s"
                TYPE raster #*#
                STATUS off
                PROCESSING "DITHER=YES"
                %(CLASS)s
                EXTENT %(extent)s
                DUMP TRUE #*# 
                CLASS 
                    NAME "everything" 
                    TITLE "everything"
                END 
              END  #LAYER """  %  {
                    'basename': self.basename,
                    'epsg': self.projection,
                    'description': self.title,
                    'dateadded': self.dateadded,
                    'CLASS': CLASS,
                    'title': self.title,
                    'extent' : ' '.join([str(b) for b in self.extent]),
                    'location': self.source,
                    'theme': self.theme,
                    'subtheme': self.subtheme,
                    'groupname': self.groupname
                }

            self.mapfile = self.mapfile % { 
                'TITLE' : self.title, 
                'basename': self.basename,
                'MAP_EPSG' : self.app_config.get('MAP_EPSG'),
                'BASE_URL' : self.app_config.get('BASE_URL'),
                'SHAPES_PATH' : self.app_config.get('SHAPES_PATH'),
                'id' : self.id,
                'EXTENT' : ' '.join([str(b) for b in self.extent]),
                'LAYERS' : LAYERS
            }
        
    def buildService(self):
        """ Query the layers in the bundle and then find the taxonomy and the geometry type for each layer in the bundle and then call addLayer for each one.
        """
        #if self.taxonomy == "geoimage":
        #   sourcesql = "SELECT url from source WHERE id = '%s' AND type = 'file'" % (self.basename)
        #   row = self.DBSession.execute(sourcesql).fetchone()
        #   self.source = row.url

        self.addLayers() 

    def getMapfile(self):
        if self.basename == 'base':
            return ('/clusterdata/rgis2/maps/base/base.map','')

        mapFilename = self.app_config.get('MAPS_PATH') + '/%s.map' % self.basename

        if os.path.isfile(mapFilename):
            map = open(mapFilename, 'r')
            mapContent = map.read()
            map.close()
            return (mapFilename, mapContent)
        else:
            map = open(mapFilename,'w')
            map.write(self.mapfile)
            map.close()
            return (mapFilename, self.mapfile)
    
    def wms(self, req_params):

        (mapfilename, mapcontent) = self.getMapfile()
        wms_request = {}

        params = [ 'REQUEST', 'SERVICE', 'LAYERS', 'SRS', 'BBOX', 'VERSION', 'FORMAT', 'WIDTH', 'HEIGHT', 'EXCEPTIONS', 'MAXEXTENT', 'TRANSPARENT', 'DISPLAYOUTSIDEMAXEXTENT', 'SINGLETILE', 'STYLES', 'EXCEPTIONS', 'QUERY_LAYERS', 'FEATURE_COUNT','INFO_FORMAT', 'FEATURE_COUNT', 'X', 'Y', 'SERVICENAME']
        # Be ready for clients that switch from UPPER to lower keys in WMS requests
        for param in params:
            if req_params.has_key(param.lower()):
                wms_request[param] = req_params[param.lower()]
            if req_params.has_key(param):
                wms_request[param] = req_params[param]
        #For some reason TileCache client is not passing TRANSPARENT=true/false
        if not wms_request.has_key('TRANSPARENT'):
            if wms_request.has_key('FORMAT'):
                if 'png' in wms_request['FORMAT'] or 'PNG' in wms_request['FORMAT']:
                    wms_request['TRANSPARENT'] = 'true'
                if 'gif' in wms_request['FORMAT'] or 'gif' in wms_request['FORMAT']:
                    wms_request['TRANSPARENT'] = 'true'
        elif wms_request.has_key('FORMAT'):
            if 'png' in wms_request['FORMAT']:
                wms_request['TRANSPARENT'] = 'true'
            if 'gif' in wms_request['FORMAT']:
                wms_request['TRANSPARENT'] = 'true'

        QUERY_STRING = 'QUERY_STRING=map=%s&' % mapfilename
        QUERY_STRING += '&'.join(['%s=%s' % (k,v)  for (k,v) in wms_request.iteritems()])       
        #print QUERY_STRING 
        
        cmd = """/usr/lib/cgi-bin/mapserv "%s" """ % QUERY_STRING
        res = commands.getstatusoutput(cmd)

        if res[0] != 0:
            return (None, None)
            
        content = re.sub('^Content-type.*?\n\n','', res[1])
        content_type = res[1].split('\n\n')[0].strip('Content-type: ')

        if 'xml' in content_type:
            content_type = 'text/xml'
        if 'html' in content_type:
            content_type = 'text/html'
    
        return (content_type, content)

    def wcs(self, req_params):

        (mapfilename, mapcontent) = self.getMapfile()
        wcs_request = {}

        params = ['SERVICE', 'VERSION', 'REQUEST', 'COVERAGE', 'CRS', 'BBOX', 'TIME', 'WIDTH', 'HEIGHT', 'RESX', 'RESY', 'FORMAT', 'RESPONSE_CRS', 'IDENTIFIER', 'BOUNDINGBOX', 'GRIDBASECRS', 'GRIDCS', 'GridType', 'GridOrigin', 'GridOffsets', 'RangeSubset']

        # Be ready for clients that switch from UPPER to lower keys in WMS requests
        for param in params:
            if req_params.has_key(param.lower()):
                wcs_request[param] = req_params[param.lower()]
            if req_params.has_key(param):
                wcs_request[param] = req_params[param]

        QUERY_STRING = 'QUERY_STRING=map=%s&' % mapfilename
        QUERY_STRING += '&'.join(['%s=%s' % (k,v)  for (k,v) in wcs_request.iteritems()])       
        #print QUERY_STRING 
    
        cmd = """/usr/lib/cgi-bin/mapserv "%s" """ % QUERY_STRING
        res = commands.getstatusoutput(cmd)
    
        if res[0] != 0:
            return (None, None)
            
        content = re.sub('^Content-type.*?\n\n','', res[1])
        content_type = res[1].split('\n\n')[0].strip('Content-type: ')

        if 'xml' in content_type:
            content_type = 'text/xml'
        if 'html' in content_type:
            content_type = 'text/html'
    
        return (content_type, content)

    def wfs(self, req_params):
        #WFS
        (mapfilename, mapcontent) = self.getMapfile()
        wfs_request = {}

        params = [ 'BBOX','REQUEST', 'SERVICE', 'TYPENAME', 'VERSION', 'MAXFEATURES', 'TYPENAME', 'VERSION', 'MAXFEATURES', 'OUTPUTFORMAT', 'PROPERTYNAME','SRSNAME','TIME', 'QUERY_LAYERS', 'FEATURE_COUNT','INFO_FORMAT', 'FEATURE_COUNT', 'X', 'Y', 'SERVICENAME']

        # Be ready for clients that switch from UPPER to lower keys in WMS requests
        for param in params:
            if req_params.has_key(param.lower()):
                wfs_request[param] = req_params[param.lower()]
            if req_params.has_key(param):
                wfs_request[param] = req_params[param]
        
        QUERY_STRING = 'QUERY_STRING=map=%s&' % mapfilename
        QUERY_STRING += '&'.join(['%s=%s' % (k,v)  for (k,v) in wfs_request.iteritems()])       
        
        cmd = """/usr/lib/cgi-bin/mapserv "%s" """ % QUERY_STRING
        res = commands.getstatusoutput(cmd)
    
        if res[0] != 0:
            return (None, None)
            
        content = re.sub('^Content-type.*?\n\n','', res[1])
        content_type = 'text/xml'

        if 'html' in content_type:
            content_type = 'text/html'

        return (content_type, content) 

if __name__ == '__main__':

    # Sample usage
    from base.scripts.connections import new_conn, old_conn, Session_new as Session
    from base.model import Bundle, Dataset, Metadata
    DBSession = Session()
    dataset1 = DBSession.query(Dataset).get(773) # this boy is a LINESTRING
    dataset2 = DBSession.query(Dataset).get(144)
    dataset3 = DBSession.query(Dataset).get(1753)
    #@setattr(dataset1,'geomtype',dataset1.geom.geom_type.upper())
    #setattr(dataset2,'geomtype',dataset2.geom.geom_type.upper())
    #setattr(dataset3,'geomtype',dataset3.geom.geom_type.upper())

    def test1():
        new_ogc = OGC([dataset1,dataset2,dataset3])
        new_ogc.buildService()


        test1 = new_ogc.wms( dict(REQUEST = 'GetMap', 
                            SERVICE = 'WMS',
                            LAYERS = 'clifton_west__773,fe_2007_35_bdy_00__144', 
                            SRS = 'EPSG:26913', 
                            BBOX = '485182,4037539,493076,4052579',
                            VERSION = '1.1.1',
                            FORMAT = 'image/png',
                            WIDTH = '600',  
                            HEIGHT = '600')
        )
        print test1

    def test2():
        new_ogc = OGC([dataset1])
        new_ogc.buildService()
        print new_ogc.mapfile
        print 

        #test2 = new_ogc.wms( REQUEST = 'GetCapabilities', SERVICE = 'WMS', VERSION = '1.1.1')
        #print test2
    
    def test3():
    
        dataset = DBSession.query(Dataset).get(2)
        new_ogc = OGC([dataset])
        new_ogc.buildService()
        test3 = new_ogc.wfs( dict( REQUEST = 'GetFeature',
                             SERVICE = 'WFS',
                             VERSION = '1.0.0',
    #                        BBOX = '139526.16594395,4000910.7841656,193120.61822166,4054505.2364433',
                             TYPENAME = 'nmsf_districts') ) 
     
        print test3 
    def test4():
        app_config = { 
            'MAP_EPSG' : 'EPSG:26913',
            'BASE_URL' : 'localhost',
            'BASE_PATH' : '/tmp'
        }
        new_ogc = OGC(app_config, [dataset1])
        new_ogc.buildService()
        test1 = new_ogc.wms( dict(REQUEST = 'GetMap', 
                            SERVICE = 'WMS',
                            LAYERS = 'tgr35041lkb__773', 
                            SRS = 'EPSG:26913', 
                            BBOX = '485182,4037539,493076,4052579',
                            VERSION = '1.1.1',
                            FORMAT = 'image/png',
                            TRANSPARENT = 'true',
                            WIDTH = '600',  
                            HEIGHT = '600'
        ))
        f = open('test.png','w')
        f.write(test1[1])
        f.close()
    
    test4()
    #test3()
