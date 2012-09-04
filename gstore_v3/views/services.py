from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPBadRequest

import os 
from email.parser import Parser 
from email.message import Message
from urlparse import urlparse

#make we sure we have pil
import Image
import mapscript
from cStringIO import StringIO

from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )
from ..models.sources import MapfileStyle

from ..lib.database import *
#from ..lib.spatial import tilecache_service
from ..lib.utils import get_image_mimetype, normalize_params
from ..lib.spatial import *
from ..lib.mongo import gMongoUri


'''
INSTALLING PIL for the virtualenv (http://justalittlebrain.wordpress.com/2011/08/21/installing-pil-in-virtualenv-in-ubuntu/)
>sudo apt-get build-dep python-imaging
>sudo ln -s /usr/lib/x86_64-linux-gnu/libfreetype.so /usr/lib/
>sudo ln -s /usr/lib/x86_64-linux-gnu/libz.so /usr/lib/
>sudo ln -s /usr/lib/x86_64-linux-gnu/libjpeg.so /usr/lib/
from the active virtualenv
>bin/easy_install PIL
'''

'''
ogc

ecw test: http://129.24.63.66/gstore_v3/apps/rgis/datasets/ef4c8cdc-bec4-43aa-8f2d-3046057e3335/services/ogc/wms?REQUEST=GetMap&SERVICE=WMS&VERSION=1.1.1&LAYERS=t20nr08e34&BBOX=-109.050173,31.332172,-103.001964,37.000293

'''
#TODO: refactor the mapfile generation part (see the awkwardness of the point symbol)

#so, neat trick, the order of the srs matters for wcs.describecoverage
#this rebuilds the list so that the check_srs (the dataset srs, for example) is listed once and is listed first
def build_supported_srs(check_srs, supported_srs):
    if not supported_srs:
        return ''
    if check_srs:
        supported_srs = [s for s in supported_srs if s != 'EPSG:%s' % (check_srs)]
        supported_srs.insert(0, 'EPSG:%s' % (check_srs))
    return ' '.join(supported_srs)


#TODO: REVISE FOR SLDs, RASTER BAND INFO, ETC
#default syle objs
def getStyle(geomtype):
    s = mapscript.styleObj()
    #s.symbol = symbol
    if geomtype.upper() in ['POLYGON', 'MULTIPOLYGON', '3D POLYGON']:
        s.size = 1
        s.color.setRGB(180, 223, 238)
        s.width = 1
        s.outlinecolor.setRGB(0,0,0)
    elif geomtype.upper() in ['LINESTRING', 'LINE', '3D LINESTRING']:
        s.width = 2
        s.color.setRGB(0, 0, 0)
    elif geomtype.upper() == 'POINT':
        s.size = 4
        s.color.setRGB(0, 0, 0)
        #point to our ellipse symbol (mapscript starts at 1, not 0)
        #and the symbol name pointer only seems to work from a file-based map
        s.symbol = 1
        #s.symbolname = 'circles'
    else:
        s.size = 3
        s.color.setRGB(100, 100, 100)

    return s

#convert geomtype to layer type
def getType(geomtype):
    if geomtype.upper() in ['MULTIPOLYGON', 'POLYGON', '3D POLYGON']:
        return mapscript.MS_LAYER_POLYGON
    elif geomtype.upper() in ['LINESTRING', 'LINE', '3D LINESTRING']:
        return mapscript.MS_LAYER_LINE
    else:
        return mapscript.MS_LAYER_POINT

#get the layer obj by taxonomy (for now)
#the bbox should be reprojected to the originnal epsg before this
def getLayer(d, src, dataloc, bbox):
    layer = mapscript.layerObj()
    layer.name = d.basename
    layer.status = mapscript.MS_ON

    layer.data = dataloc
    layer.dump = mapscript.MS_TRUE

    layer.setExtent(bbox[0], bbox[1], bbox[2], bbox[3])
    

    layer.metadata.set('layer_title', d.basename)
    layer.metadata.set('ows_abstract', d.description)
    layer.metadata.set('ows_keywordlist', '') #TODO: something
    layer.metadata.set('legend_display', 'yes')
    layer.metadata.set('wms_encoding', 'UTF-8')
    layer.metadata.set('ows_title', d.basename)
    
    if d.taxonomy == 'vector':
        style = getStyle(d.geomtype)

        #units always 4326 decimal degrees - data not reprojected
        #layer.units = mapscript.MS_DD

        layer.setProjection('+init=epsg:4326')
        layer.opacity = 50
        layer.type = getType(d.geomtype)
        layer.metadata.set('ows_srs', 'epsg:4326')
        layer.metadata.set('base_layer', 'no')
        layer.metadata.set('wms_encoding', 'UTF-8')
        layer.metadata.set('wms_title', d.basename)
        layer.metadata.set('gml_include_items', 'all')
        layer.metadata.set('gml_featureid', 'FID') 

        #add the class (and the style)
        cls = mapscript.classObj()
        #cls.name = 'Everything'
        cls.insertStyle(style)

        layer.insertClass(cls)
    elif d.taxonomy == 'geoimage':
        #TODO: possibly add accurate units based on the epsg code
        #TODO: check on the wcs_formats list & compare to outputformats - ARE THE NAMES CORRECT?
        layer.setProjection('+init=epsg:%s' % (d.orig_epsg))
        layer.setProcessing('CLOSE_CONNECTION=DEFER')
        layer.type = mapscript.MS_LAYER_RASTER
        layer.metadata.set('ows_srs', 'epsg:%s' % (d.orig_epsg))
        layer.metadata.set('queryable', 'no')
        layer.metadata.set('background', 'no')
        layer.metadata.set('time_sensitive', 'no')
        layer.metadata.set('raster_selected', 'yes')
        layer.metadata.set('static', 'no')
        layer.metadata.set('annotation_name', '%s: %s' % (d.basename, d.dateadded))
        layer.metadata.set('wcs_label', 'imagery_wcs_%s' % (d.basename))
        layer.metadata.set('wcs_formats', 'GTiff GEOTIFF_16 AAIGRID')
        layer.metadata.set('wcs_nativeformat', 'GTiff')
        layer.metadata.set('wcs_rangeset_name', d.basename)
        layer.metadata.set('wcs_rangeset_label', d.description)

        '''
        for the dems:
        CLASS
            STYLE
                COLORRANGE 0 0 0 255 255 255
                DATARANGE -100 3000
                RANGEITEM "[pixel]"
            END
        END
        '''

    #check for any mapfile settings
    #TODO: refactor this to make it nicer (woof)
    if src:
        mapsettings = src.map_settings[0] if src.map_settings else None
        if mapsettings:
            #check for the bands to use and their order
#            if mapsettings.settings and 'BANDS' in mapsettings.settings:
#                layer.setProcessing('BANDS='+mapsettings.settings['BANDS'])
#            if mapsettings.settings and 'LUT' in mapsettings.settings:
#                layer.setProcessing('LUT=' + mapsettings.settings['LUT'])
            processing_directives = mapsettings.get_processing()
            for directive in processing_directives:
                layer.setProcessing(directive)
                

            if mapsettings.classes:
                #do something with classes
                for c in mapsettings.classes:
                    cls = mapscript.classObj()
                    cls.name = c.name
                    #check for a style ref
                    if 'STYLE' in c.settings:
                        style_name = c.settings['STYLE']
                        style = DBSession.query(MapfileStyle).filter(MapfileStyle.name==style_name.replace('"', '')).first()
                        if not style:
                            continue
                        new_style = mapscript.styleObj()
                        #ew this is ugly so, you know, fix it
                        settings = style.settings
                        if 'RANGEMIN' in settings and 'RANGEMAX' in settings:
                            new_style.minvalue = float(settings['RANGEMIN'])
                            new_style.maxvalue = float(settings['RANGEMAX'])
                        if 'COLORMAX' in settings and 'COLORMIN' in settings:
                            #needs to be split into three integers
                            mincolor = [int(x) for x in settings['COLORMIN'].split(',')]
                            new_style.mincolor.setRGB(mincolor[0], mincolor[1], mincolor[2])
                            maxcolor = [int(x) for x in settings['COLORMAX'].split(',')]
                            new_style.maxcolor.setRGB(maxcolor[0], maxcolor[1], maxcolor[2])
                        if 'RANGEITEM' in settings:
                            new_style.rangeitem = settings['RANGEITEM']

                        cls.insertStyle(new_style)     
                        
                    layer.insertClass(cls)

            #TODO: add styles and make sure that they're available through the getstyles or whatever
            #       probably add styles first and then the class points to it instead of baking the style into the class
            #       like above
            if mapsettings.styles:
                #add the styles as the available styles
                
                pass
    layer.close()   
    return layer

#set the default contact info for edac
def set_contact_metadata(m):
    m.web.metadata.set('ows_contactperson', 'GStore Support')
    m.web.metadata.set('ows_contactposition', 'technical support')
    m.web.metadata.set('ows_contactinstructions', 'phone or email')
    m.web.metadata.set('ows_contactorganization', 'Earth Data Analysis Center')
    m.web.metadata.set('ows_address', 'Earth Data Analysis Center, MSC01 1110, 1 University of New Mexico')
    m.web.metadata.set('ows_contactvoicetelephone', '(505) 277-3622')
    m.web.metadata.set('ows_contactfacsimiletelephone', '(505) 277-3614')
    m.web.metadata.set('ows_contactelectronicmailaddress', 'devteam@edac.unm.edu')
    m.web.metadata.set('ows_addresstype', 'Mailing address')
    m.web.metadata.set('ows_hoursofservice', '9-5 MST, M-F')
    m.web.metadata.set('ows_role', 'data provider')

    m.web.metadata.set('ows_accessconstraints', 'none')
    m.web.metadata.set('ows_fees', 'None')
    m.web.metadata.set('ows_country', 'US')
    m.web.metadata.set('ows_stateorprovince', 'NM')
    m.web.metadata.set('ows_city', 'Albuquerque')  
    m.web.metadata.set('ows_postcode', '87131')

#the supported outputformats
def get_outputformat(fmt):
    #return the configured outputformat obj
    if fmt == 'png':
        of = mapscript.outputFormatObj('AGG/PNG', 'png')
        of.setExtension('png')
        of.setMimetype('image/png')
        of.imagemode = mapscript.MS_IMAGEMODE_RGBA
        of.transparent = mapscript.MS_TRUE
        of.setOption('GAMMA', '0.70')
    elif fmt == 'jpg':
        of = mapscript.outputFormatObj('AGG/JPEG', 'jpg')
        of.setExtension('jpg')
        of.setMimetype('image/jpeg')
        of.imagemode = mapscript.MS_IMAGEMODE_RGB
    elif fmt == 'gif':
        of = mapscript.outputFormatObj('GD/GIF', 'gif')
        of.setExtension('gif')
        of.setMimetype('image/gif')
        of.imagemode = mapscript.MS_IMAGEMODE_PC256
    elif fmt == 'tif':
        of = mapscript.outputFormatObj('GDAL/GTiff', 'GEOTIFF_16')
        of.setExtension('tif')
        of.setMimetype('image/tiff')

        #TODO: check on float32 vs. int16
        #of.imagemode = mapscript.MS_IMAGEMODE_FLOAT32
        of.imagemode = mapscript.MS_IMAGEMODE_INT16
    elif fmt == 'aaigrid':
        of = mapscript.outputFormatObj('GDAL/AAIGRID', 'AAIGRID')
        of.setExtension('grd')
        of.setMimetype('image/x-aaigrid')
        of.imagemode = mapscript.MS_IMAGEMODE_INT16
        #of.setOption('FILENAME','result.grd')
    else:
        of = None

    return of
    
'''
generic-ish mapserver response method
'''
def generateService(mapfile, params, mapname=''):
    #do all of the actual response here after the mapscript map 
    #has been read/built
    #should work with png or image/png as format

    #create the request
    request_type = params['request'] if 'request' in params else ''

    if not request_type and params:    
        return HTTPNotFound()

    request_type = request_type.lower()  
    req = mapscript.OWSRequest()

    '''
    http://129.24.63.66/gstore_v3/apps/rgis/datasets/a427563f-3c7e-44a2-8b35-68ce2a78001a/services/ogc/wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&LAYERS=mod10a1_a2002210.fractional_snow_cover&FORMAT=PNG&SRS=EPSG:4326&BBOX=-107.93,34.96,-104.99,38.53&WIDTH=256&HEIGHT=256

    '''

    #set up the params
    keys = params.keys()
    for k in keys:
        req.setParameter(k.upper(), params[k])

    #TODO add the featureinfo, getcoverage, getfeature bits

    fmt = params['format'] if 'format' in params else 'PNG'

    #now check the service type: capabilities, map, mapfile (for internal use)
    try:
        if request_type in ['getcapabilities', 'describecoverage', 'describelayer', 'getstyles', 'describefeature', 'getfeature']:
            #all of the xml/text responses
            mapscript.msIO_installStdoutToBuffer()
            mapfile.OWSDispatch(req)
            content_type = mapscript.msIO_stripStdoutBufferContentType()
            content = mapscript.msIO_getStdoutBufferBytes()

            if 'xml' in content_type:
                content_type = 'application/xml'            
            return Response(content, content_type=content_type)
        elif request_type in ['getmap', 'getlegendgraphic']:     
            #any image response  
            if request_type == 'getmap':
                mapfile.loadOWSParameters(req)
                img = Image.open(StringIO(mapfile.draw().getBytes()))
            else:
                img = Image.open(StringIO(mapfile.drawLegend().getBytes())) 
            
            buffer = StringIO()
            
            image_type = get_image_mimetype(fmt)
            if not image_type:
                image_type = ('PNG', 'image/png')
            img.save(buffer, image_type[0])
            
            buffer.seek(0)
            content_type = image_type[1]
            return Response(buffer.read(), content_type=content_type)
        elif request_type == 'getmapfile':
            #that's ours, we just want to save the mapfile to disk
            #and use the dataset id-source id.map for the filename to make sure it's unique
            #and make sure there isn't one first (chuck it if there is)
            mapfile.save(mapname)
            #TODO: make this some meaningful response (even though it isn't for public consumption)
            return Response('map generated')
        else:
            return HTTPNotFound('Invalid OGC request')
    except Exception as err:
        #TODO: catch just the mapserver errors?
        #let's try this for what's probably a bad set of params by service+request type
        return HTTPBadRequest(err)
        #return HTTPBadRequest()

    
'''
services:
wms
wfs
wcs

getmapfile (fo testing/checking purposes)
'''

#/apps/{app}/{type}/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/services/{service_type}/{service}
@view_config(route_name='services', match_param='type=datasets')
def datasets(request):
    #return Response()

    app = request.matchdict['app']
    dataset_id = request.matchdict['id']
    service_type = request.matchdict['service_type']
    service = request.matchdict['service']

    #get the query params because we like those
    params = normalize_params(request.params)

    #and get the type (if not there assume capabilities but really that should fail)
    ogc_req = params.get('request', 'GetCapabilities')

    #go get the dataset
    d = get_dataset(dataset_id)   

    if not d:
        return HTTPNotFound()

    if d.is_available == False:
        return HTTPNotFound('Temporarily unavailable')

    if service_type != 'ogc':
        return HTTPNotFound()


    #get some config stuff
    mappath = request.registry.settings['MAPS_PATH']
    tmppath = request.registry.settings['TEMP_PATH']
    srid = request.registry.settings['SRID']
    
    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = load_balancer

    #need to identify the data source file
    #so a tif, sid, ecw for a geoimage
    #or a shapefile for vector (or build if not there)
    if d.taxonomy == 'vector':
        #set up the mongo bits
        fmtpath = request.registry.settings['FORMATS_PATH']
        mconn = request.registry.settings['mongo_uri']
        mcoll = request.registry.settings['mongo_collection']
        mongo_uri = gMongoUri(mconn, mcoll)
    else:
        fmtpath = ''
        mongo_uri = None
        
    mapsrc, srcloc = d.get_mapsource(fmtpath, mongo_uri, int(srid)) # the source obj, the file path

    #need both for a raster, but just the file path for the vector (we made it!)
    if ((not mapsrc or not srcloc) and d.taxonomy == 'geoimage') or (d.taxonomy == 'vector' and not srcloc):
        return HTTPNotFound()


    #NOTE: skipping tilecache and just running with wms services here
    #maybe it's for the tile cache
    if service == 'wms_tiles':
        #baseurl, dataset, app, params, is_basemap = False
        #kargs = self._get_method_args()
        #return tilecache_service(base_url, d, app, params, request, False)
        service = 'wms'

    #get dataset BBOX from decimal
    bbox = [float(b) for b in d.box]

    #let's make sure the mapfile hasn't been cached already (dataset id.source id.map)

    #fake the mapsrc info for the dynamic vector data files
    if mapsrc:
        mapsrc_uuid = mapsrc.uuid
    else:
        #it's dynamic so it has none of this info so fake it
        mapsrc_uuid = '0'

    #get the mapfile 
    if os.path.isfile('%s/%s.%s.map' % (mappath, d.uuid, mapsrc_uuid)):
        #just read the mapfile and carry on
        m = mapscript.mapObj('%s/%s.%s.map' % (mappath, d.uuid, mapsrc_uuid))
    else: 
        #need to make a new mapfile

        #TODO: someday get the raster info to handle multiple bands, etc

        #running with mapscript
        #defaults from the original string template
        m = mapscript.mapObj()
        
        #set up the map WITH THE SRID BBOX (4326)
        init_proj = 'init=epsg:4326'
        
        #unless it's a grid
        if srid != d.orig_epsg and d.taxonomy == 'geoimage':
            #reproject the bbox
            bbox_geom = bbox_to_geom(bbox, int(srid))
            reproject_geom(bbox_geom, int(srid), d.orig_epsg)
            bbox = geom_to_bbox(bbox_geom)

            init_proj = 'init=epsg:%s' % (d.orig_epsg)
   
        m.setExtent(bbox[0], bbox[1], bbox[2], bbox[3])
        m.imagecolor.setRGB(255, 255, 255)
        m.setImageType('png24')
        #TODO: set from the default tile size unless there's a parameter, then use that
        m.setSize(600, 600)

        if 'epsg:4326' in init_proj:
            m.units = mapscript.MS_DD
            
        #TODO: set a good name for the data (remember how it appears in arc/qgis) and also, not all rgis
        m.name = '%s_Dataset' % (app.upper())

        m.setProjection(init_proj)

        #add some metadata
        srs_list = request.registry.settings['OGC_SRS'].split(',')
        m.web.metadata.set('ows_srs', build_supported_srs(d.orig_epsg, srs_list))
        #m.web.metadata.set('wms_srs', 'EPSG:4326 EPSG:4269 EPSG:4267 EPSG:26913 EPSG:26912 EPSG:26914 EPSG:26713 EPSG:26712 EPSG:26714')

        #enable the ogc services
        m.web.metadata.set('ows_enable_request', "*")

        #TODO: again, pick a decent name here
        m.web.metadata.set('ows_name', 'imagery_wms_%s' % (d.basename))
        m.web.metadata.set('wms_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wms' % (base_url, app, d.uuid))
        #m.web.metadata.set('OWS_SERVICE_ONLINERESOURCE', '%s/apps/%s/datasets/%s/services/ogc/wms' % (base_url, app, d.uuid))
        m.web.metadata.set('ows_abstract', 'WMS Service for %s dataset %s' % (app, d.description))

        m.web.metadata.set('wms_formatlist', 'image/png,image/gif,image/jpeg')
        m.web.metadata.set('wms_format', 'image/png')
        m.web.metadata.set('ows_keywordlist', '%s, New Mexico' % (app))

        #TODO: check on this
        m.web.metadata.set('wms_server_version', '1.3.0')

        #TODO: still more better names  
        m.web.metadata.set('ows_title', '%s Dataset (%s)' % (app, d.uuid))

        if d.taxonomy == 'geoimage':
            m.web.metadata.set('wcs_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wcs' % (base_url, app, d.uuid))
            m.web.metadata.set('wcs_label', 'imagery_wcs_%s' % (d.basename))
            m.web.metadata.set('wcs_name', 'imagery_wcs_%s' % (d.basename))
        if d.taxonomy == 'vector':
            m.web.metadata.set('wfs_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wfs' % (base_url, app, d.uuid))

        #add the edac info
        set_contact_metadata(m) 

        #set the paths
        m.mappath = mappath
        m.web.imageurl = tmppath
        m.web.imagepath = tmppath
        #TODO: set up real templates (this doesn't even exist)
        m.web.template = tmppath + '/client.html'

        #NOTE: no query format, legend format or browse format in mapscript

        #add the supported output formats
        #add an outputformat (start with png for kicks)
        of = get_outputformat('png')
        m.appendOutputFormat(of)

        #and the gif
        of = get_outputformat('gif')
        m.appendOutputFormat(of)

        #and the jpeg
        of = get_outputformat('jpg')
        m.appendOutputFormat(of)
        
        if service == 'wcs':
            #add geotif and ascii grid
            of = get_outputformat('tif')
            m.appendOutputFormat(of)

            of = get_outputformat('aaigrid')
            m.appendOutputFormat(of)
        #elif service == 'wfs':
            #add gml and ?
            #TODO:look into this
#            of = mapscript.outputFormatObj('OGR/GML', 'OGRGML')
#            of.setOption('STORAGE', 'memory')
#            of.setOption('FORM', 'multipart')
#            m.appendOutputFormat(of)

        #add a point symbol (REQUIRED for POINT)
        if d.taxonomy == 'vector' and d.geomtype == 'POINT':
            symbol = mapscript.symbolObj('circles')
            symbol.filled = mapscript.MS_TRUE
            symbol.type = mapscript.MS_SYMBOL_ELLIPSE
            line = mapscript.lineObj()
            line.add(mapscript.pointObj(1, 1, 0, 0))
            symbol.setPoints(line)
            symbol.sizex = 1
            symbol.sizey = 1
            #required to write it to the mapfile
            #if ogc_req == 'getmapfile':
            symbol.inmapfile = mapscript.MS_TRUE
            m.symbolset.appendSymbol(symbol)
            
        #add the legend
        lgd = mapscript.legendObj()
        lgd.imagecolor.setRGB(255, 255, 255)
        lgd.keysizex = 22
        lgd.keysizey = 12
        lgd.keyspacingx = 2
        lgd.keyspacingy = 7
        lgd.position = mapscript.MS_LL
        lgd.postlabelcache = mapscript.MS_TRUE
        lgd.status = mapscript.MS_ON
        
        lbl = mapscript.labelObj()
        lbl.antialias = mapscript.MS_TRUE
        lbl.font = 'Arial-Normal'
        lbl.maxsize = 256
        lbl.minsize = 4
        lbl.size = 1
        lbl.type = mapscript.MS_TRUETYPE
        #deprecated. do the other thing
        #lbl.backgroundcolor.setRGB(255,255,255)
        #lbl.backgroundshadowsizex = 2
        #lbl.backgroundshadowsizey = 2
        lbl.buffer = 0
        lbl.color.setRGB(0,0,0)
        lbl.force = mapscript.MS_FALSE
        lbl.mindistance = -1
        lbl.minfeaturesize = -1
        lbl.offsetx = 0
        lbl.offsety = 0
        lbl.partials = mapscript.MS_TRUE
        lgd.label = lbl
        m.legend = lgd
        
        #NOTE: no querymap in mapscript!?

        #NOTE: the scalebar is added by mapscript default
        
        #add the layer (with the reprj bbox)
        layer = getLayer(d, mapsrc, srcloc, bbox)

        #what the. i don't even. why is it adding an invalid tileitem?
        if layer.tileitem:
            layertileitem = ''
        m.insertLayer(layer)

    #post the results
    mapname = '%s/%s.%s.map' % (mappath, d.uuid, mapsrc_uuid)

    #to check the bands:
    #http://129.24.63.66/gstore_v3/apps/rgis/datasets/539117d6-bcce-4f16-b237-23591b353da1/services/ogc/wms?REQUEST=GetMap&SERVICE=WMS&VERSION=1.1.1&LAYERS=m_3110406_ne_13_1_20110512&BBOX=-104.316358581,31.9342908128,-104.246085056,32.0032463898
    
    return generateService(m, params, mapname)


#TODO: add apps to the tile indexes?
@view_config(route_name='services', match_param='type=tileindexes')
def tileindexes(request):
    '''
    build a tile index wms service based on the tile index view

    if the tile index collection contains datasets with multiple spatial references, build a set of layers per spatial reference    

    '''
    tile_id = request.matchdict['id']
    app = request.matchdict['app']
    service_type = request.matchdict['service_type']
    service = request.matchdict['service']

    params = normalize_params(request.params)

    #get the tile index by id/uuid
    tile = get_tileindex(tile_id)

    if not tile:
        return HTTPNotFound()

    if service_type.lower() != 'ogc':
        return HTTPNotFound()
        
    mappath = request.registry.settings['MAPS_PATH']
    tmppath = request.registry.settings['TEMP_PATH']
    srid = request.registry.settings['SRID']

    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = load_balancer
    
   
    #build the map file
    mapname = '%s/%s.tile.map' % (mappath, tile.uuid)

    if os.path.isfile(mapname):
        m = mapscript.mapObj(mapname)
    else:
        m = mapscript.mapObj()
        
        init_proj = 'init=epsg:%s' % (srid)
        bbox = [float(b) for b in tile.bbox]

        tile_epsgs = [int(e) for e in tile.epsgs]

        m.setExtent(bbox[0], bbox[1], bbox[2], bbox[3])
        m.imagecolor.setRGB(255,255,255)
        m.setImageType('png24')
        m.setSize(600, 600)

        if 'epsg:4326' in init_proj:
            m.units = mapscript.MS_DD

        m.name = '%s_TileIndex' % (app.upper())
        m.setProjection(init_proj)

        #add a bunch of metadata

        #we need to make sure that all of the srs for the tile index are in the supported srs list (i guess)
        check_list = request.registry.settings['OGC_SRS'].split(',')
        for epsg in tile_epsgs:
            supported_srs = build_supported_srs(epsg, check_list)
            check_list = supported_srs.split(' ')
        m.web.metadata.set('wms_srs', supported_srs)

        #use "!*" for the wms_enable_request to hide the index layer from getcapabilities
        m.web.metadata.set('wms_enable_request', '*')
        #m.web.metadata.set('wms_enable_request', '!*')

        m.web.metadata.set('wms_name', 'imagery_wms_%s' % (tile.id))
        m.web.metadata.set('wms_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wms' % (base_url, app, tile.uuid))
        m.web.metadata.set('wms_abstract', 'WMS Service for %s tile index %s' % (app, tile.name))

        set_contact_metadata(m)

        m.web.metadata.set('wms_formatlist', 'image/png,image/gif,image/jpeg')
        m.web.metadata.set('wms_format', 'image/png')
        m.web.metadata.set('ows_keywordlist', '%s, New Mexico' % (app))

        
        #TODO: check on this
        m.web.metadata.set('wms_server_version', '1.3.0')

        m.web.metadata.set('ows_title', '%s Tile Index (%s)' % (app, tile.uuid))

        #set the paths
        m.mappath = mappath
        m.web.imageurl = tmppath
        m.web.imagepath = tmppath
        #TODO: set up real templates (this doesn't even exist)
        m.web.template = tmppath + '/client.html'

        #add the output formats
        of = get_outputformat('png')
        m.appendOutputFormat(of)

        #and the gif
        of = get_outputformat('gif')
        m.appendOutputFormat(of)

        #and the jpeg
        of = get_outputformat('jpg')
        m.appendOutputFormat(of)

        #now for the tile index layers (at least two)
        for epsg in tile_epsgs:
            #we need to add two layers per spatial reference

            #reproject the bbox to the layer epsg for the correct extent
            box_geom = bbox_to_geom(bbox, int(srid))
            #reproject to the layer epsg
            reproject_geom(box_geom, int(srid), int(epsg))
            prj_bbox = geom_to_bbox(box_geom)
            
            tilename = '%s_%s' % (tile.basename, epsg)
            layer = mapscript.layerObj()
            layer.name = tilename
            layer.status = mapscript.MS_ON
            layer.setExtent(prj_bbox[0], prj_bbox[1], prj_bbox[2], prj_bbox[3])
            layer.setProjection('init=epsg:%s' % (epsg))
            layer.setProcessing('DITHER=YES')
            layer.metadata.set('layer_title', tilename)
            layer.metadata.set('base_layer', 'no')
            layer.metadata.set('wms_encoding', 'UTF-8')
            layer.metadata.set('wms_title', tilename)
            layer.metadata.set('imageformat', 'image/png')

            layer.tileindex = '%s_index' % (tilename)
            layer.tileitem = 'location'

            #TODO: modify if we ever use vector tile indexes
            if tile.taxonomy == 'raster':   
                layer.type = mapscript.MS_LAYER_RASTER
            else:
                pass

            m.insertLayer(layer)

            #and the index layer
            layer = mapscript.layerObj()
            layer.name = '%s_%s_index' % (tile.basename, epsg)
            layer.setProjection('init=epsg:%s' % (epsg))
            layer.setExtent(prj_bbox[0], prj_bbox[1], prj_bbox[2], prj_bbox[3])
            layer.metadata.set('layer_title', '%s_index' % tilename)
            layer.metadata.set('base_layer', 'no')
            layer.metadata.set('wms_encoding', 'UTF-8')
            layer.metadata.set('wms_title', '%s_index' % tilename)
            layer.metadata.set('imageformat', 'image/png')

            layer.type = mapscript.MS_LAYER_TILEINDEX
            layer.connectiontype = mapscript.MS_POSTGIS
            layer.setProcessing('CLOSE_CONNECTION=DEFER')

            #get the postgres connection
            connstr = request.registry.settings['sqlalchemy.url']
            psql = urlparse(connstr)
            #also, this is written into the mapfiles so hooray for security. or something
            layer.connection = 'dbname=%s host=%s port=%s user=%s password=%s' % (psql.path[1:], psql.hostname, psql.port, psql.username, psql.password)
            layer.data = 'the_geom from (select gid, st_transform(st_setsrid(geom, %s), %s) as the_geom, tile_id, description, location from gstoredata.get_tileindexes where tile_id = %s and orig_epsg = %s) as aview using unique gid using srid=%s' % (epsg, srid, tile.id, epsg, epsg)

            m.insertLayer(layer)

    
    return generateService(m, params, mapname)


#run the base layers for the mapper
@view_config(route_name='base_services')
def base_services(request):

    service = request.matchdict['service']
    app = request.matchdict['app']

    #get the query params because we like those
    params = normalize_params(request.params)

    #TODO: if the issue isn't with paste.request, maybe temporarily turn this off and just return from mapserver
    if service == 'wms_tiles':
#        host = request.host_url
#        g_app = request.script_name[1:]
#        base_url = '%s/%s' % (host, g_app)
#        #baseurl, dataset, app, params, config, is_basemap = False
#        kargs = request.environ
#        return tilecache_service(base_url, None, app, params, kargs, True)
        service = 'wms'

    #just point to the map file and do that
    maps_path = request.registry.settings['MAPS_PATH']

    basemap = '%s/base/base.map' % maps_path

    #open it and send it on to render the map
    m = mapscript.mapObj(basemap)

    return generateService(m, params, None)


#http://129.24.63.66/gstore_v3/apps/rgis/datasets/6965/services/ogc/wms_tiles?LAYERS=t13nr01e27_sw_image__6965&FORMAT=image%2Fpng&TRANSPARENT=true&MAXEXTENT=left-bottom%3D(333323.7916480107%2C3909958.3300974485)%20right-top%3D(334153.2481547035%2C3910799.439216765)&DISPLAYOUTSIDEMAXEXTENT=false&SINGLETILE=false&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES=&EXCEPTIONS=application%2Fvnd.ogc.se_inimage&SRS=EPSG%3A26913&BBOX=335245,3900994,337805,3903554&WIDTH=256&HEIGHT=256

'''
mapper
'''
#TODO: migrate to the interfaces only
@view_config(route_name='mapper', renderer='mapper.mako')
def mapper(request):
    #build the dict
    dataset_id = request.matchdict['id']
    app = request.matchdict['app']

    #go get the dataset
    d = get_dataset(dataset_id)   

    if not d:
        return HTTPNotFound()

    if d.is_available == False:
        return HTTPNotFound('Temporarily unavailable')

    '''
    what do we need:
    
    - url to the js/css
    - dataset description
    - dataset dateadded
    - dataset taxonomy
    - formats + url
    - services + url
    - metadata + url (v2)

    + the Layers and Description objects (for the javascript)
    '''

    media_url = request.registry.settings['MEDIA_URL']
    
#    host = request.host_url
#    g_app = request.script_name[1:]
#    base_url = '%s/%s/apps/%s/datasets/%s' % (host, g_app, app, d.uuid)

    load_balancer = request.registry.settings['BALANCER_URL']
    base_url = '%s/apps/%s/datasets/%s' % (load_balancer, app, d.uuid)

    c = {'MEDIA_URL': media_url, 'AppId': app}    

    svcs = []
    dlds = []
    fmts = d.get_formats(request)
    srcs = d.sources
    srcs = [s for s in srcs if s.active]
    
    taxonomy = d.taxonomy
    if taxonomy == 'vector':
        taxonomy = 'Vector Dataset'
        svcs =['wms', 'wfs']
        for f in fmts:
            sf = [s for s in srcs if s.extension == f]
            st = sf[0].set if sf else 'derived'
            dlds.append((st, f))
    elif taxonomy == 'geoimage':
        taxonomy = 'Raster image'
        svcs =['wms', 'wcs']

        dlds = [(s.set, s.extension) for s in srcs]
    elif taxonomy == 'services':
        taxonomy = 'Web Service'
    else:
        taxonomy = 'File'
        for f in fmts:
            sf = [s for s in srcs if s.extension == f]
            if sf:
                #if it's not in there, that's a whole other problem (i.e. why is it listed in the first place?)
                dlds.append((sf[0], f))
    
    c.update({'description': d.description, 'dateadded': d.dateadded, 'taxonomy': taxonomy})

    #lists need to be [{'title': '', 'text': url}, ..]
    fmts = []
    for ds in dlds:
        key = ds[1]
        key = 'kmz' if key == 'kml' else key
        url = '%s.%s.%s' % (base_url, ds[0], ds[1])
        fmts.append({'title': key, 'text': url})

    c.update({'formats': fmts})
        
    services = []
    for s in svcs:
        services.append({'title': s.upper(), 'text': '%s/services/ogc/%s?SERVICE=%s&REQUEST=GetCapabilities&VERSION=1.1.1' % (base_url, s, s)})
    c.update({'services': services})

    metadatas = []
    metadatas.append({'title': 'HTML', 'text': '%s/metadata/fgdc.html' % (base_url)})
    #metadatas.append({'title': 'TXT', 'text': '%s/metadata/fgdc.txt' % (base_url)})
    metadatas.append({'title': 'XML', 'text': '%s/metadata/fgdc.xml' % (base_url)})
    c.update({'metadata': metadatas})

    #add the layers
    lyrs = [{'layer': d.basename, 'id': d.id, 'title': d.description, 'features_attributes': [], 'maxExtent': [float(b) for b in d.box]}]
    c.update({'Layers': lyrs})

    #and the description
    dsc = {'what': 'dataset', 'title': d.description, 'id': d.id, 'singleTile': False, 'layers': [d.basename], 'services': services, 'formats': fmts, 'metadata': '', 'taxonomy': d.taxonomy, 'taxonomy_desc': taxonomy} 
    c.update({'Description': dsc})
    
    #and go render it
    return c








    
