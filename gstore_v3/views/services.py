from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPFound
from pyramid.threadlocal import get_current_registry

import os 
import Image
import mapscript
from cStringIO import StringIO

from ..models import DBSession
#from the generic model loader (like meta from gstore v2)
from ..models.datasets import (
    Dataset,
    )

from ..lib.database import *
from ..lib.spatial import tilecache_service


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

#TODO: REVISE FOR SLDs, RASTER BAND INFO, ETC
#TODO: figure out ecw as DATA displaying as an empty tile

#default syle objs
def getStyle(geomtype):
    s = mapscript.styleObj()
    s.symbol = 0
    if geomtype == 'MULTIPOLYGON':
        s.size = 1
        s.color.setRGB(180, 223, 238)
    elif geomtype == 'POLYGON':
        s.size = 1
        s.color.setRGB(180, 223, 238)
    elif geomtype == 'LINESTRING':
        s.size = 2
        s.color.setRGB(0, 0, 0)
    elif geomtype == 'POINT':
        s.size = 3
        s.color.setRGB(0, 0, 0)
    else:
        s.size = 3
        s.color.setRGB(0, 0, 0)

    return s

def getType(geomtype):
    if geomtype in ['MULTIPOLYGON', 'POLYGON']:
        return mapscript.MS_LAYER_POLYGON
    elif geomtype in ['LINESTRING', 'LINE']:
        return mapscript.MS_LAYER_LINE
    else:
        return mapscript.MS_LAYER_POINT

#get the layer obj by taxonomy (for now)
def getLayer(d, src, dataloc, bbox):
    layer = mapscript.layerObj()
    layer.name = d.basename
    layer.status = mapscript.MS_DEFAULT

    layer.data = dataloc
    layer.dump = mapscript.MS_TRUE

    layer.extent = bbox
    layer.units = mapscript.MS_DD

    layer.metadata.set('layer_title', d.basename)
    layer.metadata.set('ows_abstract', d.description)
    layer.metadata.set('ows_keywordlist', '') #TODO: something
    layer.metadata.set('legend_display', 'yes')
    layer.metadata.set('wms_encoding', 'UTF-8')
    layer.metadata.set('wms_title', d.basename)
    
    if d.taxonomy == 'vector':
        style = getStyle(d.geomtype)

        layer.setProjection('init=epsg:4326')
        layer.opacity = 50
        layer.type = getType(d.geomtype)
        layer.metadata.set('wms_srs', 'epsg:4326')
        layer.metadata.set('base_layer', 'no')
        layer.metadata.set('wms_encoding', 'UTF-8')
        layer.metadata.set('wms_title', d.basename)
        layer.metadata.set('gml_include_items', 'all')
        layer.metadata.set('gml_featureid', 'fid')

        #add the class (and the style)
        cls = mapscript.classObj()
        cls.name = 'Everything'
        cls.insertStyle(style)

        layer.insertClass(cls)
    elif d.taxonomy == 'geoimage':
        layer.setProjection('init=epsg:%s' % (d.orig_epsg))
        layer.setProcessing('CLOSE_CONNECTION=DEFER')
        layer.type = mapscript.MS_LAYER_RASTER
        layer.metadata.set('wms_srs', 'epsg:%s' % (d.orig_epsg))
        layer.metadata.set('queryable', 'no')
        layer.metadata.set('background', 'no')
        layer.metadata.set('time_sensitive', 'no')
        layer.metadata.set('raster_selected', 'yes')
        layer.metadata.set('static', 'no')
        layer.metadata.set('annotation_name', '%s: %s' % (d.basename, d.dateadded))
        layer.metadata.set('wcs_label', 'imagery_wcs_%s' % (d.basename))
        layer.metadata.set('wcs_formats', 'GTiff,PNG,JPEG,GIF,AAIGRID')
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
        
#        cls = mapscript.classObj()
#        #seriously, this must go
#        if d.basename[-5] == '__DEM':
#            s = mapscript.styleObj()
#            s.rangeitem = '[pixel]'
#            s.mincolor.setRGB(0,0,0)
#            s.maxcolor.setRGB(255, 255, 255)
#            s.minvalue = -100.0
#            s.maxvalue = 3000.0
#            cls.insertStyle(s)
#        cls.name = 'Everything'
#        layer.insertClass(cls)

    #check for any mapfile settings
    #TODO: change this to look for any processing flags
    #      then class flags (dem max/min, etc)
    #      and deal with those differently
    if src.map_settings:
        mapsettings = src.map_settings[0]
        if 'BANDS' in mapsettings.settings:
            layer.setProcessing('BANDS='+src.map_settings[0].settings['BANDS'])

        if mapsettings.classes:
            #do something with classes
            for c in mapsettings.classes:
                pass

            pass
            
        if mapsettings.styles:
            #do something different for the styles
            pass
       
    return layer

'''
generic-ish mapserver response method
'''
def generateService(mapfile, params, request_type, mapname=''):
    #do all of the actual response here after the mapscript map 
    #has been read/built

    #create the request
    req = mapscript.OWSRequest()
    #TODO: post the parameters that were given
    req.setParameter('SERVICE', 'WMS')
    req.setParameter('VERSION', '1.1.1') #check the version
    req.setParameter('REQUEST', request_type)

    #return Response(ogc_req)

    #now check the service type: capabilities, map, mapfile (for internal use)
    if request_type.lower() == 'getcapabilities':
        mapscript.msIO_installStdoutToBuffer()
        mapfile.OWSDispatch(req)
        content_type = mapscript.msIO_stripStdoutBufferContentType()
        content = mapscript.msIO_getStdoutBufferBytes()

        if 'xml' in content_type:
            content_type = 'application/xml'

        #TODO: double check all of this
        
        return Response(content, content_type=content_type)

    elif request_type.lower() == 'getmap':
        #TODO: set the parameters
        #TODO: and check for defaults before getting here, tidy this up
        req.setParameter('WIDTH', params.get('WIDTH', '256'))
        req.setParameter('HEIGHT', params.get('HEIGHT', '256'))

        #, ','.join([str(b) for b in dataset.box])
        req.setParameter('BBOX', params.get('BBOX', []))
        req.setParameter('STYLES', params.get('STYLES', ''))

        #dataset.basename
        layers = str(params['LAYERS']) if 'LAYERS' in params else ''
        #req.setParameter('LAYERS', params.get('LAYERS', ''))
        req.setParameter('LAYERS', layers)
        req.setParameter('FORMAT', params.get('FORMAT', 'image/png'))
        req.setParameter('SRS', params.get('SRS', 'EPSG:4326'))

        mapfile.loadOWSParameters(req)
        img = Image.open(StringIO(mapfile.draw().getBytes()))
        buffer = StringIO()
        #TODO: change png to the specified output type
        img.save(buffer, 'PNG')
        buffer.seek(0)
        #TODO: change content-type based on specified output type
        return Response(buffer.read(), content_type='image/png')
    elif request_type.lower() == 'getmapfile':
        #that's ours, we just want to save the mapfile to disk
        #and use the dataset id-source id.map for the filename to make sure it's unique
        #and make sure there isn't one first (chuck it if there is)
        mapfile.save(mapname)
        #TODO: make this some meaningful response (even though it isn't for public consumption)
        return Response('map generated')
    else:
        return HTTPNotFound('Invalid OGC request')
    
    #TODO: add the wcs/wfs methods (or at least the thing to make mapscript handle them)

    
'''
services:
getcapabilities (standard)
getmap (standard)
getmapfile (save mapfile to disk)
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
    params = request.params

    #and get the type (if not there assume capabilities but really that should fail)
    ogc_req = params.get('REQUEST', 'GetCapabilities')

    #go get the dataset
    d = get_dataset(dataset_id)   

    if not d:
        return HTTPNotFound('No results')

    if d.is_available == False:
        return HTTPNotFound('Temporarily unavailable')

    if service_type != 'ogc':
        return HTTPNotFound('just plain ogc requests today')


    #need to identify the data source file
    #so a tif, sid, ecw for a geoimage
    #or a shapefile for vector (or build if not there)
    mapsrc, srcloc = d.get_mapsource() # the source obj, the file path

    if not mapsrc or not srcloc:
        return HTTPNotFound('Invalid map source')

    #get some config stuff
    mappath = get_current_registry().settings['MAPS_PATH']
    tmppath = get_current_registry().settings['TEMP_PATH']
    srid = get_current_registry().settings['SRID']
    
    host = request.host_url
    g_app = request.script_name[1:]
    base_url = '%s/%s' % (host, g_app)

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
    #TODO: deal with the vector data source with no sources record (formats cache)
    if os.path.isfile('%s/%s.%s.map' % (mappath, d.uuid, mapsrc.uuid)):
        #just read the mapfile and carry on
        m = mapscript.mapObj('%s/%s.%s.map' % (mappath, d.uuid, mapsrc.uuid))
    else: 
        #need to make a new mapfile


        #TODO: reproject bbox to source epsg
        if srid != d.orig_epsg:
            #reproject the bbox
            pass

        #TODO: someday get the raster info to handle multiple bands, etc

        #running with mapscript
        #defaults from the original string template
        m = mapscript.mapObj()
        
        #set up the map
        m.setExtent(bbox[0], bbox[1], bbox[2], bbox[3])
        m.imagecolor.setRGB(255, 255, 255)
        m.setImageType('png24')
        #TODO: set from the default tile size unless there's a parameter, then use that
        m.setSize(600, 600)
        
        m.units = mapscript.MS_DD
        #TODO: set a good name for the data (remember how it appears in arc/qgis) and also, not all rgis
        m.name = '%s_Dataset' % (app.upper())

        m.setProjection('+init=epsg:4326')

        #add some metadata
        supported_srs = get_current_registry().settings['OGC_SRS'].replace(',', ' ')
        m.web.metadata.set('wms_srs', supported_srs)
        #m.web.metadata.set('wms_srs', 'EPSG:4326 EPSG:4269 EPSG:4267 EPSG:26913 EPSG:26912 EPSG:26914 EPSG:26713 EPSG:26712 EPSG:26714')

        #enable the ogc services
        m.web.metadata.set('ows_enable_request', "*")

        #TODO: again, pick a decent name here
        m.web.metadata.set('wms_name', 'imagery_wms_%s' % (d.basename))
        m.web.metadata.set('WMS_ONLINERESOURCE', '%s/apps/%s/datasets/%s/services/ogc/wms' % (base_url, app, d.uuid))
        m.web.metadata.set('WMS_ABSTRACT', 'WMS Service for %s dataset %s' % (app, d.description))

        if d.taxonomy == 'geoimage':
            m.web.metadata.set('WCS_ONLINERESOURCE', '%s/apps/%s/datasets/%s/services/ogc/wcs' % (base_url, app, d.uuid))
            m.web.metadata.set('wcs_label', 'imagery_wcs_%s' % (d.basename))
            m.web.metadata.set('wcs_name', 'imagery_wcs_%s' % (d.basename))
        if d.taxonomy == 'vector':
            m.web.metadata.set('WFS_ONLINERESOURCE', '%s/apps/%s/datasets/%s/services/ogc/wfs' % (base_url, app, d.uuid))
        
        #TODO: finish contact info
        m.web.metadata.set('ows_contactperson', '')
        m.web.metadata.set('ows_contactposition', '')
        m.web.metadata.set('ows_contactorganization', 'Earth Data Analysis Center')
        m.web.metadata.set('ows_address', 'Earth Data Analysis Center, MSC01 1110, 1 University of New Mexico')
        m.web.metadata.set('ows_contactvoicetelephone', '(505) 277-3622 ext. 230')
        m.web.metadata.set('ows_contactfacsimiletelephone', '(505) 277-3614')
        m.web.metadata.set('ows_contactelectronicmailaddress', '')
        m.web.metadata.set('ows_addresstype', 'Mailing address')


        m.web.metadata.set('wms_formatlist', 'image/png,image/gif,image/jpeg')
        m.web.metadata.set('wms_format', 'image/png')
        m.web.metadata.set('ows_keywordlist', '%s, New Mexico' % (app))
        m.web.metadata.set('ows_accesscontraints', 'none')
        m.web.metadata.set('ows_fees', 'None')

        #TODO: check on this
        m.web.metadata.set('wms_server_version', '1.3.0')

        m.web.metadata.set('ows_country', 'US')
        m.web.metadata.set('ows_stateorprovince', 'NM')
        m.web.metadata.set('ows_city', 'Albuquerque')  
        m.web.metadata.set('ows_postcode', '87131')

        #TODO: still more better names  
        m.web.metadata.set('ows_title', '%s Dataset (%s)' % (app, d.uuid))

        #set the paths
        m.mappath = mappath
        m.web.imageurl = tmppath
        m.web.imagepath = tmppath
        #TODO: set up real templates (this doesn't even exist)
        m.web.template = tmppath + '/client.html'

        #NOTE: no query format, legend format or browse format

        #add the supported output formats
        #add an outputformat (start with png for kicks)
        of = mapscript.outputFormatObj('AGG/PNG', 'png')
        of.setExtension('png')
        of.setMimetype('image/png')
        of.imagemode = mapscript.MS_IMAGEMODE_RGB
        of.transparent = mapscript.MS_ON
        of.setOption('GAMMA', '0.70')
        m.appendOutputFormat(of)

        #and the gif
        of = mapscript.outputFormatObj('GD/GIF', 'gif')
        of.setExtension('gif')
        of.setMimetype('image/gif')
        of.imagemode = mapscript.MS_IMAGEMODE_PC256
        m.appendOutputFormat(of)

        #and the jpeg
        of = mapscript.outputFormatObj('AGG/JPEG', 'jpg')
        of.setExtension('jpg')
        of.setMimetype('image/jpeg')
        of.imagemode = mapscript.MS_IMAGEMODE_RGB
        m.appendOutputFormat(of)
        if service == 'wcs':
            #add geotif and ascii grid
            of = mapscript.outputFormatObj('GDAL/GTiff', 'GEOTIFF_16')
            of.setExtension('tif')
            of.setMimetype('image/tiff')
            of.imagemode = mapscript.MS_IMAGEMODE_FLOAT32
            m.appendOutputFormat(of)

            of = mapscript.outputFormatObj('GDAL/AAIGRID', 'AAIGRID')
            of.setExtension('grd')
            of.setMimetype('image/x-aaigrid')
            of.imagemode = mapscript.MS_IMAGEMODE_INT16
            of.setOption('FILENAME','result.grd')
            m.appendOutputFormat(of)
        #elif service == 'wfs':
            #add gml and ?
            #TODO:look into this
#            of = mapscript.outputFormatObj('OGR/GML', 'OGRGML')
#            of.setOption('STORAGE', 'memory')
#            of.setOption('FORM', 'multipart')
#            m.appendOutputFormat(of)

    
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
    mapname = '%s/%s.%s.map' % (mappath, d.uuid, mapsrc.uuid)

    #TODO: check this some more
    #but make sure the required params have something 
#    if 'LAYERS' not in params:
#        params['LAYERS'] = d.basename
#    if 'BBOX' not in params:
#        params['BBOX'] = ','.join([str(b) for b in bbox])


    #to check the bands:
    #http://129.24.63.66/gstore_v3/apps/rgis/datasets/539117d6-bcce-4f16-b237-23591b353da1/services/ogc/wms?REQUEST=GetMap&SERVICE=WMS&VERSION=1.1.1&LAYERS=m_3110406_ne_13_1_20110512&BBOX=-104.316358581,31.9342908128,-104.246085056,32.0032463898
    
    return generateService(m, params, ogc_req, mapname)


@view_config(route_name='services', match_param='type=tileindexes')
def tileindexes(request):

    #build the map file

	return Response()


#run the base layers for the mapper
@view_config(route_name='base_services')
def base_services(request):

    service = request.matchdict['service']
    app = request.matchdict['app']

    #get the query params because we like those
    params = request.params

    #TODO: if the issue isn't with paste.request, maybe temporarily turn this off and just return from mapserver
    if service == 'wms_tiles':
        host = request.host_url
        g_app = request.script_name[1:]
        base_url = '%s/%s' % (host, g_app)
        #baseurl, dataset, app, params, config, is_basemap = False
        kargs = request.environ
        return tilecache_service(base_url, None, app, params, kargs, True)

    #just point to the map file and do that
    maps_path = get_current_registry().settings['MAPS_PATH']

    #TODO: change this so it isn't hardcoded in
    #/clusterdata/gstore/maps/base/base.map
    basemap = '%s/base/base.map' % maps_path
    #basemap = '/clusterdata/gstore/maps/base/base.map'

    #open it and send it on to render the map
    m = mapscript.mapObj(basemap)

    #and get the type (if not there assume capabilities but really that should fail)
    ogc_req = params.get('REQUEST', 'GetCapabilities')

    #return str(params['LAYERS'])

    return generateService(m, params, ogc_req, None)


#http://129.24.63.66/gstore_v3/apps/rgis/datasets/6965/services/ogc/wms_tiles?LAYERS=t13nr01e27_sw_image__6965&FORMAT=image%2Fpng&TRANSPARENT=true&MAXEXTENT=left-bottom%3D(333323.7916480107%2C3909958.3300974485)%20right-top%3D(334153.2481547035%2C3910799.439216765)&DISPLAYOUTSIDEMAXEXTENT=false&SINGLETILE=false&SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&STYLES=&EXCEPTIONS=application%2Fvnd.ogc.se_inimage&SRS=EPSG%3A26913&BBOX=335245,3900994,337805,3903554&WIDTH=256&HEIGHT=256

'''
mapper
'''
#TODO: migrate to the interfaces only
#TODO: Add html renderer for this
@view_config(route_name='mapper', renderer='mapper.mako')
def mapper(request):
    #build the dict
    dataset_id = request.matchdict['id']
    app = request.matchdict['app']

    #go get the dataset
    d = get_dataset(dataset_id)   

    if not d:
        return HTTPNotFound('No results')

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

    media_url = get_current_registry().settings['MEDIA_URL']
    
    host = request.host_url
    g_app = request.script_name[1:]
    base_url = '%s/%s/apps/%s/datasets/%s' % (host, g_app, app, d.uuid)


    c = {'MEDIA_URL': media_url, 'AppId': app}

    svcs = []
    dlds = []
    fmts = d.get_formats()
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
        services.append({'title': s.upper(), 'text': '%s/services/ogc/%s' % (base_url, s)})
    c.update({'services': services})

    metadatas = []
    metadatas.append({'title': 'HTML', 'text': '%s/metadata/fgdc.html' % (base_url)})
    metadatas.append({'title': 'TXT', 'text': '%s/metadata/fgdc.txt' % (base_url)})
    metadatas.append({'title': 'XML', 'text': '%s/metadata/fgdc.xml' % (base_url)})
    c.update({'metadata': metadatas})

    #TODO: fix all of this
    #add the layers
    lyrs = [{'layer': d.basename, 'id': d.id, 'title': d.description, 'features_attributes': [], 'maxExtent': [float(b) for b in d.box]}]
    c.update({'Layers': lyrs})

    #and the description
    dsc = {'what': 'dataset', 'title': d.description, 'id': d.id, 'singleTile': False, 'layers': [d.basename], 'services': services, 'formats': fmts, 'metadata': '', 'taxonomy': d.taxonomy, 'taxonomy_desc': taxonomy} 
    c.update({'Description': dsc})
    
    #and go render it
    return c








    
