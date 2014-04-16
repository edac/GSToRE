from pyramid.view import view_config
from pyramid.response import Response

from pyramid.httpexceptions import HTTPNotFound, HTTPFound, HTTPBadRequest, HTTPServerError

import os, sys
from email.parser import Parser 
from email.message import Message
from urlparse import urlparse

import urllib2
import json

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
from ..models.apps import GstoreApp

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
#TODO: REFACTOR LIKE ALL OF IT for "let's just keep bolting stuff on" reasons
#TODO: enjoy

'''
wcs methods to deal with oddness out of mapserver getcoverage response
''' 

#check for the geotiff chunk    
def isGeotiff(content_type):
    return content_type.split(';')[0].lower() in 'image/tiff'

#dammit freaking wcs 1.0.0 
def parse_tiff_response(content, content_type):
    '''
    this strips out just the tiff and returns the image 
    JUST FOR TESTING THE WCS because the clients are sketchy at best
    '''
    parser = Parser()
    parts = parser.parsestr("Content-type:%s\n\n%s" % (content_type, content.rstrip("--wcs--\n\n"))).get_payload()
    for p in parts:
        try:
            if isGeotiff(p.get_content_type()):
                return p.get_payload(), p.items()
        except:
            raise 
    return None, None
'''
end wcs section
'''

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
        s.width = 1
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
def getLayer(d, src, dataloc, bbox, metadata_description={}):
    valid_basename = 'g_' + d.basename if d.basename[0] in '0123456789' else d.basename

    layer = mapscript.layerObj()
    layer.name = valid_basename
    layer.status = mapscript.MS_ON

    layer.data = dataloc

    layer.setExtent(bbox[0], bbox[1], bbox[2], bbox[3]) 

    layer.metadata.set('layer_title', valid_basename)
#    layer.metadata.set('ows_abstract', d.description)
    layer.metadata.set('ows_keywordlist', '') #TODO: something
    layer.metadata.set('legend_display', 'yes')
    layer.metadata.set('wms_encoding', 'UTF-8')
    layer.metadata.set('ows_title', d.description)
#    layer.metadata.set('ows_name', d.basename)

    if metadata_description:
        service = metadata_description['service']

        #for whatever reason, the wcs layer metadata tags are not the same as the wms/wfs tags 
        flag = 'link' if service == 'wcs' else 'url'

        layer.metadata.set('%s_metadata%s_href' % (service, flag), metadata_description['url'])
        layer.metadata.set('%s_metadata%s_format' % (service, flag), metadata_description['mimetype'])
        layer.metadata.set('%s_metadata%s_type' % (service, flag), metadata_description['standard'])
    
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
        layer.metadata.set('wfs_encoding', 'UTF-8')
        layer.metadata.set('wms_title', valid_basename)
        layer.metadata.set('gml_include_items', 'all')
        layer.metadata.set('wms_include_items', 'all')
        layer.metadata.set('gml_featureid', 'FID') 

        #add the class (and the style)
        cls = mapscript.classObj()
        cls.name = 'Basic'
        cls.insertStyle(style)

        layer.insertClass(cls)

        #add the template (generic) for the vector getfeatureinfo
        #WHICH IS DOING NOTHING
        layer.template = metadata_description['template_path'] + '/' + 'generic_vector.txt'

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
        layer.metadata.set('annotation_name', '%s: %s' % (valid_basename, d.dateadded))
        layer.metadata.set('wcs_label', valid_basename)
        layer.metadata.set('wcs_formats', 'GEOTIFF_16')
  
        #TODO: change the native format - not everything is a geotiff now
        #layer.metadata.set('wcs_nativeformat', 'GTiff')
        layer.metadata.set('wcs_rangeset_name', valid_basename)
        layer.metadata.set('wcs_rangeset_label', d.description)
        layer.metadata.set('wcs_enable_request', '*')

        
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
    #TODO: how does this handle vector classes? whoops, it doesn't.
    if src:
        mapsettings = src.map_settings[0] if src.map_settings else None
        if mapsettings:
            processing_directives = mapsettings.get_processing()
            for directive in processing_directives:
                layer.setProcessing(directive)

            if d.taxonomy == 'geoimage' and 'WCS-NODATA' in mapsettings.settings:
                #add the wcs nullvalue flag
                nodata = mapsettings.settings['WCS-NODATA']
                layer.metadata.set('wcs_rangeset_nullvalue', nodata)

            other_flags = mapsettings.get_flags()
            for k, v in other_flags.iteritems():
                if k == 'FORMATS':
                    layer.metadata.set('wcs_formats', v)

            mapclasses = mapsettings.classes if mapsettings.classes else None    

            if mapclasses:
                #do something with classes
                for c in mapclasses:
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
#            if mapsettings.styles:
#                #add the styles as the available styles
#                
#                pass
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
    m.web.metadata.set('ows_contactelectronicmailaddress', 'gstore@edac.unm.edu')
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
        '''
        doesn't change the purple:
        -rgba to rgb
        turning off gamma and/or transparency

        '''
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
    elif fmt == 'tif' or fmt == 'GEOTIFF_16':
        #for integer rasters
        of = mapscript.outputFormatObj('GDAL/GTiff', 'GEOTIFF_16')
        of.setExtension('tif')
        of.setMimetype('image/tiff')
        of.imagemode = mapscript.MS_IMAGEMODE_INT16
    elif fmt == 'tif32' or fmt == 'GEOTIFF_FLOAT32':
        #for the floating point rasters like landsat img
        of = mapscript.outputFormatObj('GDAL/GTiff', 'GEOTIFF_FLOAT32')
        of.setExtension('tif')
        of.setMimetype('image/tiff')
        of.imagemode = mapscript.MS_IMAGEMODE_FLOAT32
    elif fmt == 'aaigrid':
        of = mapscript.outputFormatObj('GDAL/AAIGRID', 'AAIGRID')
        of.setExtension('grd')
        of.setMimetype('image/x-aaigrid')
        of.imagemode = mapscript.MS_IMAGEMODE_INT16
        #of.setOption('FILENAME','result.grd')
    else:
        of = None

    return of


def generateService(mapfile, params, mapname=''):
    """Return the Mapserver CGI response based on any mapfile

    Supports WMS, WFS, WCS.
    If mapserver error, dumps the XML response

    Magic methods: 
        GetTiffCoverage (strip out the tiff from the multipart mime response)
        GetMapFile (generate the mapfile to cache or, with param "show=", will display
            mapfile in browser)

    Args:
        mapfile (mapscript.mapObj): the generated mapfile
        params (dict): the query parameters from the request, normalized to lowercase
        mapname (string, optional): name of the mapfile if GetMapFile

    Returns:
        response (Response): The mapserver CGI output. This can include errors from mapserver!

    Raises:
        HTTPNotFound: Requires something in the params dict and that there's a REQUEST key
        HTTPServerError: Returns 500 server error if the error is with gstore or a generated mapserver error

    """

    #create the request
    request_type = params['request'] if 'request' in params else ''
    if not request_type and params:    
        return HTTPNotFound()
    request_type = request_type.lower()  

    mapscript.msIO_installStdoutToBuffer()
    req = mapscript.OWSRequest()

    #reset to run the standard getcoverage request against the cgi
    if request_type == 'gettiffcoverage':
        params['request'] = 'getcoverage'

    #set up the params
    for k, v in params.iteritems():
        #this seems bad. for many reasons bad. but it decodes stuff
        val = urllib2.unquote(urllib2.unquote(v).decode('unicode_escape')) 
        req.setParameter(k.upper(), val)

    #for the bots
    header={'X-Robots-Tag': 'noindex'}

    try:
        # check for the special methods (getmapfile, gettiffcoverage)
        if request_type == 'getmapfile':
            #TODO: change this to the mapobj to string method in the new mapscript version
            if not os.path.isfile(mapname):
                mapfile.save(mapname)
            if 'show' in params:
                #TODO: this may not be the best idea but it makes for easier debugging 
                #      and that has never caused anyone grief ever. right.
                with open(mapname, 'r') as f:
                    mr = f.read()
            else:
                mr = 'Mapfile generated.'
            return Response(mr, headers=header)

        # all the other supported mapserver options  
        # add noindex to all
        mapfile.OWSDispatch(req)    
        content_type = mapscript.msIO_stripStdoutBufferContentType()
        content = mapscript.msIO_getStdoutBufferBytes()  

        if 'xml' in content_type:
            content_type = 'application/xml'
          
        if request_type == 'gettiffcoverage':
            # strip out the tiff part of the multipart mime
            coverage = str(params['coverage']) if 'coverage' in params else 'output'
            if 'multipart/mixed' in content_type:
                tiff, headers = parse_tiff_response(content, content_type)
                output_headers = {}
                headers = [] if not headers else headers
                for h in headers:
                    if h[0].lower() not in ['content-disposition']:
                        output_headers[h[0]] = str(h[1])
                output_headers['Content-disposition'] = 'attachment; filename=%s.tif' % (coverage)
                output_headers = dict(header.items() + output_headers.items())
                return Response(tiff, headers=output_headers)

        #TODO: i am not sure these are the best filters for the clusterf*** of wcs stupidity.
        if request_type == 'getcoverage' and params['version'] != '1.0.0':
            #get the correct headers from the response
            output_headers = {}
            output_headers['Content-ID'] = 'coverage/out.tif'
            output_headers['Content-Description'] = 'coverage data'
            output_headers['content-transfer-encoding'] = 'binary'
            content_type = "multipart/mixed; boundary=wcs"          
            header = dict(header.items() + output_headers.items())
        elif request_type == 'getcoverage' and params['version'] == '1.0.0':
            header.update({'content-transfer-encoding': 'binary'})

        #TODO: also not sure about the setting of headers for all the services/versions
        # BUT wfs seems to be working, getlegendgraphic is working, errors are working, wms is working
        #normal processing applies   
        header.update({'Content-Type': content_type})
        return Response(content, content_type=content_type, headers=header)

    except Exception as err:
        #these shouldn't be mapserver errors at this point, but some gstore problem
        return HTTPServerError(err)


#/apps/{app}/{type}/{id:\d+|[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}}/services/{service_type}/{service}
@view_config(route_name='services', match_param='type=datasets')
def datasets(request):
    """Generate the dataset mapserver service

    Dynamically builds the mapfile and sends to generateService

    Args:

    Returns:
        response (Response): returns the result of the generateService method   

    Raises:
        HTTPNotFound: requires acitve, unembargoed spatial dataset
        HTTPServerError: likely propagated 500 error from generateService

    """

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

    if not d or d.inactive or d.is_embargoed:
        return HTTPNotFound()

    if d.is_available == False:
        return HTTPNotFound('Temporarily unavailable')

    if service_type != 'ogc':
        return HTTPNotFound()

    if d.taxonomy in ['file', 'services', 'table']:
        return HTTPNotFound('OGC Web Services not supported for this dataset')

    #get some config stuff
    mappath = request.registry.settings['MAPS_PATH']
    tmppath = request.registry.settings['TEMP_PATH']
    templatepath = request.registry.settings['MAP_TEMPLATE_PATH']
    srid = request.registry.settings['SRID']
    xslt_path = request.registry.settings['XSLT_PATH']
    base_url = request.registry.settings['BALANCER_URL']
   
    supported_standards = d.get_standards(request)

    req_app = DBSession.query(GstoreApp).filter(GstoreApp.route_key==app.lower()).first()
    if not req_app:
        app_prefs = ['FGDC-STD-001-1998','FGDC-STD-012-2002','ISO-19115:2003']
    else:
        app_prefs = req_app.preferred_metadata_standards    
    std = next(s for s in app_prefs if s in supported_standards)

    #need to identify the data source file
    #so a tif, sid, ecw for a geoimage
    #or a shapefile for vector (or build if not there)
    if d.taxonomy == 'vector':
        #set up the mongo bits
        fmtpath = request.registry.settings['FORMATS_PATH']
        mconn = request.registry.settings['mongo_uri']
        mcoll = request.registry.settings['mongo_collection']
        mongo_uri = gMongoUri(mconn, mcoll)

        metadata_info = {'app': app, 
            'base_url': base_url, 
            'standard': std, 
            'xslt_path': xslt_path + '/xslts', 
            'validate': False, 
            'request': request
        }
    else:
        fmtpath = ''
        mongo_uri = None
        metadata_info = None

    mapsrc, srcloc = d.get_mapsource(fmtpath, mongo_uri, int(srid), metadata_info) # the source obj, the file path

    #need both for a raster, but just the file path for the vector (we made it!)
    if ((not mapsrc or not srcloc) and d.taxonomy == 'geoimage') or (d.taxonomy == 'vector' and not srcloc):
        return HTTPNotFound()


    #NOTE: the wms_tiles has been deprecated but some older bots? still ping it
    service = 'wms' if service == 'wms_tiles' else service

    #get dataset BBOX from decimal
    bbox = [float(b) for b in d.box]

    #fake the mapsrc info for the dynamic vector data files
    if mapsrc:
        mapsrc_uuid = mapsrc.uuid
    else:
        #it's dynamic so it has none of this info so fake it
        mapsrc_uuid = '0'

    #get the mapfile 
    if os.path.isfile('%s/%s.%s.map' % (mappath, d.uuid, mapsrc_uuid)):
        #just read the mapfile and carry on
        #NOTE: do not assume that these files are permanent in any way
        #      this is temporary storage for testing basically
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

        if not check_for_valid_extent(bbox):
            #the extent area == 0, it is invalid for mapserver/mapscript
            bbox = buffer_point_extent(bbox, 0.00001)
   
        m.setExtent(bbox[0], bbox[1], bbox[2], bbox[3])
        m.imagecolor.setRGB(255, 255, 255)
        m.setImageType('png24')
        m.setSize(600, 600)

        valid_basename = 'g_' + d.basename if d.basename[0] in '0123456789' else d.basename

        if 'epsg:4326' in init_proj:
            m.units = mapscript.MS_DD
            
        m.name = valid_basename

        m.setProjection(init_proj)

        #add some metadata
        srs_list = request.registry.settings['OGC_SRS'].split(',')
        m.web.metadata.set('ows_srs', build_supported_srs(d.orig_epsg, srs_list))

        #enable the ogc services (except for getfeatureinfo for the rasters)
        ows_requests = '*' if d.taxonomy.lower() != 'geoimage' else '* !getFeatureInfo'
        m.web.metadata.set('ows_enable_request', ows_requests)

        m.web.metadata.set('ows_name', '%s' % (valid_basename))
        m.web.metadata.set('wms_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wms' % (base_url, app, d.uuid))

        #wcs getcapabilities needs this tag
        m.web.metadata.set('ows_service_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wms' % (base_url, app, d.uuid))
        m.web.metadata.set('ows_abstract', '%s Service for %s dataset %s (%s)' % (service.upper(), app.upper(), d.description, d.uuid))

        m.web.metadata.set('wms_formatlist', 'image/png,image/gif,image/jpeg')
        m.web.metadata.set('wms_format', 'image/png')
        m.web.metadata.set('ows_keywordlist', '%s, New Mexico' % (app.upper()))

        #TODO: check on this
        m.web.metadata.set('wms_server_version', '1.3.0')

        m.web.metadata.set('ows_title', valid_basename)

        if d.taxonomy == 'geoimage':
            m.web.metadata.set('wcs_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wcs' % (base_url, app, d.uuid))
            m.web.metadata.set('wcs_label', valid_basename)
            m.web.metadata.set('wcs_name', valid_basename)
        if d.taxonomy == 'vector':
            m.web.metadata.set('wfs_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wfs' % (base_url, app, d.uuid))

        #add the edac info
        set_contact_metadata(m) 

        #set the paths
        m.mappath = mappath
        m.web.imageurl = tmppath
        m.web.imagepath = tmppath
        m.web.template = templatepath + '/client.html'

        #NOTE: no query format, legend format or browse format in mapscript

        #add the supported output formats
        #TODO: change this to include the formats for the WCS correctly
        output_formats = ['png', 'gif', 'jpg']
        for output_format in output_formats:
            of = get_outputformat(output_format)
            m.appendOutputFormat(of)
        
        mapsettings = mapsrc.map_settings[0] if mapsrc and mapsrc.map_settings else None

        additional_formats = []
        if mapsettings:
            flags = mapsettings.get_flags()
            if 'FORMATS' in flags:
                additional_formats += flags['FORMATS'].split(',')
        if service == 'wcs' and not additional_formats:
            #just add the 16bit integer as the default
            additional_formats.append('tif')
            
        for additional_format in additional_formats:
            of = get_outputformat(additional_format)
            m.appendOutputFormat(of)

            
            #TODO: turn this back on once the freaky weird ascii issue is resolved 
            '''
            the services to test with
            > curl --globoff "http://129.24.63.109/gstore_v3/apps/rgis/datasets/0f3ca80c-2d50-4a33-8df8-c80ff9e94588/services/ogc/wcs?VERSION=1.1.2&SERVICE=WCS&REQUEST=GetCoverage&COVERAGE=mod10a1_a2002193.fractional_snow_cover&CRS=EPSG:4326&FORMAT=image/tiff&HEIGHT=500&WIDTH=500&BBOX=-107.930153271352,34.9674233731823,-104.994718803013,38.5334870384629" > wcs_ae

            > curl --globoff "http://129.24.63.109/gstore_v3/apps/rgis/datasets/0f3ca80c-2d50-4a33-8df8-c80ff9e94588/services/ogc/wcs?VERSION=1.1.2&SERVICE=WCS&REQUEST=GetCoverage&COVERAGE=mod10a1_a2002193.fractional_snow_cover&CRS=EPSG:4326&FORMAT=image/x-aaigrid&HEIGHT=500&WIDTH=500&BBOX=-107.930153271352,34.9674233731823,-104.994718803013,38.5334870384629&RangeSubset=mod10a1_a2002193.fractional_snow_cover:bilinear[bands[1]]" > wcs_af
            '''
#            of = get_outputformat('aaigrid')
#            m.appendOutputFormat(of)
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
            symbol.inmapfile = mapscript.MS_TRUE
            m.symbolset.appendSymbol(symbol)

        #add the fontset (arial only)
        m.setFontSet(templatepath + '/fontset.txt')

        #update the legend
        m.legend.imagecolor.setRGB(255, 255, 255)
        m.legend.keysizex = 22
        m.legend.keysizey = 12
        m.legend.keyspacingx = 2
        m.legend.keyspacingy = 7
        m.legend.position = mapscript.MS_LL
        m.legend.postlabelcache = mapscript.MS_TRUE
        m.legend.status = mapscript.MS_ON
        
        m.legend.label.antialias = mapscript.MS_TRUE
        m.legend.label.font = 'Arial'
        m.legend.label.maxsize = 256
        m.legend.label.minsize = 4
        m.legend.label.size = 12
        m.legend.label.type = mapscript.MS_TRUETYPE
        m.legend.label.buffer = 0
        m.legend.label.color.setRGB(0,0,0)
        m.legend.label.force = mapscript.MS_FALSE
        m.legend.label.mindistance = -1
        m.legend.label.minfeaturesize = -1
        m.legend.label.offsetx = 0
        m.legend.label.offsety = 0
        m.legend.label.partials = mapscript.MS_TRUE
        
        #NOTE: no querymap in mapscript!?

        #NOTE: the scalebar is added by mapscript default
        
        #add the layer (with the reprj bbox)
        load_balancer = request.registry.settings['BALANCER_URL']
        base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

        if not std:
            metadata_description = {}
        else:
            #now figure out the path for the xml for the chosen standard
            metadata_description = {'service': service, 
                'standard': std, 
                'mimetype': 'text/xml', 
                'url': '%s%s/metadata/%s.xml' % (base_url, d.uuid, std), 
                'template_path': templatepath #for the vector template location
            }
        layer = getLayer(d, mapsrc, srcloc, bbox, metadata_description)

        #what the. i don't even. why is it adding an invalid tileitem?
        #layer.tileitem = ''
        m.insertLayer(layer)

    #post the results
    mapname = '%s/%s.%s.map' % (mappath, d.uuid, mapsrc_uuid)

    #to check the bands:
    #http://129.24.63.66/gstore_v3/apps/rgis/datasets/539117d6-bcce-4f16-b237-23591b353da1/services/ogc/wms?REQUEST=GetMap&SERVICE=WMS&VERSION=1.1.1&LAYERS=m_3110406_ne_13_1_20110512&BBOX=-104.316358581,31.9342908128,-104.246085056,32.0032463898
    
    return generateService(m, params, mapname)

@view_config(route_name='services', match_param=('type=tileindexes','service_type=gstore','service=clip'))
def clip_tileindex(request):
    '''
    the magic of clip and zip

    from the ecws

    run a getcoverage (except just the tif part) wcs req 
    and dump it in a zip with some metadata?

    REQUIRED:
        bbox
        srs
        width/height

    TODO: time

    we will just make some assumptions about the layer name (it's just the one)
    
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

    if not tile.is_active:
        return HTTPNotFound()
        
    mappath = request.registry.settings['MAPS_PATH']
    tmppath = request.registry.settings['TEMP_PATH']
    templatepath = request.registry.settings['MAP_TEMPLATE_PATH']
    srid = request.registry.settings['SRID']

    data_path = request.registry.settings['BASE_DATA_PATH'] + '/tileindexes/%s' % tile.uuid

    load_balancer = request.registry.settings['BALANCER_URL']
   
    #build the map file
    mapname = '%s/%s.tile.map' % (mappath, tile.uuid)

    if os.path.isfile(mapname):
        m = mapscript.mapObj(mapname)
    else:
        srid_list = request.registry.settings['OGC_SRS'].split(',')
        
        init_params = {
            "srid": srid,
            "mappath": mappath,
            "tmppath": tmppath,
            "datapath": data_path,
            "base_url": load_balancer,
            "app": app,
            "srid_list": srid_list,
            "templatepath": templatepath
        }
        m = build_tileindex_mapfile(tile, init_params, params)

#> curl --globoff "http://129.24.63.109/gstore_v3/apps/rgis/datasets/0f3ca80c-2d50-4a33-8df8-c80ff9e94588/services/ogc/wcs?VERSION=1.1.2&SERVICE=WCS&REQUEST=GetCoverage&COVERAGE=mod10a1_a2002193.fractional_snow_cover&CRS=EPSG:4326&FORMAT=image/tiff&HEIGHT=500&WIDTH=500&BBOX=-107.930153271352,34.9674233731823,-104.994718803013,38.5334870384629" > wcs_ae

#msWCSGetCoverageDomain(): WCS server error. RASTER Layer with no DATA statement and no WCS virtual dataset metadata. Tileindexed raster layers not supported for WCS without virtual dataset metadata (cm->extent, wcs_res, wcs_size).

    #set up the wcs getcoverage (tiff part only)
    params['request'] = 'GetTiffCoverage'
    params['service'] = 'wcs'
    params['version'] = '1.1.2'
    params['coverage'] = ','.join([tile.basename + '_%s' % e for e in tile.epsgs])
    params['format'] = 'image/tiff'

    #return Response(json.dumps({'params': params}))

    #get the response object
    rsp =  generateService(m, params, mapname)

#    #check that the mimetype is geotiff (or what we expect it to be)
#    if rsp.content_type != 'image/tiff':
#        return HTTPServerError(rsp.content_type)

#    # get the tiff (as a string)
#    tif = rsp.body


    #build

    return rsp


#TODO: add apps to the tile indexes?
@view_config(route_name='services', match_param=('type=tileindexes', 'service_type=ogc'))
def tileindexes(request):
    '''
    build a tile index wms service based on the tile index view

    if the tile index collection contains datasets with multiple spatial references, build a set of layers per spatial reference    


http://129.24.63.115/apps/rgis/tileindexes/e305d3ed-9db2-4895-8a35-bde79150e272/services/ogc/wms?REQUEST=GetMap&SERVICE=WMS&VERSION=1.1.1&FORMAT=image/png&LAYERS=tile_mrcog_2006_2258&width=1000&height=1000&style=&SRS=epsg:2258&bbox=1462969,1371360,1467912,1376113
http://129.24.63.115/apps/rgis/tileindexes/e305d3ed-9db2-4895-8a35-bde79150e272/services/ogc/wms?REQUEST=GetMap&SERVICE=WMS&VERSION=1.1.1&FORMAT=image/png&LAYERS=tile_mrcog_2006_2258&width=1000&height=1000&style=&SRS=epsg:4326&bbox=-106.8509,34.7537,-106.8337,34.7668

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

    if not tile.is_active:
        return HTTPNotFound()
        
    mappath = request.registry.settings['MAPS_PATH']
    tmppath = request.registry.settings['TEMP_PATH']
    templatepath = request.registry.settings['MAP_TEMPLATE_PATH']
    srid = request.registry.settings['SRID']

    data_path = request.registry.settings['BASE_DATA_PATH'] + '/tileindexes/%s' % tile.uuid

    load_balancer = request.registry.settings['BALANCER_URL']
    
   
    #build the map file
    mapname = '%s/%s.tile.map' % (mappath, tile.uuid)

    if os.path.isfile(mapname):
        m = mapscript.mapObj(mapname)
    else:
        srid_list = request.registry.settings['OGC_SRS'].split(',')
        
        init_params = {
            "srid": srid,
            "mappath": mappath,
            "tmppath": tmppath,
            "datapath": data_path,
            "base_url": load_balancer,
            "app": app,
            "srid_list": srid_list,
            "templatepath": templatepath
        }
        m = build_tileindex_mapfile(tile, init_params, params)
        
    return generateService(m, params, mapname)


def build_tileindex_mapfile(tile, init_params, params):
    '''
    this is mildly stupid
    see TODOs
    shut up

    init_params = dict of paths from config, epsg, app
    params = query params for request
    '''
    m = mapscript.mapObj()

    init_proj = 'init=epsg:%s' % init_params['srid']
    app = init_params['app']
    
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
    check_list = init_params['srid_list']
    for epsg in tile_epsgs:
        supported_srs = build_supported_srs(epsg, check_list)
        check_list = supported_srs.split(' ')
    m.web.metadata.set('wms_srs', supported_srs)

    #use "!*" for the wms_enable_request to hide the index layer from getcapabilities
    m.web.metadata.set('ows_enable_request', '*')
    m.web.metadata.set('wcs_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wcs' % (init_params['base_url'], init_params['app'], tile.uuid))
    m.web.metadata.set('wcs_label', 'imagery_wcs_%s' % (tile.basename))
    m.web.metadata.set('wcs_name', 'imagery_wcs_%s' % (tile.basename))

    m.web.metadata.set('wms_name', 'imagery_wms_%s' % (tile.id))
    m.web.metadata.set('wms_onlineresource', '%s/apps/%s/datasets/%s/services/ogc/wms' % (init_params['base_url'], app, tile.uuid))
    m.web.metadata.set('wms_abstract', 'WMS Service for %s tile index %s' % (app, tile.name))

    set_contact_metadata(m)

    m.web.metadata.set('wms_formatlist', 'image/png,image/gif,image/jpeg')
    m.web.metadata.set('wms_format', 'image/png')
    m.web.metadata.set('ows_keywordlist', '%s, New Mexico' % (app))

    
    #TODO: check on this
    m.web.metadata.set('wms_server_version', '1.3.0')

    m.web.metadata.set('ows_title', '%s Tile Index (%s)' % (app, tile.uuid))

    #set the paths
    m.mappath = init_params['mappath']
    m.web.imageurl = init_params['tmppath']
    m.web.imagepath = init_params['tmppath']
    #TODO: set up real templates (this doesn't even exist)
    m.web.template = init_params['templatepath'] + '/client.html'

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
        box_geom = bbox_to_geom(bbox, int(init_params['srid']))
        #reproject to the layer epsg
        reproject_geom(box_geom, int(init_params['srid']), int(epsg))
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
        layer.metadata.set('wcs_extent', ' '.join([str(prj_bbox[0]), str(prj_bbox[1]), str(prj_bbox[2]), str(prj_bbox[3])]))

        #TODO: figure out what these should really be if not defaults (i.e. the band count - always 3?)
        layer.metadata.set('wcs_size', ' '.join([params['width'] if 'width' in init_params else '1000', params['height'] if 'height' in init_params else '1000']))
        layer.metadata.set('wcs_formats', 'GTiff')
        layer.metadata.set('wcs_bandcount', "3")

        #layer.tileindex = '%s_%s_index' % (tile.basename, epsg)
        layer.tileindex = init_params['datapath'] + '/tile_%s.shp' % epsg
        layer.tileitem = 'location'

        #TODO: modify if we ever use vector tile indexes
        if tile.taxonomy == 'raster':   
            layer.type = mapscript.MS_LAYER_RASTER
        elif tile.taxonomy == 'point':
            layer.type = mapscript.MS_LAYER_POINT
        elif tile.taxonomy == 'line':
            layer.type = mapscript.MS_LAYER_LINE
        elif tile.taxonomy == 'polygon':
            layer.type = mapscript.MS_LAYER_POLYGON
        else:
            pass

        m.insertLayer(layer)

    return m


@view_config(route_name='services', match_param='type=collections')
def collections(request):
    '''
    build a tile index wms service based on the spatial datasets in a collection   

    '''
    collection_id = request.matchdict['id']
    app = request.matchdict['app']
    service_type = request.matchdict['service_type']
    service = request.matchdict['service']

    params = normalize_params(request.params)

    collection = get_collection(collection_id)
    if not collection:
        return HTTPNotFound()

    if not collection.is_spatial:
        return HTTPBadRequest()

    return Response()


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
#TODO: figure out the issue with the cookie timing for the fileDownload plugin. the code is in the template
#      and is triggered to start but the cookie only shows up (in firebug) on reload. in rgis, the cookie
#      shows up immediately and the callback is triggered. wacky.
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

    if d.inactive or d.is_embargoed:
        return HTTPNotFound()

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

#    load_balancer = request.registry.settings['BALANCER_URL']
#    base_url = '%s/apps/%s/datasets/' % (load_balancer, app)

    base_url = request.registry.settings['BALANCER_URL']

    rsp = d.get_full_service_dict(base_url, request, app)

    c = {"MEDIA_URL": media_url, "AppId": app, "description": d.description, "dateadded": d.dateadded}

    taxonomy = d.taxonomy
    if taxonomy == 'vector':
        taxonomy = 'Vector Dataset'
    elif taxonomy == 'geoimage':
        taxonomy == 'Raster Image'
    else:
        #TODO: change this for the services, etc
        taxonomy = 'File'

    c.update({"taxonomy": taxonomy})

    downloads = [{"title": k, "text": v} for k, v in rsp['downloads'][0].iteritems()]

    key = [m for m in rsp['metadata'][0] if 'FGDC' in m]
    metadatas = []
    if key:
        s = rsp['metadata'][0][key[0]]
        metadatas = [{"title": k.upper(), "text": v} for k, v in s.iteritems()]

    services = [{"title": k.upper(), "text": v} for svc in rsp['services'] for k, v in svc.iteritems()]
    wms = [s for s in services if 'WMS' in s['title']]
    wms = wms[0]['text'].replace('GetCapabilities', 'GetMap') if wms else ''

    lyrs = [{'layer': d.basename, 'id': str(d.uuid), 'title': d.description, 'features_attributes': [], 'maxExtent': [float(b) for b in d.box]}]
    c.update({'Layers': lyrs, "metadata": metadatas, "services": services, "formats": downloads})

    dsc = {'what': 'dataset', 'title': d.description, 'id': str(d.uuid), 'singleTile': False, 'layers': [d.basename], 'services': services, 'formats': downloads, 'metadata': '', 'taxonomy': d.taxonomy, 'taxonomy_desc': taxonomy, 'wms': wms}

    c.update({'Description': dsc})
    
    #and go render it
    return c








    
