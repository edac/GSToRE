# Copyright (c) 2010 University of New Mexico - Earth Data Analysis Center
# Author: Renzo Sanchez-Silva renzo@edac.unm.edu
# See LICENSE.txt for details.

import logging

from TileCache.Service import Service, wsgiHandler
from TileCache.Caches.Disk import Disk
from TileCache.Layers import WMS as WMS

log = logging.getLogger(__name__)


def wsgi_tilecache(model, app_id, config, kargs, is_base = False):
    disk_cache_base = config.get('TILECACHE_PATH')
    epsg = config.get('MAP_EPSG')
    extent = config.get('EXTENT_BBOX')
    resolutions = config.get('RESOLUTIONS')
    size = config.get('TILESIZE')
    base_url = config.get('BASE_URL')

    True_WMS = base_url + '/apps/' + app_id + '/datasets/%s/services/ogc/wms'
    
    if 'format' in kargs.keys():
        format = kargs['format']
    elif 'FORMAT' in kargs.keys():
        format = kargs['FORMAT']
    else:
        format = ''		

    if format == 'image/jpeg':
        extension = 'jpeg'
    elif format == 'image/gif':
        extension = 'gif'
    else:
        extension = 'png'

    if is_base is True:
        basename = 'naturalearthsw,southwestutm,nmcounties,Highways'
        true_WMS = True_WMS  % 'base'
        layers = ['naturalearthsw', 'southwestutm','nmcounties','Highways' ]
        def make_wms_layer(layer, epsg):
            return WMS.WMS(
                layer,          
                true_WMS,
                srs= "EPSG:%s" % epsg,
                extension = extension,
                resolutions = resolutions,
                bbox = extent,  
                data_extent = extent, 
                size = size,
                debug = False, 
                extent_type = 'loose'
            )
        d = {}
        for layer in layers:
            d[layer] = make_wms_layer(layer, epsg)
        d[','.join(layers)] = make_wms_layer(','.join(layers), epsg)
            

        tileService = Service(
            Disk(disk_cache_base),	
            d	
        )
    else:
        dataset = model.dataset 
        basename = dataset.basename
        true_WMS = True_WMS  % dataset.id
        tileService = Service(
          Disk(disk_cache_base),
          {  
            basename: WMS.WMS(
                basename, 
                true_WMS,
                srs = "EPSG:%s" % epsg,
                extension = extension,
                bbox = extent,
                data_extent = extent, 
                # 1000, 500, 250 good for MODIS
                # 30, 10 Landsat
                # 1 and 6" = 0.1524 DOQQ
                resolutions = resolutions,
                size = size,
                debug = False, 
                extent_type = 'loose'
            )
          }
        )
        
    #log.debug(str(basename))

    return wsgiHandler(kargs['environ'], kargs['start_response'], tileService)

# cgi example
##!/usr/bin/python
#
#from TileCache.Service import Service, modPythonHandler, cgiHandler
#from TileCache.Caches.Disk import Disk as DiskCache
#import TileCache.Layers.WMS as WMS
#
#
#basename = 'doqq05_05_32106a16'
#url = 'http://129.24.63.99:5000/dataset/ogc/wms/10100'
#
#srs26913 = 'EPSG:26913'
#
#layer = WMS.WMS(basename, url, 
#    extension = 'png', 
#    srs = srs26913, 
#    bbox='-235635,3196994,1032202,4437481',
#    # 1000, 500, 250 good for MODIS
#    # 30, 10 Landsat
#    # 1 and 6" = 0.1524 DOQQ
#    resolutions ='2000,1800,1600,1400,1200,1000,500,250,30,10,1,0.1524',
#    size = '256,256',
##   metaTile = 'true',
##   metaBuffer = 10000,
#    extent_type = 'loose'
#)
#
#layers = { basename : layer
#     }
#
#tileservice = Service( DiskCache('/tmp/tilecache'), layers = layers )
#def handler (req):
#
#    return modPythonHandler(req, tileservice)
#
#if __name__ == '__main__':
#    cgiHandler(tileservice)
#
