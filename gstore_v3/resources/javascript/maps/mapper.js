var MAP_EPSG = 26913;


var mapProjection = new OpenLayers.Projection('EPSG:26913');
var origProjection = new OpenLayers.Projection('EPSG:4326');

var mapPanel;

Ext.onReady(function() {

	tilesize = new OpenLayers.Size(256,256);
	extent = new OpenLayers.Bounds(-235635,3196994,1032202,4437481); 
	var options = {
		controls: [],
		projection: "EPSG:"+ MAP_EPSG,
		units: "meters",
		tileSize: tilesize,
		resolutions: [ 2000,1800,1600,1400,1200,1000,500,250,30,10,1,0.1524],
		maxExtent: extent,
        restrictedExtent: extent
	 };

	map = new OpenLayers.Map('map', options);

	var tile_format = 'image/png';
	if ( Ext.isIE6 ){
		tile_format = 'image/gif';
	}

	rgis_base = new OpenLayers.Layer.WMS(
		"GSTORE Base", 
		"http://gstore.unm.edu/apps/rgis/datasets/base/services/ogc/wms?",
		{ 
                       layers: 'naturalearthsw,southwestutm,nmcounties',

			format: 'image/jpeg', 
			isBaseLayer : true
		}
	);
	map.addLayer(rgis_base);

	pointmLayer = new OpenLayers.Layer.Markers('Last  LatLon Search');
	map.addLayer(pointmLayer);

    layerExtent = new OpenLayers.Bounds(Layers[0].maxExtent[0], Layers[0].maxExtent[1], Layers[0].maxExtent[2], Layers[0].maxExtent[3]);
    layerExtent.transform(origProjection, mapProjection);
    var extentLayer = new OpenLayers.Layer.Boxes('Extent layer');
    extentLayer.addMarker(new OpenLayers.Marker.Box(layerExtent, 'blue', 2));
    map.addLayer(extentLayer);

//    var base_wms_url = "/apps/" + AppId + "/datasets/" + Description.id + "/services/ogc/wms_tiles?";
    var base_wms_url = Description.wms;
    var currentLayer = new OpenLayers.Layer.WMS( Description.title, base_wms_url, {
        layers: Description.layers,
        format: tile_format,
        transparent: true,
        maxExtent: layerExtent,
        displayOutsideMaxExtent: false,
        singleTile: Description.singleTile
    });

    map.addLayer(currentLayer);

    // Controls
	var mp = new OpenLayers.Control.MousePosition({
		displayProjection : origProjection,
		//numDigits:  2,
		//prefix : "Lon, Lat: ",
		formatOutput: function(lonlat){
			var lon = lonlat.lon.toFixed(4);
			var lat = lonlat.lat.toFixed(4);
			if(lon < 0) { 
				lon = lon * -1;
			}
			if(lat < 0) { 
				lat = lat * -1;
			}
			return 'Lon: ' + lon  + ', Lat: ' + lat;
		}
	});
	map.addControl(mp);

    new Ext.Viewport({
        layout: "border",
        items: [{
            region: "center",
            id: "mappanel",
            xtype: "gx_mappanel",
            height: 630,
            width: 630,
            map: map,
            tbar: new Ext.Toolbar(),
            center: layerExtent.getCenterLonLat(), 
            split: true
        }, {
            region: "south",
            title: "Description and available tools",
            contentEl: "description",
            collapsible: true,
            height: 130,
            split: true
        }]
    });

    mapPanel = Ext.getCmp("mappanel");

    zoomIncontrol =  new OpenLayers.Control.ZoomBox({
        title: 'Zoom in: click in the map or use the left mouse button and drag to create a rectangle'
    });
    mapPanel.map.addControl(zoomIncontrol);

    zoomOutcontrol =  new OpenLayers.Control.ZoomBox({
        out: true,
        title: 'Zoom out: click in the map or use the left mouse button and drag to create a rectangle'
    });
    mapPanel.map.addControl(zoomOutcontrol);

    handtool = new OpenLayers.Control.DragPan({
        isDefault: true,
        title: 'Pan map: keep the left mouse button pressed and drag the map'
    });
    mapPanel.map.addControl(handtool);

    var zoomMaxButton = new Ext.Button({
        iconCls: 'zoomfull',
        handler: function(toggled){
            mapPanel.map.zoomToMaxExtent();
        }
    });
    mapPanel.getTopToolbar().addButton(zoomMaxButton);    

    var zoomInButton = new Ext.Button({
        enableToggle: true,
        iconCls: 'zoomin',
        toggleGroup: 'map',
        handler: function(toggled){
            if (toggled) {
                zoomIncontrol.activate();
            } else {
                zoomIncontrol.deactivate();
            }
        }
    });
    mapPanel.getTopToolbar().addButton(zoomInButton);

    var zoomOutButton = new Ext.Button({
        enableToggle: true,
        iconCls: 'zoomout',
        toggleGroup: 'map',
        handler: function(toggled){
            if (toggled) {
                zoomOutcontrol.activate();
            } else {
                zoomOutcontrol.deactivate();
            }
        }
    });

    mapPanel.getTopToolbar().addButton(zoomOutButton);

    var handtoolButton = new Ext.Button({
        enableToggle: true,
        iconCls: 'pan',
        toggleGroup: 'map',
        handler: function(toggled){
            if (toggled) {
                handtool.activate();
            } else {
                handtool.deactivate();
            }
        }
    });

    mapPanel.getTopToolbar().addButton(handtoolButton);
    
    map.zoomToExtent(layerExtent);

});
