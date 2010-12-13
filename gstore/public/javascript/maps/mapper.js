var MAP_EPSG = 26913;


var map, tree, toolbar, rgis_base, vectorLayer, extent, actualProtocol, mapSearcher, bundleLayers, currentLayer, downloadMenu, pokieGrid, gridArea, store, searchStoreMediator, reader;

var kka, kkb, kkc, kkd;

var mapProjection = new OpenLayers.Projection('EPSG:26913');
var origProjection = new OpenLayers.Projection('EPSG:4326');

var advanced = false;
var layerExtent;

var totalCount = 100;

var refreshcurrentLayer = function() {
	currentLayer = tree.getChecked();
	//actualProtocol.options.url = "/datasets/"+ currentLayer[0].attributes.datasetid+"/features";
	actualProtocol.options.url = "/apps/rgis/datasets/"+ currentLayer[0].attributes.datasetid+"/features";
	actualProtocol.url = actualProtocol.options.url;
	x = Ext.getCmp('pokiegrid');
	ding = new Ext.grid.ColumnModel(currentLayer[0].attributes.grid_columns);
}


var showmetadata = function(){
	//my_items = { 'html' : '<iframe src="/datasets/'+currentLayer[0].attributes.datasetid+'/metadata" width="600" height="600" frameborder=0></iframe>' };
	my_items = { 'html' : '<iframe src="/apps/' + AppId + '/datasets/'+currentLayer[0].attributes.datasetid+'/metadata" width="600" height="600" frameborder=0></iframe>' };
	my_modal = new Ext.Window({
		title :  'Metadata' , 
		closable: true,
		width  : 616,
		Height : 580,
		modal: true,
		items: my_items
	});
	my_modal.show();
}

var addSeparator = function(){
	toolbar.addSpacer();
	toolbar.addSeparator();
	toolbar.addSpacer();
	return 1;	
} 

var buildToolbar =  function () {
    toolbar = new mapfish.widgets.toolbar.Toolbar({
			map: map, 
			configurable: false
		});
    toolbar.autoHeight = false;
    toolbar.height = 25;
}

var getGrid = function(){
	return  new Ext.grid.GridPanel({
			id: 'pokiegrid',
			title: 'Search results',
			ds: store,
			loadMask: new Ext.LoadMask(Ext.getBody(), {msg:"Loading features..."}),
			collapsible: false,
			columns: currentLayer[0].attributes.grid_columns,
			listeners: {
			  'rowclick': {
				  'fn': function(grid, index, evt) {
					  Searcher.clearMarkers('featureLayer');
					  var r = grid.getStore().getAt(index);
					  var feature = r.get('feature');
					  var rendered = false;
					  for (var i = 0; i < vectorLayer.features.length; i++) {
						  if (feature == vectorLayer.features[i]) {
							  rendered = true;
						  }
					  }
					  if (rendered) {
						  vectorLayer.removeFeatures(feature);
					  } else {
						  vectorLayer.addFeatures(feature);
					  }
				   }
			   }
			},
			bbar: new Ext.PagingToolbar({
				//pageSize: 10,
				id : 'paging_bbar',
				store: store,
				paramNames: {
					start: 'offset',
					limit: 'limit'
				},
				displayInfo: true,
				displayMsg: 'Displaying results {0} - {1} of {2}',
				emptyMsg: "No data to display",
				items: [ '-', {
					pressed: false,
					enableToggle: false,
					text: 'Clear markers',
					tooltip: 'Preview selected dataset in map',
					handler: function() {
					   vectorLayer.removeFeatures(vectorLayer.features);
					  }    
					}
				],
				listeners: {
					'beforechange': function(pt, options){
						searchStoreMediator.deactivate();
						Searcher.clearMarkers('featureLayer');		
						var params = actualProtocol.proxyParams.params;
						if(params.bbox){
							options.bbox = params.bbox;
						}
						if(params.epsg){
							options.epsg = params.epsg;
						}
						if(params.lat){
							options.lat = params.lat;
						}
						if(params.lon){
							options.lon = params.lon;
						}
						if(params.tolerance){
							options.tolerance = params.tolerance;
						}
						return true;
					}
				}
			})
		});
}

var setToolbar = function() {
    toolbar.addControl(
        new OpenLayers.Control.ZoomToMaxExtent({
            map: map,
            title: 'Zoom to maximum map extent'
        }), {
            iconCls: 'zoomfull',
            toggleGroup: 'map'
        }
    );


	toolbar.addControl(
        new OpenLayers.Control.ZoomBox({
            title: 'Zoom in: click in the map or use the left mouse button and drag to create a rectangle'
        }), {
            iconCls: 'zoomin',
            toggleGroup: 'map'
        }
	);
	toolbar.addControl(
        new OpenLayers.Control.ZoomBox({
            out: true,
            title: 'Zoom out: click in the map or use the left mouse button and drag to create a rectangle'
        }), {
            iconCls: 'zoomout',
            toggleGroup: 'map'
        }
	);
	toolbar.addControl(
        new OpenLayers.Control.DragPan({
            isDefault: true,
            title: 'Pan map: keep the left mouse button pressed and drag the map'
        }), {
            iconCls: 'pan',
            toggleGroup: 'map'
        }
    );


    addSeparator(toolbar);
    if(advanced){
		var searchBoxLayer = new OpenLayers.Layer.Vector("boxsearch");
        toolbar.addControl(
            new OpenLayers.Control.DrawFeature(searchBoxLayer, OpenLayers.Handler.Point, {
                title: 'Draw a point on the map'
            }), {
                iconCls: 'drawpoint',
                toggleGroup: 'map'
            }
		);
		toolbar.addControl(
            new OpenLayers.Control.DrawFeature(searchBoxLayer, OpenLayers.Handler.Path, {
                title: 'Draw a linestring on the map'
            }), {
                iconCls: 'drawline' ,
                toggleGroup: 'map'
            }
		);
		toolbar.addControl(
            new OpenLayers.Control.DrawFeature(searchBoxLayer, OpenLayers.Handler.Polygon, {
                title: 'Draw a polygon on the map'
            }), {
                iconCls: 'drawpolygon' ,
                toggleGroup: 'map'
            }
		);
        addSeparator(toolbar);
    }

    var nav = new OpenLayers.Control.NavigationHistory();
    map.addControl(nav);
    nav.activate();

    toolbar.add(
        new Ext.Toolbar.Button({
            iconCls: 'back',
            tooltip: 'Previous view',
            handler: nav.previous.trigger
        })
	);
	toolbar.add(
        new Ext.Toolbar.Button({
            iconCls: 'next',
            tooltip: 'Next view',
            handler: nav.next.trigger
        })
    );


	/*
    toolbar.add(
        new Ext.Toolbar.Button({
			text : 'View Metadata',
            //iconCls: 'x-add-node',
            tooltip: 'Opens a new window',
            handler: showmetadata
        })
	);
	*/
	if(Description.taxonomy == 'vector'){
		addSeparator(toolbar);

		toolbar.addControl(
			mapSearcher
			, {
				tooltip: 'Feature Info: Click to discover',
				iconCls: 'geosearch',
				toggleGroup: 'map'
			}
		);

		mapSearcher.activate();

		addSeparator(toolbar);
		toolbar.add(
			new Ext.Toolbar.Button({
				text : 'Attributes',
				//iconCls: 'x-add-node',
				id : 'rbtn',
				tooltip: 'View feature\'s attributes results in the grid below',
				handler: function (){
					var g = Ext.getCmp('searchgrid');
					if(g.collapsed){
						g.expand();
						this.setText('Hide Results');
					}
					else{
						g.collapse();
						this.setText('View Attributes');
						
					}
						
				}
			})
		);
	}
	else{ 
		pokieGrid = null;
	}
}



Ext.onReady(function() {

	Ext.QuickTips.init();
	//Ext.state.Manager.setProvider(new Ext.state.CookieProvider());

	tilesize = new OpenLayers.Size(256,256);
	extent = new OpenLayers.Bounds(-235635,3196994,1032202,4437481); 
	var options = {
		controls: [],
		projection: "EPSG:"+ MAP_EPSG,
		units: "meters",
		tileSize: tilesize,
		resolutions: [ 2000,1800,1600,1400,1200,1000,500,250,30,10,1,0.1524],
		maxExtent: extent
	 };

	map = new OpenLayers.Map('map', options);
	map.setOptions({restrictedExtent: extent});

	var tile_format = 'image/png';
	if ( Ext.isIE6 ){
		tile_format = 'image/gif';
	}

	rgis_base = new OpenLayers.Layer.WMS(
		"RGIS Base", 
		//"http://edacwms.unm.edu/cgi-bin/mapfiles/imagery_wms2?", 
		//"/datasets/base/services/ogc/wms?",
		"/apps/" + AppId + "/datasets/base/services/ogc/wms?",
		{ 
			layers: 'naturalearthsw,southwestutm,nmcounties', 
		    //transparent: true,
			format: 'image/jpeg', 
			isBaseLayer : true
		}
	);
	map.addLayer(rgis_base);

	var overview_base = new OpenLayers.Layer.WMS(
		"ImageLayer",
		"http://edacwms.unm.edu/cgi-bin/mapfiles/imagery_wms2?",
		{
			layers: 'southwestutm,nmcounties' 
		},
		{numZoomLevels: 10, alwaysInRange: false}
		
	);
	overview_base.setTileSize(new OpenLayers.Size(128, 64));
/*
	map.addControl(new OpenLayers.Control.OverviewMap({
		layers : [ overview_base ]
	}));
*/
	actualProtocol = OpenLayers.Util.extend(mapfish.Protocol.MapFish, new mapfish.Protocol.MapFish({
		//url: '/datasets/'+Layers[0].id+'/features',
		url: '/apps/' + AppId+ '/datasets/'+Layers[0].id+'/features',
		totalRecords: null, 
		proxyParams: null,
		params: {
		  //maxfeatures: 10,
		  epsg : MAP_EPSG
		},
		parseTotalCount: function(request) {
			var doc = request.responseXML;
			if (!doc || !doc.documentElement) {
				doc = request.responseText;
			}
			if (!doc || doc.length <= 0) {
				return 0;
			}
			var o = eval("("+doc+")");
			if(!o){
			  return 0;
			}
			if(o.totalRecords){
			  return o.totalRecords;
			}
			else{
			  return 0;
			}
		},
		handleRead: function(resp, options) {
			var request = resp.priv;
			if (options.callback) {
				var code = request.status;
				if (code == 200) {
					// success
					resp.features = this.parseFeatures(request);
					resp.totalRecords = this.parseTotalCount(request);
					resp.code = OpenLayers.Protocol.Response.SUCCESS;
				} else {
					// failure
					resp.features = null;
					resp.code = OpenLayers.Protocol.Response.FAILURE;
					res.totalRecords = 0;
				}
				options.callback.call(options.scope, resp);
			}
		},
		read: function(options) {
			// workaround a bug in OpenLayers
			options.params = OpenLayers.Util.applyDefaults(
				options.params, this.options.params);
			if (options) {
				this.filterAdapter(options);
			}
			// Recall last sent url request. Attach proxy later
			//this.proxyParams = options.params;
			this.proxyParams = options;
		
			return OpenLayers.Protocol.HTTP.prototype.read.call(this, options);
		}
	}));


	protocol = mapfish.Protocol.decorateProtocol({
		protocol : actualProtocol,
        TriggerEventDecorator: {    
            eventListeners: { 
                crudtriggered: function(k) {
					//pokieGrid.fireEvent('deactivate');
					//var bbar = pokieGrid.getBottomToolbar();
					//bbar.destroy();
                },
                crudfinished: function(b) {
					if(!b.features){
						st = Ext.StoreMgr.key('thestore');
						st.removeAll();
					}
					else{
						totalCount = b.totalRecords;
						vectorLayer.addFeatures(b);
						//pokieGrid.fireEvent('activate');
						s = Ext.getCmp('searchgrid');
						if (s.collapsed){
							s.expand();
						}
					}
                }
    
            }
        }
	});
	protocol.events.addEventType('radiochange');
	
	currentLayer = [
		 { attributes : 
			{ datasetid : Description.id, 
			  feature_attributes : Layers[0].feature_attributes,
			  grid_columns : Layers[0].grid_columns
			} 
		}];

	reader = new mapfish.widgets.data.FeatureReader({}, 
		currentLayer[0].attributes.feature_attributes
	);

	prestore = new Ext.data.Store({
	  proxy: new Ext.data.HttpProxy({
		  //url :  '/datasets/'+Layers[0].id + '/features',
		  url :  '/apps/' + AppId + '/datasets/'+Layers[0].id + '/features',
		  method: 'post'
		}),
	  storeId : 'thestore',
	  reader: reader
	});

	store = OpenLayers.Util.extend(prestore, {
		protocol: protocol,
		response : null,
		getTotalCount: function (){ return  totalCount },
		preloadRecords: function(response){
			var json = response.priv.responseText;
			kkc = json;
			if (!json){
				return true;
			}
			var o = eval("("+json+")");
			//this.totalLength = o.totalRecords;
			totalCount = o.totalRecords;
			var read_response = this.reader.read(response);
			this.loadRecords(read_response, this.lastOptions, true); 
			

		},
		load: function(options){
			options = options || {};
			if(this.fireEvent("beforeload", this, options) !== false){
				this.storeOptions(options);
				var p = Ext.apply(options.params || {}, this.baseParams);
				if(this.sortInfo && this.remoteSort){
					var pn = this.paramNames;
					p[pn["sort"]] = this.sortInfo.field;
					p[pn["dir"]] = this.sortInfo.direction;
				}
				//this.proxy.load(p, this.reader, this.loadRecords, this, options);
				options.callback = OpenLayers.Function.bind(this.preloadRecords, this);
				this.response = this.protocol.read(options);

				return true;
			} else {
			  return false;
			}
    	}
	});

	searchStoreMediator = new mapfish.widgets.data.SearchStoreMediator({
	  protocol: protocol,
	  store: store,
	  append: false
	});


    vectorLayer = new OpenLayers.Layer.Vector("vector", {
 	          styleMap: new OpenLayers.StyleMap({
 	              'default': new OpenLayers.Style({
 	                  'externalGraphic': '../../images/AQUA.png',
 	                  'graphicWidth': 20,
 	                  'graphicHeight': 20,
 	                  'graphicYOffset': -20,
 	                  'fillOpacity': 1.0
 	              })
 	          }),
 	          displayInLayerSwitcher: false
 	      });
	map.addLayer(vectorLayer);

	boxmLayer = new OpenLayers.Layer.Boxes('Last AOI Search');
	map.addLayer(boxmLayer);
	
	pointmLayer = new OpenLayers.Layer.Markers('Last  LatLon Search');
	map.addLayer(pointmLayer);

	Searcher = {
		boxLayer : boxmLayer,
		id : 'mysearcher',
		pointLayer : pointmLayer,
		featureLayer : vectorLayer,
		clearMarkers : function(layer) {
			if (!layer || layer=='pointLayer')
				this.pointLayer.clearMarkers();
			if (!layer || layer == 'boxLayer')
				this.boxLayer.clearMarkers();
			if (!layer || layer == 'featureLayer')
				this.featureLayer.removeFeatures(this.featureLayer.features);
			},
	    triggerSearch: function() {
			this.cancelSearch();
			searchStoreMediator.activate();
			var filter = this.getFilter();
			filter = this.isFilter(filter) ? {filter: filter} : {params: filter};
			var options = OpenLayers.Util.extend({searcher: this}, filter);

			this.response = this.protocol.read(options);
			kkb = this.response;
			//var icon = new OpenLayers.Icon('../../images/latlon.png', new OpenLayers.Size(10,10));
			this.clearMarkers();
			if (options.params){
				this.pointLayer.addMarker(new OpenLayers.Marker(new OpenLayers.LonLat(options.params.lon, options.params.lat)));
			}
			else{
				if (options.filter.value instanceof OpenLayers.Bounds) {
					this.boxLayer.addMarker(new OpenLayers.Marker.Box(options.filter.value));
				}
			}
    	}

	}
	mapSearcher = OpenLayers.Util.extend(new mapfish.Searcher.Map({
		mode: mapfish.Searcher.Map.BOX,
		protocol: protocol,
		displayDefaultPopup: false
	}), Searcher);


	if(Description.what == 'bundle'){ 
		// singleTile / Untiled deprecates starting with OL version 3.0
		var base_wms_url = "/" + Description.what + "/ogc/wms_tiles/"+ Description.id + "?";
		currentLayer[0].WMS = new OpenLayers.Layer.WMS.Untiled( Description.title, base_wms_url ,
                            { layers: Description.layers,
                              transparent: true,
                              format:  tile_format,
							  displayOutsideMaxExtent: false,
                              singleTile: Description.singleTile
                            }
		);
		map.addLayer(currentLayer[0].WMS);

		// Our tree can have more than one group. Let's use 1 for now.

		var children = [];
		for(var i = 0;  i < Layers.length; i++){
			var child = { 
				text: Layers[i].title,
				leaf: true,
				radio: true,
				datasetid: Layers[i].id,
				radioGrp: 'bundle',
				layerNames: [ Description.title + ":" + Layers[i].layer ],
				grid_columns: Layers[i].grid_columns,
			    feature_attributes : Layers[0].feature_attributes,
				//checked : i == 0 ? true : false
				checked: false
			}		
			children[i] = child;
				
		}
		var model = [
			{
				text: Description.title,
				leaf: false,
				expanded: true,
				layerName : Description.title, 
				children : children
			}
		];

		tree = new mapfish.widgets.LayerTree({
			map: map, 
			el: 'tree', 
			model: children,
			listeners: { 
				'radiochange': function(node,checked) {
					if(checked){
						refreshcurrentLayer();
					}
				},
				'checkchange': function(node,checked) {
					if(checked){
						refreshcurrentLayer();
					}
				}
			}
		});

		tree.render();

		bundles_tree = {
				region: 'east',
				split: true,
				title: Description.title + ' datasets',
				width: 200,
				height: 640,
				minSize: 50,
				maxSize: 400,
				collapsible: true,
				margins:'0 0 0 0',
				items : [
					{
					contentEl: 'tree'
					}

				]
		}
	}
	else{
		layerExtent = new OpenLayers.Bounds(Layers[0].maxExtent[0], Layers[0].maxExtent[1], Layers[0].maxExtent[2], Layers[0].maxExtent[3]);
		//layerExtent = layerExtent.transform(origProjection, mapProjection);
		layerExtent.transform(origProjection, mapProjection);
		var extentLayer = new OpenLayers.Layer.Boxes('Extent layer');
		extentLayer.addMarker(new OpenLayers.Marker.Box(layerExtent, 'blue', 2));
		map.addLayer(extentLayer);
 
		//var base_wms_url = "/" + Description.what + "/ogc/wms/"+ Description.id + "?";
		//var base_wms_url = "/datasets/" + Description.id + "/services/ogc/wms_tiles?";
		var base_wms_url = "/apps/" + AppId + "/datasets/" + Description.id + "/services/ogc/wms_tiles?";
		currentLayer[0].WMS = new OpenLayers.Layer.WMS( Description.title, base_wms_url ,
                            { layers: Description.layers,
                              format: tile_format,
                              transparent: true,
							  maxExtent: layerExtent,
							  displayOutsideMaxExtent: false,
                              singleTile: Description.singleTile
                            }
		);

		map.addLayer(currentLayer[0].WMS);

		bundles_tree = {};
	}


	var control = new OpenLayers.Control.SelectFeature(vectorLayer);
	map.addControl(control);
	control.activate();

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
//	map.addControl(new OpenLayers.Control.Scale());

	buildToolbar();




	var center = [];
	if(Ext.isIE6){
		center.push({xtype: 'panel'});
	}
	center.push({ 
		xtype: 'mapcomponent',
		tbar: toolbar,
		map: map,
		width: 610,
		height: 610
	});
	
	var viewport_regions = [ {
	  region: 'center',
	  items: center
	}];

	if(Description.taxonomy == 'vector'){
		pokieGrid = getGrid();

		viewport_regions.push({
			  region: 'south',
			  id: 'searchgrid',
			  collapsible: true,
			  collapsed: true,	
			  collapseMode: 'mini',
			  border: 'false',
			  layout: 'fit',
			  height: 200,
			  split: true,
		      items: pokieGrid,
			  listeners: {
				  'collapse': {
						fn : function(){
							var bt = Ext.getCmp('rbtn');
						    if(bt){
								bt.setText('Attributes');
							}
						}
				  }
				}
			}
		);
	}

	viewport = new Ext.Viewport({
		layout: 'border',
		height: 610,
		width: 610,
		renderTo: 'content',
		items: viewport_regions 
	});
	

	setToolbar();
    toolbar.activate();
	if(layerExtent){
		map.zoomToExtent(layerExtent);	
	}

var switchFormatOnZoom = function() {
    // Map listeners

    map.events.register('zoomend', map, function() {
        var zl = map.getZoom();
        var newParams = null;
        if(zl >=9){
            if(Description.taxonomy == 'geoimage'){
                newParams = {
                    'format': 'image/jpeg',
                    'transparent' : false
                };
				rgis_base.setVisibility(false);
            }
            else {
                newParams = {
                    'format': tile_format,
                    'transparent' : true
                };
            }

        }
        else{
            if(Description.taxonomy == 'geoimage'){
                newParams = {
                    'format': tile_format,
                    'transparent' : true
                }
				rgis_base.setVisibility(true);
            }
            else{
                newParams = {
                    'format': tile_format,
                    'transparent' : true
                }
            }
        }

        if(newParams){
            currentLayer[0].WMS.mergeNewParams(newParams);
        }

    });
}

	switchFormatOnZoom();
	return 1;

});
