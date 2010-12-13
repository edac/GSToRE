//Config
var map, extent, mapProjection, origProjection, formSearcher;
var MAP_EPSG = 26913;
var ORIG_EPSG = 4326;


Ext.onReady( function buildMap(){
	var Layers = 'Layers';
	var Description = 'Description';
	Ext.QuickTips.init();
	var styleMap = new OpenLayers.StyleMap({
		"default" : {
            fillColor: 0,
            strokeColor: 'blue',
            strokeOpacity: 0.4,
            fillOpacity: 0,
            strokeWidth: 2

		},
		"select" : {
		   fillColor: "green",
		   fillOpacity: 0.2,
		   strokeColor: "#00F5FF",
		   strokeWidth: 3,
		   strokeOpacity: 0.7
		}
	});		

	var layers =  [ 
		new OpenLayers.Layer.WMS( "RGIS Base", 
            "http://edacwms.unm.edu/cgi-bin/mapfiles/imagery_wms2?", {
	//		"http://rgisbeta.unm.edu/dataset/ogc/wms/base?", {	
		//	"http://129.24.63.99:8881/tiles/index/dataset/ogc/wms/base?", {	
				layers: 'naturalearthsw,southwestutm,nmcounties,Highways', 
				format: 'image/jpeg'
			}
		),
		new OpenLayers.Layer.Boxes("boxLayer"),
		new OpenLayers.Layer.Vector("vector", { styleMap: styleMap })
	]

	var vecLayer = layers[2];

	// Projections
	var mapProjection = new OpenLayers.Projection('EPSG:' + MAP_EPSG);
	var origProjection = new OpenLayers.Projection('EPSG:' + ORIG_EPSG);
	var extent = new OpenLayers.Bounds(-235635,3196994,1032202,4437481); 
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
			return 'Lon: -' + lon  + ', Lat: ' + lat;
		}
	});
	var scalebar = new OpenLayers.Control.ScaleLine({
        abbreviateLabel: true
    });

	var options = {
        id: 'RGISMap',
		controls: [mp, scalebar],
		projection: 'EPSG:26913',
		units: "meters",
	//	div: 'map_el',
		maxResolution: 2500,
		minResolution: 10,
		restrictedExtent: extent,	
		maxExtent: extent	
	 };

map = new OpenLayers.Map('map', options);

function searchNow(){
	var store = Ext.StoreMgr.key('spatial_store');
	store.removeAll();

	formSearcher.form.search();
	return 1;	
}

function clearFilters(){
	var store = Ext.StoreMgr.key('spatial_store');
	store.removeAll();
	formSearcher.form.reset();
	clear_markers();
	clear_box();
	map.zoomToExtent(extent);	
	return 1;	

}

function fill_bbox(newbounds){
	var filter_box = Ext.get('bbox');
	filter_box.dom.value = newbounds.left + ',' + newbounds.top + ',' + newbounds.right + ',' + newbounds.bottom;
    return 1;
}

function clear_bbox(){
	var bbox = Ext.get('bbox');
	bbox.dom.value = '';
    return 1;
}

function clear_box(){
	var n = Ext.get('northutm');
	var s = Ext.get('southutm');
	var w = Ext.get('westhutm');
	var e = Ext.get('easthutm');
	e.dom.value = '';
	n.dom.value = '';
	w.dom.value = '';
	s.dom.value = '';
	return clear_bbox();	
}

function clear_locations(){
	Ext.getCmp('county').reset();
	Ext.getCmp('quad').reset();
	Ext.getCmp('gnis').reset();
    return 1;
}

function populate_box(box){
	var n = Ext.get('northutm');
	var s = Ext.get('southutm');
	var w = Ext.get('westhutm');
	var e = Ext.get('easthutm');

	e.dom.value = Math.abs(box[2]).toFixed(4);
	n.dom.value = Math.abs(box[3]).toFixed(4);
	w.dom.value = Math.abs(box[0]).toFixed(4);
	s.dom.value = Math.abs(box[1]).toFixed(4);
    return 1;	
}

function clear_markers(){
	var boxLayer = map.getLayersByName('boxLayer')[0];
	boxLayer.clearMarkers();	
	return 1;	
}

function draw_box(box){
	var ol_box;
	if(box  instanceof OpenLayers.Bounds ){
		ol_box = box.toGeometry();
	}
	else{
		ol_box = new OpenLayers.Bounds(box[0],box[1],box[2],box[3]).toGeometry();
		ol_box.transform(origProjection, mapProjection);
	}
    var newbounds = ol_box.getBounds();
    fill_bbox(newbounds);
	clear_markers();
	var boxLayer = map.getLayersByName('boxLayer')[0];
	boxLayer.clearMarkers();	
	boxLayer.addMarker(new OpenLayers.Marker.Box(newbounds));
	return 1;	

}


	var county_store = new Ext.data.JsonStore({
		url: '/data/counties',
		root: "results",
		id: "id",
		storeId: 'county_store_id',
		fields: ['id', 'name', 'box']
	});

	var quads_store = new Ext.data.JsonStore({
		url: '/data/quads',
		root: "results",
		id: "id",
		storeId: 'quads_store_id',
		fields: ['id', 'name', 'box']
	});

	var gnis_store = new Ext.data.JsonStore({
		url: '/data/gnis',
		root: "results",
		id: "id",
		storeId: 'gnis_store_id',
		fields: ['id', 'name', 'box']
	});

	var theme_store = new Ext.data.JsonStore({
		url: '/datasets/categories',
		storeId: "theme_store_id",
		root: 'results',
		fields :  [{name: 'category'}]
	});

	protocol =  new mapfish.Protocol.MapFish.create({
		url : '/datasets.json',
		params : {
			limit : 30,
            order_by: 'relevance',
            dir: 'desc',
            start: 0,
            end: 30,
			epsg : MAP_EPSG
		}
	});

	var mergeFilterProtocol = new mapfish.Protocol.MergeFilterDecorator({
		protocol: protocol
	});

	var triggerEventProtocol = new mapfish.Protocol.TriggerEventDecorator({
		protocol: mergeFilterProtocol
	});


	var spatial_form = new GeoExt.form.FormPanel({
		title: 'Search by location',
		protocol: protocol,
		id: 'spatial_form_cmp',
		formId: 'spatial_form',
		border: true,
		buttonAlign: 'center',
		bodyStyle: 'padding: 5px 5px 0; text-align: left;',
		defaults: {
			width: 180,
			xtype: 'textfield',
			allowBlank: true,
			msgTarget: 'side',
			labelStyle: 'width: 70px',
			resizable: true
		},
		defaultType: 'textfield',
		items: [
			new Ext.form.ComboBox({
				fieldLabel: 'County',
				id: 'county',
				emptyText:'Select a County...',
				selectOnFocus:true,
				value: '',
				displayField: 'name',
				mode: 'remote',
				minChars: 1,
				valueField: 'id',
				triggerAction: 'all',
				store: county_store,
				//forceSelection: true,
				listeners: {
					select: function(c,v) { 
							Ext.getCmp('quad').reset();
							Ext.getCmp('gnis').reset();
							var box = eval(v.data.box);				
							populate_box(box);	
							draw_box(box);
								
					}
					
				}
			}),
			new Ext.form.ComboBox({
				fieldLabel: 'USGS Quad',
				id: 'quad',
				value: '',
				emptyText:'or select a Quad...',
				selectOnFocus:true,
				displayField: 'name',
				mode: 'remote',
				minChars: 3,
				typeAhead: true,
				valueField: 'id',
				store: quads_store,
				//forceSelection: true,
				listeners: {
					select: {
						fn: function(c,v) { 
							Ext.getCmp('county').reset();
							Ext.getCmp('gnis').reset();
							var box = eval(v.data.box);				
							populate_box(box);	
							draw_box(box);
							
						}
					}
				}
			}),
			new Ext.form.ComboBox({
				fieldLabel: 'Place name',
				id: 'gnis',
				value: '',
				emptyText:'or select a GNIS...',
				selectOnFocus:true,
				displayField: 'name',
				mode: 'remote',
				minChars: 3,
				typeAhead: true,
				valueField: 'id',
				store: gnis_store,
				//forceSelection: true,
				listeners: {
					select: {
						fn: function(c,v) { 
							Ext.getCmp('county').reset();
							Ext.getCmp('quad').reset();
							var box = eval(v.data.box);				
							populate_box(box);	
							draw_box(box);
							
						}
					}
				}
			}), {
			xtype : 'panel',
			border: false,
			//title: 'Area of Interest in decimal degrees',
			width: '100%',
			html : "<p class='area-notice'><b>Enter coordinates to search</b><br /> or select map area to update.</p><table id='coordtable'><tr><td></td><td><label for='northutm'>N </label><input type='text' id='northutm' name='northutm' size='5' class='x-form-text x-form-field x-form-empty-field' /> </td> <td></td></tr> <tr> <td> <label for='westutm'>W </label><input type='text' id='westhutm' name='westutm' size='5' /></td><td>&nbsp;&nbsp;&nbsp;</td> <td><label for='eastutm'>E </label><input type='text' id='easthutm' name='eastutm' size='5' /> </td> </tr> <tr> <td></td><td><label for='southutm'>S </label> <input type='text' id='southutm' name='southutm' size='5' /> </td><td></td> </tr> </table><input name='bbox' id='bbox' type='hidden' />"
			}, {
			xtype : 'panel',
			layout: 'form',
			border: false,
			bodyStyle: 'padding: 5px 5px 0; text-align: left;',
			title: 'Filter by theme or subtheme',
			width: '100%',
			items: [	
				new Ext.form.ComboBox({
						hideLabel: true,
						id: 'theme',
						emptyText:'Type theme or subtheme',
						selectOnFocus:true,
						typeAhead: false,
						value: '',
						width: 300,
						mode: 'remote',
						hiddenName: 'category',
						triggerAction: 'query',
						store: theme_store,
						minChars: 2,
						valueField: 'category',
						displayField: 'category',
						resizable: true,
						listeners: { 
							select : { 
								fn: function(combo, value) {
									if(value == '--------------'){
										combo.reset();
									}
									return 1;						
								} 
							},
							change: {
								fn: function(combo, value) {
									if(value == '--------------'){
										combo.reset();
									}
									return 1;						
								} 
							}
						} 
					}),
                new Ext.form.Label({ 
                    html: 'Ex: Transportation',
                    cls: 'area-notice'
                })
				]
			}

	  ]
		,
		buttons: [{
			text: 'Search now',
			handler: searchNow 
			//scope: this
		}, {
			text: 'Reset',
			handler: clearFilters
		}
		] 
	
	});
	var store =  new GeoExt.data.FeatureStore({
		layer: vecLayer,
		proxy: new GeoExt.data.ProtocolProxy({
            protocol: protocol 
        }),
		reader: new GeoExt.data.FeatureReader({
			idProperty: 'id',
			totalProperty: 'total',
			totalRecords: 100,
			id: 'id'}	
			, [
			{ name: 'id' }, 
			{ name: 'text' }, 
			{ name: 'taxonomy' }, 
			{ name: 'formats' }, 
			{ name: 'lastupdate' }, 
			{ name: 'config' } 
		]),
        autoLoad: false,
		storeId : 'spatial_store',
		baseParams: { 
			'node' : '',
			'filter' : '',
			'limit' : 30,
			'offset' : 0,
			'keyword' : '',
			'bbox' : '',
			'start_date': '',
			'end_date' : '',
			epsg : MAP_EPSG
		}
	});
	
	grid = RGIS.DataGrid('s', store,  new GeoExt.grid.FeatureSelectionModel());	
		
	mapSearcher = OpenLayers.Util.extend( new mapfish.Searcher.Map({
		id: 'mapsearcher',
		mode: mapfish.Searcher.Map.BOX,
		protocol: protocol,
		displayDefaultPopup: false,
		title: 'Search map features: keep the left mouse button pressed and drag the map',
		projection:  mapProjection,
		delay: 400
		}), {
		triggerSearch: function() {
			this.cancelSearch();
			var filter = this.getFilter();
			filter = this.isFilter(filter) ? {filter: filter} : {params: filter};
			var options = OpenLayers.Util.extend({searcher: this}, filter);
			if (options.filter){
				var	box = options.filter.value;
				if(options.filter.type == 'DWITHIN'){
					box = box.getBounds();
				}
				draw_box(box);
				
				var newbox = box.toGeometry();
				newbox.transform(mapProjection, origProjection);
				populate_box(newbox.getBounds().toArray());
			}
			this.response = this.protocol.read(options);
			clear_locations();
				
		}
	});

    formSearcher = new mapfish.widgets.search.Form({
        form: spatial_form,
        protocol: protocol
    });


    var searchStoreMediator = new mapfish.widgets.data.SearchStoreMediator({
          protocol: protocol,
          store: store,
          append: false
    });

	var tbarItems = [];
	tbarItems.push( new GeoExt.Action({
		map: map,
		control : new OpenLayers.Control.ZoomToMaxExtent(),
		tooltip: 'Zoom to maximum map extent',
		iconCls: 'zoomfull', 
		toggleGroup: 'map'
	}));
	
	tbarItems.push(" ");

	tbarItems.push( new GeoExt.Action({
		map: map,
		control: new OpenLayers.Control.ZoomBox(),
		tooltip: 'Zoom in: click in the map or use the left mouse button and drag to create a rectangle',
		iconCls: 'zoomin', 
		toggleGroup: 'map'
	}));
	tbarItems.push(" ");

	tbarItems.push( new GeoExt.Action({
		map: map,
		control: new OpenLayers.Control.ZoomBox({ 
			out: true
		}),
		tooltip: 'Zoom out: click in the map or use the left mouse button and drag to create a rectangle',
		iconCls: 'zoomout', 
		toggleGroup: 'map'
	}));

	tbarItems.push(" ");
  
   tbarItems.push(new GeoExt.Action({
          map: map,
          control: new OpenLayers.Control.Navigation(),
          tooltip: "Navigate",
          toggleGroup: "map",
          iconCls: "pan", 
          pressed: true
      }));
	tbarItems.push(" ");
	tbarItems.push("-");
	tbarItems.push(" ");
	tbarItems.push(new GeoExt.Action({
	  map: map,
	  control: mapSearcher,
	  tooltip: "Search on click and by box",
	  toggleGroup: "map",
	  iconCls: "geosearch",
	  pressed: false
	}));

	var selectFeature = new OpenLayers.Control.SelectFeature(vecLayer);
	map.addControl(selectFeature);
		
	tbarItems.push(" ");
	tbarItems.push(new GeoExt.Action({
	  map: map,
	  control: selectFeature,
	  tooltip: "Select feature to find in grid",
	  toggleGroup: "map",
	  iconCls: "pointer",
	  pressed: false
	}));


	viewport = new Ext.Panel({
		layout: 'border',
		height: 830,
		renderTo: 'map_content',
		width: 760,
		border: false,
		items: [{
			region: 'west',
			xtype: 'panel',
			height: 388,
			width: 320,
			split: false,
			border: true,
			contentEl: 'west',
			items:  [ 
				spatial_form
				 ]
		}, {
			region: 'center',
			id: 'mappanel',
			border: true,
			xtype: 'gx_mappanel',
			split: false,	
			center: [1,1],
			layers: layers,
			tbar: tbarItems,
			map: map,
			zoom: 9
		}, { 
			region: 'south',
			border: false,
			xtype: 'panel',
			split: true,
			height: 440,
			id: 'datasets_results',
			title: 'First 30 result datasets',
			layout: 'fit',
			split: true,
			items: [
				grid
			]
		}]
	});
	
	viewport.doLayout();

	var bb = grid.getBottomToolbar();
    bb.items.first().addListener('click', clearFilters);
    
    slider = Ext.getCmp('georelevance');
    var nextresults = function(o, p){
        var searchAction = new GeoExt.form.SearchAction(formSearcher.form, {
            protocol: new mapfish.Protocol.MapFish.create({
                url : '/datasets.json',
                params : {
                    limit : 30,
                    order_by: 'relevance',
                    dir: 'desc',
                    start: p,
                    end: p + 30,
                    epsg : MAP_EPSG
                }
            }),
            abortPrevious: true
        }); 
        
        formSearcher.form.doAction(searchAction, {
            callback: function(response){
                alert('new dinges');
            }
        });
        return 1;
    }
    slider.addListener('change', nextresults);
    
	var coords = Ext.get('coordtable').query('input');
	Ext.each(coords, function(k,v) { 
		k.className = 'x-form-text x-form-field x-form-empty-field';
		//k.disabled = true;
	});

});
