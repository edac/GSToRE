//Config
var map, extent, mapProjection, origProjection;
var MAP_EPSG = 26913;
var ORIG_EPSG = 4326;
Ext.Ajax.defaultHeaders={
'Content-Type': 'application/x-www-form-urlencoded; charser=UTF-8'
};

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
//            "http://edacwms.unm.edu/cgi-bin/mapfiles/imagery_wms2?", {
	//	"http://rgisbeta.unm.edu/datasets/base//services/ogc/wms?", {	
		// "/datasets/base//services/ogc/wms?", {	
		//"/apps/rgis/datasets/base//services/ogc/wms?", {	
		"/apps/" + AppId + "/datasets/base//services/ogc/wms?", {	
	//		"http://129.24.63.25/datasets/base/services/ogc/wms?", {	
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
		maxExtent: extent,	
		restrictedExtent: extent	
	 };

map = new OpenLayers.Map('map', options);

var loadFeatures = function(response){
	var store = Ext.StoreMgr.key('spatial_store');
    //store.removeAll();
    store.loadData(response.features);
    return 1;
}

var loadFeaturesAction = function(form, action){
	var store = Ext.StoreMgr.key('spatial_store');
    //store.removeAll();
    store.loadData(action.response.features);
    return 1;
}


function clearFilters(){
	var store = Ext.StoreMgr.key('spatial_store');
	store.removeAll();
    sp_form = Ext.getCmp('spatial_form_cmp');
	sp_form.getForm().reset();
	clear_markers();
	clear_box();
	map.zoomToExtent(extent);	
    Ext.getCmp('cnty_rd').setValue(true);
	return 1;	

}

function fill_bbox(newbounds){
	var filter_box = Ext.get('bbox');
	filter_box.dom.value = newbounds; 
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

function populate_coords(box){
	var n = Ext.get('northutm');
	var s = Ext.get('southutm');
	var w = Ext.get('westhutm');
	var e = Ext.get('easthutm');

    if(isNaN(box[0]+box[1]+box[2]+box[3])){
        Ext.Msg.alert('Error','You have entered incorrect coordinates.');
    }
	e.dom.value = -1*Math.abs(box[2]).toFixed(4);
	n.dom.value = Math.abs(box[3]).toFixed(4);
	w.dom.value = -1*Math.abs(box[0]).toFixed(4);
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
    fill_bbox(newbounds.toBBOX());
	clear_markers();
	var boxLayer = map.getLayersByName('boxLayer')[0];
	boxLayer.clearMarkers();	
	boxLayer.addMarker(new OpenLayers.Marker.Box(newbounds));
	return 1;	

}
function updatebox(){
    var n = Ext.get('northutm');
    var s = Ext.get('southutm');
    var w = Ext.get('westhutm');
    var e = Ext.get('easthutm');

    try{
        n_box = new OpenLayers.Bounds(
            w.dom.value,
            s.dom.value,
            e.dom.value,
            n.dom.value
        );
        n_box.transform(origProjection, mapProjection);        
        draw_box(n_box);
    }   
    catch(err){
        Ext.Msg.alert('Error','You have entered incorrect coordinates.');
    }
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
		//url: '/datasets/categories',
		//url: '/apps/rgis/datasets/categories',
		url: '/apps/' + AppId + '/datasets/categories',
		storeId: "theme_store_id",
		root: 'results',
		fields :  [{name: 'category'}]
	});


    protocol = OpenLayers.Util.extend( new OpenLayers.Protocol.HTTP({
		//url : '/datasets.json',
		//url : '/apps/rgis/datasets.json',
		url : '/apps/' + AppId + '/datasets.json',
        format: new OpenLayers.Format.GeoJSON(),
		params : {
			limit : 30,
            order_by: 'relevance',
            dir: 'desc',
            start: 0,
            end: 30,
			epsg : MAP_EPSG
		}}), {
            read: function(options){
                OpenLayers.Protocol.prototype.read.apply(this, arguments);
                options = OpenLayers.Util.applyDefaults(options, this.options);
                var readWithPOST = (options.readWithPOST !== undefined) ?
                                   options.readWithPOST : this.readWithPOST;
                var resp = new OpenLayers.Protocol.Response({requestType: "read"});

                if(options.filter && options.filter instanceof OpenLayers.Filter.Spatial) {
                    if(options.filter.type == OpenLayers.Filter.Spatial.BBOX) {
                        options.params = OpenLayers.Util.extend(options.params, {
                            bbox: options.filter.value.toArray()
                        });
                    }
                }
                var filter_params = {};
                for(var i =0, l=options.filter.filters.length; i<l; i++){
                    f = options.filter.filters[i];
                    filter_params[f.property] = f.value;
                }
                oo = options;
                options.params = OpenLayers.Util.extend(options.params, filter_params);
                zzz = filter_params;
                 
                if(readWithPOST) {
                    resp.priv = OpenLayers.Request.POST({
                        url: options.url,
                        callback: this.createCallback(this.handleRead, resp, options),
                        data: OpenLayers.Util.getParameterString(options.params),
                        headers: {
                            "Content-Type": "application/x-www-form-urlencoded"
                        }
                    });
                } else {
                    resp.priv = OpenLayers.Request.GET({
                        url: options.url,
                        callback: this.createCallback(this.handleRead, resp, options),
                        params: options.params,
                        headers: options.headers
                    });
                }

                return resp;
            }
        }
    );

   
    var checker =  function(){
        
        var cnty_cmb = Ext.getCmp('county');
        var quad_cmb = Ext.getCmp('quad');
        var gnis_cmb = Ext.getCmp('gnis');

        var cnty_rd = Ext.getCmp('cnty_rd');
        var quad_rd = Ext.getCmp('quad_rd');
        var gnis_rd = Ext.getCmp('gnis_rd');
    

        if(cnty_rd.getValue()){
            cnty_cmb.show();
        }
        else{
            cnty_cmb.hide();
        }

        if(quad_rd.getValue()){
            quad_cmb.show();
        }
        else{
            quad_cmb.hide();
        }

        if(gnis_rd.getValue()){
            gnis_cmb.show();
        }
        else{
            gnis_cmb.hide();
        }
        cnty_cmb.reset();
        quad_cmb.reset();
        gnis_cmb.reset();
    }
            
	spatial_form = new GeoExt.form.FormPanel({
		title: 'Search by location',
		protocol: protocol,
		id: 'spatial_form_cmp',
		formId: 'spatial_form',
		border: true,
		buttonAlign: 'center',
		bodyStyle: 'padding: 5px; text-align: center;',
		defaults: {
			//width: 180,
			xtype: 'textfield',
			allowBlank: true,
			msgTarget: 'side',
			resizable: true
		},
		items: [
            new Ext.form.RadioGroup({
                fieldLabel: 'Select ',
                hideLabel: true,
                id: 'radioselector',
                items: [
                    {
                    xtype: 'radio',
                    name: 'selector',
                    id: 'cnty_rd', 
                    boxLabel: 'County',
                    //checked: true,
                    value: 1,
                    listeners: {
                        check: checker
                    }
        
                }, {
                    xtype: 'radio',
                    name: 'selector', 
                    id: 'quad_rd',
                    boxLabel: ' USGS Quad',
                    value: 1,
                    listeners: {
                        check: checker
                    }
                }, {
                    xtype: 'radio',
                    name: 'selector',
                    id: 'gnis_rd',
                    boxLabel: 'GNIS Place',
                    value: 1,
                    listeners: {
                        check: checker
                    }
                }
                    

                ]
            }),
			new Ext.form.ComboBox({
				fieldLabel: 'County', 
                hideLabel: true,
                ctCls: 'spatialsearch-combo',
				id: 'county',
				emptyText:'Select a County',
				selectOnFocus: true,
				value: '',
				displayField: 'name',
                width: 300,
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
							populate_coords(box);	
							draw_box(box);
								
					}
					
				}
			}),
			new Ext.form.ComboBox({
				//fieldLabel: 'USGS Quad',
                hideLabel: true,
				id: 'quad',
				value: '',
				emptyText:'Search for a Quadrangle',
				selectOnFocus:true,
				displayField: 'name',
                //hidden: true,
                width: 300,
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
							populate_coords(box);	
							draw_box(box);
							
						}
					}
				}
			}),
			new Ext.form.ComboBox({
				//fieldLabel: 'Place name',
                hideLabel: 'true',
				id: 'gnis',
				value: '',
				emptyText:'Search for a GNIS place name',
                width: 300,
                //hidden: true,
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
							populate_coords(box);	
							draw_box(box);
							
						}
					}
				}
			}), {
			xtype : 'panel',
			border: false,
			//title: 'Area of Interest in decimal degrees',
			width: '100%',
			html : "<p class='area-notice'><b>Enter coordinates to search</b><br /> or select map area to update.</p><table id='coordtable'><tr><td></td><td><label for='northutm'>N </label><input type='text' id='northutm' name='northutm' size='6' class='x-form-text x-form-field x-form-empty-field' /> </td> <td></td></tr> <tr> <td> <label for='westutm'>W </label><input type='text' id='westhutm' name='westutm' size='6' /></td><td>&nbsp;&nbsp;&nbsp;</td> <td><label for='eastutm'>E </label><input type='text' id='easthutm' name='eastutm' size='6' /> </td> </tr> <tr> <td></td><td><label for='southutm'>S </label> <input type='text' id='southutm' name='southutm' size='6' /> </td><td></td> </tr> </table><input name='bbox' id='bbox' type='hidden' /><br/>"
			}, {
			xtype : 'panel',
			layout: 'form',
			border: false,
			bodyStyle: 'padding: 5px 5px0; text-align: left;',
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
                        listWidth: 500,
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
		
      /*  , 
        listeners: {
            actioncomplete: function(form, action) {
                // this listener triggers when the search request
                // is complete, the OpenLayers.Protocol.Response
                // resulting from the request is available
                // through "action.response"
	            var store = Ext.StoreMgr.key('spatial_store');
                store.removeAll();
                res = action.response;
                store.loadData(action.response.features);
               
            }
        }
    */  
        ,
        listeners: {
            actioncomplete: loadFeaturesAction
        }
	
	});

    spatial_form.addButton({
        text: 'Search now',
        handler: function(){
            var o = {
               // filter: GeoExt.form.toFilter(this),
                callback: loadFeatures
            };
            this.search(o);
            Ext.getCmp('georelevance').setValue(0);
            //this.search();
        },
        scope: spatial_form 
    });   
    spatial_form.addButton({
        text: 'Reset',
        handler: clearFilters
    }); 

	var store =  new GeoExt.data.FeatureStore({
		layer: vecLayer,
		reader: new GeoExt.data.FeatureReader({
			totalProperty: 'total',
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


	var tbarItems = [];
	tbarItems.push( new GeoExt.Action({
		map: map,
		control : new OpenLayers.Control.ZoomToMaxExtent(),
		tooltip: 'Zoom to maximum map extent',
		iconCls: 'zoomfull' 
		//toggleGroup: 'map'
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

    var mapSearcher = new OpenLayers.Control();
    OpenLayers.Util.extend(mapSearcher, {
        draw: function () {
            this.box = new OpenLayers.Handler.Box( mapSearcher,
                {"done": this.search},
                {keyMask: OpenLayers.Handler.MOD_NONE},
                {boxDivClassName: "olHandlerBoxSelectFeature"}
            );
            this.box.activate();
        },

        search: function (bounds) {
            var minXY = map.getLonLatFromPixel(new OpenLayers.Pixel(bounds.left, bounds.bottom)); 
            var maxXY = map.getLonLatFromPixel(new OpenLayers.Pixel(bounds.right, bounds.top)); 
            var box = new OpenLayers.Bounds(
                minXY.lon, minXY.lat, maxXY.lon, maxXY.lat
            );
            draw_box(box);
            var newbox = box.toGeometry();
            newbox.transform(mapProjection, origProjection);
            populate_coords(newbox.getBounds().toArray());

            sp_filter = new OpenLayers.Filter.Spatial({
                type: OpenLayers.Filter.Spatial.BBOX,
                value: newbox.getBounds(),
                projection: MAP_EPSG
            });
            o = {
                filter: sp_filter,
                callback: loadFeatures
            };
            spatial_form.search(o);
            Ext.getCmp('georelevance').setValue(0);
             
        }
    });

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
		height: 1290,
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
			layers: layers,
			tbar: tbarItems,
			map: map,
			zoom: 9
		}, { 
			region: 'south',
			border: false,
			xtype: 'panel',
			split: true,
			height: 900,
			id: 'datasets_results',
			//title: 'First 30 result datasets',
			layout: 'fit',
			split: true,
			items: [
				grid
			]
		}]
	});
	
	viewport.doLayout();

	var bb = grid.getTopToolbar();
    bb.items.first().addListener('click', clearFilters);
    
    slider = Ext.getCmp('georelevance');
    var nextresults = function(o, p){
        var searchAction = new GeoExt.form.SearchAction(spatial_form.getForm(), {
            protocol: new OpenLayers.Protocol.HTTP({
                //url : '/datasets.json',
                //url : '/apps/rgis/datasets.json',
                url : '/apps/' + AppId + '/datasets.json',
                format: new OpenLayers.Format.GeoJSON(), 
                params : {
                    limit : 30,
                    order_by: 'relevance',
                    dir: 'desc',
                    start: p,
                    end: p,
                    epsg : MAP_EPSG,
                    bbox: Ext.get('bbox').dom.value,
                    category: Ext.get('category').dom.value
                }
            }),
            abortPrevious: true
        }); 
        
        spatial_form.form.doAction(searchAction, {
            callback: function(response){
                alert('new');
            }
        });
        return 1;
    }
    slider.addListener('change', nextresults);
   
	var coords = Ext.get('coordtable').query('input');
	Ext.each(coords, function(k,v) { 
		k.className = 'x-form-text x-form-field';
        var inputel = Ext.get(k);
        inputel.on('change', updatebox);
		//k.disabled = true;
	});

    // Click County radio button
    Ext.getCmp('cnty_rd').setValue(true);
});
