Ext.namespace('GeoExt.grid');

GeoExt.grid.FeatureSelectionModelMixin = {
    autoActivateControl: true,
    layerFromStore: true,
    selectControl: null, 
    bound: false,
    superclass: null,
    constructor: function(config) {
        config = config || {};
        if(config.selectControl instanceof OpenLayers.Control.SelectFeature) { 
            if(!config.singleSelect) {
                var ctrl = config.selectControl;
                config.singleSelect = !(ctrl.multiple || !!ctrl.multipleKey);
            }
        } else if(config.layer instanceof OpenLayers.Layer.Vector) {
            this.selectControl = this.createSelectControl(
                config.layer, config.selectControl
            );
            delete config.layer;
            delete config.selectControl;
        }
        this.superclass = arguments.callee.superclass;
        this.superclass.constructor.call(this, config);
    },
    initEvents: function() {
        this.superclass.initEvents.call(this);
        if(this.layerFromStore) {
            var layer = this.grid.getStore() && this.grid.getStore().layer;
            if(layer &&
               !(this.selectControl instanceof OpenLayers.Control.SelectFeature)) {
                this.selectControl = this.createSelectControl(
                    layer, this.selectControl
                );
            }
        }
        if(this.selectControl) {
            this.bind(this.selectControl);
        }
    },
    createSelectControl: function(layer, config) {
        config = config || {};
        var singleSelect = config.singleSelect !== undefined ?
                           config.singleSelect : this.singleSelect;
        config = OpenLayers.Util.extend({
            toggle: true,
            multipleKey: singleSelect ? null :
                (Ext.isMac ? "metaKey" : "ctrlKey")
        }, config);
        var selectControl = new OpenLayers.Control.SelectFeature(
            layer, config
        );
        layer.map.addControl(selectControl);
        return selectControl;
    },
    bind: function(obj, options) {
        if(!this.bound) {
            options = options || {};
            this.selectControl = obj;
            if(obj instanceof OpenLayers.Layer.Vector) {
                this.selectControl = this.createSelectControl(
                    obj, options.controlConfig
                );
            }
            if(this.autoActivateControl) {
                this.selectControl.activate();
            }
            var layers = this.getLayers();
            for(var i = 0, len = layers.length; i < len; i++) {
                layers[i].events.on({
                    featureselected: this.featureSelected,
                    featureunselected: this.featureUnselected,
                    scope: this
                });
            }
            this.on("rowselect", this.rowSelected, this);
            this.on("rowdeselect", this.rowDeselected, this);
            this.bound = true;
        }
        return this.selectControl;
    },
    unbind: function() {
        var selectControl = this.selectControl;
        if(this.bound) {
            var layers = this.getLayers();
            for(var i = 0, len = layers.length; i < len; i++) {
                layers[i].events.un({
                    featureselected: this.featureSelected,
                    featureunselected: this.featureUnselected,
                    scope: this
                });
            }
            this.un("rowselect", this.rowSelected, this);
            this.un("rowdeselect", this.rowDeselected, this);
            if(this.autoActivateControl) {
                selectControl.deactivate();
            }
            this.selectControl = null;
            this.bound = false;
        }
        return selectControl;
    },
    featureSelected: function(evt) {
        if(!this._selecting) {
            var store = this.grid.store;
            var row = store.findBy(function(record, id) {
                return record.data.feature == evt.feature;
            });
            if(row != -1 && !this.isSelected(row)) {
                this._selecting = true;
                this.selectRow(row, !this.singleSelect);
                this._selecting = false;
                // focus the row in the grid to ensure it is visible
                this.grid.getView().focusRow(row);
            }
        }
    },
    featureUnselected: function(evt) {
        if(!this._selecting) {
            var store = this.grid.store;
            var row = store.findBy(function(record, id) {
                return record.data.feature == evt.feature;
            });
            if(row != -1 && this.isSelected(row)) {
                this._selecting = true;
                this.deselectRow(row); 
                this._selecting = false;
                this.grid.getView().focusRow(row);
            }
        }
    },
    rowSelected: function(model, row, record) {
        var feature = record.data.feature;
        if(!this._selecting && feature) {
            var layers = this.getLayers();
            for(var i = 0, len = layers.length; i < len; i++) {
                if(layers[i].selectedFeatures.indexOf(feature) == -1) {
                    this._selecting = true;
                    this.selectControl.select(feature);
                    this._selecting = false;
                    break;
                }
            }
         }
    },
    rowDeselected: function(model, row, record) {
        var feature = record.data.feature;
        if(!this._selecting && feature) {
            var layers = this.getLayers();
            for(var i = 0, len = layers.length; i < len; i++) {
                if(layers[i].selectedFeatures.indexOf(feature) != -1) {
                    this._selecting = true;
                    this.selectControl.unselect(feature);
                    this._selecting = false;
                    break;
                }
            }
        }
    },
    getLayers: function() {
        return this.selectControl.layers || [this.selectControl.layer];
    }
};

GeoExt.grid.FeatureSelectionModel = Ext.extend(
    Ext.grid.RowSelectionModel,
    GeoExt.grid.FeatureSelectionModelMixin
);
