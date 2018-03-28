var tfarray = [];
var graphattributes = [];
var app = angular.module("atest", []);
if (!Array.isArray(tabledata)) {
    var tabledata = {};
}
analyticsobj = {
    "params": []
}
var app = angular.module("myApp", []);
app.controller("paramsCtrl", function($scope, $timeout) {
    $scope.atestValue = 0;
    //  var _base_url = "http://129.24.63.66/gstore_v3/apps/energize/datasets/";
    var _base_url = "http://gstore.unm.edu/apps/energize/datasets/"
    var params = parse_query();
    var uuid = params["uuid"];
    var csv_analytics_url = _base_url + uuid + "/dataset.json?sort=obsorder=asclimit=1000" //1000000"
    var attributes_url = _base_url + uuid + "/attributes.json"
    var uuid_dataset_url = "https://gstore.unm.edu/apps/energize/search/datasets.json?version=3uuid=" + uuid
    var fullcount = 0;
    var displayat = 0;
    var currentcount = 0;
    var percent = 0;
    var line = "";
    var csvtitles = "";
    var csvdata = "";
    var isDone = false
    var nodatadict = {};
    $.getJSON(uuid_dataset_url, function(json) {
        var description = json.results[0].description;
        // var startdate = json.results[0].valid_dates.start;
        // var enddate = json.results[0].valid_dates.end;
        document.getElementById("description").innerHTML = description;
    });
    $.get(attributes_url, function(data, status) {
        var arrayLength = data.results.length;
        for (var i = 0; i < arrayLength; i++) {
            nodatadict[data.results[i].name] = data.results[i].nodata;
            if (data.results[i].datatype != "string") {
                graphattributes.push(data.results[i].name);
            }
        }
    });
    console.log(nodatadict);
    oboe(csv_analytics_url).node("subtotal", function(thecount) {
        fullcount = thecount;
    }).node("features[0]", function(feature) {
        for (var k in feature.properties) {
            if (k !== "observed" && k !== "date" && k !== "time" && k !== "DTG") {}
        }
    }).node("features.*", function(feature) {
        $timeout(function() {
            currentcount = currentcount + 1
            templine = feature.properties.observed.replace("+00", "").replace(/-/g, "/").replace("T", " ");
            for (var k in feature.properties) {
                if (feature.properties.hasOwnProperty(k)) {
                    var found = false;
                    for (var i = 0; i < analyticsobj.params.length; i++) {
                        if (analyticsobj.params[i].name == k) {
                            found = true;
                            break;
                        }
                    }
                    //            if (!found && k !== "observed" && k !== "date" && k !== "time" && k !== "DTG") {
                    var isthere = $.inArray(k, graphattributes)
                    if (!found && isthere != -1 && k !== "site_id") {
                        if (tabledata[k] === undefined) {
                            tabledata[k] = [];
                        }
                        var tmpobj = {
                            name: k,
                            analytics: {
                                count: 0,
                                max: 0,
                                max: 0,
                                min: 0,
                                range: 0,
                                midrange: 0,
                                sum: 0,
                                mean: 0,
                                median: 0,
                                modes: 0,
                                variance: 0,
                                standardDeviation: 0,
                                meanAbsoluteDeviation: 0,
                                //zScores: 0
                            }
                        }
                        analyticsobj.params.push(tmpobj);
                    }
  //                  var index = analyticsobj.params.findIndex(param => param.name === k);
                      var index = analyticsobj.params.findIndex(function(param){ param.name == k; }); 
//                    var index = analyticsobj.params.findIndex(function(param){ return param.name === k});

                    if (index !== -1) {
                        var val = isNaN(feature.properties[k]);
                        if (feature.properties[k] == nodatadict[k]) {
                            val = true;
                            templine = templine + ",";
                        }
                        if (val == false) {
                            if (typeof feature.properties[k] === "string") {
                                templine = templine + "," + Number(feature.properties[k])
                            } else {
                                templine = templine + "," + feature.properties[k]
                            }
                            if (typeof feature.properties[k] === "string") {
                                tabledata[k].push(Number(feature.properties[k]));
                            } else {
                                tabledata[k].push(feature.properties[k]);
                            }
                            var titlesarray = Object.keys(tabledata);
                            analyticsobj.params[index].analytics.count = tabledata[k].length;
                            analyticsobj.params[index].analytics.max = arr.max(tabledata[k]);
                            analyticsobj.params[index].analytics.min = arr.min(tabledata[k]);
                            analyticsobj.params[index].analytics.range = arr.range(tabledata[k]).toFixed(2);
                            analyticsobj.params[index].analytics.midrange = arr.midrange(tabledata[k]).toFixed(2);
                            analyticsobj.params[index].analytics.sum = arr.sum(tabledata[k]).toFixed(2);
                            analyticsobj.params[index].analytics.mean = arr.mean(tabledata[k]).toFixed(2);
                            analyticsobj.params[index].analytics.median = arr.median(tabledata[k]).toFixed(2);
                            analyticsobj.params[index].analytics.modes = arr.modes(tabledata[k]);
                            analyticsobj.params[index].analytics.variance = arr.variance(tabledata[k]);
                            analyticsobj.params[index].analytics.standardDeviation = arr.standardDeviation(tabledata[k]);
                            analyticsobj.params[index].analytics.meanAbsoluteDeviation = arr.meanAbsoluteDeviation(tabledata[k]);
                        }
                    }
                }
            }
            //percent=((currentcount/fullcount) * 100).toFixed(0)
            $scope.observationend = templine.split(",")[0];
            $scope.observationstart = line.split(",")[0]
            if (currentcount == fullcount) {
                $scope.isDone = true
            }
            $scope.percent = ((currentcount / fullcount) * 100).toFixed(0)
            $scope.count = currentcount; // currentcount;
            $scope.fullcount = fullcount;
            // nanobar.go(Math.trunc((currentcount / fullcount) * 100))
            $scope.params = analyticsobj.params;
            $scope.atestValue++
                line = line + "\n" + templine
            csvdata = titlesarray.join(", ")
            datestring = "date,"
            csvdata = datestring.concat(csvdata)
            csvdata = csvdata.concat(line)
            var checkboxhtml = ""
            titlesarray.forEach(function(entry) {
                if (titlesarray.indexOf(entry) <= 4) {
                    tfarray = tfarray.concat(true);
                    checkboxhtml += '\<input type="checkbox" id="'
                    checkboxhtml += titlesarray.indexOf(entry)
                    checkboxhtml += '" onClick="change(this)" checked>\n'
                } else {
                    tfarray = tfarray.concat(false);
                    checkboxhtml += '\<input type="checkbox" id="'
                    checkboxhtml += titlesarray.indexOf(entry)
                    checkboxhtml += '" onClick="change(this)">\n'
                }
                checkboxhtml += '<label for="'
                checkboxhtml += titlesarray.indexOf(entry)
                checkboxhtml += '">'
                checkboxhtml += entry
                checkboxhtml += '<\/label><br/>\n'
            });
            document.getElementById("checkboxbox").innerHTML = checkboxhtml
            g = new Dygraph(document.getElementById("div_g"), csvdata, {
                rollPeriod: 100,
                connectSeparatedPoints: false,
                visibility: tfarray,
                labelsDiv: document.getElementById('status'),
                labelsSeparateLines: true,
                labelsKMB: true,
                legend: 'always',
                showRangeSelector: true,
                hideOverlayOnMouseOut: false,
                rangeSelectorPlotFillColor: '#007680',
                //  ['#F2F3F4', '#222222', '#F3C300', '#875692', '#F38400', '#A1CAF1', '#BE0032', '#C2B280', '#848482', '#008856', '#E68FAC', '#0067A5', '#F99379', '#604E97', '#F6A600', '#B3446C', '#DCD300', '#882D17', '#8DB600', '#654522', '#E25822', '#2B3D26'],
                //    }
                colors: ['#BE0032', '#C2B280', '#848482', '#008856', '#E68FAC', '#0067A5', '#F99379', '#604E97', '#F6A600', '#B3446C', '#DCD300', '#882D17', '#882D17', '#8DB600', '#654522', '#E25822'],
            });
            $scope.csvdata = csvdata;
            $scope.$digest();
        }, 20);
    });
});

function containsObject(obj, list) {
    var i;
    for (i = 0; i < list.length; i++) {
        if (list[i] === obj) {
            return true;
        }
    }
    return false;
}
var options = {
    classname: 'my-class',
    id: 'my-id',
};
// var nanobar = new Nanobar(options);
for (var key in tabledata) {
    if (p.hasOwnProperty(key)) {}
}
for (var name in tabledata) {}

function showCheckboxes() {
    var checkboxes = document.getElementById("checkboxes");
    if (!expanded) {
        checkboxes.style.display = "block";
        expanded = true;
    } else {
        checkboxes.style.display = "none";
        expanded = false;
    }
}

function change(el) {
    g.setVisibility(parseInt(el.id), el.checked);
    setStatus();
}

function setStatus() {
    document.getElementById("visibility").innerHTML = g.visibility().toString();
}
// setInterval(function() {
//   g.updateOptions( { 'file': csvata } );
// }, 2000);
