<%! import json %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
    "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <title>GSTORE Dataset Mapper Tool - ${description}</title>
  <script type="text/javascript" src="http://extjs.cachefly.net/ext-3.2.1/adapter/ext/ext-base.js"></script>
  <script type="text/javascript" src="http://extjs.cachefly.net/ext-3.2.1/ext-all.js"></script>
  <link rel="stylesheet" type="text/css" href="http://extjs.cachefly.net/ext-3.2.1/resources/css/ext-all.css" />
  <link rel="stylesheet" type="text/css" href="${MEDIA_URL}/css/style.css" />
  <link rel="stylesheet" type="text/css" href="${MEDIA_URL}/css/mapper.css" />

  <!--<script src="http://www.openlayers.org/api/2.10/OpenLayers.js"></script>-->
  <script src="${MEDIA_URL}/javascript/openlayers/2.10/OpenLayers.js"></script>
  <script type="text/javascript" src="${MEDIA_URL}/javascript/geoext/1.0/GeoExt.js"></script>

 <!--
  <script type="text/javascript" src="${MEDIA_URL}/javascript/jquery1_8/jquery-1.8.1.min.js"></script>
  <script type="text/javascript" src="${MEDIA_URL}/javascript/jquery_plugins/jqueryui/1.8.9/jquery-ui.min.js"></script>
  <script type="text/javascript" src="${MEDIA_URL}/javascript/fileDownload/jquery.fileDownload.js"></script>
  <link rel="stylesheet" type="text/css" href="${MEDIA_URL}/javascript/jquery_plugins/jqueryui/1.8.9/css/redmond/jquery-ui-1.8.9.custom.css" />
-->

  <script type="text/javascript" src= "${MEDIA_URL}/javascript/proj4js/proj4js-compressed.js"> </script>
  <script type="text/javascript" src= "${MEDIA_URL}/javascript/proj4js/defs/EPSG4326.js"> </script>
  <script type="text/javascript" src= "${MEDIA_URL}/javascript/proj4js/defs/EPSG26913.js"> </script>


  <script type="text/javascript">
//<![CDATA[
    var Layers = ${json.dumps(Layers) | n};
    var Description = ${json.dumps(Description) | n};
    var AppId = ${json.dumps(AppId) | n};
  //]]>
  </script>
  <script type="text/javascript" src= "${MEDIA_URL}/javascript/maps/mapper.js"> </script>

<!--
<script type="text/javascript">
    $(function() {
      $(document).on("click", "a.dataset_download", function() {
        var $preparingFileModal = $('#preparing-file-modal');
        $preparingFileModal.dialog({modal: true});

        $.fileDownload($(this).attr('href'), {
            successCallback: function(url) {
                $preparingFileModal.dialog(close);
            },
            failCallback: function(responseHtml, url) {
                $preparingFileModal.dialog(close);
                $('#error-modal').dialog({modal:true});
            }
        });
        return false;
      });
    });
  </script>
-->

<style>
    #dataset_desc {
        width: 100%;
    }
    #dataset_desc td {
        text-align: left;
    }
    a {
        text-decoration: none;
    }
    div.download_notification {
        background-color: #C3D9FF;
        height: 500px;
    }
</style>
</head>

<body>
    <div id='preparing-file-modal' title='Preparing download...' style='display:none;'>
        We are preparing your download file, please be patient...
        <div class='ui-progressbar-value ui-corner-left ui-corner-right' style='width:100%;height:22px;margin-top:20px;'></div>
    </div>
    <div id='error-modal' title='Error' style='display:none;'>
        There was a problem generating your download, please try again.
    </div>
      <div id="description">
          <table id='dataset_desc'>
            <tr>
                <td width="40%"><b>${description}</b>
                  <font style='font-size: smaller'>${taxonomy}</font>
                </td>
                <td>
                <div style='float: right'>
                  <span>Date updated: ${str(dateadded)[0:-10]} </span>
                </div>
                </td>
            </tr>
            <tr>
              <td>Available formats for download
              </td>
              <td>
                    % for met in formats:
                        <a class="dataset_download" href="${met['text']}">${met['title']}</a> &nbsp; &nbsp;
                    % endfor
              </td>
            </tr>
            <tr>
                <td>Available web services</td>
                <td>
                    % for met in services:
                        <a href="${met['text']}">${met['title']}</a> &nbsp; &nbsp;
                    % endfor
                </td>
            </tr>
            <tr>
                <td>Metadata</td>
                <td>
                    % for met in metadata:
                        <a href="${met['text']}">${met['title']}</a> &nbsp; &nbsp;
                    % endfor
                </td>
            </tr>
        </table>
      </div>
</body>
</html>
