<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en">
<head>
    <title>${description}</title>
    <link rel="stylesheet" href="/css/style.css"></link>
    <style>
        dl {margin-bottom: 50px; display:block; position:relative;}
        dl dt, dl dd {display: block; position:relative; z-index:2;}
        dl dt {background: #bcdfbb; color: #285327; clear:both; float: left; font-weight:bold; margin-right: 10px; padding: 5px; width:100px;}
        dl dd {margin: 2px 0; padding: 5px 0;}
        dt:before, dd:before {content:" "; display:block; position:absolute; z-index:-1;}
        pre {white-space:pre-wrap;font-size:smaller;}
    </style>
</head>
<body>
    <h1>${description}</h1>
    <div>
        
    </div>
    <h2>Downloads</h2>
    <div>
        % for item in downloads:
            % for key in item.keys():
                <a href="${item[key]}">${key}</a>
            % endfor
        % endfor
    </div>
    <h2>Web Services</h2>
    <div>
        % for item in services:
            % for key in item.keys():
                <a href="${item[key]}">${key}</a>
            % endfor
        % endfor
    </div>
    <h2>Metadata</h2>
    <div>
        % for item in metadata:
            % for key, value in item.items():
                % for fmt, url in value.items():
                    <a href="${url}">${key}.${fmt}</a>
                % endfor
            % endfor
        % endfor
    </div>
</body>

