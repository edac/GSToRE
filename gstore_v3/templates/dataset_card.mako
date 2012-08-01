<%! import json %>
<div>
    <h1>${description}</h1>

    % for c in categories:
        <p>${c['theme']} -- ${c['subtheme']} -- ${c['groupname']}</p>
    % endfor
    
    <p>Downloads</p>
    <ul>
        % for dld in downloads:
            % for k in dld:
                <li><a>${k}</a></li>
            % endfor
        % endfor
    </ul>
    
</div>


    
