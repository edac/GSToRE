<?xml version="1.0" encoding="UTF-8"?>
<d1:log xmlns:d1="http://ns.dataone.org/service/types/v1" count="${results}" start="${offset}" total="${total}">
    % for d in docs:
    <logEntry>
        <entryId>${d['id']}</entryId>
        <identifier>${d['identifier']}</identifier>
        <ipAddress>${d['ip']}</ipAddress>
        <userAgent>${d['useragent']}</userAgent>
        <subject>${d['subject']}</subject>
        <event>${d['event']}</event>
        <dateLogged>${d['dateLogged']}</dateLogged>
        <nodeIdentifier>${d['node']}</nodeIdentifier>
    </logEntry>
    % endfor
    
</d1:log>

