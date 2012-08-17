<?xml version="1.0" encoding="UTF-8"?>
<ns1:objectList xmlns:ns1="http://ns.dataone.org/service/types/v1" count="${count}" start="${start}" total="${total}">
    % for d in docs:
    <objectInfo>
        <identifier>${d['identifier']}</identifier>
        <formatId>${d['format']}</formatId>
        <checksum algorithm="${d['algo']}">${d['checksum']}</checksum>
        <dateSysMetadataModified>${d['date']}</dateSysMetadataModified>
        <size>${d['size']}</size>
    </objectInfo>
    % endfor
</ns1:objectList>
