<?xml version="1.0" encoding="UTF-8"?>
<d1:systemMetadata xmlns:d1="http://ns.dataone.org/service/types/v1">
    <serialVersion>1</serialVersion>
    <identifier>${pid}</identifier>
    <formatId>${obj_format}</formatId>
    <size>${file_size}</size>
    <checksum algorithm="${hash_type}">${hash}</checksum>
    <submitter>uid=${uid},o=${o},dc=${dc},dc=${org}</submitter>
    <rightsHolder>uid=${uid},o=${o},dc=${dc},dc=${org}</rightsHolder>
    <accessPolicy>
        <allow>
            <subject>public</subject>
            <permission>read</permission>
        </allow>
    </accessPolicy>
    <replicationPolicy replicationAllowed="false"></replicationPolicy>
    
    % if obsoletes:
        <obsoletes>${obsoletes}</obsoletes>
    % endif

    % if obsoletedby:
        <obsoletedBy>${obsoletedby}</obsoletedBy>
    % endif
    
    <dateUploaded>${dateadded}</dateUploaded>
    <dateSysMetadataModified>${metadata_modified}</dateSysMetadataModified>
    
    <originMemberNode>${mn}</originMemberNode>
    <authoritativeMemberNode>${mn}</authoritativeMemberNode>
</d1:systemMetadata>

