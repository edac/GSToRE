<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fn="http://www.w3.org/2005/xpath-functions"
    version="2.0" exclude-result-prefixes="#all">
    <xsl:output method="text" encoding="UTF-8"/>
    
    <xsl:variable name="all-sources" select="/metadata/sources"/>
    <xsl:variable name="all-citations" select="/metadata/citations"/>
    <xsl:variable name="all-contacts" select="/metadata/contacts"/>
    
    <xsl:template match="/metadata">
        <xsl:variable name="identity-citation-id" select="identification/citation[@role='identify']/@ref"/>
        <xsl:variable name="identity-citation">
            <xsl:copy-of select="$all-citations/citation[@id=$identity-citation-id]"/>
        </xsl:variable>
        
        <xsl:variable name="publication-date">
            <xsl:variable name="pub-time">
                <xsl:choose>
                    <xsl:when test="$identity-citation/citation/publication/pubdate/@time">
                        <xsl:value-of select="$identity-citation/citation/publication/pubdate/@time"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="'00:00:00'"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:variable>
            <xsl:value-of select="fn:concat($identity-citation/citation/publication/pubdate/@date, 'T', $pub-time)"/>
        </xsl:variable>
        
        <xsl:variable name="identity-ptcontac-id" select="identification/contact[@role='point-contact']/@ref"/>
        <xsl:variable name="identity-contact">
            <xsl:if test="$all-contacts/contact[@id=$identity-ptcontac-id]">
                <xsl:copy-of select="$all-contacts/contact[@id=$identity-ptcontac-id]"/>
            </xsl:if>
        </xsl:variable>  
{
    "dataset": {
        "id": "<xsl:value-of select="identification/@dataset"/>",
        "name": "<xsl:value-of select="identification/title"/>",
        "abstract": "<xsl:value-of select="identification/abstract"/>",
        "purpose": "<xsl:value-of select="identification/purpose"/>",
        "publication_date": "<xsl:value-of select="$publication-date"/>",
        "topic": "<xsl:value-of select="identification/isotopic"/>",
        "themes": [<xsl:for-each select="identification/themes/theme/term">"<xsl:value-of select="."/>",</xsl:for-each>],
        "places": [<xsl:for-each select="identification/places/place/term">"<xsl:value-of select="."/>",</xsl:for-each>],
        "indirect_spatial_ref": "<xsl:value-of select="spatial/indspref"/>",
        "bbox": "<xsl:value-of select="fn:concat(spatial/west, ' ', spatial/south, ' ', spatial/east, ' ', spatial/north)"/>", 
        "contact_organization": "<xsl:value-of select="$identity-contact/contact/organization/name"/>",
        "contact_person": "<xsl:value-of select="$identity-contact/contact/organization/person"/>",
        "contact_position": "<xsl:value-of select="$identity-contact/contact/position"/>"
    }
}
    </xsl:template>
    
</xsl:stylesheet>