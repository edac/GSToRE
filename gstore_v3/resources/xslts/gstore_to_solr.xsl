<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:fn="http://www.w3.org/2005/xpath-functions"
    version="2.0" exclude-result-prefixes="#all">
    <xsl:output method="xml" encoding="UTF-8" indent="yes"/>
    
    <xsl:variable name="all-sources" select="/metadata/sources"/>
    <xsl:variable name="all-citations" select="/metadata/citations"/>
    <xsl:variable name="all-contacts" select="/metadata/contacts"/>
    
    <xsl:template match="/metadata">
        
        <add>
            <doc>
                <field name="id">
                    <xsl:value-of select="identification/@dataset"/>
                </field>
                <field name="name">
                    <xsl:value-of select="identification/title"/>
                </field>
                <field name="abstract">
                    <xsl:value-of select="identification/abstract"/>
                </field>
                <field name="purpose">
                    <xsl:value-of select="identification/purpose"/>
                </field>
                
                <xsl:variable name="identity-citation-id" select="identification/citation[@role='identify']/@ref"/>
                <xsl:variable name="identity-citation">
                    <xsl:copy-of select="$all-citations/citation[@id=$identity-citation-id]"></xsl:copy-of>
                </xsl:variable>
                
                <field name="publication_date">
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
                </field>
                <field name="topic">
                    <xsl:value-of select="identification/isotopic"/>
                </field>
                <xsl:for-each select="identification/themes/theme/term">
                    <field name="keyword">
                        <xsl:value-of select="."/>
                    </field>
                </xsl:for-each>
                <xsl:for-each select="identification/places/place/term">
                    <field name="placename">
                        <xsl:value-of select="."/>
                    </field>
                </xsl:for-each>
                <field name="indirect_spatial_ref">
                    <xsl:value-of select="spatial/indspref"/>
                </field>
                <field name="bbox">
                    <xsl:value-of select="fn:concat(spatial/west, ' ', spatial/south, ' ', spatial/east, ' ', spatial/north)"/>
                </field>
                
                <xsl:variable name="identity-ptcontac-id" select="identification/contact[@role='point-contact']/@ref"/>
                <xsl:if test="$all-contacts/contact[@id=$identity-ptcontac-id]">
                    <xsl:apply-templates select="$all-contacts/contact[@id=$identity-ptcontac-id]"/>
                </xsl:if>
                
                
            </doc>
        </add>
        
    </xsl:template>
    
    <xsl:template match="contact">
        
        <field name="contact_organization">
            <xsl:value-of select="organization/name"/>
        </field>
        <xsl:if test="organization/person">
            <field name="contact_person"><xsl:value-of select="organization/person"/></field>
        </xsl:if>
        <xsl:if test="position">
            <field name="contact_position"><xsl:value-of select="position"/></field>
        </xsl:if>
        
        <!--<cntinfo>
            <xsl:if test="address">
                <cntaddr>
                    <addrtype><xsl:value-of select="address/@type"/></addrtype>
                    <xsl:for-each select="address/addr">
                        <address><xsl:value-of select="."/></address>
                    </xsl:for-each>
                    <city><xsl:value-of select="address/city"/></city>
                    <state><xsl:value-of select="address/state"/></state>
                    <postal><xsl:value-of select="address/postal"/></postal>
                    <xsl:if test="address/country and address/country != ''">
                        <country><xsl:value-of select="address/country"/></country>
                    </xsl:if>
                </cntaddr>
            </xsl:if>
            <xsl:if test="voice">
                <cntvoice><xsl:value-of select="voice"/></cntvoice>
            </xsl:if>
            <xsl:if test="fax">
                <cntfax><xsl:value-of select="fax"/></cntfax>
            </xsl:if>
            <xsl:if test="email">
                <cntemail><xsl:value-of select="email"/></cntemail>
            </xsl:if>
            <xsl:if test="hours">
                <hours><xsl:value-of select="hours"/></hours>
            </xsl:if>
            <xsl:if test="instructions">
                <cntinst><xsl:value-of select="instructions"/></cntinst>
            </xsl:if>
        </cntinfo>-->
    </xsl:template>
</xsl:stylesheet>