<?xml version="1.0" encoding="utf-8"?>
<application xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xsi:schemaLocation="http://research.sun.com/wadl/2006/10 wadl.xsd" xmlns="http://research.sun.com/wadl/2006/10">
<resources base="http://rgisbeta.unm.edu/">
<resource path="browsedata">
<resource path="tree">
<resource path="themes">
<method name="POST">
<request>
<param href="#nodeParam"/>
<param href="#offsetParam"/>
<param href="#limitParam"/>
<param href="#nodeParam"/>
<representation mediaType="application/xml" element="newRepresentation"/>
</request>
</method>
</resource>
</resource>
</resource>
<resource path="datasets">
<resource path="10031">
<resource path="mapper">
<method name="GET">
<request/>
</method>
</resource>
</resource>
</resource>
</resources>
<param name="node" style="query" id="nodeParam">
<doc xml:lang="en" title="node (Sample Value: Cadastral_|_NSDI_|_PLSS - (Preliminary))">Estimated Type (unsure): []</doc>
</param>
<param name="offset" type="xsd:integer" style="query" id="offsetParam">
<doc xml:lang="en" title="offset (Sample Value: 0)">Estimated Type (supposed): [xsd:integer]</doc>
</param>
<param name="limit" type="xsd:integer" style="query" id="limitParam">
<doc xml:lang="en" title="limit (Sample Value: 25)">Estimated Type (sure): [xsd:integer]</doc>
</param>
</application>
