<?xml version="1.0" standalone='yes'?>

<!--

xml-to-text.xsl, version 1.0, 06/August/2002

This stylesheet transforms files from XML format to text format. It
was written with the idea of transforming FGDC metadata records, but
could be used for other types of XML files. The code could probably
stand to be cleaned up.

The stylesheet works as follows: The tags of the XML source document
are replaced by labels that are read from an external lookup table
(more about this below). Attributes associated with the tags are
ignored. If a tag appears in the source document and does not have a
translation, no tag label will be written. The element itself will
still be processed, however.

Handling of the content of the XML elements depends on type of
element. A compound element, which contains other elements, will
display those elements indented at a level that corresponds to the
nesting level. An element that contains text will display formatted
text. The look of the text depends on a global parameter. The default
is to display text with the same line wrapping as the original XML
file, but with indentation altered to fit the current indentation
level. This option would make sense if the XML source already had line
breaks that should be preserved. The other option is to fold lines so
they are no longer than a maximum specified length. Lines at broken at
some specified character (typically a space). If it is not possible to
break a line so that its length is less than the maximum length, then
it will be broken at the first possible place after the maximum
length. This option would make sense for documents with elements that
contain long blocks of text without line breaks. XML elements
containing mixed content (other XML elements as well as text) are
treated as compound elements, and the text is ignored. XML elements
such as comments and CDATA elements are ignored.

As an example, the following input:

<foo>
<bar>
        Text.
</bar>
<baz>
<quux>
        Even more text.
        On two or
        three lines.
</quux>
</baz>
</foo>

will be translated to:

Foo:
  Bar: Text
  Baz:
    Quux: Even more text.
      On two or
      three lines.

if using the default settings and assuming tag translations of the
nature of foo -> Foo.

This stylesheet relies on an external XML document to provide a
translation from the XML tag names to the words or phrases that
explain the tags. That document is accessed via the document call in
the match=* template. The document URI is contained in a global
parameter. Change the global parameter as appropriate. In particular
if delivering this stylesheet to a browser to apply the transform, be
sure that the URI refers to a network accessible resource. The format
of the external document should be:

<names>
  <name tagname="idinfo">Identification Information</name>
  <name tagname="citation">Citation</name>
...
</names>

The <name> elements should have the XML tag as a tagname attribute and
the translation as the content of the element.

This stylesheet has been tested with Xalan 2.4.D1 and Saxon 6.5.2.

This stylesheet was developed by Joseph Shirley of
Systems Engineering and Security Incorporated
Suite 700
Greenbelt, Maryland 20770

on behalf of

The National Oceanographic Data Center
1315 East West Highway
Silver Spring, MD 20910


-->


<!-- Top level directives -->

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="text"/>


<!-- Global parameters -->

<!-- 
  g-tag-translation-URI 
    This parameter specifies the URI of the external document
    containing the lookup table for the tag name translation. The
    default is the name of a file in the same directory as the
    stylesheet. Make sure this is a network accessible URI if
    delivering to a browser for translation.
-->
<xsl:param name="g-tag-translation-URI" select="'elements.xml'"/>

<!--
  g-indent-increment
    This parameter specifies the character string to be added before a
    label for each level of indentation. I.e. if you want each level
    indented by four spaces, then make this a four space character
    string. Set this to the empty string to forego indenting.
-->
<xsl:param name="g-indent-increment" select="'  '"/>

<!--
  g-text-field-handler
    This parameter specifies the handler to use for formatting text
    fields. The choices are 'fold' to fold lines at a maximum length
    and 'print-lines' to print lines with line breaks preserved as in
    the source XML document. If unspecified, or if an invalid choice
    is specified, 'print-lines' will be used.
-->
<xsl:param name="g-text-field-handler" select="'fold'"/>

<!--
  g-fold-width
    This parameter specifies the maximum length of a line when the
    'fold' text formatting option is chosen. This parameter is ignored
    for other text formatting options.
-->
<xsl:param name="g-fold-width" select="'80'"/>

<!--
  g-fold-character
    This parameter specifies where a line may be broken when the
    'fold' text formatting option is chosen. This parameter is ignored
    for other text formatting options.
-->
<xsl:param name="g-fold-character" select="' '"/>


<!-- Global variables -->

<xsl:variable name="newline">
<xsl:text>
</xsl:text>
</xsl:variable>


<!-- Templates -->

<xsl:template match="/">
  <xsl:apply-templates select="*"/>
</xsl:template>


<!--
  Apply to all elements. Determine whether it's a compound or text element
  and call the appropriate template.
-->

<xsl:template match="*">
  <xsl:param name="indent" select="''"/>
  <xsl:param name="indent-increment" select="$g-indent-increment"/>
  <xsl:variable name="tagname" select="name()"/>
  <xsl:variable name="tag-translation" select="document($g-tag-translation-URI)/*/name[@tagname=$tagname]"/>
  <xsl:variable name="output" select="concat($indent, $tag-translation, ': ')"/>
  <xsl:if test="string-length(normalize-space($tag-translation)) &gt; 0">
    <xsl:value-of select="$output"/>
  </xsl:if>

  <xsl:choose>
    <xsl:when test="*">
      <xsl:comment>This is a compound element (i.e. it has children)</xsl:comment>
      <xsl:choose>
        <xsl:when test="string-length(normalize-space($tag-translation)) &gt; 0">
          <xsl:comment>There is a tag translation</xsl:comment>
          <xsl:value-of select="$newline"/>
          <xsl:apply-templates select="*">
            <xsl:with-param name="indent" select="concat($indent, $indent-increment)"/>
          </xsl:apply-templates>
        </xsl:when>
        <xsl:otherwise>
          <xsl:comment>No tag translation, don't indent</xsl:comment>
&lt;pre&gt;
<xsl:apply-templates select="*">
   <xsl:with-param name="indent" select="concat($indent, '')"/>
</xsl:apply-templates>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:when>
    <xsl:when test="contains($g-text-field-handler,'fold')">
      <xsl:comment>This is a text element to be formatted with 'fold'</xsl:comment>
      <xsl:call-template name="fold">
        <xsl:with-param name="original-string" select="normalize-space(.)"/>
        <xsl:with-param name="length" select="number($g-fold-width) - string-length($output)"/>
        <xsl:with-param name="indent" select="concat($indent, $indent-increment)"/>
      </xsl:call-template>
    </xsl:when>
    <xsl:otherwise>
      <xsl:comment>This is a text element to be formatted with 'print-lines'</xsl:comment>
      <xsl:variable name="original-string">
        <xsl:call-template name="strip-leading-whitespace">
          <xsl:with-param name="content" select="."/>
        </xsl:call-template>
      </xsl:variable>
      <xsl:call-template name="print-lines">
        <xsl:with-param name="original-string" select="$original-string"/>
        <xsl:with-param name="indent" select="concat($indent, $indent-increment)"/>
      </xsl:call-template>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


<!--
  Text formatting template. Use existing line breaks. Indent each line
  to the current indent level. Return the next line each time the template
  is called.
-->

<xsl:template name="print-lines">
  <xsl:param name="original-string"/>
  <xsl:param name="indent"/>
  <xsl:param name="print-indent" select="0"/>
  <xsl:variable name="str1" select="substring-before($original-string,$newline)"/>
  <xsl:variable name="str2" select="substring-after($original-string,$newline)"/>
  <xsl:variable name="printstring">
    <xsl:call-template name="strip-leading-whitespace">
      <xsl:with-param name="content" select="$str1"/>
    </xsl:call-template>
  </xsl:variable>
  <xsl:comment>Print next line, unless it's a blank line after final text line.</xsl:comment>
  <xsl:choose>
    <xsl:when test="(string-length($printstring) &gt; 0) or (string-length(normalize-space($str2)) &gt; 0)">
      <xsl:if test="$print-indent">
        <xsl:comment>
          The first line may not be indented, because it's on the same line as
          the label.
        </xsl:comment>
        <xsl:value-of select="$indent"/>
      </xsl:if>
      <xsl:value-of select="$printstring"/>
      <xsl:value-of select="$newline"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:if test="normalize-space($original-string)">
        <xsl:comment>There's only one line.</xsl:comment>
        <xsl:if test="$print-indent">
          <xsl:comment>
            The first line may not be indented, because it's on the same line as
            the label.
          </xsl:comment>
          <xsl:value-of select="$indent"/>
        </xsl:if>
        <xsl:value-of select="$original-string"/>
        <xsl:value-of select="$newline"/>
      </xsl:if>
    </xsl:otherwise>
  </xsl:choose>
  <xsl:if test="$str2">
    <xsl:comment>There is more text to break, call recursively.</xsl:comment>
    <xsl:call-template name="print-lines">
      <xsl:with-param name="original-string" select="$str2"/>
      <xsl:with-param name="indent" select="$indent"/>
      <xsl:with-param name="print-indent" select="1"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>


<!--
  Strip the leading white space (including newlines or other characters
  that get translated to white space by normalize-space) from a block
  of text.
-->

<xsl:template name="strip-leading-whitespace">
  <xsl:param name="content"/>
  <xsl:variable name="normalized-text" select="normalize-space($content)"/>
  <xsl:variable name="first-char" select="substring($normalized-text,1,2)"/>
  <xsl:variable name="leading-spaces" select="substring-before($content,$first-char)"/>
  <xsl:value-of select="substring-after($content,$leading-spaces)"/>
</xsl:template>


<!--
  Text formatting template. Create line breaks. Indent each line to
  the current indent level. Return the next line each time the
  template is called.
-->

<xsl:template name="fold">
  <xsl:param name="original-string"/>
  <xsl:param name="length"/>
  <xsl:param name="indent"/>
  <xsl:param name="fold-width" select="$g-fold-width"/>
  <xsl:variable name="printstring">
    <xsl:choose>
      <xsl:when test="string-length($original-string) &gt; number($length)">
        <xsl:comment>Text is longer than max, chop it down and print next line.</xsl:comment>
        <xsl:call-template name="chop">
          <xsl:with-param name="newstring" select="''"/>
          <xsl:with-param name="original-string" select="$original-string"/>
          <xsl:with-param name="length" select="$length"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$original-string"/>
      </xsl:otherwise>
   </xsl:choose>
  </xsl:variable>
  <xsl:value-of select="$printstring"/>
  <xsl:value-of select="$newline"/>
  <xsl:variable name="str" select="substring-after($original-string, $printstring)"/>
  <xsl:if test="string-length($str)">
    <xsl:comment>More text, call fold recursively.</xsl:comment>
    <xsl:value-of select="$indent"/>
    <xsl:call-template name="fold">
      <xsl:with-param name="original-string" select="$str"/>
      <xsl:with-param name="length" select="number($fold-width) - string-length($indent)"/>
      <xsl:with-param name="indent" select="$indent"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>


<!--
  Create line breaks. Break only at specified line break
  character. If possible keep lines less than a specified maximum
  length, otherwise break at first acceptable character after
  maximum length. Return one line each time the template is called.
-->

<xsl:template name="chop">
  <xsl:param name="newstring"/>
  <xsl:param name="original-string"/>
  <xsl:param name="char" select="$g-fold-character"/>
  <xsl:param name="length"/>
  <xsl:variable name="str1">
    <xsl:comment>str1 is the part before the break.</xsl:comment>
    <xsl:choose>
      <xsl:when test="contains($original-string, $char)">
        <xsl:comment>The text contains a break character, chop it off.</xsl:comment>
        <xsl:value-of select="concat($newstring, substring-before($original-string, $char), $char)"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:comment>The text contains no break character, use it all.</xsl:comment>
        <xsl:value-of select="concat($newstring, $original-string)"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:variable name="str2">
    <xsl:comment>str2 is the part after the break.</xsl:comment>
    <xsl:choose>
      <xsl:when test="contains($original-string, $char)">
        <xsl:comment>The text contains a break character, take what is after that.</xsl:comment>
        <xsl:value-of select="substring-after($original-string, $char)"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:comment>The text contains no break character, use an empty string.</xsl:comment>
        <xsl:value-of select="''"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:variable>
  <xsl:choose>
    <xsl:when test="(string-length($str1) &lt; number($length)) and $str2">
      <xsl:variable name="return-value">
        <xsl:call-template name="chop">
          <xsl:with-param name="newstring" select="$str1"/>
          <xsl:with-param name="original-string" select="$str2"/>
          <xsl:with-param name="char" select="$char"/>
          <xsl:with-param name="length" select="$length"/>
        </xsl:call-template>
      </xsl:variable>
      <xsl:value-of select="$return-value"/>
    </xsl:when>
    <xsl:when test="$newstring">
      <xsl:value-of select="$newstring"/>
    </xsl:when>
    <xsl:otherwise>
      <xsl:value-of select="$str1"/>
    </xsl:otherwise>
  </xsl:choose>
</xsl:template>


</xsl:stylesheet>

