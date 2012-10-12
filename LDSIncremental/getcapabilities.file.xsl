<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns="http://www.opengis.net/wfs" 
  xmlns:wfs="http://www.opengis.net/wfs"
  xmlns:ows="http://www.opengis.net/ows"
>
<xsl:output method="text"/>
<xsl:strip-space elements="*"/>

<xsl:template match="wfs:WFS_Capabilities">
	<xsl:text># LDS Layer Properties Initialiser - File&#xa;</xsl:text>
	<xsl:apply-templates/>
</xsl:template>

<xsl:template match="wfs:FeatureTypeList">
	<xsl:for-each select="wfs:FeatureType">
		<xsl:sort select="wfs:Name"/>
		<xsl:text>&#xa;[</xsl:text><xsl:value-of select="normalize-space(wfs:Name)"/><xsl:text>]&#xa;</xsl:text>
		<xsl:text>pkey = id&#xa;</xsl:text>
		<xsl:text>name = </xsl:text><xsl:value-of select="normalize-space(wfs:Title)"/><xsl:text>&#xa;</xsl:text>
		<xsl:text>category = </xsl:text>
		<xsl:for-each select="ows:Keywords/ows:Keyword">
				<xsl:value-of select="normalize-space(.)"/>
				<xsl:choose>
					<xsl:when test="position() != last()">
						<xsl:text>,</xsl:text>
					</xsl:when>
					<xsl:otherwise>
						<xsl:text>&#xa;</xsl:text>
					</xsl:otherwise>
				</xsl:choose>
		</xsl:for-each>
		<xsl:text>lastmodified = &#xa;</xsl:text>
		<xsl:text>geocolumn = shape&#xa;</xsl:text>
		<xsl:text>epsg = &#xa;</xsl:text>
		<xsl:text>discard = &#xa;</xsl:text>
		<xsl:text>cql = &#xa;</xsl:text>
	</xsl:for-each>
</xsl:template>

<xsl:template match="*">
    <xsl:message terminate="no">
        WARNING: Unmatched element: <xsl:value-of select="name()"/>
    </xsl:message>
</xsl:template>

</xsl:stylesheet>
