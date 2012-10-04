<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns="http://www.opengis.net/wfs" 
  xmlns:wfs="http://www.opengis.net/wfs"
  xmlns:ows="http://www.opengis.net/ows"
>

<xsl:strip-space elements="*"/>
<xsl:template match="/">
	<!--
	<xsl:message>Matched root node</xsl:message>
	<xsl:message>
		<xsl:for-each select="ancestor-or-self::*">
		  <xsl:value-of select="name(.)"/> /
		</xsl:for-each>
	</xsl:message>
  	-->
	<xsl:text># LDS Layer Properties Template</xsl:text>
	<xsl:apply-templates select="*"/>
</xsl:template>



<xsl:template match="wfs:FeatureTypeList">
	<!--
	<xsl:message>Matched FTL node</xsl:message>	
	<xsl:message>
		<xsl:for-each select="ancestor-or-self::*">
		  <xsl:value-of select="name(.)"/> /
		</xsl:for-each>
	</xsl:message>
	-->
	<xsl:for-each select="wfs:FeatureType">
		<xsl:sort select="wfs:Name"/>
		<xsl:text>&#xa;[</xsl:text><xsl:value-of select="normalize-space(wfs:Name)"/><xsl:text>]&#xa;</xsl:text>
		<xsl:text>pkey = id&#xa;</xsl:text>
		<xsl:text>name = </xsl:text><xsl:value-of select="normalize-space(wfs:Title)"/><xsl:text>&#xa;</xsl:text>
		<xsl:text>group = </xsl:text>
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
		<xsl:text>cql = &#xa;</xsl:text>
	</xsl:for-each>
</xsl:template>

</xsl:stylesheet>
