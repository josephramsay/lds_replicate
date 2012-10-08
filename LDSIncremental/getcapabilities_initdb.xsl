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
	<xsl:text>CREATE TABLE LDS_CONFIG (ID VARCHAR(8), PKEY VARCHAR(32) DEFAULT 'ID', NAME VARCHAR(128), GRP VARCHAR(256), LASTMODIFIED TIMESTAMP DEFAULT NULL, GEOCOLUMN VARCHAR(32) DEFAULT 'SHAPE',EPSG INT DEFAULT NULL,CQL VARCHAR(256) DEFAULT NULL);&#xa;</xsl:text>
	<xsl:for-each select="wfs:FeatureType">
		<xsl:sort select="wfs:Name"/>
		<xsl:text>INSERT INTO LDS_CONFIG (ID,NAME,GRP) VALUES(</xsl:text>
		<!--ID field, v:x###-->
		<xsl:text>'</xsl:text><xsl:value-of select="normalize-space(wfs:Name)"/><xsl:text>',</xsl:text>
		<!--PKEY field, default to 'ID'-->
		<!--NAME field, descriptive string used to name table-->
		<xsl:text>'</xsl:text><xsl:value-of select="normalize-space(wfs:Title)"/><xsl:text>',</xsl:text>
		<!--GROUP field, tag values init to keywords field-->
		<xsl:text>'</xsl:text>
		<xsl:for-each select="ows:Keywords/ows:Keyword">
				<xsl:value-of select="normalize-space(.)"/>
				<xsl:choose>
					<xsl:when test="position() != last()">
						<xsl:text>,</xsl:text>
					</xsl:when>
					<xsl:otherwise>
						<xsl:text>'</xsl:text>
					</xsl:otherwise>
				</xsl:choose>
		</xsl:for-each>
		<!--LASTMODIFIED field, in SQL cant default to null-->
		<!--GEOCULUMN field, default to 'SHAPE'-->
		<!--EPSG field, default to null-->
		<!--CQL field, default to null-->
		<xsl:text>);&#xa;</xsl:text>
	</xsl:for-each>
</xsl:template>

</xsl:stylesheet>
