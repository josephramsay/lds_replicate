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
	<xsl:text>-- LDS Layer Properties Initialiser - SQL MSSQL&#xa;</xsl:text>
	<xsl:apply-templates/>
</xsl:template>

<xsl:template match="wfs:FeatureTypeList">
	<xsl:text>IF OBJECT_ID('LDS_CONFIG', 'U') IS NOT NULL DROP TABLE dbo.LDS_CONFIG;&#xa;</xsl:text>
	<xsl:text>CREATE TABLE LDS_CONFIG (ID VARCHAR(8) primary key,PKEY VARCHAR(32) DEFAULT 'ID',NAME VARCHAR(128), CATEGORY VARCHAR(256),LASTMODIFIED DATETIME NULL DEFAULT NULL,GEOCOLUMN VARCHAR(32) DEFAULT 'SHAPE',EPSG INT NULL DEFAULT NULL,DISCARD VARCHAR(256) NULL DEFAULT NULL,CQL VARCHAR(256) NULL DEFAULT NULL);&#xa;</xsl:text>
	<xsl:for-each select="wfs:FeatureType">
		<xsl:sort select="wfs:Name"/>
		<xsl:text>INSERT INTO LDS_CONFIG (ID,NAME,CATEGORY) VALUES(</xsl:text>
		<!--ID field, v:x###-->
		<xsl:text>'</xsl:text><xsl:value-of select="normalize-space(wfs:Name)"/><xsl:text>',</xsl:text>
		<!--PKEY field, default to 'ID'-->
		<!--NAME field, descriptive string used to name table-->
		<xsl:text>'</xsl:text>
		<!--recursive replace to deal with single quotes in text, Postgres escapes SQ with SQSQ-->
		<xsl:call-template name="replace-string">
            <xsl:with-param name="text" select="normalize-space(wfs:Title)"/>
            <xsl:with-param name="replace" select='"&apos;"' />
            <xsl:with-param name="with" select='"&apos;&apos;"'/>
        </xsl:call-template>
		<xsl:text>',</xsl:text>
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
		<!--DISCARD field, default to null-->
		<!--CQL field, default to null-->
		<xsl:text>);&#xa;</xsl:text>
	</xsl:for-each>
</xsl:template>

<xsl:template name="replace-string">
    <xsl:param name="text"/>
    <xsl:param name="replace"/>
    <xsl:param name="with"/>
    <xsl:choose>
      <xsl:when test="contains($text,$replace)">
        <xsl:value-of select="substring-before($text,$replace)"/>
        <xsl:value-of select="$with"/>
        <xsl:call-template name="replace-string">
          <xsl:with-param name="text" select="substring-after($text,$replace)"/>
          <xsl:with-param name="replace" select="$replace"/>
          <xsl:with-param name="with" select="$with"/>
        </xsl:call-template>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$text"/>
      </xsl:otherwise>
    </xsl:choose>
 </xsl:template>


<xsl:template match="*">
  <xsl:message terminate="no">
   WARNING: Unmatched element: <xsl:value-of select="name()"/>
  </xsl:message>
</xsl:template>




</xsl:stylesheet>
