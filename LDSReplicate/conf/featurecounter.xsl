<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns="http://www.opengis.net/wfs" 
  xmlns:wfs="http://www.opengis.net/wfs"
  xmlns:gml="http://www.opengis.net/gml"
  xmlns:v="http://data.linz.govt.nz/ns/v"
>
<xsl:output method="text"/>
<xsl:strip-space elements="*"/>

<xsl:template match="wfs:FeatureCollection">
	<xsl:value-of select="@numberOfFeatures"/>
</xsl:template>


<xsl:template match="*">
    <xsl:message terminate="no">
        WARNING: Unmatched element: <xsl:value-of select="name()"/>
    </xsl:message>
</xsl:template>


</xsl:stylesheet>
