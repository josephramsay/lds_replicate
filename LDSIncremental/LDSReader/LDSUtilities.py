'''
Simple LDS specific utilities class

Created on 28/08/2012

@author: jramsay
'''


import re
import os
import logging
from StringIO import StringIO

from lxml import etree

ldslog = logging.getLogger('LDS')

class LDSUtilities(object):
    '''Does the LDS related stuff not specifically part of the datastore''' 

    
    @classmethod
    def splitLayerName(cls,layername):
        '''Splits a layer name typically in the format v:x### into /v/x### for URI inclusion'''
        return "/"+layername.split(":")[0]+"/"+layername.split(":")[1]
    
    @classmethod
    def cropChangeset(cls,layername):
        '''Removes changeset identifier from layer name'''
        return layername.rstrip("-changeset")
    
    @classmethod
    def checkDateFormat(cls,xdate):
        '''Checks a date parameter conforms to yyyy-MM-ddThh:mm:ss format'''        
        return type(xdate) is str and re.search('^\d{4}\-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2})?)$',xdate)

    # 772 time test string
    # http://wfs.data.linz.govt.nz/ldskey/v/x772-changeset/wfs?service=WFS&version=1.0.0&request=GetFeature&typeName=v:x772-changeset&viewparams=from:2012-09-29T07:00:00;to:2012-09-29T07:30:00&outputFormat=GML2
    
    @classmethod
    def checkLayerName(cls,lname):
        '''Makes sure a layer name conforms to v:x format'''
        return type(lname) is str and re.search('^v:x\d+$',lname) 
        
    @classmethod
    def checkCQL(cls,cql):
        '''Since CQL commands are freeform strings we need to try and validate at least the most basic errors. This is very simple
        RE matcher that just looks for valid predicates.
        
        <predicate> ::= <comparison predicate> | <text predicate> | <null predicate> | <temporal predicate> | <classification predicate> | <existence_predicate> | <between predicate> | <include exclude predicate>
               
        BNF http://docs.geotools.org/latest/userguide/library/cql/internal.html'''
        v = 0
        
        #comp pred
        if re.match('.*(?:!=|=|<|>|<=|>=)',cql):
            v+=1
        #text pred
        if re.match('.*(?:not\s*)?like.*',cql,re.IGNORECASE):
            v+=2
        #null pred
        if re.match('.*is\s*(?:not\s*)?null.*',cql,re.IGNORECASE):
            v+=4
        #time pred
        if re.match('.*(?:before|during|after)',cql,re.IGNORECASE):
            v+=8
        #clsf pred, not defined
        #exst pred
        if re.match('.*(?:does-not-)?exist',cql,re.IGNORECASE):
            v+=32
        #btwn pred
        if re.match('.*(?:not\s*)?between',cql,re.IGNORECASE):
            v+=64
        #incl pred
        if re.match('.*(?:include|exclude)',cql,re.IGNORECASE):
            v+=128
        #geo predicates just for good measure, returns v=16 overriding classification pred
        if re.match('.*(?:equals|disjoint|intersects|touches|crosses|within|contains|overlaps|bbox|dwithin|beyond|relate)',cql,re.IGNORECASE):
            v+=16
            
        ldslog.debug("CQL check:"+cql+":"+str(v))
        if v>0:
            return "&cql_filter="+cql
        else:
            return ""
    
    
    

class ConfigInitialiser(object):
    '''Initialises configuration, for use at first run'''

    @classmethod
    def buildConfiguration(cls,src,dst):
        '''Given a destination DS use this to select an XSL transform object and generate an output document that will initialise a new config file/table'''
        #df = os.path.normpath(os.path.join(os.path.dirname(__file__), "../debug.log"))
        
        uri = src.getCapabilities()
        xml = src.readDocument(uri)
        
        '''if we're going to the trouble of building a config initialiser then we're probably gonna want to run it'''
        if dst.config=='internal' and dst.CONFIG_XSL is not None:
            page = open(os.path.join(os.path.dirname(__file__), '../',dst.CONFIG_XSL),'r').read()
        else:
            page = open(os.path.join(os.path.dirname(__file__), '../getcapabilities_config.xsl'),'r').read()

        
        xslt = etree.XML(page)
        transform = etree.XSLT(xslt)
        
        doc = etree.parse(StringIO(xml))
        res = transform(doc)
        ldslog.debug(res)
        
        if dst.config=='internal':
            dst.executeSQL(str(res))
        else:
            open(os.path.join(os.path.dirname(__file__), '../',dst.DRIVER_NAME.lower()+".layer.properties"),'w').write(str(res))
        
        return res
        
