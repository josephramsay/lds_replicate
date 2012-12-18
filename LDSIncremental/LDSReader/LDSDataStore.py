'''
v.0.0.1

LDSIncremental -  LDSDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

LDSDataStore convenience subclass of WFSDataStore wrapping the LDS specific WFS instance. 

Created on 23/07/2012

@author: jramsay
'''
import re

import logging

from WFSDataStore import WFSDataStore
from urllib2 import urlopen
from LDSUtilities import LDSUtilities


ldslog = logging.getLogger('LDS')

class LDSDataStore(WFSDataStore):
    '''
    LDS DataStore provides standard options and URI methods along with convenience methods for common functions/documents expressed as 
    URI builders. For incremental specifically the change-column is defined here
    '''

    def __init__(self,conn_str=None,user_config=None):
        '''
        cons init driver
        '''
        #super WFS sets WFS driver and gets WFS config params
        #supersuper DataStore sets def flags (eg INCR)
        super(LDSDataStore,self).__init__(conn_str,user_config)
        
        self.CHANGE_COL = "__change__"
        self.psize = None
        
        (self.url,self.key,self.svc,self.ver,self.fmt,self.cql) = self.params

    #TEST. data partitions
    def setPartitionSize(self,psize):
        self.psize = psize
        
    def getCapabilities(self):
        '''GetCapabilities endpoint constructor'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        #uri = self.url+self.key+"/wfs?service=WFS"+"&version="+self.ver+"&request=GetCapabilities"
        #keyword specifier different between 1.0.0 (<ows:Keywords><ows:Keyword>) and 1.1.0 (<Keywords>) We enforce 1.1.0 to return per keyword version and more accurately parse layer groups
        uri = self.url+self.key+"/wfs?service=WFS&version=1.1.0&request=GetCapabilities"
        ldslog.debug(uri)
        return uri
    
    
    def sourceURI(self,layername):
        '''Basic Endpoint constructor'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        cql = self._buildCQLStr()
        typ = "&typeName="+layername
        fmt = "&outputFormat="+self.fmt
        uri = self.url+self.key+"/wfs?service="+self.svc+"&version="+self.ver+"&request=GetFeature"+typ+fmt+cql
        ldslog.debug(uri)
        return uri

        
    def sourceURI_incrd(self,layername,fromdate,todate):
        '''Endpoint constructor fetching specific layer with incremental date fields'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str            
        cql = self._buildCQLStr()
        vep = LDSUtilities.splitLayerName(layername)+"-changeset"
        typ = "&typeName="+layername+"-changeset"
        inc = "&viewparams=from:"+fromdate+";to:"+todate
        fmt = "&outputFormat="+self.fmt
        uri = self.url+self.key+vep+"/wfs?service="+self.svc+"&version="+self.ver+"&request=GetFeature"+typ+inc+fmt+cql
        ldslog.debug(uri)
        return uri
    
    
    
    def _buildCQLStr(self):
        '''Fetches filter set by utils precedence with some basic checking'''
        cqlfilter = self.getFilter()
        if cqlfilter is not None:
            cql = LDSUtilities.checkCQL(cqlfilter)
        else:
            cql=""
        return cql
    
    @staticmethod
    def fetchLayerNames(url):
        '''Non-Driver method for fetching LDS layer ID's using standard URL open library.
        TODO. Investigate implementing this using the WFS driver and the relative expense for each'''
        res = []
        mm = re.compile('<Name>('+LDSUtilities.LDS_TN_PREFIX+'\d+)<\/Name>')
        lds = urlopen(url)
        for line in lds:    
            res += re.findall(mm,line)
        lds.close()
        return res
    
    @staticmethod
    def readDocument(url):
        '''Non-Driver method for fetching LDS DS as a document'''
        ldslog.debug("LDs URL "+url)
        lds = urlopen(url)
        data = lds.read()
        lds.close()
        return data
        
        

       

#    def read(self,dsn):
#        '''Simple Read method for LDS data store'''
#        ldslog.info("LDS read "+dsn)
#        self.ds = self.driver.Open(dsn)
        
    #write uses WFS write exception message
        

        
        