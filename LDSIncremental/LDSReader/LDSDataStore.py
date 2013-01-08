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
    
    '''Default GDAL page size'''
    LDS_PAGE_SIZE = 10000

    def __init__(self,conn_str=None,user_config=None):
        '''
        LDS init/constructor subclassing WFSDataStore
        '''
        #super WFS sets WFS driver and gets WFS config params
        #supersuper DataStore sets def flags (eg INCR)
        super(LDSDataStore,self).__init__(conn_str,user_config)
        
        self.CHANGE_COL = "__change__"
        self.psize = None
        
        (self.url,self.key,self.svc,self.ver,self.fmt,self.cql) = self.params


    def getOptions(self,layer_id):
        '''Adds GDAL options at driver initialisation, pagination_allowed and page_size'''
        local_opts = ['OGR_WFS_PAGING_ALLOWED=ON','OGR_WFS_PAGE_SIZE='+str(self.getPartitionSize() if self.getPartitionSize() is not None else LDSDataStore.LDS_PAGE_SIZE)]
        
        return super(LDSDataStore,self).getOptions(layer_id) + local_opts
    
    def setPrimaryKey(self,pkey):
        '''Sets the name of the primary key column in the datasource object'''
        self.pkey = pkey
        
    def setPartitionSize(self,psize):
        '''Sets the partition size i.e. the number of features to be returned per WFS request'''
        self.psize = psize
        
    def getPartitionSize(self):
        return self.psize
        
    def setPartitionStart(self,pstart):
        '''Sets the starts point for LDS requests using the primary key as the index. Assumes the request will also be sorted by this same key'''
        self.pstart = pstart
        
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
        #pql = self._buildPageStr()     
            
        typ = "&typeName="+layername
        fmt = "&outputFormat="+self.fmt
        uri = self.url+self.key+"/wfs?service="+self.svc+"&version="+self.ver+"&request=GetFeature"+typ+fmt+cql
        ldslog.debug(uri)
        return uri

        
    def sourceURI_incrd(self,layername,fromdate,todate):
        '''Endpoint constructor fetching specific layers with incremental date fields'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str       

        cql = self._buildCQLStr()
        #pql = self._buildPageStr()     
        
        vep = LDSUtilities.splitLayerName(layername)+"-changeset"
        typ = "&typeName="+layername+"-changeset"
        inc = "&viewparams=from:"+fromdate+";to:"+todate
        fmt = "&outputFormat="+self.fmt
        uri = self.url+self.key+vep+"/wfs?service="+self.svc+"&version="+self.ver+"&request=GetFeature"+typ+inc+fmt+cql
        ldslog.debug(uri)
        return uri
    
    
    
    def _buildPageStr(self):
        '''Manual paging using startIndex instead of cql'''
        page = ""
        if self.psize is not None:
            page = "&startIndex="+str(self.pstart)+"&pagingallowed=On&sortBy="+self.pkey+"&maxFeatures="+str(self.psize)
            
        return page
    
    def _buildCQLStr(self):
        '''Builds a cql_filter string as set by the user appending an 'id>...' partitioning string if needed. NB. Manual partitioning is accomplished using the parameters, 'maxFeatures' to set feature quantity, a page-by-page recorded 'id' value and a 'sortBy=id' argument'''
        cql = ()
        maxfeat = ""
        
        #if implementing pagination in cql      
        if self.psize is not None:
            cql += (self.pkey+">"+str(self.pstart),)
            #sortBy used so last feature will have the new maximum key, saves a comparison
            maxfeat = "&sortBy="+self.pkey+"&maxFeatures="+str(self.psize)            

        if self.getFilter() is not None:
            cql += (LDSUtilities.checkCQL(self.getFilter()),)

        return maxfeat+"&cql_filter="+';'.join(cql) if len(cql)>0 else ""
    
    
    @staticmethod
    def fetchLayerNames(url):
        '''Non-GDAL static method for fetching LDS layer ID's using standard URLopen library.
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
        '''Non-GDAL static method for fetching a LDS Datasource as a document'''
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
        

        
        