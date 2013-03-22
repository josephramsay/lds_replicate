'''
v.0.0.1

LDSReplicate -  LDSDataStore

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
import gdal

import logging

from WFSDataStore import WFSDataStore
from urllib2 import urlopen
from LDSUtilities import LDSUtilities
from DataStore import MalformedConnectionString

ldslog = logging.getLogger('LDS')

class LDSDataStore(WFSDataStore):
    '''
    LDS DataStore provides standard options and URI methods along with convenience methods for common functions/documents expressed as 
    URI builders. For incremental specifically the change-column is defined here
    '''
    
    '''Default GDAL page size'''
    LDS_PAGE_SIZE = 10000
    SUPPORTED_OUTPUT_FORMATS = ('GML2','GML3','JSON')

    def __init__(self,conn_str=None,user_config=None):
        '''
        LDS init/constructor subclassing WFSDataStore
        '''
        #super WFS sets WFS driver and gets WFS config params
        #supersuper DataStore sets def flags (eg INCR)
        self.pkey = None
        self.psize = None
        self.pstart = None
        
        super(LDSDataStore,self).__init__(conn_str,user_config)
        
        self.CHANGE_COL = "__change__"

        
        (self.url,self.key,self.svc,self.ver,self.fmt,self.cql) = self.params
        if self.conn_str:
            self.key = self.extractAPIKey(self.conn_str,False)
        
        #we're not going to try and overwrite LDS    
        self.clearOverwrite()
            


    def getConfigOptions(self):
        '''Adds GDAL options at driver initialisation, pagination_allowed and page_size'''
        #CPL_CURL_VERBOSE for those ogrerror/generalerror
        #OGR_WFS_PAGING_ALLOWED, OGR_WFS_PAGE_SIZE, OGR_WFS_BASE_START_INDEX
        local_opts = ['OGR_WFS_PAGING_ALLOWED=ON','OGR_WFS_PAGE_SIZE='+str(self.getPartitionSize() if self.getPartitionSize() is not None else LDSDataStore.LDS_PAGE_SIZE)]
        local_opts += ['OGR_WFS_USE_STREAMING=NO']
        return super(LDSDataStore,self).getConfigOptions() + local_opts    
    
    def getLayerOptions(self,layer_id):
        '''Adds GDAL options at driver initialisation, pagination_allowed and page_size'''
        local_opts = []
        return super(LDSDataStore,self).getLayerOptions(layer_id) + local_opts
    
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
        ###this was a bug, trying to build a GC url from the user conn str
        ###if hasattr(self,'conn_str') and self.conn_str is not None:
        ###    return self.conn_str
        #uri = self.url+self.key+"/wfs?service=WFS"+"&version="+self.ver+"&request=GetCapabilities"
        #keyword specifier different between 1.0.0 (<ows:Keywords><ows:Keyword>) and 1.1.0 (<Keywords>) We enforce 1.1.0 to return per keyword version and more accurately parse layer groups
        '''validate the key by checking that the key can be extracted from the conn_str'''
        if not self.validateAPIKey(self.key):
            self.key = self.extractAPIKey(self.conn_str,True)
        #capabilities doc is fetched using urlopen, not wfs, so escaping isnt needed
        #uri = LDSUtilities.xmlEscape(self.url+self.key+"/wfs?service=WFS&version=1.1.0&request=GetCapabilities")
        uri = self.url+self.key+"/wfs?service=WFS&version=1.1.0&request=GetCapabilities"
        ldslog.debug(uri)
        return uri
    
    
    def validateAPIKey(self,kstr):
        '''Make sure the provided key conforms to the required format'''
        srch = re.search('[a-f0-9]{32}',kstr,flags=re.IGNORECASE)
        if srch is None:
            raise MalformedConnectionString('Cannot parse API key')
        return True
        
    def extractAPIKey(self,cs,raiseerr=False):
        '''if the user has supplied a connection string then they dont need to specify an API key in their config file, therefore we must extract it from the cs'''
        srch = re.search('/([a-f0-9]{32})/(v/x|wfs\?)',cs,flags=re.IGNORECASE)
        if srch is None and raiseerr:
            raise MalformedConnectionString('Cannot parse API key')
        return srch.group(1) if srch is not None else None
        
        
    def validateConnStr(self,cs):
        '''WFS basic checks. 1 url format,2 api key,3 ask for wfs'''
        if not re.search('^http://',cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('\'http\' declaration required in LDS request')
        if not re.search('wfs\.data\.linz\.govt\.nz',cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('Require \'wfs.data.linz.govt.nz\' in LDS address string')
        if not re.search('/[a-f0-9]{32}/(v/x|wfs\?)',cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('Require API key (32char hex) in LDS address string')
        if not re.search('wfs\?',cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('Need to specify \'wfs?\' service in LDS request')
        #look for conflicts
        ulayer = LDSUtilities.getLayerNameFromURL(cs)

        return cs,ulayer
        
        
    def sourceURI(self,layername):
        '''Basic Endpoint constructor'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            valid,urilayer = self.validateConnStr(self.conn_str)
            if layername is not None and urilayer!=layername:
                raise MalformedConnectionString('Layer specifications in URI differs from selected layer (-l); '+str(layername)+'!='+str(urilayer))
            return valid

        cql = self._buildCQLStr()
        #pql = self._buildPageStr()     
            
        typ = "&typeName="+layername
        #if omitted the outputformat parameter is null and default used, GML2
        fmt = "&outputFormat="+self.fmt if (self.fmt in self.SUPPORTED_OUTPUT_FORMATS) else ''
        uri = self.url+self.key+"/wfs?service="+self.svc+"&version="+self.ver+"&request=GetFeature"+typ+fmt+cql
        ldslog.debug(uri)
        return uri

        
    def sourceURI_incrd(self,layername,fromdate,todate):
        '''Endpoint constructor fetching specific layers with incremental date fields'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            valid,urilayer = self.validateConnStr(self.conn_str)
            #I don't know why you would attempt to specify dates in the CL and in the URL as well but we might as well attempt to catch diffs
            if layername is not None and urilayer!=layername:
                raise MalformedConnectionString('Layer specifications in URI differs from selected layer (-l); '+str(layername)+'!='+str(urilayer))
            if (fromdate is not None and re.search('from:'+fromdate[:10],valid) is None) or (todate is not None and re.search('to:'+todate[:10],valid) is None):
                raise MalformedConnectionString("Date specifications in URI don't match those referred to with -t|-f "+str(todate)+'/'+str(fromdate)+" not in "+valid)
            return valid

        cql = self._buildCQLStr()
        #pql = self._buildPageStr()     
        
        vep = LDSUtilities.splitLayerName(layername)+"-changeset"
        typ = "&typeName="+layername+"-changeset"
        inc = "&viewparams=from:"+fromdate+";to:"+todate
        #if omitted the outputformat parameter is null and default used, GML2
        fmt = "&outputFormat="+self.fmt if (self.fmt in self.SUPPORTED_OUTPUT_FORMATS) else ''
        uri = self.url+self.key+vep+"/wfs?service="+self.svc+"&version="+self.ver+"&request=GetFeature"+typ+inc+fmt+cql
        ldslog.debug(uri)
        return uri
    
    def sourceURI_feats(self,layername):
        '''Endpoint constructor to fetch number of features for a specific layer. for: Trigger manual paging for broken JSON'''
        #version must be 1.1.0 or > for this to work. NB outputFormat doesn't seem to have any effect here either so its omitted
        ver="1.1.0"
        typ = "&typeName="+layername
        uri = self.url+self.key+"/wfs?service="+self.svc+"&version="+ver+"&request=GetFeature&resultType=hits"+typ
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
        if self.pstart is not None and self.psize is not None:
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
        tt = re.compile('<Title>(.+)<\/Title>')
        lds = urlopen(url)
        for line in lds:    
            res1 = re.findall(mm,line)
            res2 = re.findall(tt,line)
            if len(res1)>0:
                res += ((res1[0],res2[0]),)
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
        

        
        