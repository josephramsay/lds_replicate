'''
v.0.0.9

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

import logging

from contextlib import closing

from lxml import etree
from lxml.etree import XMLSyntaxError

from urllib2 import urlopen, build_opener, install_opener, ProxyHandler

from lds.WFSDataStore import WFSDataStore
from lds.RequestBuilder import RequestBuilder
from lds.LDSUtilities import LDSUtilities
from lds.DataStore import MalformedConnectionString
from lds.VersionUtilities import AppVersion

ldslog = LDSUtilities.setupLogging()

class LDSDataStore(WFSDataStore):
    '''
    LDS DataStore provides standard options and URI methods along with convenience methods for common functions/documents expressed as 
    URI builders. For incremental specifically the change-column is defined here
    '''
    
    OGR_WFS_USE_STREAMING = 'NO'
    OGR_WFS_PAGE_SIZE = 10000
    OGR_WFS_PAGING_ALLOWED = 'OFF'
    
    PAGE_REDUCTION_STEP = 0.5
    
    OGR_WFS_LOAD_MULTIPLE_LAYER_DEFN = 'OFF'
    OGR_WFS_BASE_START_INDEX = 0
    
    GDAL_HTTP_USERAGENT = 'LDSReplicate/'+str(AppVersion.getVersion())    
    GDAL_HTTP_TIMEOUT = 600 #10min. has no affect on 504
    
    #Namespace declarations
    NS = {'g'       : '{http://data.linz.govt.nz/ns/g}', 
          'gml'     : '{http://www.opengis.net/gml}', 
          'xlink'   : '{http://www.w3.org/1999/xlink}', 
          'r'       : '{http://data.linz.govt.nz/ns/r}', 
          'ows'     : '{http://www.opengis.net/ows/1.1}', 
          'v'       : '{http://data.linz.govt.nz/ns/v}', 
          'wfs'     : '{http://www.opengis.net/wfs/2.0}', 
          'xsi'     : '{http://www.w3.org/2001/XMLSchema-instance}', 
          'ogc'     : '{http://www.opengis.net/ogc}',
          'gco'     :'{http://www.isotc211.org/2005/gco}',
          'gmd'     :'{http://www.isotc211.org/2005/gmd}', 
          'gmx'     :'{http://www.isotc211.org/2005/gmx}',
          'gsr'     :'{http://www.isotc211.org/2005/gsr}',
          'gss'     :'{http://www.isotc211.org/2005/gss}',
          'gts'     :'{http://www.isotc211.org/2005/gts}'}

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
        self.idp = LDSUtilities.getLDSIDPrefix(self.ver,self.svc)
#         if self.conn_str:
#             self.key = self.extractAPIKey(self.conn_str,False)
        
        self.requestbuilder = RequestBuilder.getInstance(self.params,conn_str)
        
        #we're not going to try and overwrite LDS    
        self.clearOverwrite()

    def getConfigOptions(self):
        '''Adds GDAL options at driver initialisation, pagination_allowed and page_size'''
        #CPL_CURL_VERBOSE for those ogrerror/generalerror
        #OGR_WFS_PAGING_ALLOWED, OGR_WFS_PAGE_SIZE, OGR_WFS_BASE_START_INDEX
        local_opts  = ['GDAL_HTTP_USERAGENT='+str(self.GDAL_HTTP_USERAGENT)]
        local_opts  = ['GDAL_HTTP_TIMEOUT='+str(self.GDAL_HTTP_TIMEOUT)]
        local_opts += ['OGR_WFS_PAGING_ALLOWED='+str(self.OGR_WFS_PAGING_ALLOWED)]
        local_opts += ['OGR_WFS_PAGE_SIZE='+str(self.getPartitionSize() if self.getPartitionSize() else self.OGR_WFS_PAGE_SIZE)]
        local_opts += ['OGR_WFS_USE_STREAMING='+str(self.OGR_WFS_USE_STREAMING)]
        local_opts += ['OGR_WFS_LOAD_MULTIPLE_LAYER_DEFN='+str(self.OGR_WFS_LOAD_MULTIPLE_LAYER_DEFN)]
        local_opts += ['OGR_WFS_BASE_START_INDEX='+str(self.OGR_WFS_BASE_START_INDEX)]
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
        return self.requestbuilder.getCapabilities()    
        
    def validateConnStr(self,cs):
        return self.requestbuilder.validateConnStr(cs)
        
    def buildIndex(self,lce,dst_layer_name):
        pass
        
    def destinationURI(self,layername):
        pass
    
    def sourceURI(self,layername):
        '''Basic Endpoint constructor'''
        return self.requestbuilder.sourceURI(layername)
        
    def sourceURIIncremental(self,layername,fromdate,todate):
        '''Endpoint constructor fetching specific layers with incremental date fields'''
        return self.requestbuilder.sourceURIIncremental(layername, fromdate, todate)
    
    def sourceURIFeatureCount(self,layername):
        '''Endpoint constructor to fetch number of features for a specific layer. for: Trigger manual paging for broken JSON'''
        return self.requestbuilder.sourceURIFeatureCount(layername)   
                    
    def rebuildDS(self):
        '''Resets the DS. Needed if the URI is edited'''
        self.setURI(LDSUtilities.reVersionURL(self.getURI(),LDSDataStore.VERSION_COUNT))
        self.read(self.getURI(),False)  
    
    def closeDS(self):
        '''Close a DS with sync and destroy'''
        #probably not needed. prefer use superclass close()
        ldslog.info("WFS DS close")
        if self.ds: 
            self.ds.Release()
            self.ds = None

    
    
    @classmethod
    def fetchLayerInfo(cls,url,proxy=None):
        '''Non-GDAL static method for fetching LDS layer ID's using etree parser.'''
        res = []
        content = None
        ftxp = "//{0}FeatureType".format(cls.NS['wfs'])
        nmxp = "./{0}Name".format(cls.NS['wfs'])
        ttxp = "./{0}Title".format(cls.NS['wfs'])
        kyxp = "./{0}Keywords/{0}Keyword".format(cls.NS['ows'])
        
        try:            
            if not LDSUtilities.mightAsWellBeNone(proxy): install_opener(build_opener(ProxyHandler(proxy)))
            #content = urlopen(url)#bug in lxml doesnt close url/files using parse method
            with closing(urlopen(url)) as content:
                tree = etree.parse(content)
                for ft in tree.findall(ftxp):
                    name = ft.find(nmxp).text.encode('utf8')
                    title = ft.find(ttxp).text.encode('utf8')
                    keys = [x.text.encode('utf8') for x in ft.findall(kyxp)]
                    
                    res += ((name,title,keys),)
                
        except XMLSyntaxError as xe:
            ldslog.error('Error parsing URL;'+str(url)+' ERR;'+str(xe))
            
        return res
  
    def versionCheck(self):
        '''Nothing to check?'''
        #TODO maybe check gdal/wfs/gml etc
        return super(LDSDataStore,self).versionCheck()
    


        
        