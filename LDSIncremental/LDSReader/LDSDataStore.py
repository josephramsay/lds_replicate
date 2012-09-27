'''
v.0.0.1

LDSIncremental -  LDS Incremental Utilities

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

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
    PostgreSQL DataStore
    '''

    def __init__(self,conn_str=None):
        '''
        cons init driver
        '''
        #super WFS sets WFS driver and gets WFS config params
        #supersuper DataStore sets def flags (eg INCR)
        super(LDSDataStore,self).__init__(conn_str)
        
        self.CHANGE_COL = "__change__"


        
    def getCapabilities(self):
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        uri = self.url+self.key+"/wfs?service=WFS"+"&version="+self.ver+"&request=GetCapabilities"
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
    
    
    def sourceURI_incr(self,layername):
        '''Endpoint constructor without date fields but uses the .../v/xXXX addressing construct. Only really useful for testing the incremental URL format'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        cql = self._buildCQLStr()
        vep = LDSUtilities.splitLayerName(layername)+"-changeset"
        typ = "&typeName="+layername+"-changeset"
        fmt = "&outputFormat="+self.fmt
        uri = self.url+self.key+vep+"/wfs?service="+self.svc+"&version="+self.ver+"&request=GetFeature"+typ+fmt+cql
        ldslog.debug(uri)
        return uri
        
    def sourceURI_incrd(self,layername,fromdate,todate):
        '''Endpoint constructor fetching specific feature with incr date fields'''
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
        cqlfilter = self.getFilter()
        if cqlfilter is not None:
            cql = LDSUtilities.checkCQL(cqlfilter)
        else:
            cql=""
        return cql
    
    @classmethod
    def fetchLayerNames(self,url):
        res = []
        mm = re.compile('<Name>(v:x\d+)<\/Name>')
        lds = urlopen(url)
        for line in lds:    
            res += re.findall(mm,line)
        lds.close()
        return res
    
    
#    def getLayerName(self,url):
#        '''JP code to be added'''
#        import ogr 
#        import gdal 
#
#        wfs_drv = ogr.GetDriverByName('WFS') 
#        
#        gdal.SetConfigOption('OGR_WFS_LOAD_MULTIPLE_LAYER_DEFN', 'NO') 
#        
#        ds = ogr.Open('WFS:http://wfs.data.linz.govt.nz/3b7124f23806431c8371f139ec84c40e/wfs') 
#        
#        for i in range(0, ds.GetLayerCount()): 
#        
#            lyr = ds.GetLayer(i) 
#            print lyr.GetName() 
#        
#        # or more info 
#        
#        layermetadata = ds.GetLayerByName("WFSLayerMetadata") 
#        
#        feat = layermetadata.GetNextFeature() 
#        while feat is not None: 
#            name = feat.GetFieldAsString('layer_name') 
#            title = feat.GetFieldAsString('title') 
#            print( "%s, %s" % (name, title) ) 
#            feat = layermetadata.GetNextFeature() 
#        feat = None 


       

    def read(self,dsn):
        ldslog.info("LDS read "+dsn)
        self.ds = self.driver.Open(dsn)
        
    def write(self,dsn):
        raise NotImplementedError("Unable to write to LDS Data Source")

        
        