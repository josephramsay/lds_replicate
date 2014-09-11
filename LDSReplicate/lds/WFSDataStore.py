'''
v.0.0.9

LDSReplicate -  WFSDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 23/07/2012

@author: jramsay
'''

import os 
import re

from abc import ABCMeta, abstractmethod

from lds.DataStore import DataStore
from lds.LDSUtilities import LDSUtilities, Encrypt

class WFSDataStore(DataStore):
    '''
    WFS DataStore, intended to be overridden by SRC specific implementation
    '''

    __metaclass__ = ABCMeta
    
    DRIVER_NAME = "WFS"
    PROXY_AUTH = ('BASIC','NTLM','DIGEST','ANY')    
    PROXY_TYPE = ('DIRECT','SYSTEM','USER_DEFINED')
    #PROXY_AUTH = ('BASIC','NTLM','GSSNEGOTIATE','ANY')    
    
    #SUPPORTED_OUTPUT_FORMATS = ('GML2','GML3','JSON')
    
    VERSION_COUNT = '1.1.0'
    VERSION_REPLICATE = '1.0.0'
    
    def __init__(self,conn_str=None,user_config=None):
        '''
        Init driver, read config and set up proxy data. 
        Notes on Proxies: 
        DIRECT implies no proxy but urllib2 requests may bypass this and read proxy data from the reg/env
        SYSTEM explicitly reads reg/env proxy settings and uses them in a ProxyHandler
        '''
        
        super(WFSDataStore,self).__init__(conn_str,user_config)
        
        #We set the proxy options here as this is the collection point for all WFS/network requests
        self.PP = LDSUtilities.interceptSystemProxyInfo(self.confwrap.readDSParameters('Proxy'),self.PROXY_TYPE[1])
        #convenience proxy map (for urlopen proxyhandler)
        self.pxy = {'http':'{}:{}'.format(self.PP['HOST'],self.PP['PORT'])}
        
        #(self.url,self.key,self.svc,self.ver,self.fmt,self.cql) = self.params
        
    def getConfigOptions(self):
        '''Pass up getConfigOptions call'''
        
        #system, read from env/reg

        proxyconfigoptions = []
        #(type, host, port, auth, usr, pwd) = self.PP
        
        type2 = LDSUtilities.mightAsWellBeNone(self.PP['TYPE'])
        if type2 == self.PROXY_TYPE[1]:
            if LDSUtilities.mightAsWellBeNone(self.PP['HOST']):
                hp = 'GDAL_HTTP_PROXY='+str(self.PP['HOST'])
                if LDSUtilities.mightAsWellBeNone(self.PP['PORT']):
                    hp += ':'+str(self.PP['PORT'])
                proxyconfigoptions += [hp] 
                #if no h/p no point doing u/p
                proxyconfigoptions += ['GDAL_HTTP_PROXYUSERPWD= : '] 
                
        
        if type2 == self.PROXY_TYPE[2]:
            #user difined, expect all fields filled
            if LDSUtilities.mightAsWellBeNone(self.PP['HOST']):
                hp = 'GDAL_HTTP_PROXY='+str(self.PP['HOST'])
                if LDSUtilities.mightAsWellBeNone(self.PP['PORT']):
                    hp += ':'+str(self.PP['PORT'])
                proxyconfigoptions += [hp] 
            if LDSUtilities.mightAsWellBeNone(self.PP['USR']):
                up = 'GDAL_HTTP_PROXYUSERPWD='+str(self.PP['USR'])
                if LDSUtilities.mightAsWellBeNone(self.PP['PWD']):
                    if self.PP['PWD'].startswith(Encrypt.ENC_PREFIX):
                        up += ":"+str(Encrypt.unSecure(self.PP['PWD']))
                    else:
                        up += ":"+str(self.PP['PWD'])
                proxyconfigoptions += [up]
                #NB do we also need to set GDAL_HTTP_USERPWD?
            if LDSUtilities.mightAsWellBeNone(self.PP['AUTH']):
                proxyconfigoptions += ['GDAL_PROXY_AUTH='+str(self.PP['AUTH'])]
                #NB do we also need to set GDAL_HTTP_AUTH?   
            
        return super(WFSDataStore,self).getConfigOptions()+proxyconfigoptions
    
    def getLayerOptions(self,layer_id):
        '''Pass up getLayerOptions call'''
        return super(WFSDataStore,self).getLayerOptions(layer_id)

        
    def sourceURI(self,layername):
        '''URI method returns source file name'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        #possible defaults?
        fmt = 'GML2'
        key = None
        svc = 'wfs'
        ver = '1.0.0'
        typ = "&typeName="+layername
        fmt = "&outputFormat="+fmt
        return self.uri+key+"/?service="+svc+"&version="+ver+"&request=GetFeature"+typ+fmt
    
#     @abstractmethod
#     def destinationURI(self,layername):
#         '''URI method returns destination file name'''
#         #return NotImplementedError("No destination for WFS")

    def versionCheck(self):
        '''Nothing to check?'''
        #TODO maybe check gdal/wfs/gml etc
        return super(WFSDataStore,self).versionCheck()
    
    
    def testURL(self,url):
        '''Connect to a URL using the configured proxy (using urlopen method)'''
        return LDSUtilities.readDocument(url, self.pxy)
        
        
        
        