'''
v.0.0.1

LDSReplicate -  WFSDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 23/07/2012

@author: jramsay
'''

from DataStore import DataStore
from LDSUtilities import LDSUtilities, Encrypt

class WFSDataStore(DataStore):
    '''
    WFS DataStore, intended to be overridden by SRC specific implementation
    '''

    DRIVER_NAME = "WFS"
    PROXY_AUTH = ('BASIC','NTLM','DIGEST','ANY')    
    #PROXY_AUTH = ('BASIC','NTLM','GSSNEGOTIATE','ANY')    
    def __init__(self,conn_str=None,user_config=None):
        '''
        Init driver and read config
        '''
        
        super(WFSDataStore,self).__init__(conn_str,user_config)
        
        #We set the proxy options here as this is the collection point for all WFS/network requests
        self.proxyparams = self.mainconf.readDSParameters('Proxy')
        
        #(self.url,self.key,self.svc,self.ver,self.fmt,self.cql) = self.params
        
    def getConfigOptions(self):
        '''Pass up getConfigOptions call'''
        proxyconfigoptions = []
        (host, port, auth, usr, pwd) = self.proxyparams
        if LDSUtilities.mightAsWellBeNone(host) is not None:
            hp = 'GDAL_HTTP_PROXY='+str(host)
            if LDSUtilities.mightAsWellBeNone(port) is not None:
                hp += ':'+str(port)
            proxyconfigoptions += hp 
        if LDSUtilities.mightAsWellBeNone(usr) is not None:
            up = 'GDAL_HTTP_PROXYUSERPWD='+str(usr)
            if LDSUtilities.mightAsWellBeNone(pwd) is not None:
                if pwd.startswith(Encrypt.ENC_PREFIX):
                    up += ":"+str(Encrypt.unSecure(pwd))
                else:
                    up += ":"+str(pwd)
            proxyconfigoptions += up
            #NB do we also need to set GDAL_HTTP_USERPWD?
        if LDSUtilities.mightAsWellBeNone(auth) is not None:
            proxyconfigoptions += ['GDAL_PROXY_AUTH='+str(usr)]
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
    
    def destinationURI(self,layername):
        '''URI method returns destination file name'''
        return NotImplementedError("No destination for WFS")
        
        
    def write(self,src_ds,dsn):
        '''Write method deliberately raises exception discouraging writing to a WFS source'''
        return NotImplementedError("No destination for WFS")
        
        