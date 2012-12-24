'''
v.0.0.1

LDSIncremental -  WFSDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 23/07/2012

@author: jramsay
'''

from DataStore import DataStore

class WFSDataStore(DataStore):
    '''
    PostgreSQL DataStore
    '''

    DRIVER_NAME = "WFS"
    
    def __init__(self,conn_str=None,user_config=None):
        '''
        Init driver and read config
        '''
        
        super(WFSDataStore,self).__init__(conn_str,user_config)
        
        #(self.url,self.key,self.svc,self.ver,self.fmt,self.cql) = self.params
        
    def getOptions(self,layer_id):
        '''Pass up getOptions call'''
        return super(WFSDataStore,self).getOptions(layer_id)
        
    def sourceURI(self,layername):
        '''URI method returns source file name'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        typ = "&typeName="+layername
        fmt = "&outputFormat="+self.fmt
        return self.url+self.key+"/?service="+self.svc+"&version="+self.ver+"&request=GetFeature"+typ+fmt
    
    def destinationURI(self,layername):
        '''URI method returns destination file name'''
        return NotImplementedError("No destination for WFS")

    
    def write(self,src_ds,dsn):
        '''Write method deliberately raises exception discouraging writing to a WFS source'''
        return NotImplementedError("No destination for WFS")
        
        