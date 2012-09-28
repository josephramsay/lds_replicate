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

from DataStore import DataStore
from ReadConfig import Reader

class MapinfoDataStore(DataStore):
    '''
    Mapinfo DataStore
    '''

    def __init__(self):
        '''
        cons init driver
        '''
        super(MapinfoDataStore,self).__init__()
        
        self.getDriver("MapInfo File")
        
        rc = Reader(None)
        (self.prefix,self.path) = rc.readFileConfig()

        
    def sourceURI(self,layer):
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        return self._commonURI(layer)
        
    def _commonURI(self,filename):
        '''refers to common connection instance for example in a DB where it doesn't matter whether your reading or writing'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        return self.path+filename
   

#    def read(self,dsn):
#        print "MIF read"
#        self.ds = self.driver.Open(dsn)
#        
#    def write(self,src,dsn):
#        print "MIF write"
#        self.ds = self.driver.CopyDataSource(src.ds,dsn)
    

        
        