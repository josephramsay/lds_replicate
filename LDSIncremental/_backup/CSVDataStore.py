#'''
#v.0.0.1
#
#LDSIncremental -  LDS Incremental Utilities
#
#Copyright 2011 Crown copyright (c)
#Land Information New Zealand and the New Zealand Government.
#All rights reserved
#
#This program is released under the terms of the new BSD license. See the 
#LICENSE file for more information.
#
#Created on 23/07/2012
#
#@author: jramsay
#'''
#
#from DataStore import DataStore
#from ReadConfig import Reader
#
#class CSVDataStore(DataStore):
#    '''
#    Simple CSV DataStore
#    '''
#
#    def __init__(self,conn_str=None):
#        '''
#        cons init driver
#        '''
#        super(CSVDataStore,self).__init__(conn_str)
#        
#        self.getDriver("CSV")
#        
#        rc = Reader(None)
#        (self.prefix,self.path) = rc.readFileConfig()
#
#        
#    def sourceURI(self,layer):
#        return self._commonURI(layer)
#    
#    def destinationURI(self,layer):
#        return self._commonURI(layer)
#        
#    def _commonURI(self,filename):
#        '''refers to common connection instance for example in a DB where it doesn't matter whether your reading or writing'''
#        '''since a shapefile is multipart it wants a directory'''
#        if hasattr(self,'conn_str') and self.conn_str is not None:
#            return self.conn_str
#        return self.path+filename+".csv"
#   
#
#    def read(self,dsn):
#        raise NotImplementedError("No Geometry available in CSV Data Source")
#        
#    def write(self,src,dsn):
#        print "CSV write... incremental column changes not implemented/needed"
#        self.ds = self.driver.CopyDataSource(src.ds,dsn)
#        
#        #self.ds = self.driver.CreateDataSource(dsn)
#        #self.replicateLayers(src.ds)
#        
#        #self.ds.SyncToDisk()
#        #self.ds.Destroy()
#    
#
#        
#        