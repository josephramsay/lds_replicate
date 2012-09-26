'''
Created on 9/08/2012

@author: jramsay
'''
import os

from ESRIDataStore import ESRIDataStore
from ReadConfig import Reader
from MetaLayerInformation import MetaLayerReader

#from osr import SpatialReference 

class FileGDBDataStore(ESRIDataStore):
    '''
    FileGDB DataStore
    '''

    def __init__(self,conn_str=None):
        '''
        cons init driver
        '''
        super(FileGDBDataStore,self).__init__(conn_str)
        
        self.DRIVER_NAME = "FileGDB"
        
        self.getDriver(self.DRIVER_NAME)
        
        self.mlr = MetaLayerReader("filegdb.layer.properties")
        
        self.path = self.mlr.readDSSpecificParameters(self.DRIVER_NAME)
        
        self.suffix = '.gdb'

        
    def sourceURI(self,layer):
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        return self._commonURI(layer)
        
    def _commonURI(self,layer):
        '''FileGDB organises tables as individual .gdb directories'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        return os.path.join(self.path,layer+self.suffix)
        

#    def read(self,dsn):
#        self.ds = self.driver.Open(dsn)
#    
#    def write(self,src,dsn):
#        #naive implementation? change SR per layer in place
#        self.convertDatasourceESRI(src)
#        super.write(src,dsn)
#        #self.ds = self.driver.CopyDataSource(src.ds, dsn)
        
        
    def getOptions(self,layer_id):
        '''add PG options for SCHEMA and GEO_NAME'''
        local_opts = []
        gname = self.mlr.readGeometryColumnName(layer_id)
        
        if gname is not None:
            local_opts += ['GEOMETRY_NAME='+gname]
        
        return super(FileGDBDataStore,self).getOptions(layer_id) + local_opts
        