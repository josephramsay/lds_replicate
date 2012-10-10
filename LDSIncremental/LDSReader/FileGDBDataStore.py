'''
FileGDB wrapper class.

Created on 9/08/2012

@author: jramsay
'''
import os

from ESRIDataStore import ESRIDataStore
from MetaLayerInformation import MetaLayerReader

#from osr import SpatialReference 

class FileGDBDataStore(ESRIDataStore):
    '''
    FileGDB DataStore wrapper for file location and options 
    '''

    def __init__(self,conn_str=None,user_config=None):
        
        self.DRIVER_NAME = "FileGDB"
        self.CONFIG_XSL = "getcapabilities_initdb.xsl"
        
        super(FileGDBDataStore,self).__init__(conn_str,user_config)
        
        (self.path) = self.params
        
        self.suffix = '.gdb'

        
    def sourceURI(self,layer):
        '''URI method returns source file name'''
        return self._commonURI(layer)
    
    
    def destinationURI(self,layer):
        '''URI method returns destination file name'''
        return self._commonURI(layer)
        
        
    def _commonURI(self,layer):
        '''FileGDB organises tables as individual .gdb file/directories into which contents are written. The layer is configured as if it were a file'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        return os.path.join(self.path,layer+self.suffix)
        
        
    def getOptions(self,layer_id):
        '''Adds FileGDB options for GEOMETRY_NAME'''
        local_opts = []
        gname = self.mlr.readGeometryColumnName(layer_id)
        
        if gname is not None:
            local_opts += ['GEOMETRY_NAME='+gname]
        
        return super(FileGDBDataStore,self).getOptions(layer_id) + local_opts
        