'''
Created on 9/08/2012

@author: jramsay
'''
import logging

from DataStore import DataStore
from MetaLayerInformation import MetaLayerReader

ldslog = logging.getLogger('LDS')

class MSSQLSpatialDataStore(DataStore):
    '''
    MS SQL DataStore
    MSSQL:server=.\MSSQLSERVER2008;database=dbname;trusted_connection=yes
    '''

    def __init__(self,conn_str=None):
        '''
        cons init driver
        '''
            
        super(MSSQLSpatialDataStore,self).__init__(conn_str)

        self.DRIVER_NAME = "MSSQLSpatial"
        #MS driver doesnt have any documented config options
  
        self.getDriver(self.DRIVER_NAME)
        
        self.mlr = MetaLayerReader("mssql.layer.properties")
        
        (self.odbc,self.server,self.dsn,self.trust,self.dbname,self.schema,self.usr,self.pwd) = self.mlr.readDSSpecificParameters(self.DRIVER_NAME)

        
    def sourceURI(self,layer):
        '''URI method returns source file name'''
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        '''URI method returns destination file name'''
        return self._commonURI(layer)
        
    def _commonURI(self,layer):
        '''Refers to common connection instance for example in a DB where it doesn't matter whether your reading or writing'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        #return "MSSQL:server={};database={};trusted_connection={};".format(self.server, self.dbname, self.trust)
        sstr = ";Schema={}".format(self.schema) if self.schema is not None and self.schema !='' else ""
        uri = "MSSQL:server={};database={};UID={};PWD={};Driver={}".format(self.server, self.dbname, self.usr, self.pwd,self.odbc)+sstr
        ldslog.debug(uri)
        return uri
        


    def getOptions(self,layer_id):
        '''Get MS options for GEO_NAME'''
        local_opts = []
        gname = self.mlr.readGeometryColumnName(layer_id)
        
        if gname is not None:
            local_opts += ['GEOMETRY_NAME='+gname]
        
        return super(MSSQLSpatialDataStore,self).getOptions() + local_opts
    