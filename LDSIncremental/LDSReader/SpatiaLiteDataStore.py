'''
Created on 9/08/2012

@author: jramsay
'''

import logging
import os

from DataStore import DataStore

ldslog = logging.getLogger('LDS')

class SpatiaLiteDataStore(DataStore):
    '''
    PostgreSQL DataStore
    '''

    def __init__(self,conn_str=None,user_config=None):
        '''
        cons init driver
        '''
        self.DRIVER_NAME = "SQLite"
        self.CONFIG_XSL = "getcapabilities.sqlite.xsl"  
        
        super(SpatiaLiteDataStore,self).__init__(conn_str,user_config)

        (self.path,self.name,self.config,self.srs,self.cql) = self.params 
        #because sometimes ~ isnt translated to home
        self.path = os.path.expanduser(self.path)
        self.suffix = '.db'

        
    def sourceURI(self,layer):
        '''URI method returns source file name'''
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        '''URI method returns destination file name'''
        return self._commonURI(layer)
        
    def _commonURI(self,layer):
        '''Since SpatiaLite databases are self contained files this only needs to return a file path'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        #return self.file #+"SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE"
        return os.path.join(self.path,self.name+self.suffix)

#    def read(self,dsn):
#        print "PG read"
#        self.ds = self.driver.Open(dsn)
#    
#    def write(self,src,dsn):
#        print "PG write",dsn
#        super.write(self,src,dsn)

    def getOptions(self,layer_id):
        '''add PG options for SCHEMA and GEO_NAME'''
        local_opts = []
        gname = self.layerconf.readLayerProperty(layer_id,'geocolumn')
        
        #TODO Figure out how to set geom_name for SL... this wont do it
        if gname is not None:
            local_opts += ['GEOMETRY_NAME='+gname]
            
        #Not really needed since default is YES
        #if index == 'spatial or index == 's' or re.match(index,gname.lower()):
        #    local_opts += ['SPATIAL_INDEX=YES']
        
        return super(SpatiaLiteDataStore,self).getOptions() + local_opts
        

    '''
    returned by ogr.GetFieldTypeName(i)
    0 Integer
    1 IntegerList
    2 Real
    3 RealList
    4 String
    5 StringList
    6 (unknown)
    7 (unknown)
    8 Binary
    9 Date
    10 Time
    11 DateTime
    12 (unknown) ...
    '''
    def convertToDestinationType(self,key):
        return {0: 'integer', 1: 'integer',
                2: 'double precision', 3: 'double precision',
                4: 'character varying', 5: 'character varying',
                8: 'byte',
                9: 'date', 10: 'time', 11: 'timestamp'
         }.get(key,'character varying')  
         


        