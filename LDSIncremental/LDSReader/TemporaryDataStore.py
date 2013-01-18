'''
v.0.0.1

LDSIncremental -  MemoryDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 9/08/2012

@author: jramsay
'''
import gdal
import logging

from DataStore import DataStore

ldslog = logging.getLogger('LDS')

    

class TemporaryDataStore(DataStore):
    
    TEMP_LAYER = 'temp'

    def __init__(self):
        super(TemporaryDataStore,self).__init__()


    @staticmethod
    def getInstance(self,driver):
        if driver=='Memory':
            return MemoryDataStore()
        elif driver == 'GeoJSON':
            return GeoJSONDataStore()
        elif driver == 'GMT':
            return GMTDataStore()
        else:
            ldslog('no interim ds selected')
            raise
            
        
    def initDS(self,layer=None):
        return super(TemporaryDataStore,self).initDS(layer)

    def sourceURI(self,layer):
        '''URI method returns source DB instance'''
        return ''
    
    def destinationURI(self,layer):
        '''URI method returns destination DB instance'''
        return ''


    def getOptions(self,layer_id):
        '''Memory DS options not supported'''
        return []
    
    def buildIndex(self,ref_index,ref_pkey,ref_gcol,dst_layer_name):
        '''Do you really need an index? I don't think so'''
        return
    
    def deleteFileDS(self):
        '''Many file based temporary datasources are not editable and have to be deleted before being accessed'''
        pass
    
    
class MemoryDataStore(TemporaryDataStore):
    '''
    Memory DataStore
    '''
    
    DRIVER_NAME = "Memory"
    
    def __init__(self):
        '''
        Memory DataStore constructor
        '''
        
        super(MemoryDataStore,self).__init__()
        
    def initDS(self,layer=None):
        '''null string required for Memory dsn'''
        return super(MemoryDataStore,self).initDS('')

    
class GeoJSONDataStore(TemporaryDataStore):
    '''GeoJSON interim data store for a memory constrained system. NB can't delete columns!'''
    
    DRIVER_NAME = "GeoJSON"

    def __init__(self):
        '''
        GJ DataStore constructor
        '''
        
        super(GeoJSONDataStore,self).__init__()

        #(self.host,self.port,self.dbname,self.schema,self.usr,self.pwd, self.overwrite,self.config,self.srs,self.cql) = self.params
    def initDS(self,layer=None):
        if layer == None:
            dsn = self.destinationURI(self.TEMP_LAYER)
        else:
            dsn = self.destinationURI(layer)
        return super(GeoJSONDataStore,self).initDS(dsn)
        
    def getOverwrite(self):
        return 'NO'
    
    def destinationURI(self,layer):
        '''URI method returns destination DB instance'''
        return str(layer)+'.'+self.DRIVER_NAME.lower()    
    

class GMTDataStore(TemporaryDataStore):
    '''GMT interim data store for a memory constrained system. NB can't delete columns!''' 
    
    DRIVER_NAME = "GMT"
    
    def __init__(self):
        '''
        GMT DataStore constructor
        '''
        
        super(GMTDataStore,self).__init__()

        #(self.host,self.port,self.dbname,self.schema,self.usr,self.pwd, self.overwrite,self.config,self.srs,self.cql) = self.params
        
    def initDS(self,layer=None):
        if layer == None:
            dsn = self.destinationURI(self.TEMP_LAYER)
        else:
            dsn = self.destinationURI(layer)
        return super(GMTDataStore,self).initDS(dsn)
        
    def getOverwrite(self):
        return 'NO'
    
    def destinationURI(self,layer):
        '''URI method returns destination DB instance'''
        return str(layer)+'.'+self.DRIVER_NAME.lower()    



    
    
    