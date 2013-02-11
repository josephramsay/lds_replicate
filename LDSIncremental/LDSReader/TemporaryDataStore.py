'''
v.0.0.1

LDSIncremental -  TemporaryDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Data store classes used for temporary copying using driver copy. Used to prevent pollution of final DS with hidden/deleted columns
NB. Requirements; Drivers part of standard OGR pkg, support creation (not RO) and geo referencing


Created on 9/08/2012

@author: jramsay
'''

import logging

from DataStore import DataStore

ldslog = logging.getLogger('LDS')

    
'''All subclasses here are supplied in the standard OGR install. They also support creation (not RO) and geo referencing'''


    
class TemporaryDataStore(DataStore):
    
    
    TEMP_LAYER = 'temp'
    TEMP_MAP = {}
    
    def __init__(self):
        super(TemporaryDataStore,self).__init__()
        

    @classmethod
    def registerSub(cls,subcls):
        cls.TEMP_MAP[subcls.DRIVER_NAME] = subcls
        return cls
        
        
    @staticmethod
    def getInstance(driver):
        
        if TemporaryDataStore.TEMP_MAP.has_key(driver):
            return TemporaryDataStore.TEMP_MAP[driver]
            #return MemoryDataStore()
        else:
            ldslog.error('No valid interim DS selected')
            raise         
        
    def initDS(self,layer=None):
        return super(TemporaryDataStore,self).initDS(layer)

    def sourceURI(self,layer):
        '''URI method returns source DB instance'''
        return ''
    
    def destinationURI(self,layer):
        '''URI method returns destination DB instance'''
        return ''


    def validateConnStr(self,conn_str):
        '''only needed for abstract inst'''
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
    
@TemporaryDataStore.registerSub   
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

@TemporaryDataStore.registerSub    
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
    
@TemporaryDataStore.registerSub   
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

@TemporaryDataStore.registerSub   
class ESRIShapefileDataStore(TemporaryDataStore):
    '''GMT interim data store for a memory constrained system. NB can't delete columns!''' 
    
    DRIVER_NAME = "ESRI Shapefile"
    
    def __init__(self):
        '''
        ESRI Shapefile DataStore constructor
        '''
        
        super(ESRIShapefileDataStore,self).__init__()

        #(self.host,self.port,self.dbname,self.schema,self.usr,self.pwd, self.overwrite,self.config,self.srs,self.cql) = self.params
        
    def initDS(self,layer=None):
        if layer == None:
            dsn = self.destinationURI(self.TEMP_LAYER)
        else:
            dsn = self.destinationURI(layer)
        return super(ESRIShapefileDataStore,self).initDS(dsn)
        
    def getOverwrite(self):
        return 'YES'
    
    def destinationURI(self,layer):
        '''URI method returns destination DB instance'''
        return str(layer)+'.shp'   
    
@TemporaryDataStore.registerSub   
class MapinfoDataStore(TemporaryDataStore):
    '''GMT interim data store for a memory constrained system. NB can't delete columns!''' 
    
    DRIVER_NAME = "Mapinfo File"
    
    def __init__(self):
        '''
        Mapinfo DataStore constructor
        '''
        
        super(MapinfoDataStore,self).__init__()

        #(self.host,self.port,self.dbname,self.schema,self.usr,self.pwd, self.overwrite,self.config,self.srs,self.cql) = self.params
        
    def initDS(self,layer=None):
        if layer == None:
            dsn = self.destinationURI(self.TEMP_LAYER)
        else:
            dsn = self.destinationURI(layer)
        return super(MapinfoDataStore,self).initDS(dsn)
        
    def getOverwrite(self):
        return 'YES'
    
    def destinationURI(self,layer):
        '''URI method returns destination DB instance'''
        return str(layer)+'.tab'   


@TemporaryDataStore.registerSub   
class AutoCADDXFDataStore(TemporaryDataStore):
    '''GMT interim data store for a memory constrained system. NB can't delete columns!''' 
    
    DRIVER_NAME = "DXF"
    
    def __init__(self):
        '''
        AutoCAD DXF DataStore constructor
        '''
        
        super(AutoCADDXFDataStore,self).__init__()

        #(self.host,self.port,self.dbname,self.schema,self.usr,self.pwd, self.overwrite,self.config,self.srs,self.cql) = self.params
        
    def initDS(self,layer=None):
        if layer == None:
            dsn = self.destinationURI(self.TEMP_LAYER)
        else:
            dsn = self.destinationURI(layer)
        return super(AutoCADDXFDataStore,self).initDS(dsn)
        
    def getOverwrite(self):
        return 'YES'
    
    def destinationURI(self,layer):
        '''URI method returns destination DB instance'''
        return str(layer)+'.'+self.DRIVER_NAME.lower()    


#not recommended
#TemporaryDataStore.TEMP_MAP = {
#    'Memory':MemoryDataStore(),
#    'ESRI Shapefile':ESRIShapefileDataStore(),
#    'Mapinfo File':MapinfoDataStore(),
#    'GeoJSON':GeoJSONDataStore(),
#    'GMT':GMTDataStore(),
#    'DXF':AutoCADDXFDataStore()
#    }

    
    
    