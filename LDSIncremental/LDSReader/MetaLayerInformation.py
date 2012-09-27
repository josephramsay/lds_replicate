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

from ReadConfig import Reader


class MetaLayerReader(object):
    '''
    Simple CSV DataStore intended for meta storage; schemas, layer-mod-dates etc
    For now simply wraps calls to ReadConfig with a predefined filename and some preprocessing
    '''

    def __init__(self,props_file=None):
        '''this is not a ogr function so we dont need a driver and therefore wont call super'''
        #some layer config properties may not be needed and wont be read eg WFS so None arg wont set layerconfig
        if props_file is not None:
            self.layerconfig = Reader("../"+props_file)
        self.mainconfig = Reader("../ldsincr.conf")
        
   
    def getLayerNames(self):
        return self.layerconfig.getSections()
    
    def readConvertedLayerName(self,layer_id):
        (pkey,name,gcol,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return name
    
    def lookupConvertedLayerName(self,layer_name):
        '''given a converted layer name look up its ID'''
        return self.layerconfig.findLayerIdByName(layer_name)




    def readLastModified(self,layer_id):
        (pkey,name,gcol,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return lmod
        
    def writeLastModified(self,layer_id,lmod):
        print "Writing "+lmod+" for layer="+layer_id+" to config file"
        self.layerconfig.writeLayerSchemaConfig(layer_id, lmod)

        


    def readOptionalColmuns(self,layer_id):
        (pkey,name,gcol,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return disc.strip('[]{}()').split(',') if disc is not None else []
    
    def readPrimaryKey(self,layer_id):
        (pkey,name,gcol,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return pkey
    
    def readCQLFilter(self,layer_id):
        (pkey,name,gcol,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return cql
    
    
    
    def readGeometryColumnName(self,layer_id):
        (pkey,name,gcol,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return gcol
    
    
    def readAllLayerParameters(self,layer_id):
        return self.layerconfig.readLayerSchemaConfig(layer_id)
        
    #unless there is a pk change we wont need to write pk

    def readDSSpecificParameters(self,drv):

        if drv=='PostgreSQL':
            return self.mainconfig.readPostgreSQLConfig()
        elif drv=='MSSQLSpatial':
            return self.mainconfig.readMSSQLConfig()
        elif drv=='FileGDB':
            return self.mainconfig.readFileGDBConfig()
        elif drv=='SQLite':
            return self.mainconfig.readSpatiaLiteConfig()
        elif drv=='WFS':
            return self.mainconfig.readWFSConfig()
        else:
            return None

        
        