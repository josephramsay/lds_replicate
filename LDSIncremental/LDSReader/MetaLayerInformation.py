'''
v.0.0.1

LDSIncremental -  (Meta) Layer Reader

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 23/07/2012

@author: jramsay
'''
import logging

from ReadConfig import ReaderFile, ReaderTable

ldslog = logging.getLogger('LDS')

class MetaLayerReader(object):
    '''
    Convenience wrapper class to config-file reader instances.
    '''

    def __init__(self,parent,config_file=None,properties=None):
        '''this is not a ogr function so we dont need a driver and therefore wont call super'''
        #some layer config properties may not be needed and wont be read eg WFS so None arg wont set layerconfig
        
        #reference to calling class
        self.parent = parent
        #name of properties file
        self.properties = properties

        self.setupMainConfig(config_file)
        #dont set up layerconfig by default. Wait till we know whether we want a new build (initconfig) 
        #self.setupLayerConfig()


    def setupMainConfig(self,config_file):
        '''Sets up a reader to the main configuration file or alternatively, a user specified config file.
        Userconfig is not mean't to replace mainconfig, just overwrite the parts the user has decided to customise'''
        self.userconfig = None
        if config_file is not None:
            self.userconfig = ReaderFile("../"+config_file)
        self.mainconfig = ReaderFile("../ldsincr.conf")
        
    def setupLayerConfig(self):
        '''Adds a layerconfig object'''
        #HACK. Assumes last value is the config value. Might be better to search all params for either external or internal
        config = self.readDSSpecificParameters(self.parent.DRIVER_NAME)[-1:][0]
        
        if config.lower() == 'internal':
            self.layerconfig = ReaderTable(self.parent)
        elif self.properties is not None:
            if config is None:
                ldslog.warning('No config type specified')
            self.layerconfig = ReaderFile("../"+self.properties)
        
#    def setLayerConfig(self,layerconfig):
#        '''Setter for Layerconfig, useful for external classes'''
#        self.layerconfig = layerconfig
        
        
    def getLayerNames(self):
        '''Returns configured layers for respective layer properties file'''
        return self.layerconfig.getSections()
    
    def readLayerGroups(self,layer_id):
        '''Reads configured name for a provided layer id'''
        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return group
    
    def readConvertedLayerName(self,layer_id):
        '''Reads configured name for a provided layer id'''
        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return name
    
    def lookupConvertedLayerName(self,layer_name):
        '''Reverse lookup of layer id given a layer name, again using the layer properties file'''
        return self.layerconfig.findLayerIdByName(layer_name)




    def readLastModified(self,layer_id):
        '''Reads last modified date for a provided layer id per destination'''
        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return lmod
        
    def writeLastModified(self,layer_id,lmod):
        '''Writes a new last modified date for a provided layer id per destination'''
        ldslog.info("Writing "+lmod+" for layer="+layer_id+" to config file")
        self.layerconfig.writeLayerSchemaConfig(layer_id, lmod)

        


    def readOptionalColmuns(self,layer_id):
        '''Returns a list of columns being discarded for the named layer (with removal of brackets)'''
        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return disc.strip('[]{}()').split(',') if disc is not None else []
    
    def readPrimaryKey(self,layer_id):
        '''Returns a list of columns being discarded for the named layer'''
        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return pkey
    
    def readIndexRef(self,layer_id):
        '''Returns a list of columns being discarded for the named layer'''
        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return index
    
    def readCQLFilter(self,layer_id):
        '''Reads the CQL filter for the layer if provided'''
        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return cql
    
    
    
    def readGeometryColumnName(self,layer_id):
        '''Returns preferred geometry column name. If not provided uses the existing layer name'''
        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = self.layerconfig.readLayerSchemaConfig(layer_id)
        return gcol
    
    
    def readAllLayerParameters(self,layer_id):
        '''Returns a list of all layer parameters'''
        return self.layerconfig.readLayerSchemaConfig(layer_id)
        
    #unless there is a pk change we wont need to write pk

    def readDSSpecificParameters(self,drv):
        '''Returns the datasource parameters. By request updated to let users override parts of the basic config file'''
        ul = ()

        if drv=='PostgreSQL':
            ml = self.mainconfig.readPostgreSQLConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readPostgreSQLConfig()
        elif drv=='MSSQLSpatial':
            ml = self.mainconfig.readMSSQLConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readMSSQLConfig()
        elif drv=='FileGDB':
            ml = self.mainconfig.readFileGDBConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readFileGDBConfig()
        elif drv=='SQLite':
            ml = self.mainconfig.readSpatiaLiteConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readSpatiaLiteConfig()
        elif drv=='WFS':
            ml = self.mainconfig.readWFSConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readWFSConfig()
        else:
            return None
        
        params = map(lambda x,y: y if x is None else x,ul,ml)
        
        
        return params

        
        