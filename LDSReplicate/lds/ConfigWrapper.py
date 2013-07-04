'''
v.0.0.1

LDSReplicate -  ConfigWrapper

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 23/07/2012

@author: jramsay
'''
import logging
import os

from ReadConfig import MainFileReader#, LayerFileReader
from LDSUtilities import LDSUtilities
ldslog = logging.getLogger('LDS')

class ConfigWrapper(object):
    '''
    Convenience wrapper class to main and user config-file reader instances. Main function of this class is to 
    allow user to override selected portions of the main config file.
    '''
    #TODO. since we dont use this as a Layer reader anymore consider removing it completely

    def __init__(self,config_file=None):
        
        #self.layerconfig = None #internal/external; but only external is coded an never used
        self.mainconfig = None  #always a file
        self.userconfig = None  #always a file

        self.setupMainAndUserConfig(config_file)
        #dont set up layerconfig by default. Wait till we know whether we want a new build (initconfig) 
        #self.setupLayerConfig()


    def setupMainAndUserConfig(self,inituserconfig):
        '''Sets up a reader to the main configuration file or alternatively, a user specified config file.
        Userconfig is not mean't to replace mainconfig, just overwrite the parts the user has decided to customise'''
        self.userconfig = None
        if inituserconfig is not None:
            self.userconfig = MainFileReader(LDSUtilities.standardiseUserConfigName(inituserconfig),False)
        self.mainconfig = MainFileReader()
        
        
#    def setupLayerConfig(self,filename):
#        '''Adds a layerconfig file object which will be requested if external sepcified in main config'''
#        self.layerconfig = LayerFileReader(filename)
#        
#        
#    def getLayerNames(self):
#        '''Returns configured layers for respective layer properties file'''
#        return self.layerconfig.getSections()
    
        
#    #==============MAINCONFIG===========================================================


    def readDSParameters(self,drv):
        '''Returns the datasource parameters. By request updated to let users override parts of the basic config file'''
        from DataStore import DataStore
        ul = ()

        if drv==DataStore.DRIVER_NAMES['pg']:
            ml = self.mainconfig.readPostgreSQLConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readPostgreSQLConfig()
        elif drv==DataStore.DRIVER_NAMES['ms']:
            ml = self.mainconfig.readMSSQLConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readMSSQLConfig()
        elif drv==DataStore.DRIVER_NAMES['fg']:
            ml = self.mainconfig.readFileGDBConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readFileGDBConfig()
        elif drv==DataStore.DRIVER_NAMES['sl']:
            ml = self.mainconfig.readSpatiaLiteConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readSpatiaLiteConfig()
        elif drv=='WFS':
            ml = self.mainconfig.readWFSConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readWFSConfig()
        elif drv=='Proxy':
            '''Proxy parameters'''
            ml = self.mainconfig.readProxyConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readProxyConfig()
        elif drv=='Misc':
            '''Misc global parameters'''
            ml = self.mainconfig.readMiscConfig()
            if self.userconfig is not None:
                ul = self.userconfig.readMiscConfig()
        else:
            return None
        
        params = map(lambda x,y: y if x is None else x,ul,ml)
        #params = [y if x is None else x for x,y in zip(ul,ml)]  
        
        return params


    def readDSProperty(self,drv,prop):
        '''Gets a single property from a selected driver config'''
        
        pval = None if self.userconfig is None else self.userconfig.readMainProperty(drv,prop)
        return self.mainconfig.readMainProperty(drv,prop) if pval is None else pval

    @classmethod
    def buildNewUserConfig(cls,ucfilename,uctriples):
        '''Class method to initialise a user config from an array of parameters'''
        uc = MainFileReader(ucfilename,False)
        #uc.initMainFile(os.path.join(os.path.dirname(__file__), '../conf/ldsincr.conf'))
        uc.initMainFile()
        cls.writeUserConfigData(uc,uctriples)

            
    @classmethod
    def writeUserConfigData(cls,ucfile,uctriples):
        '''Write config data to config file'''
        for sfv in uctriples:
            ucfile.writeMainProperty(sfv[0],sfv[1],sfv[2])
        
        
        