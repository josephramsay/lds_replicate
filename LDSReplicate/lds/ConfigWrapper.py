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

from ReadConfig import MainFileReader, LayerFileReader

ldslog = logging.getLogger('LDS')

class ConfigWrapper(object):
    '''
    Convenience wrapper class to config-file reader instances. Facade for file reader/table reader 
    '''

    def __init__(self,config_file=None):
        #                    user cf         ,layer props file name
        '''this is not a ogr function so we dont need a driver and therefore wont call super'''
        #some layer config properties may not be needed and wont be read eg WFS so None arg wont set layerconfig
        
        
        self.CONFIG_FILE = "ldsincr.conf"
        
        self.layerconfig = None
        self.mainconfig = None
        self.userconfig = None

        self.setupMainConfig(config_file)
        #dont set up layerconfig by default. Wait till we know whether we want a new build (initconfig) 
        #self.setupLayerConfig()


    def setupMainConfig(self,inituserconfig):
        '''Sets up a reader to the main configuration file or alternatively, a user specified config file.
        Userconfig is not mean't to replace mainconfig, just overwrite the parts the user has decided to customise'''
        self.userconfig = None
        if inituserconfig is not None:
            self.userconfig = MainFileReader("../conf/"+inituserconfig,False)
        self.mainconfig = MainFileReader("../conf/"+self.CONFIG_FILE,True)
        
        
    def setupLayerConfig(self,filename):
        '''Adds a layerconfig file object which will be requested if external sepcified in main config'''
        self.layerconfig = LayerFileReader(filename)
        
        
    def getLayerNames(self):
        '''Returns configured layers for respective layer properties file'''
        return self.layerconfig.getSections()
    
        
#    #==============MAINCONFIG===========================================================


    def readDSParameters(self,drv):
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
        
        
        return params


    def readDSProperty(self,drv,prop):
        '''gets a single property from a selected driver config'''
        
        pval = None if self.userconfig is None else self.userconfig.readMainProperty(drv,prop)
        return self.mainconfig.readMainProperty(drv,prop) if pval is None else pval

    @staticmethod
    def buildNewUserConfig(ucfileid,uctriples):
        
        uc = MainFileReader(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../conf/'+str(ucfileid)+'.conf'),False)
        #uc.initMainFile(os.path.join(os.path.dirname(__file__), '../conf/ldsincr.conf'))
        uc.initMainFile()
        for sfv in uctriples:
            section = sfv[0]
            field = sfv[1]
            value = sfv[2]
            uc.writeMainProperty(section, field, value)
        
        
        