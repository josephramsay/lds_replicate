'''
v.0.0.9

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
import re

from lds.ReadConfig import MainFileReader#, LayerFileReader
from lds.LDSUtilities import LDSUtilities


class ConfigFormatException(Exception): pass
class ConfigContentException(Exception): pass


ldslog = LDSUtilities.setupLogging()

class ConfigWrapper(object):
    '''
    Convenience wrapper class to main and user config-file reader instances. Main purpose of this class is to 
    allow user to override selected portions of the main config file.
    '''
    
    def __init__(self,configdata=None):
        
        self.confdict = {}
        #self.layerconfig = None #internal/external; but only external is coded an never used
        #self.mainconfig = None  #always a file
        #self.userconfig = None  #always a file

        #check to see if conf file is string ie file path or not eg list, dict
        ##if isinstance(configdata,basestring):
        self.setupMainAndUserConfig(configdata)
        if isinstance(configdata,dict):
            #self.setupMainAndUserConfig(None)
            self.setupTempParameters(configdata)
        #else:
            #raise ConfigFormatException('Provided Config specifier is neither a parameter array  or a file path')
            
            
        #dont set up layerconfig by default. Wait till we know whether we want a new build (initconfig) 
        #self._setupLayerConfig()


    def setupMainAndUserConfig(self,inituserconfig):
        '''Sets up a reader to the main configuration file or alternatively, a user specified config file.
        Userconfig is not mean't to replace mainconfig, just overwrite the parts the user has decided to customise'''
        #self.userconfig = None
        self.userconfig = MainFileReader(LDSUtilities.standardiseUserConfigName(inituserconfig),False) if inituserconfig else None
        self.mainconfig = MainFileReader()
        
    def setupTempParameters(self,confdict):
        '''Build a dict matching returned values for use when doing a temporary setup e.g. to test a connection'''
        #this is only used for proxy testing at the moment. will configure others if needed
        self.confdict = confdict
        
        
#    #==============MAINCONFIG===========================================================

    

    def readDSParameters(self,drv,params=None):
        '''Returns the datasource parameters. By request updated to let users override parts of the basic config file'''
        from DataStore import DataStore
        ul = ()
        
        #convert from abbrev to full driver name
        drv = DataStore.DRIVER_NAMES[drv] if drv in DataStore.DRIVER_NAMES else drv
        #read main config
        ml = self.mainconfig.readDriverConfig(drv)        
        if self.confdict.has_key(drv):
            ul = self.readTempParameters(drv)
        elif self.userconfig:
            ul = self.userconfig.readDriverConfig(drv)
        #else:
        #    return None
        
        if drv == 'Misc': 
            ml = self._substIDP(params['idp'],ml)
            ul = self._substIDP(params['idp'],ul)
        
        rconfdata = [x if x else y for x,y in zip(ul if ul else (None,)*len(ml),ml)]  
        
        return rconfdata

    def readTempParameters(self,drv):
        cdd = self.confdict[drv]
        if drv=='Proxy':
            return (cdd['type'],cdd['host'],cdd['port'],cdd['auth'],cdd['user'],cdd['pass'])
        elif drv=='WFS':
            return ('',cdd['key'],'','','','')
        else:
            raise ConfigContentException('Support for Proxy config type only')
    
    def _substIDP(self,idp,mul):
        '''add requested prefix to layer list. IDP = ID Prefix'''
        #64layers
        m0 = tuple([idp+str(s) for s in mul[0]]) if mul[0] else (None,)
        #ptnlayers
        m1 = tuple([idp+str(s) for s in mul[1]]) if mul[1] else (None,)
        return m0+m1+mul[2:]

            
    def readDSProperty(self,drv,prop):
        '''Gets a single property from a selected driver config'''
        #NB uprop can be none if there is no uc object or if the prop isnt listed in the uc
        uprop = self.userconfig.readMainProperty(drv,prop) if self.userconfig else None
        return uprop if uprop else self.mainconfig.readMainProperty(drv,prop)


    @classmethod
    def buildNewUserConfig(cls,ucfilename,uctriples):
        '''Class method to initialise a user config from an array of parameters'''
        uc = MainFileReader(ucfilename,False)
        #uc.initMainFile(os.path.join(os.path.dirname(__file__), '../conf/template.conf'))
        uc.initMainFile()
        cls.writeUserConfigData(uc,uctriples)

            
    @classmethod
    def writeUserConfigData(cls,ucfile,uctriples):
        '''Write config data to config file'''
        for sfv in uctriples:
            ucfile.writeMainProperty(sfv[0],sfv[1],sfv[2])
        
        
        