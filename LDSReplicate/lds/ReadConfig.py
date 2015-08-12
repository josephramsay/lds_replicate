'''
v.0.0.9

LDSReplicate -  ReadConfig

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 24/07/2012

@author: jramsay
'''

import os
import re
import json
import ogr
import codecs

#from ConfigParser import ConfigParser, NoSectionError, NoOptionError, ParsingError,  Error
from backports.configparser import ConfigParser, NoSectionError, NoOptionError, ParsingError,  Error
from lds.LDSUtilities import LDSUtilities as LU

ldslog = LU.setupLogging()

class MainFileReader(object):
    '''
    Config file reader/writer
    '''
    LDSN = 'LDS'
    PROXY = 'Proxy'
    MISC = 'Misc'
    
    DEFAULT_MF = 'template.conf'


    def __init__(self,cfpath=None,use_defaults=True):
        '''
        Constructor
        '''
        
        from lds.DataStore import DataStore
        self.driverconfig = {i:() for i in DataStore.DRIVER_NAMES}
            
        self.use_defaults = use_defaults
        #if we dont give the constructor a file path is uses the template file which will fill in any default values missed in the user conf
        #but if cpath is requested and doesn't exist we initialise it, otherwise you end up trying to overwrite the template
        if cfpath is None:
            self.filename = LU.standardiseUserConfigName(self.DEFAULT_MF)
        else:
            #Standardise since there is no guarantee cfpath is going to be in the reqd format to start with (since its a user entry)
            self.filename = LU.standardiseUserConfigName(cfpath)

        
        self.cp = None
        self.initMainFile()
        self.fn = re.search('(.+)\.conf',os.path.basename(self.filename)).group(1)
        
    def __str__(self):
        return self.filename
        
    def initMainFile(self,template=''):
        '''Open and populate a new config file with 'template' else just touch it. Then call the reader'''
        if self.filename.split('/')[-1] != self.DEFAULT_MF:
            mode = 'a' if template is '' else 'w'
            with codecs.open(self.filename, mode, 'utf-8') as f:
                f.write(template)
        self._readConfigFile(self.filename)    
     
    def hasSection(self,secname):
        return secname in self.getSections()
    
    def getSections(self):
        '''List of sections (layernames/datasources)'''
        return self.cp.sections()
        
    def _readConfigFile(self,fn):
        '''Reads named config file'''
        #Split off so you can override the config file on the same reader object if needed
        self.cp = ConfigParser()
        with codecs.open(fn,'r','utf-8') as cf:
            self.cp.readfp(cf)

    
    #database
    def readDriverConfig(self,dname):
        dnd = {'PostgreSQL':self.readPostgreSQLConfig,
               'MSSQLSpatial':self.readMSSQLConfig,
               'FileGDB':self.readFileGDBConfig,
               'SQLite':self.readSpatiaLiteConfig,
               'WFS':self.readWFSConfig,
               'Proxy':self.readProxyConfig,
               'Misc':self.readMiscConfig}
        return dnd[dname]()
        
    def readPostgreSQLConfig(self):
        '''PostgreSQL specific config file reader'''            
        from PostgreSQLDataStore import PostgreSQLDataStore as PG
        
        ref = self.fn+':PG. '
        usr = None
        pwd = None
        over = None
        epsg = None
        cql = None
        schema = None
        
        if self.use_defaults:
            host = "127.0.0.1"
            port = 5432
            dbname = "ldsincr"
            schema = "lds"
            config = "external"
        else:
            host = None
            port = None
            dbname = None
            config = None
        
        
        try:
            host = self.cp.get(PG.DRIVER_NAME, 'host')
        except NoSectionError:
            ldslog.warn(ref+"No PostgreSQL section")
            raise
            #return (None,)*10
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            port = self.cp.get(PG.DRIVER_NAME, 'port')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            dbname = self.cp.get(PG.DRIVER_NAME, 'dbname')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            schema = self.cp.get(PG.DRIVER_NAME, 'schema')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            usr = self.cp.get(PG.DRIVER_NAME, 'user')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            pwd = self.cp.get(PG.DRIVER_NAME, 'pass')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            config = self.cp.get(PG.DRIVER_NAME, 'config')
        except NoOptionError:
            ldslog.warn(ref+"No config preference specified, default to "+str(config))
        
        try:
            over = self.cp.get(PG.DRIVER_NAME, 'overwrite')
        except NoOptionError:
            ldslog.warn(ref+"Overwrite not specified, Setting to True")
            over = True
            
        try:
            epsg = self.cp.get(PG.DRIVER_NAME, 'epsg')
        except NoOptionError:
            ldslog.warn(ref+"EPSG not specified, default to none keeping existing SRS")
            epsg = True
            
        try: 
            cql = self.cp.get(PG.DRIVER_NAME, 'cql')
        except NoOptionError:
            ldslog.warn(ref+"No CQL Filter specified, fetching all results")
            cql = None
        
        return (host,port,dbname,schema,usr,pwd,over,config,epsg,cql)
    
    def readMSSQLConfig(self):
        '''MSSQL specific config file reader'''
        
        from MSSQLSpatialDataStore import MSSQLSpatialDataStore as MS
        
        ref = self.fn+':MS. '
        usr = None
        pwd = None
        epsg = None
        cql = None
        schema = None
        
        if self.use_defaults:
            odbc = "FreeTDS"
            server = "127.0.0.1\SQLExpress"
            dbname = "LDSINCR"
            trust = "yes"
            dsn = "LDSINCR"
            config = "external"
        else:
            odbc = None
            server = None
            dbname = None
            trust = None
            dsn = None
            config = None
             
        try:
            odbc = self.cp.get(MS.DRIVER_NAME, 'odbc')
        except NoSectionError:
            ldslog.warn(ref+"No MSSQLSpatial section")
            raise
            #return (None,)*11
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            server = self.cp.get(MS.DRIVER_NAME, 'server')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            dsn = self.cp.get(MS.DRIVER_NAME, 'dsn')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            trust = self.cp.get(MS.DRIVER_NAME, 'trust')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            dbname = self.cp.get(MS.DRIVER_NAME, 'dbname')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            schema = self.cp.get(MS.DRIVER_NAME, 'schema')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            usr = self.cp.get(MS.DRIVER_NAME, 'user')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
            
        try:
            pwd = self.cp.get(MS.DRIVER_NAME, 'pass')
        except NoOptionError as noe:
            ldslog.warn(ref+str(noe))
        
        try:
            config = self.cp.get(MS.DRIVER_NAME, 'config')
        except NoOptionError:
            ldslog.warn(ref+"No config preference specified, default to "+str(config))
            
        try:
            epsg = self.cp.get(MS.DRIVER_NAME, 'epsg')
        except NoOptionError:
            ldslog.warn(ref+"EPSG not specified, default to None keeping existing SRS")
            
        try: 
            cql = self.cp.get(MS.DRIVER_NAME, 'cql')
        except NoOptionError:
            ldslog.warn(ref+"No CQL Filter specified, fetching all results")
            cql = None
        
        return (odbc,server,dsn,trust,dbname,schema,usr,pwd,config,epsg,cql)
    
    def readSpatiaLiteConfig(self):
        '''SpatiaLite specific config file reader'''
        from SpatiaLiteDataStore import SpatiaLiteDataStore as SL

        ref = self.fn+':SL. '
        epsg = None
        cql = None
        
        if self.use_defaults:
            lcfname = "~/LDSSLITE.sqlite3"
            config = "external"
        else:
            lcfname = None
            config = None
        
        try:
            lcfname = self.cp.get(SL.DRIVER_NAME, 'file')
        except NoSectionError:
            ldslog.warn(ref+"No SpatiaLite section")
            raise
            #return (None,)*4
        except NoOptionError:
            ldslog.warn(ref+"No DB name provided, default to "+str(lcfname))
            
        try:
            config = self.cp.get(SL.DRIVER_NAME, 'config')
        except NoOptionError:
            ldslog.warn(ref+"No config preference specified, default to "+str(config))
            
        try:
            epsg = self.cp.get(SL.DRIVER_NAME, 'epsg')
        except NoOptionError:
            ldslog.warn(ref+"EPSG not specified, default to "+str(epsg)+" keeping existing SRS")
            
        try: 
            cql = self.cp.get(SL.DRIVER_NAME, 'cql')
        except NoOptionError:
            ldslog.warn(ref+"No CQL Filter specified, fetching all results")
        
        return (lcfname,config,epsg,cql)
    
    def readFileGDBConfig(self):
        '''FileGDB specific config file reader'''
        from FileGDBDataStore import FileGDBDataStore as FG
        
        ref = self.fn+':FG. '
        epsg = None
        cql = None
        
        if self.use_defaults:
            lcfname = "~/LDSFGDB.gdb"
            config = "external"
        else:
            lcfname = None
            config = None

            
        try:
            lcfname = self.cp.get(FG.DRIVER_NAME, 'file')
        except NoSectionError:
            ldslog.warn(ref+"No FileGDB section")
            raise
            #return (None,)*4
        except NoOptionError:
            ldslog.warn(ref+"No DB name provided, default to "+str(lcfname))
            
        try:
            config = self.cp.get(FG.DRIVER_NAME, 'config')
        except NoOptionError:
            ldslog.warn(ref+"No config preference specified, default to "+str(config))
            
        try:
            epsg = self.cp.get(FG.DRIVER_NAME, 'epsg')
        except NoOptionError:
            ldslog.warn(ref+"EPSG not specified, default to "+str(epsg)+" none keeping existing SRS")
            
        try: 
            cql = self.cp.get(FG.DRIVER_NAME, 'cql')
        except NoOptionError:
            ldslog.warn(ref+"No CQL Filter specified, fetching all results")
        
        return (lcfname,config,epsg,cql)
        
    def readWFSConfig(self):
        '''Generic WFS config file reader'''
        '''Since this now keys on the driver name, WFS is read before LDS and LDS not at all, So...'''
        
        return self.readLDSConfig()
    
    def readLDSConfig(self):
        '''LDS specific config file reader'''
        #***HACK *** reimport NSE aliased
        from ConfigParser import NoSectionError as NSE
        #use_defaults determines whether we use default values. For a user config this may not be wise
        #since a user config is a custom file relying on the main config for absent values not last-resort defaults
        ref = self.fn+':LDS. '
        cql = None
        key = None
        
        #url = "http://data.linz.govt.nz/"
        #ver = "2.0.0"
        if self.use_defaults:
            url = "http://wfs.data.linz.govt.nz/"
            fmt = "GML2"
            svc = "WFS"
            ver = "1.0.0"
        else:
            url = None
            fmt = None
            svc = None
            ver = None
        
        try:
            url = self.cp.get(self.LDSN, 'url')
        #for some reason we cant use global NoSectionError in this function (even though its fine everywhere else) 
        except NSE:
            ldslog.warn(ref+"LDS: No LDS Section")
            return (None,)*6
        except NoOptionError:
            ldslog.warn(ref+"Default URL assumed ")

        
            
        try:   
            key = self.cp.get(self.LDSN, 'key') 
        except NoOptionError, NoSectionError:
            ldslog.error(ref+"Key required to connect to LDS...")
            raise
            
        try: 
            fmt = self.cp.get(self.LDSN, 'fmt')
        except NoOptionError:
            ldslog.warn(ref+"No output format specified")
        
        try: 
            svc = self.cp.get(self.LDSN, 'svc')
        except NoOptionError:
            ldslog.warn(ref+"No service type specified, default to "+str(svc))
        
        try: 
            ver = self.cp.get(self.LDSN, 'ver')
        except NoOptionError:
            ldslog.warn(ref+"No Version specified, assuming WFS and default to version "+str(ver))        
            
        try: 
            cql = self.cp.get(self.LDSN, 'cql')
        except NoOptionError:
            ldslog.warn(ref+"No CQL Filter specified, fetching all results")
            

        
        return (url,key,svc,ver,fmt,cql)
    
    def readProxyConfig(self):
        '''Proxy config reader'''
        
        #use_defaults determines whether we use default values. For a user config this may not be wise
        #since a user config is a custom file relying on the main config for absent values not last-resort defaults
        ref = self.fn+':Proxy. '
        
        host = None
        port = None
        usr = None
        pwd = None
        
        if self.use_defaults:
            auth = 'NTLM'
            #an NTLM default is still valid if type returns not DIRECT
            type = 'DIRECT'
        else:
            auth = None
            type = None

        try:
            type = self.cp.get(self.PROXY, 'type')
        except NoSectionError:
            ldslog.warn(ref+"No Proxy section")
            return (type,)+(None,)*5
        except NoOptionError:
            ldslog.warn(ref+"Type not defined, Direct assumed ")
            
            
        try:
            host = self.cp.get(self.PROXY, 'host')
        except NoSectionError:
            ldslog.warn(ref+"No Proxy section")
            return (None,)*6
        except NoOptionError:
            ldslog.warn(ref+"Host not defined, no-Proxy assumed ")
            
        try:
            port = self.cp.get(self.PROXY, 'port')
        except NoOptionError:
            ldslog.warn(ref+"Port not defined, no-Proxy assumed")
            
        try:
            auth = self.cp.get(self.PROXY, 'auth')
        except NoOptionError:
            ldslog.warn(ref+"Auth not defined, NTLM assumed")

        try:
            usr = self.cp.get(self.PROXY, 'user')
        except NoOptionError:
            ldslog.warn(ref+"No user defined")        
        
        try:
            pwd = self.cp.get(self.PROXY, 'pass')
        except NoOptionError:
            ldslog.warn(ref+"No pass defined") 
            
        return (type,host,port,auth,usr,pwd)
    
    def readMiscConfig(self):
        ref = self.fn+':Misc. '
        
        sixtyfourlayers = None
        partitionlayers = None
        #NB. for v:x772 ps=1000000 is too small, 100000-1999999 returns None but ps=10000000 is too large, hangs or quits on XML parse fail
        partitionsize = None
        
        #strip off any prefixes, anythingpreceeding - or :
        try: 
            #sixtyfourlayers = map(lambda s: s if s[:3]==LU.LDS_VX_PREFIX else LU.LDS_VX_PREFIX+s, self.cp.get(self.MISC, '64bitlayers').split(','))
            #sixtyfourlayers = [s if s[:3] == prefix[:3] else prefix+s for s in str(self.cp.get(self.MISC, '64bitlayers')).split(',')]
            sixtyfourlayers = [s.split(':layer-|:table-')[-1].split(':x')[-1] for s in str(self.cp.get(self.MISC, '64bitlayers')).split(',')]
        except NoSectionError:
            ldslog.warn(ref+"No Misc section detected looking for 64bit Layer specification")
        except NoOptionError:
            ldslog.warn(ref+"No 64bit Layers specified. NB. '64bitlayers'")
            
        try: 
            #partitionlayers = map(lambda s: s if s[:3]==LU.LDS_VX_PREFIX else LU.LDS_VX_PREFIX+s, self.cp.get(self.MISC, 'partitionlayers').split(','))
            #partitionlayers = [s if s[:3]==prefix[:3] else prefix+s for s in str(self.cp.get(self.MISC, 'partitionlayers')).split(',')]
            partitionlayers = [s.split(':layer-|:table-')[-1].split(':x')[-1] for s in str(self.cp.get(self.MISC, 'partitionlayers')).split(',')]
        except NoSectionError:
            ldslog.warn(ref+"No Misc section detected looking for Problem Layer specification")
        except NoOptionError:
            ldslog.warn(ref+"No Partition Layers specified. NB. 'partitionlayers'")
            
        try: 
            partitionsize = self.cp.get(self.MISC, 'partitionsize')
        except NoSectionError:
            ldslog.warn(ref+"No Misc section detected looking for Partition Size specification. Default = "+str(partitionsize)+". NB. 'partitionsize'")
        except NoOptionError:
            ldslog.warn(ref+"No Partition Size specified. Default = 'Memory'. NB. 'partitionsize'")
            
        try: 
            prefetchsize = self.cp.get(self.MISC, 'prefetchsize')
        except NoSectionError:
            ldslog.warn(ref+"No Misc section detected looking for incremental Prefetch Size. Default (partitionsize) = "+str(partitionsize))
            prefetchsize = partitionsize
        except NoOptionError:
            ldslog.warn(ref+"No incremental Prefetch Size specified. Default (partitionsize) = "+str(partitionsize))
            prefetchsize = partitionsize
        
        return (sixtyfourlayers,partitionlayers,partitionsize,prefetchsize)
        
        
    def readConfig(self,dname):
        from lds.PostgreSQLDataStore import PostgreSQLDataStore as PG
        from lds.MSSQLSpatialDataStore import MSSQLSpatialDataStore as MS
        from lds.FileGDBDataStore import FileGDBDataStore as FG
        from lds.SpatiaLiteDataStore import SpatiaLiteDataStore as SL
        
        if dname == PG.DRIVER_NAME:
            return self.readPostgreSQLConfig()
        elif dname == MS.DRIVER_NAME:
            return self.readMSSQLConfig()
        elif dname == FG.DRIVER_NAME:
            return self.readFileGDBConfig()
        elif dname == SL.DRIVER_NAME:
            return self.readSpatiaLiteConfig()
        elif dname == 'LDS':
            return self.readLDSConfig()
        elif dname == 'Misc':
            return self.readMiscConfig()
        elif dname == 'Proxy':
            return self.readProxyConfig()
            
    def readAllConfig(self):
        '''Reads the entire config file. Needed in case of missing config options. eg If guiprefs has MS set but not configured, find the next best one'''
        for drn in self.driverconfig.keys()+['LDS','Proxy','Misc']:
            self.driverconfig[drn] = self.readConfig(drn)
        
    
    def readMainProperty(self,driver,key):
        try:
            return LU.assessNone(self.cp.get(driver, key))
#             if LU.assessNone(value) is None:
#                 return None
        except:
            '''return a default value otherwise none which would also be a default for some keys'''
            ldslog.warn("Cannot find requested driver/key in {}; self.cp.get('{}','{}') combo".format(self.filename[self.filename.rfind('/'):],driver,key))
        return None
    
    
    
    def writeMainProperty(self,section,field,value):
        '''Write changes to named config table'''
        try:
            if not self.cp.has_section(section):
                self.cp.add_section(section)            
            self.cp.set(section,field,value if value else '')
            with codecs.open(self.filename, 'w','utf-8') as configfile:
                self.cp.write(configfile)
            ldslog.debug("Check "+str(field)+" for section "+str(section)+" is set to "+str(value)+" : GetField="+self.cp.get(section, field))                                                                                        
        except Exception as e:
            ldslog.warn('Problem writing to config file. '+str(e))
    
    
    @classmethod
    def validate(cls,uconf):
        '''Make sure a guipref file is valid, check pref points to alt least one valid DST'''
        from lds.DataStore import DataStore
        filename = os.path.join(os.path.dirname(__file__),'../conf',uconf+'.conf')
        uc = MainFileReader(filename)
        #validate UC check it has a key and at least one DST sec
        d = False
        for sec in DataStore.DRIVER_NAMES.values():
            d |= uc.cp.has_section(sec)
        return bool(re.search('[a-zA-Z0-9]{32}',uc.cp.get('LDS','key'))) & d
    
# Functions above relate to connection config info
#----------------------------------------------------------------------------------------------------------------------
# Functions [4] below relate to layer config data
    
from abc import ABCMeta, abstractmethod    

#override function to ensure method names stay consistent
def override(interface_class):
    def overrider(method):
        assert(method.__name__ in dir(interface_class))
        return method
    return overrider

class LayerReader(object):

    __metaclass__ = ABCMeta
    '''abstract super class for holding common functionality'''
    def __init__(self,lcfname):
        self.lcfname = lcfname
    
    @abstractmethod
    def readLayerProperty(self,layer,key):
        pass
    
    @abstractmethod
    def writeLayerProperty(self,layer,key,value):
        pass
    
    @abstractmethod
    def readLayerParameters(self,layer):
        pass
    
    @abstractmethod
    def readAllLayerParameters(self):
        pass
    
    @abstractmethod
    def findLayerIdByName(self,lname):
        pass
    
    @abstractmethod
    def getLayerNames(self,refresh):
        pass
    
    @abstractmethod
    def buildConfigLayer(self,res):
        pass
    
    @abstractmethod
    def isCurrent(self):
        pass
    
    #for non in-DB LC's we don't ned to access a DS instance
    def getDS(self):
        return False
     
    def syncDS(self):
        pass
    
    def addCustomTag(self,layerlist,tagname):        
        '''Write a keyword to all the layers in the provided list'''
        self._reTag(layerlist, tagname, True)
                
    def delCustomTag(self,layerlist,tagname):        
        '''Delete a keyword from all the layers in the provided list'''
        self._reTag(layerlist, tagname, False)
                
    def _reTag(self,layerlist,tagname,addtag):        
        '''Add/Delete a keyword from all the layers in the provided list'''
        for layer in layerlist:
            keywords = set([LU.recode(f,uflag='encode') for f in self.readLayerProperty(layer, 'Category').split(',')])
            if addtag:
                keywords.add(tagname)
            else:
                keywords.remove(tagname)
                
            self.writeLayerProperty(layer, 'Category', ','.join(keywords))    

    
class LayerFileReader(LayerReader):
    
    def __init__(self,lcfname):
        '''
        Constructor
        '''
        super(LayerFileReader,self).__init__(lcfname)
        
        self.cp = ConfigParser()
        self.lcfilename = LU.standardiseLayerConfigName(self.lcfname)
            
        self._readConfigFile(self.lcfilename)
        
    def isCurrent(self):
        '''TF test to decide whether to init'''
        return self.lcfilename and self._fileexists() and len(self.cp.sections())>0
    
    def close(self):
        self.cp = None
        self.lcfilename = None
        self.lcfname = None
        
    def _fileexists(self):
        return os.path.exists(self.lcfilename)
    
    def buildConfigLayer(self,res):
        '''Just write a file in conf with the name <driver>.layer.properties'''
        ###This should eb a UTF8 write, but only works as ASCII
        #open(self.filename,'w').write(str(res))
        with codecs.open(self.lcfilename,'w','utf-8') as lconf: 
            lconf.write(res)
        self._readConfigFile(self.lcfilename)
        
    def _readConfigFile(self,fn):
        '''Reads named config file'''
        #Split off so you can override the config file on the same reader object if needed
        try:
            with codecs.open(fn,'r','utf-8') as cf:
                self.cp.readfp(cf)
        except ParsingError as pe:
            ldslog.error('{0} file corrupt. Please correct the error; {1} OR delete and rebuild'.format(fn,str(pe)))
            raise
    
    @override(LayerReader)
    def findLayerIdByName(self,name):
        '''Reverse lookup of section by associated name, finds first occurance only'''
        lid = filter(lambda n: name==self.cp.get(n,'name'),self.cp.sections())
        return lid[0] if len(lid)>0 else None
    
    @override(LayerReader)
    def getLayerNames(self,refresh=False):
        '''Returns sections from properties file'''
        lcnames = []
        for sec in self.cp.sections():
            lcn = self.cp.get(sec, 'name')
            lcc = self.cp.get(sec,'category')
            lcnames += [(sec if type(sec)==unicode else unicode(sec,'utf8'),
                         lcn if type(lcn)==unicode else unicode(lcn,'utf8'),
                        (lcc if type(lcc)==unicode else unicode(lcc,'utf8')).split(',')),]
        return lcnames
    
    @override(LayerReader)
    def readLayerProperty(self,layer,key):
        try:
            if isinstance(layer,tuple) or isinstance(layer,list):
                value = () 
                for l in layer:
                    value += (LU.assessNone(self.cp.get(l, key)),)
            else:
                value = LU.assessNone(self.cp.get(layer, key))
        except:
            '''return a default value otherwise none which would also be a default for some keys'''
            #the logic here may be a bit suss, if the property is blank return none but if there is an error assume a default is needed?
            return {'pkey':'ID','name':layer,'geocolumn':'SHAPE'}.get(key)
        return value
    
    def _readSingleLayerProperty(self,layer,key):
        return LU.assessNone(self.cp.get(layer, key))
        
    
    @override(LayerReader)
    def writeLayerProperty(self,layer,field,value):
        '''Write changes to layer config table'''
        #if value: value = value.strip()
        try:    
            if (isinstance(layer,tuple) or isinstance(layer,list)) and (isinstance(value,tuple) or isinstance(value,list)): 
                for l,v in zip(layer,value):
                    self.cp.set(l,field,v.strip() if v else '')
            else:
                self.cp.set(layer,field,value.strip() if value else '')
            with codecs.open(self.lcfilename, 'w','utf-8') as configfile:
                self.cp.write(configfile)
            #ldslog.debug("Check "+str(field)+" for layer "+str(layer)+" is set to "+str(value)+" : GetField="+self.cp.get(layer, field))                                                                                        
        except Exception as e:
            ldslog.warn('Problem writing LM date to layer config file. '+str(e))
            
    @override(LayerReader)
    def readLayerParameters(self,id):
    #def readLayerSchemaConfig(self,layer):
        '''Full Layer config reader. Returns the config values for the whole layer or makes sensible guesses for defaults'''
        from LDSUtilities import LayerConfEntry
#        
#        try:
#            defn = self.cp.get(layer, 'sql')
#            #if the user has gone to the trouble of defining their own schema in SQL just return that
#            return (defn,None,None,None,None,None,None,None,None)
#        except:
#            pass
            
        '''optional but one way to record the type and name of a column is to save a string tuple (name,type) and parse this at build time'''
        try:
            pkey = self.cp.get(id, 'pkey')
        except NoOptionError:
            ldslog.warn("LayerSchema: No Primary Key Column defined, default to 'ID'")
            pkey = 'ID'
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            name = self.cp.get(id, 'name')
        except NoOptionError:
            ldslog.warn("LayerSchema: No Name saved in config for this layer, returning ID")
            name = id
            
        if name is None:
            name = id
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            group = LU.assessNone(self.cp.get(id, 'category'))
        except NoOptionError:
            ldslog.warn("Group List: No Groups defined for this layer")
            group = None
        
        if not group:
            pass     
            
        try:
            gcol = self.cp.get(id, 'geocolumn')
        except NoOptionError:
            ldslog.warn("LayerSchema: No Geo Column defined, default to 'SHAPE'")
            gcol = 'SHAPE'
        
        #i dont think we need this anymore using the new logic, if gcol->spatial, if pkey->unique    
#        try:
#            index = self.cp.get(layer, 'index')
#        except NoOptionError:
#            ldslog.warn("LayerSchema: No Index Column/Specification defined, default to None")
#            index = None
            
        try:
            epsg = self.cp.get(id, 'epsg')
        except NoOptionError:
            #print "No Projection Transformation defined"#don't really need to state the default occurance
            epsg = None
            
        try:
            lmod = self.cp.get(id, 'lastmodified')
        except NoOptionError:
            ldslog.warn("LayerSchema: No Last-Modified date recorded, successful update will write current time here")
            lmod = None
            
        try:
            disc = self.cp.get(id, 'discard')
        except NoOptionError:
            disc = None 
            
        try:
            cql = self.cp.get(id, 'cql')
        except NoOptionError:
            cql = None
            
        return LayerConfEntry(id,pkey,name,group,gcol,epsg,lmod,disc,cql)
    
    @override(LayerReader)
    def readAllLayerParameters(self):
        '''Gets all LC entries as a list of LCEs using readLayerParameters'''
        lcel = []
        for ln in self.getLayerNames():
            lcel += [self.readLayerParameters(ln),]
        return lcel
            
        
            
class LayerDSReader(LayerReader):
    '''
    Layer config wrapper for internal format config file.
    '''
    # Ported from DS location so some optimisation needed


    def __init__(self,lcfname):
        '''
        Constructor
        '''
        #in the DS context lcfname refers to a DS object
        super(LayerDSReader,self).__init__(lcfname)
        #self.ds = self.lcfname.ds
        self.namelist = ()  
    
    #acquire and release DS instance to for each function call to prevent DS locking        
    def getDS(self):
        return self.lcfname.ds###fname -> lcfname
    
    def syncDS(self):
        pass
        #self.fname.ds.SyncToDisk()
        #self.ds = None #Because this DS is inherited from the parent, don't kill it. Maybe better to call syncDS?

    def close(self):
        self.namelist = None
        self.lcfname = None #i expect this to cause an error
        
    def isCurrent(self):
        '''Test for DS table'''
        if self.lcfname.ds: ###fname -> lcfname
            try:
                if self.lcfname.ds.GetLayer(self.lcfname.LDS_CONFIG_TABLE): ###fname -> lcfname
                    return True
            except RuntimeError as rte:
                if not re.search('No table/field definitions found for',str(rte)): 
                    ldslog.warn('Unable to open '+str(self.lcfname.LDS_CONFIG_TABLE))#raise###fname -> lcfname
                else:#gets raised anyway
                    raise
        return False
        
    def buildConfigLayer(self,res):
        '''Builds the config table into and using the active DS'''
        config_layer = None
        try:
            self.lcfname.ds.DeleteLayer(self.lcfname.LDS_CONFIG_TABLE)###fname -> lcfname
        except Exception as e:
            ldslog.warn("Exception deleting config layer: "+str(e))
        #CreateLayer(self, char name, SpatialReference srs = None, OGRwkbGeometryType geom_type = wkbUnknown, char options = None) -> Layer
        try:
            config_layer = self.lcfname.ds.CreateLayer(self.lcfname.LDS_CONFIG_TABLE, None, self.lcfname.selectValidGeom(ogr.wkbNone), ['OVERWRITE=YES'])###fname -> lcfname
        except Exception as e:
            ldslog.warn("Exception creating config layer: "+str(e))
            
        if config_layer is None:
            ldslog.error("Cannot create lds config layer: " + self.lcfname.LDS_CONFIG_TABLE)###fname -> lcfname
        
        feat_def = ogr.FeatureDefn()
        for name in self.lcfname.CONFIG_COLUMNS:###fname -> lcfname
            #create new field defn with name=name and type OFTString
            fld_def = ogr.FieldDefn(name,ogr.OFTString)
            #in the feature defn, define a new field
            feat_def.AddFieldDefn(fld_def)
            #also add a field to the table definition, i.e. column
            if config_layer.TestCapability(ogr.OLCCreateField):
                config_layer.CreateField(fld_def,True)
              
        
        for row in json.loads(res):
            config_feat = ogr.Feature(feat_def)
            #HACK
            #if self.DRIVER_NAME == 'MSSQLSpatial':
            #    do something hack-y
            for col in range(0,10):     
                val = (','.join(row[col]) if row[col] else None) if col in (3,8) else row[col]
                config_feat.SetField(self.lcfname.CONFIG_COLUMNS[col],val.encode('utf-8') if val else None)
                #config_feat.SetField(self.lcfname.CONFIG_COLUMNS[col],LU.recodeForDriver(val,self.lcfname.ds.Driver.__name__))
            
            config_layer.CreateFeature(config_feat)
            
        config_layer.ResetReading()
        config_layer.SyncToDisk()

    
    @override(LayerReader)
    def findLayerIdByName(self,lname):
        '''Reverse lookup of section by associated name, finds first occurance only'''
        layer = self.lcfname.ds.GetLayer(self.lcfname.LDS_CONFIG_TABLE)###fname -> lcfname
        layer.ResetReading()
        #HACK Win7
        layer.GetFeatureCount()
        feat = layer.GetNextFeature() 
        while feat:
            if LU.unicodeCompare(lname,feat.GetField('name')):#.encode('utf8'):
                return feat.GetField('id')#.encode('utf8')
            feat = layer.GetNextFeature()
        return None
        
    @override(LayerReader)
    def getLayerNames(self,refresh=False):
        '''Returns configured layers for respective layer properties file'''
        #gdal.SetConfigOption('CPL_DEBUG','ON')
        #gdal.SetConfigOption('CPL_LOG_ERRORS','ON')
        if refresh or not self.namelist:
            self.namelist = []
            layer = self.lcfname.ds.GetLayer(self.lcfname.LDS_CONFIG_TABLE)###fname -> lcfname
            if layer:
                layer.SetIgnoredFields(('OGR_GEOMETRY',))
                layer.ResetReading()
                #HACK Win7
                layer.GetFeatureCount()
                feat = layer.GetNextFeature() 
                while feat:
                    try:
                        lcid = feat.GetField('id')
                        lcname = feat.GetField('name')
                        lccats = [LU.recode(f) if f else None for f in feat.GetField('category').split(',')]
                        self.namelist += ((LU.recode(lcid) if lcid else None, LU.recode(lcname) if lcname else None,lccats),) 
                        #self.namelist += ((feat.GetField('id').encode('utf8'),feat.GetField('name').encode('utf8'),[f.encode('utf8').strip() for f in feat.GetField('category').split(',')]),)
                        feat = layer.GetNextFeature()
                    except UnicodeEncodeError as uee:
                        raise
                    except UnicodeDecodeError as ude:
                        raise
            else:
                ldslog.error('REMINDER! TRIGGER CONF BUILD')
            #print '>>>>> NAMELIST',self.namelist
        return self.namelist
    
    @override(LayerReader) 
    def readLayerParameters(self,id):
        '''Full Feature config reader'''
        from DataStore import InaccessibleFeatureException
        layer = self.lcfname.ds.GetLayer(self.lcfname.LDS_CONFIG_TABLE)###fname -> lcfname
        layer.ResetReading()
        #HACK Win7
        layer.GetFeatureCount()
        feat = self.lcfname._findMatchingFeature(layer, 'id', id)###fname -> lcfname
        if not feat:
            InaccessibleFeatureException('Cannot access feature with id='+str(id)+' in layer '+str(layer.GetName()))
        return LU.extractFields(feat)
    
    @override(LayerReader) 
    def readAllLayerParameters(self):
        '''Full Layer config reader'''
        lcel = []
        layer = self.lcfname.ds.GetLayer(self.lcfname.LDS_CONFIG_TABLE)### fname -> lcfname
        if layer:
            layer.SetIgnoredFields(('OGR_GEOMETRY',))
            layer.ResetReading()
            #HACK Win7
            layer.GetFeatureCount()
            feat = layer.GetNextFeature()
            while feat:
                #ii = LU.extractFields(feat)
                #print '>1>',ii
                #lcel += [ii,]
                lcel += [LU.extractFields(feat),]
                feat = layer.GetNextFeature()
                #if feat: print '>2>','fid=',feat.GetFID(),'rc=',layer.GetRefCount(),'fc=',x
        return lcel 
        
    
    @override(LayerReader)     
    def readLayerProperty(self,pkey,field):
        '''Single property reader'''
        plist = ()
        layer = self.lcfname.ds.GetLayer(self.lcfname.LDS_CONFIG_TABLE)###fname -> lcfname
        layer.ResetReading()
        #HACK Win7
        layer.GetFeatureCount()
        if isinstance(pkey,tuple) or isinstance(pkey,list):
            for p in pkey:
                f = self.lcfname._findMatchingFeature(layer, 'id', p)###fname -> lcfname
                #plist += ('' if f is None else f.GetField(field).encode('utf8'),)
                plist += ('' if f is None else LU.recode(f.GetField(field)),)
            return plist
        else:
            feat = self.lcfname._findMatchingFeature(layer, 'id', pkey)
            if feat is None:
                return None
            prop = feat.GetField(field)
        return LU.recode(prop) if LU.assessNone(prop) else None#.encode('utf8')
    
    @override(LayerReader)
    def writeLayerProperty(self,pkey,field,value):
        '''Write changes to layer config table. Keyword changes are written as a comma-seperated value '''
        #ogr.UseExceptions()
        try:
            layer = self.lcfname.ds.GetLayer(self.lcfname.LDS_CONFIG_TABLE)###fname -> lcfname
            if (isinstance(pkey,tuple) or isinstance(pkey,list)) and (isinstance(value,tuple) or isinstance(value,list)):
                for p,v in zip(pkey,value):
                    if v: v = v.strip()
                    self._setFeatureValue(layer,p,field,v)
            else:
                if value: value = value.strip()
                self._setFeatureValue(layer,pkey,field,value)
        except Exception as e:
            ldslog.error(e)
            
    def _setFeatureValue(self,layer,p,field,value):
        feat = self.lcfname._findMatchingFeature(layer, 'id', p)###fname -> lcfname
        feat.SetField(field,LU.recode(value,uflag='encode'))
        layer.SetFeature(feat)
        #ldslog.debug("Check "+field+" for layer "+p+" is set to "+value+" : GetField="+feat.GetField(field))

                   
class GUIPrefsReader(object):
    '''
    Reader for GUI prefs. To save re inputting every time 
    '''

    PREFS_SEC = 'prefs'
    GUI_PREFS = '../conf/gui.prefs'
    
    def __init__(self):
        '''
        Constructor
        '''
        self.dvalue = None
        self.dselect = 'dest'
        #v:x111|MYGROUP, myconf.conf, 2193, 2013-01-01, 2013-01-02
        self.plist = ('lgvalue','uconf','epsg','fd','td')
        
        self.cp = ConfigParser()
        self.fn = os.path.join(os.path.dirname(__file__),self.GUI_PREFS)
        with codecs.open(self.fn,'r','utf-8') as cf:
            self.cp.readfp(cf)
        
    def read(self):
        '''Read stored DS value and return this and its matching params'''
        try:
            with codecs.open(self.fn, 'r','utf-8') as cf:
                self.cp.readfp(cf)
            self.dvalue = self.cp.get(self.PREFS_SEC, self.dselect) 
            if LU.assessNone(self.dvalue) is None:
                return (None,)*(len(self.plist)+1)
        except NoSectionError as nse:
            #if no sec init sec and opt and ret nones
            ldslog.warn('Error getting GUI prefs section :: '+str(nse))
            if not self._initSection(self.PREFS_SEC):
                raise
            self._initOption(self.PREFS_SEC,self.dselect)
            return (None,)*(len(self.plist)+1)
        except NoOptionError as noe:
            #if no opt init opt and ret nones
            ldslog.warn('Error getting GUI prefs :: '+str(noe))
            if not self._initOption(self.PREFS_SEC,self.dselect):
                raise
            return (None,)*(len(self.plist)+1)
        #if dval is okay ret it and res of a read of that sec
        return (self.dvalue,)+self.readsec(self.dvalue)
    
    
    def readall(self):
        '''Reads entire grp into dict'''
        gpra = {}
        secs = self.cp.sections()
        secs.remove(self.PREFS_SEC)
        for sec in secs:
            gpra[sec] = self.readsec(sec)
        return gpra
        
    def getDestinations(self):
        return self.cp.sections()
    
    def readsec(self,section):
        #options per DS type
        rlist = ()
        
        for p in self.plist:
            try:
                rlist += (self.cp.get(section, p),)
            except NoSectionError as nse:
                #if not ds sec init sec then init opt
                ldslog.warn('Error getting GUI '+section+' :: '+str(nse))
                if not self._initSection(section):
                    raise
                self._initOption(section, p)
                rlist += (None,)
            except NoOptionError as noe:
                #if no opt init the opt
                ldslog.warn('Error getting GUI '+section+' pref, '+p+' :: '+str(noe))
                if not self._initOption(section,p):
                    raise
                rlist += (None,)
        return rlist
    
    def writeline(self,field,value):
        #not the best solution since depends on a current gpr and a recent read/write. 
        if self.dvalue:
            self.writesecline(self.dvalue,field,value)
        
    def writesecline(self,section,field,value):
        try:            
            self.cp.set(section,field,value if LU.assessNone(value) else '')
            with codecs.open(self.fn, 'w','utf-8') as configfile:
                self.cp.write(configfile)
            ldslog.debug(str(section)+':'+str(field)+'='+str(value))                                                                                        
        except Exception as e:
            ldslog.warn('Problem writing GUI prefs. {} - sfv={}'.format(e,(section,field,value)))
            
            
    def write(self,rlist):
        self.dvalue = rlist[0]
        
        if self.cp.has_section(self.PREFS_SEC):
            self.cp.set(self.PREFS_SEC,self.dselect,self.dvalue)
        else:
            self.cp.add_section(self.PREFS_SEC)
            self.cp.set(self.PREFS_SEC,self.dselect,self.dvalue)
            
        for pr in zip(self.plist,rlist[1:]):
            if not self.cp.has_section(self.dvalue):
                self.cp.add_section(self.dvalue)
            try:
                if LU.assessNone(pr[1]):         
                    self.cp.set(self.dvalue,pr[0],pr[1])
                    ldslog.debug(self.dvalue+':'+pr[0]+'='+pr[1])                                                                                        
            except Exception as e:
                ldslog.warn('Problem writing GUI prefs. '+str(e))
        with codecs.open(self.fn, 'w','utf-8') as configfile:
            self.cp.write(configfile)
                      
    
    def _initSection(self,section):
        checksec = LU.standardiseDriverNames(section)
        if checksec:
            self.cp.add_section(checksec)
            return True
        elif section == self.PREFS_SEC:
            self.cp.add_section(section)
            return True
        return False
            
    def _initOption(self,section,option):
        if option in self.plist+(self.dselect,):
            self.writesecline(section,option,None)
            return True
        return False
    
    @classmethod
    def validate():
        '''Make sure a guipref file is valid, check pref points to alt least one valid DST'''
        filename = os.path.join(os.path.dirname(__file__),GUIPrefsReader.GUI_PREFS)
        gp = GUIPrefsReader(filename)
        #validate UC check it has a valid dest named and configured
        p = gp.cp.get('prefs', 'dest')
        return gp.cp.has_section(p)

        

