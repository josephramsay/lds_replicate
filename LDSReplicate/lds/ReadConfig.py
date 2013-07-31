'''
v.0.0.1

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
import logging
import json
import ogr
import gdal

from ConfigParser import ConfigParser, NoSectionError, NoOptionError, ParsingError,  Error
from LDSUtilities import LDSUtilities as LU


ldslog = logging.getLogger('LDS')

class MainFileReader(object):
    '''
    Config file reader/writer
    '''
    LDSN = 'LDS'
    PROXY = 'Proxy'
    MISC = 'Misc'
    
    DEFAULT_MF = 'ldsincr.conf'


    def __init__(self,cfpath=None,use_defaults=True):
        '''
        Constructor
        '''
        self.use_defaults = use_defaults
        #if we dont give the constructor a file path is uses the template file (which may not be a good idea...)
        if cfpath is None:
            self.filename = LU.standardiseUserConfigName(self.DEFAULT_MF)
        else:
            #Standardise since there is no guarantee cfpath is going to be in the reqd format to start with (since its a user entry)
            self.filename = LU.standardiseUserConfigName(cfpath)

        
        self.cp = None
        self.initMainFile()
        
    def initMainFile(self,template=''):
        '''Open and populate a new config file with 'template' else just touch it. Then call the reader'''

        with file(self.filename,'a' if template is '' else 'w') as f:
            f.write(template)
        self._readConfigFile(self.filename)    
     
    def hasSection(self,secname):
        return secname in self.getSections()
    
    def getSections(self):
        '''List of sections (layernames/datasources)'''
        return self.cp.sections()
        
    def _readConfigFile(self,fname):
        '''Reads named config file'''
        #Split off so you can override the config file on the same reader object if needed
        self.cp = ConfigParser()
        self.cp.read(fname)

    
    #database
        
    def readPostgreSQLConfig(self):
        '''PostgreSQL specific config file reader'''            
        from PostgreSQLDataStore import PostgreSQLDataStore as PG
        
        usr = None
        pwd = None
        over = None
        epsg = None
        cql = None
        
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
            schema = None
            config = None
        
        
        try:
            host = self.cp.get(PG.DRIVER_NAME, 'host')
        except NoSectionError:
            ldslog.warn("No PostgreSQL section")
            return (None,)*10
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            port = self.cp.get(PG.DRIVER_NAME, 'port')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            dbname = self.cp.get(PG.DRIVER_NAME, 'dbname')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            schema = self.cp.get(PG.DRIVER_NAME, 'schema')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            usr = self.cp.get(PG.DRIVER_NAME, 'user')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            pwd = self.cp.get(PG.DRIVER_NAME, 'pass')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            config = self.cp.get(PG.DRIVER_NAME, 'config')
        except NoOptionError:
            ldslog.warn("PostgreSQL: No config preference specified, default to "+str(config))
        
        try:
            over = self.cp.get(PG.DRIVER_NAME, 'overwrite')
        except NoOptionError:
            ldslog.warn("PG: Overwrite not specified, Setting to True")
            over = True
            
        try:
            epsg = self.cp.get(PG.DRIVER_NAME, 'epsg')
        except NoOptionError:
            ldslog.warn("PG: EPSG not specified, default to none keeping existing SRS")
            epsg = True
            
        try: 
            cql = self.cp.get(PG.DRIVER_NAME, 'cql')
        except NoOptionError:
            ldslog.warn("PG: No CQL Filter specified, fetching all results")
            cql = None
        
        return (host,port,dbname,schema,usr,pwd,over,config,epsg,cql)
    
    def readMSSQLConfig(self):
        '''MSSQL specific config file reader'''
        
        from MSSQLSpatialDataStore import MSSQLSpatialDataStore as MS
        
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
            ldslog.warn("No MSSQLSpatial section")
            return (None,)*11
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            server = self.cp.get(MS.DRIVER_NAME, 'server')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            dsn = self.cp.get(MS.DRIVER_NAME, 'dsn')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            trust = self.cp.get(MS.DRIVER_NAME, 'trust')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            dbname = self.cp.get(MS.DRIVER_NAME, 'dbname')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            schema = self.cp.get(MS.DRIVER_NAME, 'schema')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            usr = self.cp.get(MS.DRIVER_NAME, 'user')
        except NoOptionError as noe:
            ldslog.warn(noe)
            
        try:
            pwd = self.cp.get(MS.DRIVER_NAME, 'pass')
        except NoOptionError as noe:
            ldslog.warn(noe)
        
        try:
            config = self.cp.get(MS.DRIVER_NAME, 'config')
        except NoOptionError:
            ldslog.warn("MSSQL: No config preference specified, default to "+str(config))
            
        try:
            epsg = self.cp.get(MS.DRIVER_NAME, 'epsg')
        except NoOptionError:
            ldslog.warn("MSSQL: EPSG not specified, default to None keeping existing SRS")
            
        try: 
            cql = self.cp.get(MS.DRIVER_NAME, 'cql')
        except NoOptionError:
            ldslog.warn("MSSQL: No CQL Filter specified, fetching all results")
            cql = None
        
        return (odbc,server,dsn,trust,dbname,schema,usr,pwd,config,epsg,cql)
    
    def readSpatiaLiteConfig(self):
        '''SpatiaLite specific config file reader'''
        from SpatiaLiteDataStore import SpatiaLiteDataStore as SL

        epsg = None
        cql = None
        
        if self.use_defaults:
            fname = "~/LDSSLITE.sqlite3"
            config = "external"
        else:
            fname = None
            config = None
        
        try:
            fname = self.cp.get(SL.DRIVER_NAME, 'file')
        except NoSectionError:
            ldslog.warn("No SpatiaLite section")
            return (None,)*4
        except NoOptionError:
            ldslog.warn("SpatiaLite: No DB name provided, default to "+str(fname))
            
        try:
            config = self.cp.get(SL.DRIVER_NAME, 'config')
        except NoOptionError:
            ldslog.warn("SpatiaLite: No config preference specified, default to "+str(config))
            
        try:
            epsg = self.cp.get(SL.DRIVER_NAME, 'epsg')
        except NoOptionError:
            ldslog.warn("SL: EPSG not specified, default to "+str(epsg)+" keeping existing SRS")
            
        try: 
            cql = self.cp.get(SL.DRIVER_NAME, 'cql')
        except NoOptionError:
            ldslog.warn("SL: No CQL Filter specified, fetching all results")
        
        return (fname,config,epsg,cql)
    
    def readFileGDBConfig(self):
        '''FileGDB specific config file reader'''
        from FileGDBDataStore import FileGDBDataStore as FG
        
        epsg = None
        cql = None
        
        if self.use_defaults:
            fname = "~/LDSFGDB.gdb"
            config = "external"
        else:
            fname = None
            config = None

            
        try:
            fname = self.cp.get(FG.DRIVER_NAME, 'file')
        except NoSectionError:
            ldslog.warn("No FileGDB section")
            return (None,)*4
        except NoOptionError:
            ldslog.warn("FileGDB: No DB name provided, default to "+str(fname))
            
        try:
            config = self.cp.get(FG.DRIVER_NAME, 'config')
        except NoOptionError:
            ldslog.warn("FileGDB: No config preference specified, default to "+str(config))
            
        try:
            epsg = self.cp.get(FG.DRIVER_NAME, 'epsg')
        except NoOptionError:
            ldslog.warn("FileGDB: EPSG not specified, default to "+str(epsg)+" none keeping existing SRS")
            
        try: 
            cql = self.cp.get(FG.DRIVER_NAME, 'cql')
        except NoOptionError:
            ldslog.warn("FileGDB: No CQL Filter specified, fetching all results")
        
        return (fname,config,epsg,cql)
        
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
        cql = None
        key = None
        
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
            ldslog.warn("LDS: No LDS Section")
            return (None,)*6
        except NoOptionError:
            ldslog.warn("LDS: Default URL assumed ")

        
            
        try:   
            key = self.cp.get(self.LDSN, 'key') 
        except NoOptionError, NoSectionError:
            ldslog.warn("LDS: Key required to connect to LDS...")
            
        try: 
            fmt = self.cp.get(self.LDSN, 'fmt')
        except NoOptionError:
            ldslog.warn("LDS: No output format specified")
        
        try: 
            svc = self.cp.get(self.LDSN, 'svc')
        except NoOptionError:
            ldslog.warn("LDS: No service type specified, default to "+str(svc))
        
        try: 
            ver = self.cp.get(self.LDSN, 'ver')
        except NoOptionError:
            ldslog.warn("LDS: No Version specified, assuming WFS and default to version "+str(ver))        
            
        try: 
            cql = self.cp.get(self.LDSN, 'cql')
        except NoOptionError:
            ldslog.warn("LDS: No CQL Filter specified, fetching all results")
            

        
        return (url,key,svc,ver,fmt,cql)
    
    def readProxyConfig(self):
        '''Proxy config reader'''
        
        #use_defaults determines whether we use default values. For a user config this may not be wise
        #since a user config is a custom file relying on the main config for absent values not last-resort defaults
        
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
            ldslog.warn("No Proxy section")
            return (type,)+(None,)*5
        except NoOptionError:
            ldslog.warn("Proxy: Type not defined, Direct assumed ")
            
            
        try:
            host = self.cp.get(self.PROXY, 'host')
        except NoSectionError:
            ldslog.warn("No Proxy section")
            return (None,)*6
        except NoOptionError:
            ldslog.warn("Proxy: Host not defined, no-Proxy assumed ")
            
        try:
            port = self.cp.get(self.PROXY, 'port')
        except NoOptionError:
            ldslog.warn("Proxy: Port not defined, no-Proxy assumed")
            
        try:
            auth = self.cp.get(self.PROXY, 'auth')
        except NoOptionError:
            ldslog.warn("Proxy: Auth not defined, NTLM assumed")

        try:
            usr = self.cp.get(self.PROXY, 'user')
        except NoOptionError:
            ldslog.warn("Proxy: No user defined")        
        
        try:
            pwd = self.cp.get(self.PROXY, 'pass')
        except NoOptionError:
            ldslog.warn("Proxy: No pass defined") 
            

        return (type,host,port,auth,usr,pwd)
    
    def readMiscConfig(self):
        
        sixtyfourlayers = None
        partitionlayers = None
        #NB. for v:x772 ps=1000000 is too small, 100000-1999999 returns None but ps=10000000 is too large, hangs or quits on XML parse fail
        partitionsize = None
        temptable = None
        
        try: 
            #sixtyfourlayers = map(lambda s: s if s[:3]==LU.LDS_TN_PREFIX else LU.LDS_TN_PREFIX+s, self.cp.get(self.MISC, '64bitlayers').split(','))
            sixtyfourlayers = [s if s[:3]==LU.LDS_TN_PREFIX else LU.LDS_TN_PREFIX+s for s in str(self.cp.get(self.MISC, '64bitlayers')).split(',')]
        except NoSectionError:
            ldslog.warn("Misc: No Misc section detected looking for 64bit Layer specification")
        except NoOptionError:
            ldslog.warn("Misc: No 64bit Layers specified. NB. '64bitlayers'")
            
        try: 
            #partitionlayers = map(lambda s: s if s[:3]==LU.LDS_TN_PREFIX else LU.LDS_TN_PREFIX+s, self.cp.get(self.MISC, 'partitionlayers').split(','))
            partitionlayers = [s if s[:3]==LU.LDS_TN_PREFIX else LU.LDS_TN_PREFIX+s for s in str(self.cp.get(self.MISC, 'partitionlayers')).split(',')]
        except NoSectionError:
            ldslog.warn("Misc: No Misc section detected looking for Problem Layer specification")
        except NoOptionError:
            ldslog.warn("Misc: No Partition Layers specified. NB. 'partitionlayers'")
            
        try: 
            partitionsize = self.cp.get(self.MISC, 'partitionsize')
        except NoSectionError:
            ldslog.warn("Misc: No Misc section detected looking for Partition Size specification. Default = "+str(partitionsize)+". NB. 'partitionsize'")
        except NoOptionError:
            ldslog.warn("Misc: No Partition Size specified. Default = 'Memory'. NB. 'partitionsize'")
            
        try: 
            temptable = self.cp.get(self.MISC, 'temptable')
        except NoSectionError:
            ldslog.warn("Misc: No Misc section detected looking for Temporary Table type. Default = "+str(temptable)+". NB. 'temptable'")
        except NoOptionError:
            ldslog.warn("Misc: No Temporary Table type specified. Default = 'Memory'. NB. 'temptable'")
        
        return (sixtyfourlayers,partitionlayers,partitionsize,temptable)
        
    
    def readMainProperty(self,driver,key):
        try:
            value = self.cp.get(driver, key)
            if LU.mightAsWellBeNone(value) is None:
                return None
        except:
            '''return a default value otherwise none which would also be a default for some keys'''
            ldslog.warn("Cannot find requested driver/key ("+str(driver)+"/"+str(key)+")combo")
            return None
        return value
    
    
    
    def writeMainProperty(self,section,field,value):
        '''Write changes to named config table'''
        try:
            if not self.cp.has_section(section):
                self.cp.add_section(section)            
            self.cp.set(section,field,value if value is not None else '')
            with open(self.filename, 'w') as configfile:
                self.cp.write(configfile)
            ldslog.debug("Check "+str(field)+" for section "+str(section)+" is set to "+str(value)+" : GetField="+self.cp.get(section, field))                                                                                        
        except Exception as e:
            ldslog.warn('Problem writing to config file. '+str(e))
    
    # Functions above relate to connection config info
    #----------------------------------------------------------------------------------------------
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
    def __init__(self,fname):
        self.fname = fname
    
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
    def findLayerIdByName(self,lname):
        pass
    
    @abstractmethod
    def getLayerNames(self):
        pass
    
    @abstractmethod
    def buildConfigLayer(self,res):
        pass
    
    @abstractmethod
    def exists(self):
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
            keywords = set(str(self.readLayerProperty(layer, 'Category')).split(','))
            if addtag:
                keywords.add(tagname)
            else:
                keywords.remove(tagname)
                
            self.writeLayerProperty(layer, 'Category', ','.join(keywords))

    def getLConfAs3Array(self):
        '''Specialised conversion of the entire lconf to an array of layer-id,name and keys... for GUI menu use'''
        return [(i[0],i[2],i[3]) for i in self.getLConfAsArray()] 
    
    def getLConfAsArray(self):
        '''Returns a read lconf as an array'''
        lca = []
        for layer in self.getLayerNames():
            lce = self.readLayerParameters(layer)
            #pkey,name,group,gcol,epsg,lmod,disc,cq
            groups = [g.strip() for g in lce.group.split(',')]
            lca.append((layer,lce.pkey,lce.name,groups,lce.gcol,lce.epsg,lce.lmod,lce.disc,lce.cql),)
        return lca
    
#--------------------------------------------------------------------------------------------------
    
class LayerFileReader(LayerReader):
    
    def __init__(self,fname):
        '''
        Constructor
        '''
        super(LayerFileReader,self).__init__(fname)
        
        self.cp = ConfigParser()
        self.filename = LU.standardiseLayerConfigName(self.fname)
            
        self._readConfigFile(self.filename)
        
    def exists(self):
        '''TF test to decide whether to init'''
        return self._fileexists() and len(self.cp.sections())>0
    
    def _fileexists(self):
        return os.path.exists(self.filename)
    
    def buildConfigLayer(self,res):
        '''Just write a file in conf with the name <driver>.layer.properties'''
        #open(self.filename,'w').write(str(res))
        with open(self.filename,'w') as lconf: 
            lconf.write(str(res))
        self._readConfigFile(self.filename)
        
    def _readConfigFile(self,fname):
        '''Reads named config file'''
        #Split off so you can override the config file on the same reader object if needed
        try:
            self.cp.read(fname)
        except ParsingError as pe:
            ldslog.error('{0} file corrupt. Please correct the error; {1} OR delete and rebuild'.format(fname,str(pe)))
            raise
    
    @override(LayerReader)
    def findLayerIdByName(self,name):
        '''Reverse lookup of section by associated name, finds first occurance only'''
        lid = filter(lambda n: name==self.cp.get(n,'name'),self.getLayerNames())
        return lid[0] if len(lid)>0 else None
    
    @override(LayerReader)
    def getLayerNames(self):
        '''Returns sections from properties file'''
        return self.cp.sections()
    
    @override(LayerReader)
    def readLayerProperty(self,layer,key):
        try:
            #HACK. Bad practise to rely on data type check
            if isinstance(layer,tuple) or isinstance(layer,list):
                value = () 
                for l in layer:
                    value += (LU.mightAsWellBeNone(self.cp.get(l, key)),)
            else:
                value = LU.mightAsWellBeNone(self.cp.get(layer, key))

        except:
            '''return a default value otherwise none which would also be a default for some keys'''
            #the logic here may be a bit suss, if the property is blank return none but if there is an error assume a default is needed?
            return {'pkey':'ID','name':layer,'geocolumn':'SHAPE'}.get(key)
        return value
    
    def _readSingleLayerProperty(self,layer,key):
        return LU.mightAsWellBeNone(self.cp.get(layer, key))
        
    
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
            with open(self.filename, 'w') as configfile:
                self.cp.write(configfile)
            #ldslog.debug("Check "+str(field)+" for layer "+str(layer)+" is set to "+str(value)+" : GetField="+self.cp.get(layer, field))                                                                                        
        except Exception as e:
            ldslog.warn('Problem writing LM date to layer config file. '+str(e))
            
    @override(LayerReader)
    def readLayerParameters(self,layer):
    #def readLayerSchemaConfig(self,layer):
        '''Full Layer config reader. Returns the config values for the whole layer or makes sensible guesses for defaults'''
        from LDSUtilities import LayerConfEntry
#        
#        try:
#            defn = self.cp.get(layer, 'sql')
#            #not advertised... if the user has gone to the trouble of defining their own schema in SQL just return that
#            return (defn,None,None,None,None,None,None,None,None)
#        except:
#            pass
        
        '''optional but one way to record the type and name of a column is to save a string tuple (name,type) and parse this at build time'''
        try:
            pkey = self.cp.get(layer, 'pkey')
        except NoOptionError:
            ldslog.warn("LayerSchema: No Primary Key Column defined, default to 'ID'")
            pkey = 'ID'
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            name = self.cp.get(layer, 'name')
        except NoOptionError:
            ldslog.warn("LayerSchema: No Name saved in config for this layer, returning ID")
            name = layer
            
        if name is None:
            name = layer
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            group = self.cp.get(layer, 'category')
        except NoOptionError:
            ldslog.warn("Group List: No Groups defined for this layer")
            group = None
            
            
        try:
            gcol = self.cp.get(layer, 'geocolumn')
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
            epsg = self.cp.get(layer, 'epsg')
        except NoOptionError:
            #print "No Projection Transformation defined"#don't really need to state the default occurance
            epsg = None
            
        try:
            lmod = self.cp.get(layer, 'lastmodified')
        except NoOptionError:
            ldslog.warn("LayerSchema: No Last-Modified date recorded, successful update will write current time here")
            lmod = None
            
        try:
            disc = self.cp.get(layer, 'discard')
        except NoOptionError:
            disc = None 
            
        try:
            cql = self.cp.get(layer, 'cql')
        except NoOptionError:
            cql = None
            
        return LayerConfEntry(pkey,name,group,gcol,epsg,lmod,disc,cql)
        
#--------------------------------------------------------------------------------------------------            
            
class LayerDSReader(LayerReader):
    '''
    Layer config wrapper for internal format config file.
    '''
    # Ported from DS location so some optimisation needed


    def __init__(self,fname):
        
        '''
        Constructor
        '''
        #in the DS context fname refers to a DS object
        super(LayerDSReader,self).__init__(fname)
        self.ds = self.fname.ds
        self.namelist = ()
            

    def exists(self):
        '''Test for DS table'''
        if self.ds is not None:
            try:
                if self.ds.GetLayer(self.fname.LDS_CONFIG_TABLE):
                    return True
            except RuntimeError as rte:
                if not re.search('No table/field definitions found for',str(rte)): 
                    ldslog.warn('Unable to open '+str(self.fname.LDS_CONFIG_TABLE))#raise
        return False
        
    def buildConfigLayer(self,res):
        '''Builds the config table into and using the active DS'''
        #TODO check initds for conf table name
        #if not hasattr(self.dso,'ds') or self.dso.ds is None:
        #    self.ds = self.dso.initDS(self.dso.destinationURI(DataStore.LDS_CONFIG_TABLE))  

        #First, try to delete any previous config
        try:
            self.ds.DeleteLayer(self.fname.LDS_CONFIG_TABLE)
        except Exception as e:
            ldslog.warn("Exception deleting config layer: "+str(e))
        #CreateLayer(self, char name, SpatialReference srs = None, OGRwkbGeometryType geom_type = wkbUnknown, char options = None) -> Layer
        config_layer = self.ds.CreateLayer(self.fname.LDS_CONFIG_TABLE, None, self.fname.selectValidGeom(ogr.wkbNone), ['OVERWRITE=YES'])
        if config_layer is None:
            ldslog.error("Cannot create lds config layer: " + self.fname.LDS_CONFIG_TABLE)
        
        feat_def = ogr.FeatureDefn()
        for name in self.fname.CONFIG_COLUMNS:
            #create new field defn with name=name and type OFTString
            fld_def = ogr.FieldDefn(name,ogr.OFTString)
            #in the feature defn, define a new field
            feat_def.AddFieldDefn(fld_def)
            #also add a field to the table definition, i.e. column
            if config_layer.TestCapability('CreateField'):
                config_layer.CreateField(fld_def,True)
              
        
        for row in json.loads(res):
            config_feat = ogr.Feature(feat_def)
            #HACK
            #if self.DRIVER_NAME == 'MSSQLSpatial':
            #    do something hack-y
            config_feat.SetField(self.fname.CONFIG_COLUMNS[0],str(row[0]))
            config_feat.SetField(self.fname.CONFIG_COLUMNS[1],str(row[1]))
            config_feat.SetField(self.fname.CONFIG_COLUMNS[2],str(row[2]))
            config_feat.SetField(self.fname.CONFIG_COLUMNS[3],str(','.join(row[3])))#keywords
            config_feat.SetField(self.fname.CONFIG_COLUMNS[4],str(row[4]))
            config_feat.SetField(self.fname.CONFIG_COLUMNS[5],str(row[5]))
            config_feat.SetField(self.fname.CONFIG_COLUMNS[6],str(row[6]))
            config_feat.SetField(self.fname.CONFIG_COLUMNS[7],str(row[7]))
            config_feat.SetField(self.fname.CONFIG_COLUMNS[8],None if row[8] is None else str(','.join(row[8])))
            config_feat.SetField(self.fname.CONFIG_COLUMNS[9],str(row[9]))
            
            config_layer.CreateFeature(config_feat)
            
        config_layer.ResetReading()
        config_layer.SyncToDisk()

    
    @override(LayerReader)
    def findLayerIdByName(self,lname):
        '''Reverse lookup of section by associated name, finds first occurance only'''
        layer = self.ds.GetLayer(self.fname.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = layer.GetNextFeature() 
        while feat is not None:
            if lname == feat.GetField('name'):
                return feat.GetField('id')
            feat = layer.GetNextFeature()
        return None
        
    @override(LayerReader)
    def getLayerNames(self):
        '''Returns configured layers for respective layer properties file'''
        #gdal.SetConfigOption('CPL_DEBUG','ON')
        #gdal.SetConfigOption('CPL_LOG_ERRORS','ON')
        if not self.namelist:
            layer = self.ds.GetLayer(self.fname.LDS_CONFIG_TABLE)
            layer.SetIgnoredFields(('OGR_GEOMETRY',))
            layer.ResetReading()
            feat = layer.GetNextFeature() 
            while feat is not None:
                self.namelist += (feat.GetField('id'),)
                feat = layer.GetNextFeature()
        
        return self.namelist
    
    @override(LayerReader) 
    def readLayerParameters(self,pkey):
        '''Full Layer config reader'''
        from DataStore import InaccessibleFeatureException

        layer = self.ds.GetLayer(self.fname.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = self.fname._findMatchingFeature(layer, 'id', pkey)
        if feat is None:
            InaccessibleFeatureException('Cannot access feature with id='+str(pkey)+' in layer '+str(layer.GetName()))
        return LU.extractFields(feat)
    
    @override(LayerReader)     
    def readLayerProperty(self,pkey,field):
        '''Single property reader'''
        plist = ()
        layer = self.ds.GetLayer(self.fname.LDS_CONFIG_TABLE)
        layer.ResetReading()
        if isinstance(pkey,tuple) or isinstance(pkey,list):
            for p in pkey:
                f = self.fname._findMatchingFeature(layer, 'id', p)
                plist += ('' if f is None else f.GetField(field),)
            return plist
        else:
            feat = self.fname._findMatchingFeature(layer, 'id', pkey)
            if feat is None:
                return None
            prop = feat.GetField(field)
        return None if LU.mightAsWellBeNone(prop) is None else prop
    
    @override(LayerReader)
    def writeLayerProperty(self,pkey,field,value):
        '''Write changes to layer config table. Keyword changes are written as a comma-seperated value '''
        #ogr.UseExceptions()
        try:
            layer = self.ds.GetLayer(self.fname.LDS_CONFIG_TABLE)
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
        feat = self.fname._findMatchingFeature(layer, 'id', p)
        feat.SetField(field,value)
        layer.SetFeature(feat)
        #ldslog.debug("Check "+field+" for layer "+pkey+" is set to "+value+" : GetField="+feat.GetField(field))
         
#------------------------------------------------------------------------------
        
                    
class GUIPrefsReader(object):
    '''
    Reader for GUI prefs. To save re inputting every time 
    '''

    PREFS_SEC = 'prefs'
    
    def __init__(self):
        '''
        Constructor
        '''
        thisdir = os.path.dirname(__file__)
        guiprefs = '../conf/gui.prefs'
        
        
        self.dvalue = None
        
        self.dselect = 'dest'
        #v:x111|MYGROUP, myconf.conf, 2193, 2013-01-01, 2013-01-02
        self.plist = ('lgvalue','uconf','epsg','fd','td')
        
        self.cp = ConfigParser()
        self.fn = os.path.join(thisdir,guiprefs)
        self.cp.read(self.fn)
        
    def read(self):
        '''Read stored DS value and return this and its matching params'''
        try:
            self.dvalue = self.cp.get(self.PREFS_SEC, self.dselect) 
            if LU.mightAsWellBeNone(self.dvalue) is None:
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
        if self.dvalue is not None:
            self.writesecline(self.dvalue,field,value)
        
    def writesecline(self,section,field,value):
        try:            
            self.cp.set(section,field,value)
            with open(self.fn, 'w') as configfile:
                self.cp.write(configfile)
            ldslog.debug(str(section)+':'+str(field)+'='+str(value))                                                                                        
        except Exception as e:
            ldslog.warn('Problem writing GUI prefs. '+str(e))
            
            
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
                if LU.mightAsWellBeNone(pr[1]) is not None:         
                    self.cp.set(self.dvalue,pr[0],pr[1])
                    with open(self.fn, 'w') as configfile:
                        self.cp.write(configfile)
                    ldslog.debug(str(self.dvalue)+':'+str(pr[0])+'='+str(pr[1]))                                                                                        
            except Exception as e:
                ldslog.warn('Problem writing GUI prefs. '+str(e))
                      
    
    def _initSection(self,section):
        checksec = LU.standardiseDriverNames(section)
        if checksec is not None:
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

