'''
v.0.0.1

LDSIncremental -  ReadConfig

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 24/07/2012

@author: jramsay
'''
import sys
import os
import logging
import ConfigParser

from ConfigParser import NoOptionError,NoSectionError,Error

ldslog = logging.Logger("LDS.ReadConfig")

class Reader(object):
    '''
    Config file reader/writer
    '''


    def __init__(self,params):
        '''
        Constructor
        '''
        thisdir = os.path.dirname(__file__)

        self.fname=os.path.join(thisdir,params)
            
        self._readConfigFile(self.fname)
        
        
    def getSections(self):
        '''List of sections (layernames/datasources)'''
        return self.cp.sections()
        
    def _readConfigFile(self,fname):
        '''Reads named config file'''
        #Split off so you can override the config file on the same reader object if needed
        self.cp = ConfigParser.ConfigParser()
        self.cp.read(fname)
                
#    #incremental
#    def readIncrementalConfig(self):
#        try:
#            layer = self.cp.get('Incremental', 'layer')
#        except NoSectionError:
#            #no section will be caught for whole read
#            print "Missing Section [Incremental] in config file, setting all to default"
#            return ("All",None,None,None)
#        except NoOptionError:
#            print "No layer specified, default to All"
#            layer = "All"
#            
#        try:
#            fdate = self.cp.get('Incremental', 'from')
#            tdate = self.cp.get('Incremental', 'to')
#        except NoOptionError:
#            print "One or more missing dates, default to Full Replicate"
#            fdate = None
#            tdate = None
#        
#        try:
#            cql = self.cp.get('Incremental', 'cql')
#        except NoOptionError:
#            #silently discard, not implemented
#            cql = None
#        
#        return (layer,fdate,tdate,cql)
    
    #database
        
    def readPostgreSQLConfig(self):
        '''PostgreSQL specific config file reader'''
        host = self.cp.get('PostgreSQL', 'host')
        port = self.cp.get('PostgreSQL', 'port')
        dbname = self.cp.get('PostgreSQL', 'dbname')
        schema = self.cp.get('PostgreSQL', 'schema')
        usr = self.cp.get('PostgreSQL', 'user')
        pwd = self.cp.get('PostgreSQL', 'pass')
        
        try:
            over = self.cp.get('PostgreSQL', 'overwrite')
        except NoOptionError:
            ldslog.debug("PG: Overwrite not specified, Setting to True")
            over = True
        
        return (host,port,dbname,schema,usr,pwd,over)
    
    def readMSSQLConfig(self):
        '''MSSQL specific config file reader'''
        odbc = self.cp.get('MSSQL', 'odbc')
        server = self.cp.get('MSSQL', 'server')
        dsn = self.cp.get('MSSQL', 'dsn')
        trust = self.cp.get('MSSQL', 'trust')
        dbname = self.cp.get('MSSQL', 'dbname')
        schema = self.cp.get('MSSQL', 'schema')
        
        usr = self.cp.get('MSSQL', 'user')
        pwd = self.cp.get('MSSQL', 'pass')
        
        return (odbc,server,dsn,trust,dbname,schema,usr,pwd)
    
    def readSpatiaLiteConfig(self):
        '''SpatiaLite specific config file reader'''
        path = self.cp.get('SpatiaLite', 'path')
        
        return path
    
    
#    def readOracleConfig(self):
#
#        instance = self.cp.get('Oracle', 'instance')
#        usr = self.cp.get('Oracle', 'user')
#        pwd = self.cp.get('Oracle', 'pass')
#        
#        return (instance,usr,pwd)
#        
#        
#    def readMySQLConfig(self):
#
#        host = self.cp.get('MySQL', 'host')
#        port = self.cp.get('MySQL', 'port')
#        dbname = self.cp.get('MySQL', 'dbname')
#        usr = self.cp.get('MySQL', 'user')
#        pwd = self.cp.get('MySQL', 'pass')
#        
#        return (host,port,dbname,usr,pwd)
#    
#    def readArcSDEConfig(self):
#
#        server = self.cp.get('ArcSDE', 'server')
#        instance = self.cp.get('ArcSDE', 'instance')
#        database = self.cp.get('ArcSDE', 'database')
#        username = self.cp.get('ArcSDE', 'user')
#        password = self.cp.get('ArcSDE', 'pass')
#        
#        return (server, instance, database, username, password) 
    
    #file    
    
    def readFileConfig(self):
        '''Generic File-storage-type config file reader, could be used for FileGDB, SpatiaLite, Mapinfo, Shapefile since these only need a filename'''
        try:
            prefix = self.cp.get('File', 'prefix')
        except NoOptionError:
            prefix = None
        
        try: 
            path = self.cp.get('File', 'path')
        except NoOptionError:
            ldslog.debug("File: No path specified, default to Home directory")
            path = "~"
        
        return (prefix,path)
    
    def readFileGDBConfig(self):
        '''FileGDB specific config file reader'''
        try: 
            path = self.cp.get('FileGDB', 'path')
        except NoOptionError:
            ldslog.debug("FileGDB: No path specified, default to Home directory")
            path = "~"
        
        return path
    
    
    #web
    
    def readWFSConfig(self):
        '''Generic WFS specific config file reader'''
        url = self.cp.get('WFS', 'url') 
        key = self.cp.get('WFS', 'key') 
        svc = self.cp.get('WFS', 'svc') 
        ver = self.cp.get('WFS', 'ver')
        fmt = self.cp.get('WFS', 'fmt')
        cql = self.cp.get('WFS', 'cql')
        
        
        return (url,key,svc,ver,fmt,cql)    
    
    def readLDSConfig(self):
        '''LDs specific config file reader'''
        try:
            url = self.cp.get('LDS', 'url')
        except NoOptionError:
            url = "http://wfs.data.linz.govt/"
        except NoSectionError:
            ldslog.debug("LDS: No LDS Section... Cannot recover, quitting")
            sys.exit(1)
            
        try: 
            fmt = self.cp.get('LDS', 'fmt')
        except NoOptionError:
            ldslog.debug("LDS: No output format specified, default to GML2")
            fmt = "GML2"
        
        try: 
            svc = self.cp.get('LDS', 'svc')
        except NoOptionError:
            ldslog.debug("LDS: No service type specified, default to WFS")
            svc = "WFS"
        
        try: 
            ver = self.cp.get('LDS', 'ver')
        except NoOptionError:
            ldslog.debug("LDS: No Version specified, assuming WFS and default to version 1.0.0")
            ver = "1.0.0"
        
        key = self.cp.get('LDS', 'key') 
        
        return (url,key,svc,ver,fmt)
    
    def readProxyConfig(self):
        '''Proxy config reader is needed. Not really supported anymore'''
        host = self.cp.get('Proxy', 'host') 
        port = self.cp.get('Proxy', 'port') 
        usr = self.cp.get('Proxy', 'user') 
        pwd = self.cp.get('Proxy', 'pass')
        
        return (host,port,usr,pwd)
    
    
    
    #----------------------------------------------------------------------------------------------
    
    def findLayerIdByName(self,name):
        '''Reverse lookup of section by associated name, finds first occurance only'''
        for section_name in self.cp.sections():
            if name == self.cp.get(section_name,'name'):
                return section_name
        return name
    
    def readLayerSchemaConfig(self,layer):
        '''Full Layer config reader'''
        try:
            defn = self.cp.get(layer, 'sql')
            #if the user has gone to the trouble of defining their own schema in SQL who are we to argue. assume pk and geo defined within
            return (defn,None,None)
        except:
            pass
        
        '''optional but one way to record the type and name of a column is to save a string tuple (name,type) and parse this at build time'''
        try:
            pkey = self.cp.get(layer, 'pkey')
        except NoOptionError:
            ldslog.debug("LayerSchema: No Primary Key Column defined, default to 'ID'")
            pkey = '(ID, Integer)'
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            name = self.cp.get(layer, 'name')
        except NoOptionError:
            ldslog.debug("LayerSchema: No Name saved in config for this layer, returning ID")
            name = layer
            
        if name is None:
            name = layer
            
        try:
            gcol = self.cp.get(layer, 'geocolumn')
        except NoOptionError:
            ldslog.debug("LayerSchema: No Geo Column defined, default to 'SHAPE'")
            gcol = 'SHAPE'
            
        try:
            index = self.cp.get(layer, 'index')
        except NoOptionError:
            ldslog.debug("LayerSchema: No Index Column/Specification defined, default to None")
            index = None
            
        try:
            epsg = self.cp.get(layer, 'epsg')
        except NoOptionError:
            #print "No Projection Transformation defined"#don't really need to state the default occurance
            epsg = None
            
        try:
            lmod = self.cp.get(layer, 'lastmodified')
        except NoOptionError:
            ldslog.debug("LayerSchema: No Last-Modified date recorded, successful update will write current time here")
            lmod = None
            
        try:
            disc = self.cp.get(layer, 'discard')
        except NoOptionError:
            disc = None 
            
        try:
            cql = self.cp.get(layer, 'cql')
        except NoOptionError:
            cql = None
            
        return (pkey,name,gcol,index,epsg,lmod,disc,cql)
    
    
    
    def writeLayerSchemaConfig(self,layer,lmod):
        '''Write changes to layer config file'''
        try:
            self.cp.set(layer,'lastmodified',lmod)
        except Error:
            ldslog.debug("LayerSchema(W): Last-Modified date not saved!")
        
        with open(self.fname,'wb') as conffile:
            self.cp.write(conffile)
        
        
             
        
        
        
        
        
        
    