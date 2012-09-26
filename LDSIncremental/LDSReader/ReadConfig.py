'''
v.0.0.1

LDSIncremental -  LDS Incremental Utilities

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 24/07/2012

@author: jramsay
'''

from ConfigParser import NoOptionError,NoSectionError,Error
import ConfigParser, sys, os

class Reader(object):
    '''
    reads a config file of user editable settings for PostgreSQL,FileGDB,ArcSDE,MySQL,MSSQL,DB2,Informix,Oracle
    '''


    def __init__(self,params):
        '''
        Constructor
        '''
        thisdir = os.path.dirname(__file__)

        self.fname=os.path.join(thisdir,params)
            
        self._readConfigFile(self.fname)
        
        
    def _readConfigFile(self,fname):
        '''split off so you can override the config file on the same reader object if needed'''
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

        host = self.cp.get('PostgreSQL', 'host')
        port = self.cp.get('PostgreSQL', 'port')
        dbname = self.cp.get('PostgreSQL', 'dbname')
        schema = self.cp.get('PostgreSQL', 'schema')
        usr = self.cp.get('PostgreSQL', 'user')
        pwd = self.cp.get('PostgreSQL', 'pass')
        
        try:
            over = self.cp.get('PostgreSQL', 'overwrite')
        except NoOptionError:
            print "Overwrite not specified, Setting to True"
            over = True
        
        return (host,port,dbname,schema,usr,pwd,over)
    
    def readMSSQLConfig(self):

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
        try:
            prefix = self.cp.get('File', 'prefix')
        except NoOptionError:
            prefix = None
        
        try: 
            path = self.cp.get('File', 'path')
        except NoOptionError:
            print "No path specified, default to Home directory"
            path = "~"
        
        return (prefix,path)
    
    def readFileGDBConfig(self):
        
        try: 
            path = self.cp.get('FileGDB', 'path')
        except NoOptionError:
            print "No path specified, default to Home directory"
            path = "~"
        
        return path
    
    
    #web
    
    def readWFSConfig(self):
        url = self.cp.get('WFS', 'url') 
        key = self.cp.get('WFS', 'key') 
        svc = self.cp.get('WFS', 'svc') 
        ver = self.cp.get('WFS', 'ver')
        fmt = self.cp.get('WFS', 'fmt')
        cql = self.cp.get('WFS', 'cql')
        
        
        return (url,key,svc,ver,fmt,cql)    
    
    def readLDSConfig(self):
        try:
            url = self.cp.get('LDS', 'url')
        except NoOptionError:
            url = "http://wfs.data.linz.govt/"
        except NoSectionError:
            print "No LDS Section... Cannot recover, quitting"
            sys.exit(1)
            
        try: 
            fmt = self.cp.get('LDS', 'fmt')
        except NoOptionError:
            print "No output format specified, default to GML2"
            fmt = "GML2"
        
        try: 
            svc = self.cp.get('LDS', 'svc')
        except NoOptionError:
            print "No service type specified, default to WFS"
            svc = "WFS"
        
        try: 
            ver = self.cp.get('LDS', 'ver')
        except NoOptionError:
            print "No Version specified, assuming WFS and default to version 1.0.0"
            ver = "1.0.0"
        
        key = self.cp.get('LDS', 'key') 
        
        return (url,key,svc,ver,fmt)
    
    def readProxyConfig(self):
        host = self.cp.get('Proxy', 'host') 
        port = self.cp.get('Proxy', 'port') 
        usr = self.cp.get('Proxy', 'user') 
        pwd = self.cp.get('Proxy', 'pass')
        
        return (host,port,usr,pwd)
    
    
    
    #----------------------------------------------------------------------------------------------
    
    def findLayerIdByName(self,name):
        '''reverse lookup of section by associated name, finds first occurance only'''
        for section_name in self.cp.sections():
            if name == self.cp.get(section_name,'name'):
                return section_name
        return name
    
    def readLayerSchemaConfig(self,layer):
        '''assuming that when the reader for this method call is instantiated it will point at some non-def 
        config file might be expecting too much, so uncomment below if needed'''
        
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
            print "No Primary Key Column defined, default to 'ID'"
            pkey = '(ID, Integer)'
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            name = self.cp.get(layer, 'name')
        except NoOptionError:
            print "No Name saved in config for this layer, returning ID"
            name = layer
            
        if name is None:
            name = layer
            
        try:
            gcol = self.cp.get(layer, 'geocolumn')
        except NoOptionError:
            print "No Geo Column defined, default to 'SHAPE'"
            gcol = 'SHAPE'
            
        try:
            epsg = self.cp.get(layer, 'epsg')
        except NoOptionError:
            #print "No Projection Transformation defined"#don't really need to state the default occurance
            epsg = None
            
        try:
            lmod = self.cp.get(layer, 'lastmodified')
        except NoOptionError:
            print "No Last-Modified date recorded, successful update will write current time here"
            lmod = None
            
        try:
            disc = self.cp.get(layer, 'discard')
        except NoOptionError:
            disc = None 
            
        try:
            cql = self.cp.get(layer, 'cql')
        except NoOptionError:
            cql = None
            
        return (pkey,name,gcol,epsg,lmod,disc,cql)
    
    
    
    def writeLayerSchemaConfig(self,layer,lmod):
        '''write changes to config file remembering the config file was chosen in the object call. '''
        try:
            self.cp.set(layer,'lastmodified',lmod)
        except Error:
            print "Last-Modified date not saved!"
        
        with open(self.fname,'wb') as conffile:
            self.cp.write(conffile)
        
        
             
        
        
        
        
        
        
    