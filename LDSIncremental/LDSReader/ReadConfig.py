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

from ConfigParser import NoOptionError,NoSectionError

ldslog = logging.getLogger('LDS')

class MainFileReader(object):
    '''
    Config file reader/writer
    '''


    def __init__(self,cfpath,use_defaults):
        '''
        Constructor
        '''
        thisdir = os.path.dirname(__file__)

        self.use_defaults = use_defaults
        self.filename=os.path.join(thisdir,cfpath)
            
        self._readConfigFile(self.filename)
        
        
    def getSections(self):
        '''List of sections (layernames/datasources)'''
        return self.cp.sections()
        
    def _readConfigFile(self,fname):
        '''Reads named config file'''
        #Split off so you can override the config file on the same reader object if needed
        self.cp = ConfigParser.ConfigParser()
        self.cp.read(fname)

    
    #database
        
    def readPostgreSQLConfig(self):
        '''PostgreSQL specific config file reader'''            
        
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
            config = "internal"
        else:
            host = None
            port = None
            dbname = None
            schema = None
            config = None
        
        
        try:
            host = self.cp.get('PostgreSQL', 'host')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            port = self.cp.get('PostgreSQL', 'port')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            dbname = self.cp.get('PostgreSQL', 'dbname')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            schema = self.cp.get('PostgreSQL', 'schema')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            usr = self.cp.get('PostgreSQL', 'user')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            pwd = self.cp.get('PostgreSQL', 'pass')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        
        
        try:
            config = self.cp.get('PostgreSQL', 'config')
        except NoOptionError:
            ldslog.debug("PostgreSQL: No config preference specified, default to 'external'")
            config = 'external'
        
        try:
            over = self.cp.get('PostgreSQL', 'overwrite')
        except NoOptionError:
            ldslog.debug("PG: Overwrite not specified, Setting to True")
            over = True
            
        try:
            epsg = self.cp.get('PostgreSQL', 'epsg')
        except NoOptionError:
            ldslog.debug("PG: EPSG not specified, default to none keeping existing SRS")
            epsg = True
            
        try: 
            cql = self.cp.get('PostgreSQL', 'cql')
        except NoOptionError:
            ldslog.debug("PG: No CQL Filter specified, fetching all results")
            cql = None
        
        return (host,port,dbname,schema,usr,pwd,over,config,epsg,cql)
    
    
    
    def readMSSQLConfig(self):
        '''MSSQL specific config file reader'''
        
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
            config = "internal"
        else:
            odbc = None
            server = None
            dbname = None
            trust = None
            dsn = None
            config = None
        
        
        
        try:
            odbc = self.cp.get('MSSQL', 'odbc')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            server = self.cp.get('MSSQL', 'server')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            dsn = self.cp.get('MSSQL', 'dsn')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            trust = self.cp.get('MSSQL', 'trust')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            dbname = self.cp.get('MSSQL', 'dbname')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            schema = self.cp.get('MSSQL', 'schema')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            usr = self.cp.get('MSSQL', 'user')
        except NoOptionError as noe:
            ldslog.error(noe)
            
        try:
            pwd = self.cp.get('MSSQL', 'pass')
        except NoOptionError as noe:
            ldslog.error(noe)
        

        
        try:
            config = self.cp.get('MSSQL', 'config')
        except NoOptionError:
            ldslog.debug("MSSQL: No config preference specified, default to 'external'")
            config = 'internal'
            
        try:
            epsg = self.cp.get('MSSQL', 'epsg')
        except NoOptionError:
            ldslog.debug("MSSQL: EPSG not specified, default to None keeping existing SRS")
            
        try: 
            cql = self.cp.get('MSSQL', 'cql')
        except NoOptionError:
            ldslog.debug("MSSQL: No CQL Filter specified, fetching all results")
            cql = None
        
        return (odbc,server,dsn,trust,dbname,schema,usr,pwd,config,epsg,cql)
    
    def readSpatiaLiteConfig(self):
        '''SpatiaLite specific config file reader'''
        

        epsg = None
        cql = None
        
        if self.use_defaults:
            path = "~"
            name = "LDSSLITE"
            config = "internal"
        else:
            path = None
            name = None
            config = None

        
        try: 
            path = self.cp.get('SpatiaLite', 'path')
        except NoOptionError:
            ldslog.debug("SpatiaLite: No path specified, default to Home directory, "+str(path))
        
        try:
            name = self.cp.get('SpatiaLite', 'name')
        except NoOptionError:
            ldslog.debug("SpatiaLite: No DB name provided, default to "+str(name))
            
        try:
            config = self.cp.get('SpatiaLite', 'config')
        except NoOptionError:
            ldslog.debug("SpatiaLite: No config preference specified, default to "+str(config))
            config = 'internal'
            
        try:
            epsg = self.cp.get('SpatiaLite', 'epsg')
        except NoOptionError:
            ldslog.debug("SL: EPSG not specified, default to "+str(epsg)+" keeping existing SRS")
            
        try: 
            cql = self.cp.get('SpatiaLite', 'cql')
        except NoOptionError:
            ldslog.debug("SL: No CQL Filter specified, fetching all results")
        
        return (path,name,config,epsg,cql)
    
    
    
    def readFileGDBConfig(self):
        '''FileGDB specific config file reader'''
        epsg = None
        cql = None
        
        if self.use_defaults:
            path = "~"
            name = "LDSFGDB"
            config = "internal"
        else:
            path = None
            name = None
            config = None

        
        try: 
            path = self.cp.get('FileGDB', 'path')
        except NoOptionError:
            ldslog.debug("FileGDB: No path specified, default to Home directory, "+str(path))
            
        try:
            name = self.cp.get('FileGDB', 'name')
        except NoOptionError:
            ldslog.debug("FileGDB: No DB name provided, default to "+str(name))
            
        try:
            config = self.cp.get('FileGDB', 'config')
        except NoOptionError:
            ldslog.debug("FileGDB: No config preference specified, default to "+str(config))
            
        try:
            epsg = self.cp.get('FileGDB', 'epsg')
        except NoOptionError:
            ldslog.debug("FileGDB: EPSG not specified, default to "+str(epsg)+" none keeping existing SRS")
            
        try: 
            cql = self.cp.get('FileGDB', 'cql')
        except NoOptionError:
            ldslog.debug("FileGDB: No CQL Filter specified, fetching all results")
        
        return (path,name,config,epsg,cql)
    
    
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

    
    
    #web
    
    def readWFSConfig(self):
        '''Generic WFS config file reader'''
        '''Since this now keys on the driver name, WFS is read before LDS and LDS not at all, So...'''
        
        return self.readLDSConfig()
    
#        url = self.cp.get('WFS', 'url') 
#        key = self.cp.get('WFS', 'key') 
#        svc = self.cp.get('WFS', 'svc') 
#        ver = self.cp.get('WFS', 'ver')
#        fmt = self.cp.get('WFS', 'fmt')
#        cql = self.cp.get('WFS', 'cql')    
#        
#        return (url,key,svc,ver,fmt,cql)    
    
    def readLDSConfig(self):
        '''LDs specific config file reader'''
        
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
            url = self.cp.get('LDS', 'url')
        except NoOptionError as noe:
            ldslog.debug("LDS: Default URL assumed "+str(noe))
        except NoSectionError as nse:
            ldslog.debug("LDS: No LDS Section... "+str(nse))
            
        try:   
            key = self.cp.get('LDS', 'key') 
        except NoOptionError, NoSectionError:
            ldslog.debug("LDS: Key required to connect to LDS...")
            
        try: 
            fmt = self.cp.get('LDS', 'fmt')
        except NoOptionError:
            ldslog.debug("LDS: No output format specified")
        
        try: 
            svc = self.cp.get('LDS', 'svc')
        except NoOptionError:
            ldslog.debug("LDS: No service type specified, default to "+str(svc))
        
        try: 
            ver = self.cp.get('LDS', 'ver')
        except NoOptionError:
            ldslog.debug("LDS: No Version specified, assuming WFS and default to version "+str(ver))        
            
        try: 
            cql = self.cp.get('LDS', 'cql')
        except NoOptionError:
            ldslog.debug("LDS: No CQL Filter specified, fetching all results")
            

        
        return (url,key,svc,ver,fmt,cql)
    
    
    # Functions above relate to connection config info
    #----------------------------------------------------------------------------------------------
    # Functions [4] below relate to layer config data
    
class LayerFileReader(object):
    
    def __init__(self,fname):
        '''
        Constructor
        '''

        self.filename=os.path.join(os.path.dirname(__file__), '../',fname)
            
        self._readConfigFile(self.filename)
        
        
    def _readConfigFile(self,fname):
        '''Reads named config file'''
        #Split off so you can override the config file on the same reader object if needed
        self.cp = ConfigParser.ConfigParser()
        self.cp.read(fname)
        
        
    def getSections(self):
        '''List of sections (layernames/datasources)'''
        return self.cp.sections()    
    
    def findLayerIdByName(self,name):
        '''Reverse lookup of section by associated name, finds first occurance only'''
        for section_name in self.cp.sections():
            if name == self.cp.get(section_name,'name'):
                return section_name
        return name
    
    def readLayerProperty(self,layer,key):
        try:
            value = self.cp.get(layer, key)
        except:
            '''return a default value otherwise none which would also be a default for some keys'''
            return {'pkey':'ID','name':layer,'geocolumn':'SHAPE'}.get(key)
        return value
    

    def getLayerNames(self):
        '''Returns sections from properties file'''
        return self.cp.sections()
    
    def readLayerParameters(self,layer):
    #def readLayerSchemaConfig(self,layer):
        '''Full Layer config reader. Returns the config values for the whole layer or makes sensible guesses for defaults'''
        
        effunc = self.cp.get
        
        try:
            defn = self.cp.get(layer, 'sql')
            #if the user has gone to the trouble of defining their own schema in SQL just return that
            return (defn,None,None,None,None,None,None,None,None)
        except:
            pass
        
        '''optional but one way to record the type and name of a column is to save a string tuple (name,type) and parse this at build time'''
        try:
            pkey = self.cp.get(layer, 'pkey')
        except NoOptionError:
            ldslog.debug("LayerSchema: No Primary Key Column defined, default to 'ID'")
            pkey = 'ID'
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            name = self.cp.get(layer, 'name')
        except NoOptionError:
            ldslog.debug("LayerSchema: No Name saved in config for this layer, returning ID")
            name = layer
            
        if name is None:
            name = layer
            
        '''names are/can-be stored so we can reverse search by layer name'''
        try:
            group = self.cp.get(layer, 'category')
        except NoOptionError:
            ldslog.debug("Group List: No Groups defined for this layer")
            group = None
            
            
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
            
        return (pkey,name,group,gcol,index,epsg,lmod,disc,cql)

        
    def writeLayerProperty(self,layer,field,value):
        '''Write changes to layer config table'''
        try:            
            self.cp.set(layer,field,value if value is not None else '')
            with open(self.filename, 'w') as configfile:
                self.cp.write(configfile)
            ldslog.debug("Check "+str(field)+" for layer "+str(layer)+" is set to "+str(value)+" : GetField="+self.cp.get(layer, field))                                                                                        
        except Exception as e:
            ldslog.error('Problem writing LM date to layer config file. '+str(e))


# ----------------------------------------------------------------------------------------------------------------------------
# File Reader /\ Table Reader \/
# ----------------------------------------------------------------------------------------------------------------------------
#             
# NOTES. If syntax between different destinations RT will need to be moved/rebuilt for each DST case. If there is any crossover 
# can still subclass ReadConfig but write Module in DST connector file
#        
#        
#class LayerTableReader(object):
#    '''
#    Config file reader/writer for internal storage subclasses normal reader to use file reading capabilities
#    but overrides the layer data functions
#    '''
#
#
#    def __init__(self,parent):
#        '''
#        Constructor
#        '''
#        import DataStore
#        self._setConfigFile(parent,DataStore.LDS_CONFIG_TABLE)
#
#        
#    def _setConfigFile(self,parent,tablename):
#        '''Reads named config location'''
#        #Split off so you can override the config file on the same reader object if needed
#        self.ds = parent
#        self.tablename = tablename
#
#        
#    def getSections(self):
#        '''List of sections (layernames/datasources)'''
#        result = self.ds.executeSQL('SELECT ID FROM {}'.format(self.tablename))
#        lid = []
#        row = result.GetNextFeature()
#        while row is not None:
#            try:
#                lid.append(row.GetField('ID'))
#                row = result.GetNextFeature()
#            except:
#                ldslog.debug("ID column not found")
#        return lid
#    
#    def findLayerIdByName(self,name):
#        '''Lookup of id (section header in CP) by associated name, finds first occurance only'''
#        result = self.ds.executeSQL('SELECT ID FROM {} WHERE NAME = {}'.format(self.tablename,name))
#        row = result.GetNextFeature()
#        try:
#            lid =  row.GetField('ID')
#        except:
#            ldslog.debug("LayerSchema: No matching ID column found")
#        return lid
#
#
#    def readLayerProperty(self,layer,key):
#        
#        result = self.ds.executeSQL("SELECT {} FROM {} WHERE ID = '{}'".format(key,self.tablename,layer))
#        row = result.GetNextFeature()
#        try:
#            value = row.GetField(key)
#        except:
#            '''return a default value otherwise none which would also be a default for some keys'''
#            return {'pkey':'ID','name':layer,'geocolumn':'SHAPE'}.get(key)
#        return value
#    
#    
#    def readLayerSchemaConfig(self,layer):
#        '''Full Layer config reader'''
#        result = self.ds.executeSQL("SELECT * FROM {} WHERE ID = '{}'".format(self.tablename,layer))
#        
#        #Assume layer id is unique in the config table... it had better be
#        row = result.GetNextFeature()
#        
#        return LDSUtilities.extractFields(row)
#        
##        try:
##            pkey =  row.GetField('PKEY')
##        except:
##            ldslog.debug("LayerSchema: No Primary Key Column defined, default to 'ID'")
##            pkey = 'ID'
##            
##        '''names are/can-be stored so we can reverse search by layer name'''
##        try:
##            name = row.GetField('NAME')
##        except:
##            ldslog.debug("LayerSchema: No Name saved in config for this layer, returning ID")
##            name = layer
##            
##        if name is None:
##            name = layer
##            
##        '''names are/can-be stored so we can reverse search by layer name'''
##        try:
##            group = row.GetField('CATEGORY')
##        except:
##            ldslog.debug("Group List: No Groups defined for this layer")
##            group = None
##            
##            
##        try:
##            gcol = row.GetField('GEOCOLUMN')
##        except:
##            ldslog.debug("LayerSchema: No Geo Column defined, default to 'SHAPE'")
##            gcol = 'SHAPE'
##            
##        try:
##            index = row.GetField('INDEX')
##        except:
##            ldslog.debug("LayerSchema: No Index Column/Specification defined, default to None")
##            index = None
##            
##        try:
##            epsg = row.GetField('EPSG')
##        except:
##            #print "No Projection Transformation defined"#don't really need to state the default occurance
##            epsg = None
##            
##        try:
##            lmod = row.GetField('LASTMODIFIED')
##        except:
##            ldslog.debug("LayerSchema: No Last-Modified date recorded, successful update will write current time here")
##            lmod = None
##            
##        try:
##            disc = row.GetField('DISCARD')
##        except:
##            disc = None 
##            
##        try:
##            cql = row.GetField('CQL')
##        except:
##            cql = None
##            
##        return (pkey,name,group,gcol,index,epsg,lmod,disc,cql)
#    
#    
#    
#    def writeLayerSchemaConfig(self,layer,lmod):
#        '''Write changes to layer config file'''
#        try:
#            self.ds.executeSQL('UPDATE {} SET LASTMODIFIED = {} WHERE ID = {}'.format(self.tablename,lmod,layer))
#        except:
#            ldslog.debug("LayerSchema(W): Last-Modified date not saved!")
        
        

        
        
    