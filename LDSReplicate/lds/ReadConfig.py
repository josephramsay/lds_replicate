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
import string
import logging
import json
import ogr
import ConfigParser

from ConfigParser import NoOptionError,NoSectionError
from LDSUtilities import LDSUtilities as LU



    
ldslog = logging.getLogger('LDS')

class MainFileReader(object):
    '''
    Config file reader/writer
    '''
    LDSN = 'LDS'
    PROXY = 'Proxy'
    MISC = 'Misc'


    def __init__(self,cfpath,use_defaults):
        '''
        Constructor
        '''
        thisdir = os.path.dirname(__file__)
        
        self.cp = None
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
            path = "~"
            name = "LDSSLITE"
            config = "external"
        else:
            path = None
            name = None
            config = None

        
        try: 
            path = self.cp.get(SL.DRIVER_NAME, 'path')
        except NoOptionError:
            ldslog.warn("SpatiaLite: No path specified, default to Home directory, "+str(path))
        
        try:
            name = self.cp.get(SL.DRIVER_NAME, 'name')
        except NoOptionError:
            ldslog.warn("SpatiaLite: No DB name provided, default to "+str(name))
            
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
        
        return (path,name,config,epsg,cql)
    
    
    
    def readFileGDBConfig(self):
        '''FileGDB specific config file reader'''
        from FileGDBDataStore import FileGDBDataStore as FG
        
        epsg = None
        cql = None
        
        if self.use_defaults:
            path = "~"
            name = "LDSFGDB"
            config = "external"
        else:
            path = None
            name = None
            config = None

        
        try: 
            path = self.cp.get(FG.DRIVER_NAME, 'path')
        except NoOptionError:
            ldslog.warn("FileGDB: No path specified, default to Home directory, "+str(path))
            
        try:
            name = self.cp.get(FG.DRIVER_NAME, 'name')
        except NoOptionError:
            ldslog.warn("FileGDB: No DB name provided, default to "+str(name))
            
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
        
        return (path,name,config,epsg,cql)
    
    
    
    def readWFSConfig(self):
        '''Generic WFS config file reader'''
        '''Since this now keys on the driver name, WFS is read before LDS and LDS not at all, So...'''
        
        return self.readLDSConfig()
    
    
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
            url = self.cp.get(self.LDSN, 'url')
        except NoOptionError as noe:
            ldslog.warn("LDS: Default URL assumed "+str(noe))
        except NoSectionError as nse:
            ldslog.warn("LDS: No LDS Section... "+str(nse))
            
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
        else:
            auth = None
        
        try:
            host = self.cp.get(self.PROXY, 'host')
        except NoOptionError as noe:
            ldslog.warn("Proxy: No Proxy assumed "+str(noe))
        except NoSectionError as nse:
            ldslog.warn("Proxy: No Proxy assumed "+str(nse))
            
        try:
            port = self.cp.get(self.PROXY, 'port')
        except NoOptionError, NoSectionError:
            ldslog.warn("Proxy: No Proxy assumed")
            
        try:
            auth = self.cp.get(self.PROXY, 'auth')
        except NoOptionError, NoSectionError:
            ldslog.warn("Proxy: No Proxy assumed")

        try:
            usr = self.cp.get(self.PROXY, 'user')
        except NoOptionError, NoSectionError:
            ldslog.warn("Proxy: No Proxy assumed")        
        
        try:
            pwd = self.cp.get(self.PROXY, 'pass')
        except NoOptionError, NoSectionError:
            ldslog.warn("Proxy: No Proxy assumed") 
            

        
        return (host,port,auth,usr,pwd)
    
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
            self.cp.set(section,field,value if value is not None else '')
            with open(self.filename, 'w') as configfile:
                self.cp.write(configfile)
            ldslog.debug("Check "+str(field)+" for section "+str(section)+" is set to "+str(value)+" : GetField="+self.cp.get(section, field))                                                                                        
        except Exception as e:
            ldslog.warn('Problem writing to config file. '+str(e))
    
    # Functions above relate to connection config info
    #----------------------------------------------------------------------------------------------
    # Functions [4] below relate to layer config data
    
class LayerFileReader(object):
    
    def __init__(self,fname):
        '''
        Constructor
        '''

        self.cp = None
        self.filename = os.path.join(os.path.dirname(__file__), '../conf/',fname)
            
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
        lid = filter(lambda n: name==self.cp.get(n,'name'),self.getLayerNames())
        return lid[0] if len(lid)>0 else None
    
    
    def readLayerProperty(self,layer,key):
        try:
            value = self.cp.get(layer, key)
            if LU.mightAsWellBeNone(value) is None:
                return None
        except:
            '''return a default value otherwise none which would also be a default for some keys'''
            #the logic here may be a bit suss, if the property is blank return none but if there is an error assume a default is needed?
            return {'pkey':'ID','name':layer,'geocolumn':'SHAPE'}.get(key)
        return value
    
    def getLayerNames(self):
        '''Returns sections from properties file'''
        return self.cp.sections()
    
    def readLayerParameters(self,layer):
    #def readLayerSchemaConfig(self,layer):
        '''Full Layer config reader. Returns the config values for the whole layer or makes sensible guesses for defaults'''
        from LDSUtilities import LayerConfEntry
        
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
            
        try:
            index = self.cp.get(layer, 'index')
        except NoOptionError:
            ldslog.warn("LayerSchema: No Index Column/Specification defined, default to None")
            index = None
            
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
            
        return LayerConfEntry(pkey,name,group,gcol,index,epsg,lmod,disc,cql)

        
    def writeLayerProperty(self,layer,field,value):
        '''Write changes to layer config table'''
        try:            
            self.cp.set(layer,field,value if value is not None else '')
            with open(self.filename, 'w') as configfile:
                self.cp.write(configfile)
            ldslog.debug("Check "+str(field)+" for layer "+str(layer)+" is set to "+str(value)+" : GetField="+self.cp.get(layer, field))                                                                                        
        except Exception as e:
            ldslog.warn('Problem writing LM date to layer config file. '+str(e))


            
class LayerDSReader(object):
    '''
    Layer config wrapper for internal format config file.
    '''
    # Ported from DS location so some optimisation needed


    def __init__(self,dso):
        
        '''
        Constructor
        '''
        self.dso = dso
        self.ds = self.dso.ds
        self.namelist = ()
            

    def buildConfigLayer(self,config_array):
        '''Builds the config table into and using the active DS'''
        #TODO check initds for conf table name
        #if not hasattr(self.dso,'ds') or self.dso.ds is None:
        #    self.ds = self.dso.initDS(self.dso.destinationURI(DataStore.LDS_CONFIG_TABLE))  

        #First, try to delete any previous config
        try:
            self.ds.DeleteLayer(self.dso.LDS_CONFIG_TABLE)
        except Exception as e:
            ldslog.warn("Exception deleting config layer: "+str(e))
        
        config_layer = self.ds.CreateLayer(self.dso.LDS_CONFIG_TABLE,None,self.getConfigGeometry(),['OVERWRITE=YES'])
        
        feat_def = ogr.FeatureDefn()
        for name in self.dso.CONFIG_COLUMNS:
            #create new field defn with name=name and type OFTString
            fld_def = ogr.FieldDefn(name,ogr.OFTString)
            #in the feature defn, define a new field
            feat_def.AddFieldDefn(fld_def)
            #also add a field to the table definition, i.e. column
            config_layer.CreateField(fld_def,True)                
        
        for row in json.loads(config_array):
            config_feat = ogr.Feature(feat_def)
            #HACK
            #if self.DRIVER_NAME == 'MSSQLSpatial':
            #    do something hack-y
            config_feat.SetField(self.dso.CONFIG_COLUMNS[0],str(row[0]))
            config_feat.SetField(self.dso.CONFIG_COLUMNS[1],str(row[1]))
            config_feat.SetField(self.dso.CONFIG_COLUMNS[2],str(row[2]))
            config_feat.SetField(self.dso.CONFIG_COLUMNS[3],str(','.join(row[3])))
            config_feat.SetField(self.dso.CONFIG_COLUMNS[4],str(row[4]))
            config_feat.SetField(self.dso.CONFIG_COLUMNS[5],str(row[5]))
            config_feat.SetField(self.dso.CONFIG_COLUMNS[6],str(row[6]))
            config_feat.SetField(self.dso.CONFIG_COLUMNS[7],str(row[7]))
            config_feat.SetField(self.dso.CONFIG_COLUMNS[8],None if row[8] is None else str(','.join(row[8])))
            config_feat.SetField(self.dso.CONFIG_COLUMNS[9],str(row[9]))
            
            config_layer.CreateFeature(config_feat)
            
        config_layer.ResetReading()
        config_layer.SyncToDisk()

    
    def getConfigGeometry(self):
        return ogr.wkbNone
    
    def findLayerIdByName(self,lname):
        '''Reverse lookup of section by associated name, finds first occurance only'''
        layer = self.ds.GetLayer(self.dso.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = layer.GetNextFeature() 
        while feat is not None:
            if lname == feat.GetField('name'):
                return feat.GetField('id')
            feat = layer.GetNextFeature()
        return None
        

    def getLayerNames(self):
        '''Returns configured layers for respective layer properties file'''
        if not self.namelist:
            layer = self.ds.GetLayer(self.dso.LDS_CONFIG_TABLE)
            layer.ResetReading()
            feat = layer.GetNextFeature() 
            while feat is not None:
                self.namelist += (feat.GetField('id'),)
                feat = layer.GetNextFeature()
        
        return self.namelist
     
    def readLayerParameters(self,pkey):
        '''Full Layer config reader'''
        from DataStore import InaccessibleFeatureException

        layer = self.ds.GetLayer(self.dso.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = self.dso._findMatchingFeature(layer, 'id', pkey)
        if feat is None:
            InaccessibleFeatureException('Cannot access feature with id='+str(pkey)+' in layer '+str(layer.GetName()))
        return LU.extractFields(feat)
         
    def readLayerProperty(self,pkey,field):
        '''Single property reader'''
        layer = self.ds.GetLayer(self.dso.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = self.dso._findMatchingFeature(layer, 'id', pkey)
        if feat is None:
            return None
        prop = feat.GetField(field)
        return None if LU.mightAsWellBeNone(prop) is None else prop

    def writeLayerProperty(self,pkey,field,value):
        '''Write changes to layer config table'''
        #ogr.UseExceptions()
        try:
            layer = self.ds.GetLayer(self.dso.LDS_CONFIG_TABLE)
            feat = self.dso._findMatchingFeature(layer, 'id', pkey)
            feat.SetField(field,value)
            layer.SetFeature(feat)
            ldslog.debug("Check "+field+" for layer "+pkey+" is set to "+value+" : GetField="+feat.GetField(field))
        except Exception as e:
            ldslog.error(e)
            
         
         
            
class GUIPrefsReader(object):
    '''
    Reader for GUI prefs. To save re inputting every time 
    '''

    def __init__(self):
        '''
        Constructor
        '''
        thisdir = os.path.dirname(__file__)
        guiprefs = '../conf/gui.prefs'
        
        self.plist = ('dest','layer','uconf','group','epsg','fd','td','int')
        
        self.cp = ConfigParser.ConfigParser()
        self.fn = os.path.join(thisdir,guiprefs)
        self.cp.read(self.fn)
        
    def read(self):
        #options likely to be stored, does not include clean and init since these are destructive
        #and mean't to be one time use only
        
        rlist = ()
        
        for p in self.plist:
            try:
                rlist += (self.cp.get('prefs', p),)
            except NoOptionError as noe:
                ldslog.warn('Error getting GUI pref, '+p+' :: '+noe)
        return rlist
    
    
    def write(self,rlist):
        for pr in zip(self.plist,rlist):
            try:            
                self.cp.set('prefs',pr[0],pr[1])
                with open(self.fn, 'w') as configfile:
                    self.cp.write(configfile)
                ldslog.debug(str(pr[0])+'='+str(pr[1]))                                                                                        
            except Exception as e:
                ldslog.warn('Problem writing GUI prefs. '+str(e))

