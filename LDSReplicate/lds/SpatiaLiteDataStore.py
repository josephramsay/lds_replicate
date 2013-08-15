'''
v.0.0.9

LDSReplicate -  SpatiaLiteDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 9/08/2012

@author: jramsay
'''

import logging
import os
import ogr
import re


from lds.DataStore import DataStore
from lds.DataStore import MalformedConnectionString
from lds.LDSUtilities import LDSUtilities

ldslog = LDSUtilities.setupLogging()

class SpatiaLiteDataStore(DataStore):
    '''
    SpatiaLite  DataStore
    '''
    #TODO. Figure out why replications fail around the 1M mark with a "Failed Writing body" error
    
    DRIVER_NAME = DataStore.DRIVER_NAMES['sl']#"SQLite"
    
    #db create options
    SPATIALITE = 'yes'
    
    SQLITE_LIST_ALL_TABLES = 'YES'
    OGR_SQLITE_CACHE = 1024
    OGR_SQLITE_SYNCHRONOUS = 'ON' #by default
    OGR_SQLITE_PRAGMA = 'journal_mode=WAL' #only works for OGR>=2.0
    OGR_SQLITE_JOURNAL = 'WAL'
    
    DEFAULT_GCOL = 'GEOMETRY'
      
    def __init__(self,parent,conn_str=None,user_config=None):
        '''
        cons init driver
        '''

        super(SpatiaLiteDataStore,self).__init__(parent,conn_str,user_config)

        (self.fname,self.config,self.srs,self.cql) = self.params 
        #because sometimes ~ isnt translated to home
        self.fname = os.path.expanduser(self.fname)
        self.SUFFIX = ['.db','.sqlite','.sqlite3']
        
    def clone(self):
        clone = SpatiaLiteDataStore(self.parent,self.conn_str,None)
        clone.name = str(self.name)+'C'
        return clone
        
    def sourceURI(self,layer):
        '''URI method returns source file name'''
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        '''URI method returns destination file name'''
        return self._commonURI(layer)
        
    def validateConnStr(self,cs):
        '''SLITE basic checks. 1 correct file suffix. 2 the directory can be accessed'''
        #-d "/home/<username>/temp/spatialite/ldsincr.db
        if not hasattr(self,'SUFFIX') or not any([re.search(s+'$',cs,flags=re.IGNORECASE) for s in self.SUFFIX]):
            raise MalformedConnectionString('SpatiaLite file suffix must be one of '+self.SUFFIX)
        if not os.access(os.path.dirname(cs), os.W_OK):
            raise MalformedConnectionString('Data file path cannot be found')
        return cs
        
        
    def _commonURI(self,layer):
        '''Since SpatiaLite databases are self contained files this only needs to return a file path'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.validateConnStr(self.conn_str)
        #return self.file #+"SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE"
        file = self.fname+('' if re.search('|'.join(self.SUFFIX)+'$',self.fname,flags=re.IGNORECASE) else self.SUFFIX[0])
        prag = ';'+self.OGR_SQLITE_PRAGMA
        return file
    
        
    def getConfigOptions(self):
        '''SL config opts'''
        #DS options: METADATA, SPATIALITE, INIT_WITH_EPSG
        local_opts = ['SQLITE_LIST_ALL_TABLES='+self.SQLITE_LIST_ALL_TABLES]
        local_opts += ['OGR_SQLITE_CACHE='+str(self.OGR_SQLITE_CACHE)]
        local_opts += ['OGR_SQLITE_SYNCHRONOUS='+str(self.OGR_SQLITE_SYNCHRONOUS)]
        #local_opts += ['OGR_SQLITE_PRAGMA='+str(self.OGR_SQLITE_PRAGMA)]
        local_opts += ['OGR_SQLITE_JOURNAL='+str(self.OGR_SQLITE_JOURNAL)]
        
        return super(SpatiaLiteDataStore,self).getConfigOptions() + local_opts
    
    def getDBOptions(self):
        '''Need to set DB options for SL to initialise geo_cols correctly'''
        local_opts = ['SPATIALITE=yes']
        return super(SpatiaLiteDataStore,self).getDBOptions() + local_opts
    
    def getLayerOptions(self,layer_id):
        '''SL layer'''
        #FORMAT=WKB/WKT/SPATIALITE, LAUNDER, SPATIAL_INDEX, COMPRESS_GEOM, SRID, COMPRESS_COLUMNS
        
        self.sl_local_opts = []
        
        return super(SpatiaLiteDataStore,self).getLayerOptions(layer_id) + self.sl_local_opts
        
#     def initDS(self,dsn=None,create=True):
#         '''Seperate initDS to insert pragma command once DS is available. Depends on SL/GDAL version'''
#         #HACK
#         self.ds = super(SpatiaLiteDataStore,self).initDS(dsn,create)
#         #self.executePragma(self.OGR_SQLITE_PRAGMA)
#         #redundant to return DS and then reassign to self...
#         return self.ds
        
    def executePragma(self,pragma):
        '''Hack to turn WAL on when OGR version<2.0'''
        rv = self.executeSQL('PRAGMA '+str(pragma))
        print rv.GetName()

    def buildIndex(self,lce,dst_layer_name):
        '''Builds an index creation string for a new full replicate in PG format'''
        tableonly = dst_layer_name.split('.')[-1]
        ALLOWS_CONSTRAINT_CREATION=False
        #SpatiaLite doesnt have a unique constraint but since we're using a pk might a well declare it as such
        if ALLOWS_CONSTRAINT_CREATION and LDSUtilities.mightAsWellBeNone(lce.pkey) is not None:
            #spatialite won't do post create constraint additions (could to a re-create?)
            cmd = 'ALTER TABLE {0} ADD PRIMARY KEY {1}_{2}_PK ({2})'.format(dst_layer_name,tableonly,lce.pkey)
            try:
                self.executeSQL(cmd)
                ldslog.info("Index = {}({}). Execute = {}".format(tableonly,lce.pkey,cmd))
            except RuntimeError as rte:
                if re.search('already exists', str(rte)): 
                    ldslog.warn(rte)
                else:
                    raise        
        
        #Unless we select SPATIAL_INDEX=no as a Layer option this should never be needed
        #because gcol is also used to determine whether a layer is spatial still do this check   
        if LDSUtilities.mightAsWellBeNone(lce.gcol) is not None and 'SPATIAL_INDEX=NO' in [opt.replace(' ','').upper() for opt in self.sl_local_opts]:
            cmd = "SELECT CreateSpatialIndex('{}','{}')".format(dst_layer_name,self.DEFAULT_GCOL)
            try:
                self.executeSQL(cmd)
                ldslog.info("Index = {}({}). Execute = {}. NB Cannot override Geo-Column Name.".format(tableonly,self.DEFAULT_GCOL,cmd))
            except RuntimeError as rte:
                if re.search('already exists', str(rte)): 
                    ldslog.warn(rte)
                else:
                    raise

    def changeColumnIntToString(self,table,column):
        '''SQLite column type changer. Used to change 64 bit integer columns to string. Brutal converter that deletes and recreates layer table'''
        '''No longer needed but the method is useful so retained for reference''' 
    
        #sql_mstr = "select sql from sqlite_master where name = '"+table+"'";
        sql_mstr = "select * from "+table
        sql_tabl = self.executeSQL(sql_mstr)
        feat_defn = sql_tabl.GetLayerDefn()
        #/////not supported by the driver
        #import ogr
        #sql_tabl.DeleteField(2)
        #sql_tabl.CreateField(ogr.FieldDefn(column))
        #\\\\\
        sql_build = "create table "+table+"(ogc_fid integer primary key," 
        for field_no in range(0,feat_defn.GetFieldCount()):
            field_def = feat_defn.GetFieldDefn(field_no)
            field_name = field_def.GetName()
            field_type = ogr.OFTString if field_name == column else field_def.GetType()
            sql_build += field_name+" "+self.convertToDestinationType(field_type)+","
        #"CREATE TABLE 'asp_name_associations' ( OGC_FID INTEGER PRIMARY KEY , 'id' INTEGER, 'name_1_sufi' INTEGER, 'name_2_sufi' INTEGER, 'alias' VARCHAR, 'status' VARCHAR, 'modified' VARCHAR)"
        sql_repl=sql_build[:-1]+")"
        #sql_repl = sql_tabl.replace("'"+column+"' INTEGER","'"+column+"' VARCHAR")
        sql_drop = "drop table "+table
        
        self.executeSQL(sql_drop)
        self.executeSQL(sql_repl)

    def _baseDeleteColumn(self,table,column):
        '''Basic column delete function for when regular deletes fail. Spatialite doesn't do column drops so we recreate instead'''
        #THIS DOESN'T GET CALLED ANYMORE AND CAN PROBABLY BE DELETED
        sql_mstr = "select * from "+table
        sql_tabl = self.executeSQL(sql_mstr)
        feat_defn = sql_tabl.GetLayerDefn()

        sql_build = "create table "+table+"(ogc_fid integer primary key," 
        for field_no in range(0,feat_defn.GetFieldCount()):
            field_def = feat_defn.GetFieldDefn(field_no)
            field_name = field_def.GetName()
            if field_name != column:
                sql_build += field_name+" "+self.convertToDestinationType(field_def.GetType())+","
        #"CREATE TABLE 'asp_name_associations' ( OGC_FID INTEGER PRIMARY KEY , 'id' INTEGER, 'name_1_sufi' INTEGER, 'name_2_sufi' INTEGER, 'alias' VARCHAR, 'status' VARCHAR, 'modified' VARCHAR)"
        sql_repl=sql_build[:-1]+")"
        #sql_repl = sql_tabl.replace("'"+column+"' INTEGER","'"+column+"' VARCHAR")
        sql_drop = "drop table "+table
        
        self.executeSQL(sql_drop)
        self.executeSQL(sql_repl)
        
        
        #TODO. Implement for all DS types
        sql_str = "alter table "+table+" drop column "+column
        return self.executeSQL(sql_str)
        

    def _baseDeleteLayer(self,table):
        '''Basic layer delete function intended for SpatiaLite includes geo_col syntax and idx tables'''
        GCOL = 'GEOMETRY'
        sql_tbd = "drop table "+table
        sql_gcd = "delete from geometry_columns where f_table_name = "+table
        sql_i1d = "drop table 'idx_{}_{}'".format(table,GCOL)
        sql_i2d = "drop table 'idx_{}_{}_node'".format(table,GCOL)
        sql_i3d = "drop table 'idx_{}_{}_parent'".format(table,GCOL)
        sql_i4d = "drop table 'idx_{}_{}_rowid'".format(table,GCOL)
        #sql_idx delete matching index?
        self.executeSQL(sql_tbd)    
        self.executeSQL(sql_gcd)   
        self.executeSQL(sql_i1d)   
        self.executeSQL(sql_i2d)   
        self.executeSQL(sql_i3d)   
        self.executeSQL(sql_i4d)   
        
        
        
    '''Spatialite has datatypes INT, INTEGER, SMALLINT, TINYINT, DEC, DECIMAL, LONGCHAR, LONGVARCHAR, DATETIME, SMALLDATETIME which are only
    remaned INTEGER, REAL, TEXT, BLOB and NULL. This converts and aggregates from gdal to these'''
    def convertToDestinationType(self,key):
        #NB not really needed anymore
        return {0: 'integer', 1: 'integer',
                2: 'real', 3: 'real',
                4: 'text', 5: 'text',
                8: 'byte',
                9: 'text', 10: 'text', 11: 'text'
         }.get(key,'text')  
         

    def versionCheck(self):
        '''SpatiaLite/SQLite version checker'''
        from VersionUtilities import VersionChecker,UnsupportedVersionException

        #gets the version number of the sqlite data file
        #slv_cmd = 'file '+str(self._commonURI(None))
        #slv_ver = VersionChecker.getVersionFromShell(slv_cmd,'SQLite (\d+\.\w+) database')
        
        #Gets the version of the sqlite application
        #slv_cmd = 'sqlite -version'
        #slv_ver = VersionChecker.getVersionFromShell(slv_cmd,'(\d+\.*\d*\.*\d*)')
        
        slv_cmd = 'select spatialite_version()'
        slv_ver = self.executeSQL(slv_cmd).GetFeature(0).GetField(0)
        
        
        if VersionChecker.compareVersions(VersionChecker.SpatiaLite_MIN, slv_ver if slv_ver is not None else VersionChecker.SpatiaLite_MIN):
            raise UnsupportedVersionException('SpatiaLite version '+str(slv_ver)+' does not meet required minumum '+str(VersionChecker.SpatiaLite_MIN))
        
        ldslog.info(self.DRIVER_NAME+' version '+str(slv_ver))
        return True
        