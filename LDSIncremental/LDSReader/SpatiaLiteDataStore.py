'''
v.0.0.1

LDSIncremental -  SpatiaLiteDataStore

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
import gdal
import re

from DataStore import DataStore
from DataStore import MalformedConnectionString

ldslog = logging.getLogger('LDS')

class SpatiaLiteDataStore(DataStore):
    '''
    SpatiaLite  DataStore
    '''
    
    DRIVER_NAME = "SQLite"
    SQLITE_LIST_ALL_TABLES = 'YES'
      
    def __init__(self,conn_str=None,user_config=None):
        '''
        cons init driver
        '''

        super(SpatiaLiteDataStore,self).__init__(conn_str,user_config)
        
        gdal.SetConfigOption('SQLITE_LIST_ALL_TABLES',self.SQLITE_LIST_ALL_TABLES)

        (self.path,self.name,self.config,self.srs,self.cql) = self.params 
        #because sometimes ~ isnt translated to home
        self.path = os.path.expanduser(self.path)
        self.SUFFIX = '.db'

        
    def sourceURI(self,layer):
        '''URI method returns source file name'''
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        '''URI method returns destination file name'''
        return self._commonURI(layer)
        
    def validateConnStr(self,cs):
        '''SLITE basic checks. 1 correct file suffix. 2 the directory can be accessed'''
        #-d "/home/<username>/temp/spatialite/ldsincr.db
        if not hasattr(self,'SUFFIX') or not re.search(self.SUFFIX+'$',cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('SpatiaLite file suffix must be '+self.SUFFIX)
        if not os.access(os.path.dirname(cs), os.W_OK):
            raise MalformedConnectionString('Data file path cannot be found')
        return cs
        
        
    def _commonURI(self,layer):
        '''Since SpatiaLite databases are self contained files this only needs to return a file path'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.validateConnStr(self.conn_str)
        #return self.file #+"SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE"
        return os.path.join(self.path,self.name+self.SUFFIX)
    

    def getOptions(self,layer_id):
        '''add PG options for SCHEMA and GEO_NAME'''
        local_opts = []
        gname = self.layerconf.readLayerProperty(layer_id,'geocolumn')
        
        #TODO Figure out how to set geom_name for SL... this wont do it
        if gname is not None:
            local_opts += ['GEOMETRY_NAME='+gname]
            
        #Not really needed since default is YES
        #if index == 'spatial or index == 's' or re.match(index,gname.lower()):
        #    local_opts += ['SPATIAL_INDEX=YES']
        
        return super(SpatiaLiteDataStore,self).getOptions(layer_id) + local_opts
        

    def buildIndex(self,lce,dst_layer_name):
        '''Default index string builder for new fully replicated layers'''
        ref_index = DataStore.parseStringList(lce.index)
        if ref_index.intersection(set(('spatial','s'))):
            ldslog.warn('Spatial indexing is only supported at layer creation and is enabled by default')
            return
        elif ref_index.intersection(set(('primary','pkey','p'))):
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+lce.pkey,dst_layer_name,lce.pkey)
        elif ref_index is not None:
            #maybe the user wants a non pk/spatial index? Try to filter the string
            clst = ','.join(ref_index)
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+DataStore.sanitise(clst),dst_layer_name,clst)
        else:
            return
        ldslog.info("Index="+','.join(ref_index)+". Execute "+cmd)
        
        try:
            self.executeSQL(cmd)
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
        
#    '''
#    returned by ogr.GetFieldTypeName(i)
#    0 Integer
#    1 IntegerList
#    2 Real
#    3 RealList
#    4 String
#    5 StringList
#    6 (unknown)
#    7 (unknown)
#    8 Binary
#    9 Date
#    10 Time
#    11 DateTime
#    12 (unknown) ...
#    '''
#    def convertToDestinationType(self,key):
#        return {0: 'integer', 1: 'integer',
#                2: 'double precision', 3: 'double precision',
#                4: 'character varying', 5: 'character varying',
#                8: 'byte',
#                9: 'date', 10: 'time', 11: 'timestamp'
#         }.get(key,'character varying') 

    '''Spatialite has datatypes INT, INTEGER, SMALLINT, TINYINT, DEC, DECIMAL, LONGCHAR, LONGVARCHAR, DATETIME, SMALLDATETIME which are only
    remaned INTEGER, REAL, TEXT, BLOB and NULL. This converts and aggregates from gdal to these'''
    def convertToDestinationType(self,key):
        return {0: 'integer', 1: 'integer',
                2: 'real', 3: 'real',
                4: 'text', 5: 'text',
                8: 'byte',
                9: 'text', 10: 'text', 11: 'text'
         }.get(key,'text')  
         

    def versionCheck(self):
        '''SpatiaLite/SQLite version checker'''
        from VersionChecker import VersionChecker,UnsupportedVersionException

        slv_cmd = 'file '+str(self._commonURI(None))
        
        slv_ver = VersionChecker.getVersionFromShell(slv_cmd,'SQLite (\d+\.\w+) database')
        
        if VersionChecker.compareVersions(VersionChecker.SpatiaLite_MIN, slv_ver if slv_ver is not None else VersionChecker.SpatiaLite_MIN):
            raise UnsupportedVersionException('SpatiaLite version '+str(slv_ver)+' does not meet required minumum '+str(VersionChecker.SpatiaLite_MIN))
        

        return True
        