'''
v.0.0.1

LDSReplicate -  PostgreSQLDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 9/08/2012

@author: jramsay
'''

import logging
import re

from lds.DataStore import DataStore, MalformedConnectionString, DatasourcePrivilegeException
from lds.LDSUtilities import LDSUtilities, Encrypt

ldslog = logging.getLogger('LDS')

class PostgreSQLDataStore(DataStore):
    '''
    PostgreSQL DataStore
    '''
    
    DRIVER_NAME = DataStore.DRIVER_NAMES['pg']#PostgreSQL"
    PG_USE_COPY = "NO"
    PG_USE_BASE64 = "YES"
    
    PGSQL_OGR_FID = "ID"  
    
    GEOM_TYPE = 'GEOMETRY'
    
    SPATIAL_INDEX = 'ON'
    
    def __init__(self,parent,conn_str=None,user_config=None):
        '''
        PostgreSQL DataStore constructor
        '''
        
        super(PostgreSQLDataStore,self).__init__(parent,conn_str,user_config)
              
        (self.host,self.port,self.dbname,self.schema,self.usr,self.pwd, self.overwrite,self.config,self.srs,self.cql) = self.params

        
    def sourceURI(self,layer):
        '''URI method returns source DB instance'''
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        '''URI method returns destination DB instance'''
        return self._commonURI(layer)
        
    def validateConnStr(self,cs):
        '''The PostgreSQL connection string must be something like PG:"dbname='databasename' host='addr' port='5432' user='x' password='y'" '''
        #-d PG:"dbname='ldsincr' host='127.0.0.1' port='5432' user='pguser' password='pgpass'"
        if not re.search('^PG:',cs,flags=re.IGNORECASE):
            '''TODO. We could append a PG here instead'''
            raise MalformedConnectionString('PostgreSQL declaration must begin with \'PG\'')
        if not re.search("dbname='\S+'",cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('\'dbname\' parameter required in PostgreSQL config string')
        if not re.search("host='\S+'",cs,flags=re.IGNORECASE):
            ldslog.warn('\'host\' parameter not provided in PostgreSQL config string')
            #raise MalformedConnectionString('\'host\' parameter required in PostgreSQL config string')
        if not re.search("port='\d+'",cs,flags=re.IGNORECASE):
            ldslog.warn('\'port\' parameter not provided in PostgreSQL config string')
            #raise MalformedConnectionString('\'port\' parameter required in PostgreSQL config string')
        #HACK. active schema, unlike the other PG parameters, cannot have single quotes! this silently removes them if they've been mistakenly added
        return re.sub(r"active_schema='(\S+)'",r"active_schema=\1",cs)

    @staticmethod
    def buildConnStr(host,port,dbname,schema,usr,pwd):
        cs = "PG:dbname='{2}' host='{0}' port='{1}' user='{3}' password='{4}'".format(host,port,dbname,usr,pwd)
        return cs if schema is None else cs+" active_schema="+str(schema)
            
    def _commonURI(self,layer):
        '''Refers to common connection instance for reading or writing'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.validateConnStr(self.conn_str)
        #can't put schema in quotes, causes error but without quotes tables get created in public anyway, still need schema.table syntax
        if LDSUtilities.mightAsWellBeNone(self.pwd) is not None:
            if self.pwd.startswith(Encrypt.ENC_PREFIX):
                pwd = " password='{}'".format(Encrypt.unSecure(self.pwd))
            else:
                pwd = " password='{}'".format(self.pwd)
        else:
            pwd = ""
        
        sch = " active_schema={}".format(self.schema) if LDSUtilities.mightAsWellBeNone(self.schema) is not None else ""
        usr = " user='{}'".format(self.usr) if LDSUtilities.mightAsWellBeNone(self.usr) is not None else ""
        hst = " host='{}'".format(self.host) if LDSUtilities.mightAsWellBeNone(self.host) is not None else ""
        prt = " port='{}'".format(self.port) if LDSUtilities.mightAsWellBeNone(self.port) is not None else ""
        uri = "PG:dbname='{}'".format(self.dbname)+hst+prt+usr+pwd+sch
        ldslog.debug(uri)
        return uri


    def getConfigOptions(self):
        '''Add PG options for SCHEMA and GEO_NAME'''
        #PG_USE_COPY,PGSQL_OGR_FID,PG_USE_BASE64
        local_opts = []
        #doesnt work with createlayer... but not needed if we want to overwrite FID with PK
        #local_opts += ['PGSQL_OGR_FID='+str(self.PGSQL_OGR_FID)]
        
        #do not use PG_USE_COPY if you want FID preserved
        local_opts += ['PG_USE_COPY='+str(self.PG_USE_COPY)]
        local_opts += ['PG_USE_BASE64='+str(self.PG_USE_BASE64)]  
              
        return super(PostgreSQLDataStore,self).getConfigOptions() + local_opts    
    
    def getLayerOptions(self,layer_id):
        '''PG layer creation options'''
        #GEOM_TYPE, OVERWRITE,LAUNDER,PRECISION,DIM={2,3},GEOMETRY_NAME,SCHEMA,SPATIAL_INDEX,TEMPORARY,NONE_AS_UNKNOWN,FID,EXTRACT_SCHEMA_FROM_LAYER_NAME,COLUMN_TYPES
        #This should default to geometry but it doesn't, gdal creates bytea instead
        local_opts  = ['GEOM_TYPE='+str(self.GEOM_TYPE)]
        local_opts += ['SPATIAL_INDEX='+str(self.SPATIAL_INDEX)]
        gname = self.layerconf.readLayerProperty(layer_id,'geocolumn')
        
        if gname is not None:
            local_opts += ['GEOMETRY_NAME='+gname]
        
        return super(PostgreSQLDataStore,self).getLayerOptions(layer_id) + local_opts
    
    
    
    def buildIndex(self,lce,dst_layer_name):
        '''Builds an index creation string for a new full replicate in PG format'''
        tableonly = dst_layer_name.split('.')[-1]
        
        if LDSUtilities.mightAsWellBeNone(lce.pkey):
            cmd = 'ALTER TABLE {0} ADD CONSTRAINT {1}_{2}_PK UNIQUE({2})'.format(dst_layer_name,tableonly,lce.pkey)
            try:
                self.executeSQL(cmd)
                ldslog.info("Index = {}({}). Execute = {}".format(tableonly,lce.pkey,cmd))
            except RuntimeError as rte:
                if re.search('already exists', str(rte)): 
                    ldslog.warn(rte)
                else:
                    raise
                        
        #If a spatial index has already been created don't try to create another one
        if self.SPATIAL_INDEX == 'OFF' and LDSUtilities.mightAsWellBeNone(lce.gcol):
            cmd = 'CREATE INDEX {1}_{2}_GK ON {0} USING GIST({2})'.format(dst_layer_name,tableonly,lce.gcol)
            try:
                self.executeSQL(cmd)
                ldslog.info("Index = {}({}). Execute = {}".format(tableonly,lce.gcol,cmd))
            except RuntimeError as rte:
                if re.search('already exists', str(rte)): 
                    ldslog.warn(rte)
                else:
                    raise
        
        
    def checkGeoPrivileges(self,schema,user):
        #cmd1 = "select * from information_schema.role_table_grants where grantee='{}' and table_name='spatial_ref_sys".format(user)

        cmd1 = "SELECT has_table_privilege('{}','public.spatial_ref_sys', 'select')".format(user)
        cmd2 = "SELECT has_table_privilege('{}','public.geometry_columns', 'select')".format(user)
        cmd3 = "SELECT has_schema_privilege('{1}','{0}', 'create')".format(schema,user)
        try:
            #1=True, 0=False
            rv1 = self.executeSQL(cmd1).GetFeature(1).GetField(0)
            rv2 = self.executeSQL(cmd2).GetFeature(1).GetField(0)
            if rv1!=1 or rv2!=1:
                raise DatasourcePrivilegeException('User '+str(user)+'doesn\'t have SELECT access to Geometry tables')
            rv3 = self.executeSQL(cmd3).GetFeature(1).GetField(0)
            if rv3!=1:
                raise DatasourcePrivilegeException('User '+str(user)+'doesn\'t have CREATE access to Schema '+str(schema))
        except RuntimeError as rte:
            raise
        
    def versionCheck(self):
        '''Postgres/Postgis version checker'''
        from VersionUtilities import VersionChecker,UnsupportedVersionException

        pgv_cmd = 'SELECT version()'
        pgisv_cmd = 'SELECT postgis_full_version()'
        
        pgv_res = re.search('PostgreSQL\s+(\d+\.\d+\.\d+)',self.executeSQL(pgv_cmd).GetNextFeature().GetFieldAsString(0))
        pgisv_res = re.search('POSTGIS=\"(\d+\.\d+\.\d+)',self.executeSQL(pgisv_cmd).GetNextFeature().GetFieldAsString(0))
        
        if VersionChecker.compareVersions(VersionChecker.PostgreSQL_MIN, pgv_res.group(1) if pgv_res is not None else VersionChecker.PostgreSQL_MIN):
            raise UnsupportedVersionException('PostgreSQL version '+str(pgv_res.group(1))+' does not meet required minumum '+str(VersionChecker.PostgreSQL_MIN))
        
        if VersionChecker.compareVersions(VersionChecker.PostGIS_MIN, pgisv_res.group(1) if pgisv_res is not None else VersionChecker.PostGIS_MIN):
            raise UnsupportedVersionException('PostGIS version '+str(pgisv_res.group(1))+' does not meet required minumum '+str(VersionChecker.PostGIS_MIN))
        
        return True
        
        
        

    