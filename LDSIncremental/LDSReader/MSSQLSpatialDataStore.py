'''
v.0.0.1

LDSIncremental -  MSSQLSpatialDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 9/08/2012

@author: jramsay
'''
import logging
import ogr
import json

from DataStore import DataStore
from ProjectionReference import Geometry

ldslog = logging.getLogger('LDS')

class MSSQLSpatialDataStore(DataStore):
    '''
    MS SQL DataStore
    MSSQL:server=.\MSSQLSERVER2008;database=dbname;trusted_connection=yes
    '''

    DRIVER_NAME = "MSSQLSpatial"
      
    def __init__(self,conn_str=None,user_config=None):
        '''
        cons init driver
        '''
        
        super(MSSQLSpatialDataStore,self).__init__(conn_str,user_config)
        
        (self.odbc,self.server,self.dsn,self.trust,self.dbname,self.schema,self.usr,self.pwd,self.config,self.srs,self.cql) = self.params

        
    def sourceURI(self,layer):
        '''URI method returns source file name'''
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        '''URI method returns destination file name'''
        return self._commonURI(layer)
        
    def _commonURI(self,layer):
        '''Refers to common connection instance for example in a DB where it doesn't matter whether your reading or writing'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        #return "MSSQL:server={};database={};trusted_connection={};".format(self.server, self.dbname, self.trust)
        sstr = ";Schema={}".format(self.schema) if self.schema is not None and self.schema !='' else ""
        uri = "MSSQL:server={};database={};UID={};PWD={};Driver={}".format(self.server, self.dbname, self.usr, self.pwd,self.odbc)+sstr
        ldslog.debug(uri)
        return uri
        

    def deleteFieldFromLayer(self,layer,field_id,field_name):
        '''per DS delete field since some do not support this'''
        dsql = "alter table "+layer.GetName()+" drop column "+field_name
        self.executeSQL(dsql)
        #ldslog.error("Field deletion not supported in MSSQLSpatial driver")

    def buildIndex(self,ref_index,ref_pkey,ref_gcol,dst_layer_name):
        '''Builds an index creation string for a new full replicate'''
        ref_index = DataStore.parseStringList(ref_index)
        if ref_index.intersection(set(('spatial','s'))):
            bb = Geometry.getBoundingBox()
            cmd1 = 'CREATE SPATIAL INDEX {}_SK ON {}({}) '.format(dst_layer_name.split('.')[-1]+"_"+ref_gcol,dst_layer_name,ref_gcol)
            cmd2 = 'USING GEOMETRY_GRID WITH ( BOUNDING_BOX = (XMIN = {}, YMIN = {}, XMAX = {}, YMAX = {}),' \
                    'GRIDS = (LEVEL_1 = MEDIUM, LEVEL_2 = MEDIUM, LEVEL_3 = MEDIUM, LEVEL_4 = MEDIUM),' \
                    'CELLS_PER_OBJECT = 256)'.format(str(bb[0]),str(bb[1]),str(bb[2]),str(bb[3]))
            cmd = cmd1+cmd2
            
            #magic command...
            cmd = 'create spatial index on '+dst_layer_name.split('.')[-1]
        elif ref_index.intersection(set(('primary','pkey','p'))):
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+ref_pkey,dst_layer_name,ref_pkey)
        elif ref_index is not None:
            #maybe the user wants a non pk/spatial index? Try to filter the string
            clst = ','.join(ref_index)
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+DataStore.sanitise(clst),dst_layer_name,clst)
        else:
            return
        ldslog.info("Index="+','.join(ref_index)+". Execute "+cmd)
        self.executeSQL(cmd)
                

    def getOptions(self,layer_id):
        '''Get MS options for GEO_NAME'''
        local_opts = []
        gname = self.layerconf.readLayerProperty(layer_id,'geocolumn')
        
        if gname is not None:
            local_opts += ['GEOM_NAME='+gname]
        
        return super(MSSQLSpatialDataStore,self).getOptions() + local_opts
    
    #Possibly use this override if attempting to use FreeTDS or SQLServer for Linux
    def buildConfigLayer_OVERRIDE_FOR_LNX2WIN(self,config_array):

        '''Builds the config table into and using the active DS but does it using SQL commands since CreateLayer/Feature etc is flakey'''
        #TODO check initds for conf table name
        if not hasattr(self,'ds') or self.ds is None:
            self.ds = self.initDS(self.destinationURI(DataStore.LDS_CONFIG_TABLE))  
            
        #bypass (probably not needed) if external (alternatively set [layerconf = self or layerconf = self.confwrapper])
        if not self.isConfInternal():
            return self.layerconf.buildConfigLayer()
        #TODO unify the naming for the config tables

        cols = ('id','pkey','name','category','lastmodified','geocolumn','epsg','discard','cql')

        self.executeSQL("drop table "+DataStore.LDS_CONFIG_TABLE)
        
        
        #TODO what about ogc_fid/ogc_geometry?
        sql_crt = "create table "+DataStore.LDS_CONFIG_TABLE+"("
        for name in cols: 
            #varchar(max) because this is the same type the gdal driver creates by default
            sql_crt += name+" VARCHAR(MAX),"
        sql_crt = sql_crt[:-1]+")"
        self.executeSQL(sql_crt)            
        
        
        for row in json.loads(config_array):
            sql_ins = "insert into "+DataStore.LDS_CONFIG_TABLE+" (id,pkey,name,category,lastmodified,geocolumn,epsg,discard,cql) VALUES ("
            for col in row:
                if col is None:
                    sql_ins += "'',"
                elif isinstance(col,basestring):
                    sql_ins += "'"+col+"',"
                else:
                    sql_ins += "'"+','.join(col)+"',"
            sql_ins = sql_ins[:-1]+")"        
            print sql_ins    
            self.executeSQL(sql_ins)

    def getConfigGeometry(self):
        return ogr.wkbPoint;
    
    def _clean(self):
        '''Deletes the entire DS layer by layer'''
        #for MSSQL deleted indices don't decrement
        for li in range(0,self.ds.GetLayerCount()):
            self._cleanLayerByIndex(self.ds,li)
    