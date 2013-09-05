'''
v.0.0.9

LDSReplicate -  FileGDBDataStore

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
import re
import ogr

from lds.ESRIDataStore import ESRIDataStore
from lds.DataStore import DataStore,MalformedConnectionString
from lds.LDSUtilities import LDSUtilities

ldslog = LDSUtilities.setupLogging()

class FileGDBDataStore(ESRIDataStore):
    '''
    FileGDB DataStore wrapper for file location and options 
    '''
    DRIVER_NAME = DataStore.DRIVER_NAMES['fg']#"FileGDB"
    
    FGDB_BULK_LOAD = 'YES'
    SUFFIX = '.gdb'
    
        #wkbNone removed
    ValidGeometryTypes = (ogr.wkbUnknown, ogr.wkbPoint, ogr.wkbLineString,
                      ogr.wkbPolygon, ogr.wkbMultiPoint, ogr.wkbMultiLineString, 
                      ogr.wkbMultiPolygon, ogr.wkbGeometryCollection, 
                      ogr.wkbLinearRing, ogr.wkbPoint25D, ogr.wkbLineString25D,
                      ogr.wkbPolygon25D, ogr.wkbMultiPoint25D, ogr.wkbMultiLineString25D, 
                      ogr.wkbMultiPolygon25D, ogr.wkbGeometryCollection25D)

    def __init__(self,parent,conn_str=None,user_config=None):
        
        
        super(FileGDBDataStore,self).__init__(parent,conn_str,user_config)
        
        (self.fname,self.config,self.srs,self.cql) = self.params
        #because sometimes ~ (if included) isnt translated to home
        self.fname = os.path.expanduser(self.fname)
        

    def clone(self):
        clone = FileGDBDataStore(self.parent,self.conn_str,None)
        clone.name = str(self.name)+'C'
        return clone
    
    def sourceURI(self,layer):
        '''URI method returns source file name'''
        return self._commonURI(layer)
    
    
    def destinationURI(self,layer):
        '''URI method returns destination file name'''
        return self._commonURI(layer)
        
        
    def validateConnStr(self,cs):
        '''FGDB basic checks. 1 correct file suffix. 2 the directory can be accessed'''
        #-d "/home/<username>/temp/filegdb/ldsincr.gdb
        if not hasattr(self,'SUFFIX') or not re.search(self.SUFFIX+'$',cs,flags=re.IGNORECASE):
            raise MalformedConnectionString('FileGDB file suffix must be '+self.SUFFIX)
        if not os.access(os.path.dirname(cs), os.W_OK):
            raise MalformedConnectionString('Data file path cannot be found')
        return cs

        
    def _commonURI(self,layer):
        '''FileGDB organises tables as individual .gdb file/directories into which contents are written. The layer is configured as if it were a file'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.validateConnStr(self.conn_str)
        return self.fname+(''if re.search(self.SUFFIX+'$',self.fname,flags=re.IGNORECASE) else self.SUFFIX)
        
    
    def deleteFieldFromLayer(self,layer,field_id,field_name):
        '''per DS delete field since some do not support this'''
        dsql = "alter table "+layer.GetName()+" drop column "+field_name
        self.executeSQL(dsql)
        
    def _buildIndex(self,lce,dst_layer_name):
        ldslog.warn('Table indexing not supported by '+self.DRIVER_NAME+' at present')
        return
    
    def buildIndex(self,lce,dst_layer_name):
        '''Builds an index creation string for a new full replicate in PG format'''
        tableonly = dst_layer_name.split('.')[-1]
        ALLOW_TABLE_INDEX_CREATION=True
        #SpatiaLite doesnt have a unique constraint but since we're using a pk might a well declare it as such
        if ALLOW_TABLE_INDEX_CREATION and LDSUtilities.mightAsWellBeNone(lce.pkey) is not None:
            #spatialite won't do post create constraint additions (could to a re-create?)
            cmd = 'CREATE INDEX {0}_{1}_PK ON {0}({1})'.format(tableonly,lce.pkey)
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
        if LDSUtilities.mightAsWellBeNone(lce.gcol):
            #untested and unlikely to work
            cmd = "CREATE INDEX {0}_{1}_SK ON {0}({1})".format(dst_layer_name,lce.gcol)
            try:
                self.executeSQL(cmd)
                ldslog.info("Index = {}({}). Execute = {}.".format(tableonly,lce.gcol,cmd))
            except RuntimeError as rte:
                if re.search('already exists', str(rte)): 
                    ldslog.warn(rte)
                else:
                    raise
    
    def getConfigOptions(self):
        '''FGDB doesn't have any dataset creation options'''
        self.fg_local_copts = ['FGDB_BULK_LOAD='+str(self.FGDB_BULK_LOAD)]        
        return super(FileGDBDataStore,self).getConfigOptions() + self.fg_local_copts    
    
    def getLayerOptions(self,layer_id):
        '''Adds FileGDB options for GEOMETRY_NAME'''
        #FEATURE_DATASET, GEOMETRY_NAME, OID_NAME, XYTOLERANCE, ZTOLERANCE, XORIGIN, YORIGIN, ZORIGIN, XYSCALE, ZSCALE, XML_DEFINITION 
        self.fg_local_lopts = []
        gname = self.layerconf.readLayerProperty(layer_id,'geocolumn')
        
        if gname is not None:
            self.fg_local_lopts += ['GEOMETRY_NAME='+gname]
        
        return super(FileGDBDataStore,self).getLayerOptions(layer_id) + self.fg_local_lopts
    
#    def selectValidGeom(self,geom):
#        '''Override for wkbNone'''
#        return geom if geom in self.ValidGeometryTypes else ogr.wkbPoint
    
    
    def changeColumnIntToString(self,table,column):
        '''Default column type changer, to be overriden but works on PG. Used to change 64 bit integer columns to string''' 
        self.executeSQL('alter table '+table+' alter '+column+' type varchar')
    
    def closeDS(self):
        '''Close a DS with sync and destroy'''
        ldslog.info("Sync DS and Close")
        self.ds.SyncToDisk()
        #FileGDB locks up on destroy, do we even need this? Supposedly for backward compatibility
        self.ds.Destroy()  
        self.ds = None
        
        
#    def formatWhereClause(self,ref_pkey,key_val):
#        '''FGDB where clause doesn't use single quotes in int matching string'''
#        #return "{0} = {1}".format(ref_pkey,key_val) #new driver fixes this behaviour
#        return super(FileGDBDataStore,self).formatWhereClause(ref_pkey,key_val)

    def versionCheck(self):
        '''Nothing to check?'''
        v = super(FileGDBDataStore,self).versionCheck()
        ldslog.info(self.DRIVER_NAME+' version '+str(v))
        return v
        