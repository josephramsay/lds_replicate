'''
v.0.0.1

LDSIncremental -  FileGDBDataStore

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

from ESRIDataStore import ESRIDataStore
from DataStore import MalformedConnectionString

ldslog = logging.getLogger('LDS')

class FileGDBDataStore(ESRIDataStore):
    '''
    FileGDB DataStore wrapper for file location and options 
    '''
    DRIVER_NAME = "FileGDB"

    def __init__(self,conn_str=None,user_config=None):
        
        
        super(FileGDBDataStore,self).__init__(conn_str,user_config)
        
        (self.path,self.name,self.config,self.srs,self.cql) = self.params
        #because sometimes ~ (if included) isnt translated to home
        self.path = os.path.expanduser(self.path)
        self.SUFFIX = '.gdb'

        
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
        return os.path.join(self.path,self.name+self.SUFFIX)
        
        
    def deleteFieldFromLayer(self,layer,field_id,field_name):
        '''per DS delete field since some do not support this'''
        dsql = "alter table "+layer.GetName()+" drop column "+field_name
        self.executeSQL(dsql)
        
    def buildIndex(self,lce,dst_layer_name):
        ldslog.warn('Table indexing not supported by '+self.DRIVER_NAME+' at present')
        return
    
    def getOptions(self,layer_id):
        '''Adds FileGDB options for GEOMETRY_NAME'''
        local_opts = []
        gname = self.layerconf.readLayerProperty(layer_id,'geocolumn')
        
        if gname is not None:
            local_opts += ['GEOMETRY_NAME='+gname]
        
        return super(FileGDBDataStore,self).getOptions(layer_id) + local_opts
    
    def changeColumnIntToString(self,table,column):
        '''Default column type changer, to be overriden but works on PG. Used to change 64 bit integer columns to string''' 
        self.executeSQL('alter table '+table+' alter '+column+' type varchar')
    
#This was the case for versions of gdal<9.1, uncomment if youre having problems with nonetypes when looking up feature fields with fgdb 
#    def _findMatchingFeature(self,search_layer,ref_pkey,key):
#        '''Find the Feature matching a primary key value. FileGDB version doesnt use string quotes'''
#        qry = ref_pkey+" = "+str(key)
#        search_layer.SetAttributeFilter(qry)
#        return search_layer.GetNextFeature()
        