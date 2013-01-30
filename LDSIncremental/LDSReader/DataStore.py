'''
v.0.0.1

LDSIncremental -  DataSource

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

DataStore is the base Datasource wrapper object 

Created on 9/08/2012

@author: jramsay
'''

import sys
import ogr
import osr
import re
import logging
import string

import json

from datetime import datetime
from abc import ABCMeta, abstractmethod

from LDSUtilities import LDSUtilities,SUFIExtractor
from ProjectionReference import Projection
from ConfigWrapper import ConfigWrapper
#from LDSDataStore import LDSDataStore

ldslog = logging.getLogger('LDS')
#Enabling exceptions halts program on non critical errors i.e. create DS throws exception but builds valid DS anyway 
#ogr.UseExceptions()

#exceptions
class DSReaderException(Exception): pass
class LDSReaderException(DSReaderException): pass
class IncompleteWFSRequestException(LDSReaderException): pass
class CannotInitialiseDriverType(LDSReaderException): pass
class DatasourceCopyException(LDSReaderException): pass
class DatasourceCreateException(LDSReaderException): pass
class DatasourceOpenException(DSReaderException): pass
class LayerCreateException(LDSReaderException): pass
class InvalidLayerException(LDSReaderException): pass
class InvalidFeatureException(LDSReaderException): pass
class ASpatialFailureException(LDSReaderException): pass
class UnknownTemporaryDSType(LDSReaderException): pass
class MalformedConnectionString(DSReaderException): pass
class InaccessibleLayerException(DSReaderException): pass
class InaccessibleFeatureException(DSReaderException): pass


class DataStore(object):
    '''
    DataStore superclasses PostgreSQL, LDS(WFS), FileGDB and SpatiaLite datastores.
    This class contains the main copy functions for each datasource and sets up default connection parameters. Common options are also set up in this class 
    but variations are implemented in the appropriate subclasses
    '''
    __metaclass__ = ABCMeta


    LDS_CONFIG_TABLE = 'lds_config'
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
    EARLIEST_INIT_DATE = '2000-01-01T00:00:00'
    
    CONFIG_COLUMNS = ('id','pkey','name','category','lastmodified','geocolumn','index','epsg','discard','cql')
    #TEMP_DS_TYPES = ('Memory','ESRI Shapefile','Mapinfo File','GeoJSON','GMT','DXF')
    
    ValidGeometryTypes = (ogr.wkbUnknown, ogr.wkbPoint, ogr.wkbLineString,
                      ogr.wkbPolygon, ogr.wkbMultiPoint, ogr.wkbMultiLineString, 
                      ogr.wkbMultiPolygon, ogr.wkbGeometryCollection, ogr.wkbNone, 
                      ogr.wkbLinearRing, ogr.wkbPoint25D, ogr.wkbLineString25D,
                      ogr.wkbPolygon25D, ogr.wkbMultiPoint25D, ogr.wkbMultiLineString25D, 
                      ogr.wkbMultiPolygon25D, ogr.wkbGeometryCollection25D)
    
    
    def __init__(self,conn_str=None,user_config=None):
        '''
        Constructor inits driver and some date specific settings. Arguments are for config overrides 
        '''
        
        self.CONFIG_XSL = "getcapabilities."+self.DRIVER_NAME.lower()+".xsl"
         
        if conn_str is not None and not all(i in string.whitespace for i in conn_str):
            self.conn_str = conn_str
        
        self.setSRS(None)
        self.setFilter(None)     

        self.setOverwrite()
        
        self.getDriver(self.DRIVER_NAME)
            
        self.mainconf = ConfigWrapper(user_config)
        
        self.params = self.mainconf.readDSParameters(self.DRIVER_NAME)
        
        
        '''set of <potential> columns not needed in final output, global'''
        self.optcols = set(['__change__','gml_id'])
        
    def getDriver(self,driver_name):

        self.driver = ogr.GetDriverByName(driver_name)
        if self.driver==None:
            raise CannotInitialiseDriverType, "Driver cannot be initialised for type "+driver_name
            sys.exit(1)

    def setURI(self,uri):
        self.uri = uri
        
    def getURI(self):
        return self.uri

    def setFilter(self,cql):
        self.cql = cql
         
    def getFilter(self):
        return self.cql
    
    def setSRS(self,srs):
        '''Sets the destination SRS EPSG code'''
        self.srs = srs
         
    def getSRS(self):
        return self.srs  
    
    def setConfInternal(self):
        self.config = True
            
    def setConfExternal(self):
        self.config = False
         
    def isConfInternal(self):
        return self.config       
    
    #--------------------------  
    
    def setOverwrite(self):
        self.OVERWRITE = "YES"
         
    def clearOverwrite(self):
        self.OVERWRITE = "NO"
         
    def getOverwrite(self):
        return self.OVERWRITE    
    
    def getOptions(self):
        '''Returns common options, overridden in subclasses for source specifc options'''
        return ['OVERWRITE='+self.getOverwrite()]#,'OGR_ENABLE_PARTIAL_REPROJECTION=True']
    
    '''Both Source and Destination URI for the generic situation where we want to transfer between similar Ds formats. e.g. PG->PG'''
    
    @abstractmethod
    def sourceURI(self,layer):
        '''Abstract URI method for returning source. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method sourceURI not implemented")
    
    @abstractmethod
    def destinationURI(self,layer):
        '''Abstract URI method for returning destination. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method destinationURI not implemented")
    
    @abstractmethod
    def validateConnStr(self,conn_str):
        '''Abstract method to check user supplied connection strings. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method destinationURI not implemented")
        
    #@abstractmethod
    #def buildExternalLayerDefinition(self,name,fdef_list):
    #    raise NotImplementedError("Abstract method buildExternalLayerDefinition not implemented")
    
    def initDS(self,dsn=None):
        ds = None
        '''initialise a DS for writing'''
        try:
            ds = self.driver.Open(dsn, update = 1 if self.getOverwrite() else 0)
            if ds is None:
                raise DSReaderException("Error opening DS on Destination "+str(dsn)+", attempting DS Create")
        #catches DSReader (but not runtime) error, but don't fail since we'll try to init a new DS
        except (RuntimeError,DSReaderException) as dsre1:
            #print "DSReaderException",dsre1 
            ldslog.error(dsre1)
            try:
                ds = self.driver.CreateDataSource(dsn)
                if ds is None:
                    raise DSReaderException("Error creating DS on Destination "+str(dsn)+", quitting")
            except DSReaderException as dsre2:
                print "DSReaderException, Cannot create DS.",dsre2
                ldslog.error(dsre2,exc_info=1)
                raise
            except RuntimeError as rte:
                '''this is only caught if ogr.UseExceptions() is enabled'''
                print "GDAL RuntimeError. Error creating DS.",rte
                ldslog.error(rte,exc_info=1)
                raise
        return ds
        
    def read(self,dsn):
        '''Main DS read method'''
        ldslog.info("DS read "+dsn)#.split(":")[0])
        #5050 initDS for consistency and utilise if-ds-is-none check OR quick open and overwrite
        #self.initDS(dsn)
        self.ds = self.driver.Open(dsn)
    
    def write(self,src,dsn,incr_haspk,fbf,sixtyfour,temptable,srsconv):
        '''Main DS write method. Attempts to open or alternatively, create a datasource'''
        #mild hack. src_link created so we can re-query the source as a doc to get 64bit ints as strings
        self.src_link = src
        #we need to store 64 beyond fC/dC flag to identify need for sufi-to-str conversion
        self.sixtyfour = sixtyfour
        max_key = None
        
        #ldslog.info("DS Write "+dsn)#.split(":")[0]
        #shouldnt be needed but sometimes instances of DS disconnect occur
        if not hasattr(self,'ds') or self.ds is None:
            self.ds = self.initDS(dsn)
        
        '''IF not(haspk) and (64 or srs) THEN what happens? 
        1) no pk means we have to use dC
        2) 64 or srs means we have to use fC
        trying to do fC without a pk will fail if we have a partition table
        trying to do dC with 64 means sufis get converted wrongly (ints will overflow)
        trying to do dC with srs means the conversion wont happen
        SO best option is attempt fC and hope we dont have to partition the table, if we do throw an exception!
        ''' 
        #if incr&haspk then fC
        if incr_haspk:
            # standard incremental featureCopyIncremental. change_col used in delete list and as change (INS/DEL/UPD) indicator
            max_key = self.featureCopyIncremental(src.ds,self.ds,src.CHANGE_COL)
        #if not(incr&haspk) & 64b attempt fC
        elif sixtyfour or srsconv or fbf:
            #do a featureCopyIncremental if override asks or if a table has big ints
            max_key = self.featureCopy(src.ds,self.ds)
        else:
            # no cols to delete and no operational instructions, just duplicate. No good for partition copying since entire layer is specified
            self.driverCopy(src.ds,self.ds,temptable) 
            
            #Alternative, bare feature copy. Still very slow though 10x slower than driverCopy
            #self.featureCopy(src.ds,self.ds,None)
            
        return max_key
        
    def closeDS(self):
        '''close a DS with sync and destroy'''
        ldslog.info("Sync DS and Close")
        self.ds.SyncToDisk()
        self.ds.Destroy()  
              
    def driverCopy(self,src_ds,dst_ds,temptable):
        '''Copy from source to destination using the driver copy and without manipulating data'''       
        from TemporaryDataStore import TemporaryDataStore
        
        ldslog.info("Using driverCopy. Non-Incremental driver copy")
        for li in range(0,src_ds.GetLayerCount()):
            src_layer = src_ds.GetLayer(li)
            src_layer_name = LDSUtilities.cropChangeset(src_layer.GetName())
            
            #ref_name = self.layerconf.readConvertedLayerName(src_layer_name)
            (ref_pkey,ref_name,ref_group,ref_gcol,ref_index,ref_epsg,ref_lmod,ref_disc,ref_cql) = self.layerconf.readLayerParameters(src_layer_name)
            
            dst_layer_name = self.generateLayerName(ref_name)
            self.optcols |= set(ref_disc.strip('[]{}()').split(',') if all(i in string.whitespace for i in ref_disc) else [])
            
            try:
                #TODO test on MSSQL since schemas sometimes needed ie non dbo
                dst_ds.DeleteLayer(dst_layer_name)          
            except ValueError as ve:
                ldslog.warn("Cannot delete layer "+dst_layer_name+". It probably doesn't exist. "+str(ve))
                

            try:
                if temptable == 'DIRECT':                    
                    layer = dst_ds.CopyLayer(src_layer,dst_layer_name,self.getOptions(src_layer_name))
                    self.deleteOptionalColumns(layer)
                elif temptable in TemporaryDataStore.TEMP_MAP.keys():
                    tds = TemporaryDataStore.getInstance(temptable)()
                    tds_ds = tds.initDS()
                    tds_layer = tds_ds.CopyLayer(src_layer,dst_layer_name,[])
                    tds.deleteOptionalColumns(tds_layer)
                    layer = dst_ds.CopyLayer(tds_layer,dst_layer_name,self.getOptions(src_layer_name))
                    #tds_ds.SyncToDisk()
                    tds_ds.Destroy()  
                else:
                    ldslog.error('Cannot match DS type "'+str(temptable)+'" with known types '+str(TemporaryDataStore.TEMP_MAP.keys()))
                    raise UnknownTemporaryDSType('Cannot match DS type "'+str(temptable)+'" with known types '+str(TemporaryDataStore.TEMP_MAP.keys()))
            except RuntimeError as rte:
                if 'General function failure' in str(rte):
                    #GFF usually indicates a driver copy error (FGDB)
                    ldslog.error('GFF on driver copy. Recommend upgrade to GDAL > 1.9.2')
                else:
                    raise

            #if the copy succeeded we now need to build an index and delete unwanted columns so get the new layer     
            #layer = dst_ds.GetLayer(dst_layer_name)
            if layer is None:
                # **HACK** the only way to get around driver copy failures seems to be by doing a feature-by-feature featureCopyIncremental and changing the sref 
                ldslog.error('Layer not created, attempting feature-by-feature copy')
                return self.featureCopyIncremental(src_ds,dst_ds,None)

            if ref_index is not None:
                self.buildIndex(ref_index,ref_pkey,ref_gcol,dst_layer_name)
            
        return
    
        
    def deleteOptionalColumns(self,dst_layer):
        '''Delete unwanted columns from layer'''
        #because column deletion behaviour is different for each driver (advancing index or not) split out and subclass
        dst_layer_defn = dst_layer.GetLayerDefn()
        #loop layer fields and discard the unwanted columns
        offset = 0
        for fi in range(0,dst_layer_defn.GetFieldCount()):
            fdef = dst_layer_defn.GetFieldDefn(fi-offset)
            fdef_nm = fdef.GetName()
            #print '>>>>>',fi,fi-offset,fdef_nm
            if fdef is not None and fdef_nm in self.optcols:
                self.deleteFieldFromLayer(dst_layer, fi-offset,fdef_nm)
                offset += 1
                
    def deleteFieldFromLayer(self,layer,field_id,fdef_nm):
        '''per DS delete field since some do not support this'''
        layer.DeleteField(field_id)

    def generateLayerName(self,ref_name):
        '''Generic layer name constructor'''
        '''Doesn't use schema prefix since its not used in FileGDB, SpatiaLite 
        and PostgreSQL impements an "active_schema" option bypassing the need for a schema declaration'''
        return self.sanitise(ref_name)
        
    #--------------------------------------------------------------------------            
    
    def featureCopy(self,src_ds,dst_ds):
        '''Feature copy without the change column (and other incremental) overhead. Replacement for driverCopy(cloneDS).''' 
        '''REF #4'''
        for li in range(0,src_ds.GetLayerCount()):
            new_layer = False
            src_layer = src_ds.GetLayer(li)

            #TODO. resolve conflict between lastmodified and fdate
            ref_layer_name = LDSUtilities.cropChangeset(src_layer.GetName())
            
            '''retrieve per-layer settings from props'''
            (ref_pkey,ref_name,ref_group,ref_gcol,ref_index,ref_epsg,ref_lmod,ref_disc,ref_cql) = self.layerconf.readLayerParameters(ref_layer_name)
            
            dst_layer_name = self.generateLayerName(ref_name)
            
            ldslog.info("Dest layer: "+dst_layer_name)
            
            '''parse discard columns'''
            self.optcols |= set(ref_disc.strip('[]{}()').split(',') if ref_disc is not None else [])

            ldslog.warning(dst_layer_name+" does not exist. Creating new layer")
            '''create a new layer if a similarly named existing layer can't be found on the dst'''
            src_layer_sref = src_layer.GetSpatialRef()
            src_layer_geom = src_layer.GetGeomType()
            src_layer_defn = src_layer.GetLayerDefn()
            #transforms from SRC to DST sref if user requests a different EPSG, otherwise SRC returned unchanged
            dst_sref = self.transformSRS(src_layer_sref)
            
            (dst_layer,new_layer) = self.buildNewDataLayer(dst_layer_name,dst_ds,dst_sref,src_layer_defn,src_layer_geom,src_layer_sref,ref_layer_name)
        
            dst_layer.StartTransaction()
            #add/copy features
            #src_layer.ResetReading()
            src_feat = src_layer.GetNextFeature()
            '''since the characteristics of each feature wont change between layers we only need to define a new feature definition once'''
            if src_feat is not None:
                new_feat_def = self.partialCloneFeatureDef(src_feat)
                
            while src_feat is not None:
                #slowest part of this copy operation is the insert since we have to build a new feature from defn and check fields for discards and sufis
                e = self.insertFeature(dst_layer,src_feat,new_feat_def)
                
                src_feat = src_layer.GetNextFeature()
                    

            #self._showLayerData(dst_layer)
            
            '''Builds an index on a newly created layer'''
            #May need to be pushed out to subclasses depending on syntax differences
            if new_layer and ref_index is not None and ref_pkey is not None:
                self.buildIndex(ref_index,ref_pkey,ref_gcol,dst_layer_name)
                
            dst_layer.CommitTransaction()
            src_layer.ResetReading()
            dst_layer.ResetReading()            
    
    def featureCopyIncremental(self,src_ds,dst_ds,changecol):
        #TDOD. decide whether C_C is better as an arg or a src.prop
        '''DataStore feature-by-feature replication for incremental queries'''
        #build new layer by duplicating source layers  
        max_index = None
        ldslog.info("Using featureCopyIncremental. Per-feature copy")
        for li in range(0,src_ds.GetLayerCount()):
            new_layer = False
            src_layer = src_ds.GetLayer(li)

            #TODO. resolve conflict between lastmodified and fdate
            ref_layer_name = LDSUtilities.cropChangeset(src_layer.GetName())
            
            '''retrieve per-layer settings from props'''
            (ref_pkey,ref_name,ref_group,ref_gcol,ref_index,ref_epsg,ref_lmod,ref_disc,ref_cql) = self.layerconf.readLayerParameters(ref_layer_name)
            
            dst_layer_name = self.generateLayerName(ref_name)
            
                
            ldslog.info("Dest layer: "+dst_layer_name)
            
            '''parse discard columns'''
            self.optcols |= set(ref_disc.strip('[]{}()').split(',') if ref_disc is not None else [])
            
            try:
                dst_layer = dst_ds.GetLayer(dst_layer_name)
            except RuntimeError as re:
                '''Instead of returning none, runtime errors sometimes occur if the layer doesn't exist and needs to be created'''
                ldslog.warning("Runtime Error fetching layer. "+str(re))
                dst_layer = None
                
            if dst_layer is None:
                ldslog.warning(dst_layer_name+" does not exist. Creating new layer")
                '''create a new layer if a similarly named existing layer can't be found on the dst'''
                src_layer_sref = src_layer.GetSpatialRef()
                src_layer_geom = src_layer.GetGeomType()
                src_layer_defn = src_layer.GetLayerDefn()
                dst_sref = self.transformSRS(src_layer_sref)
                (dst_layer,new_layer) = self.buildNewDataLayer(dst_layer_name,dst_ds,dst_sref,src_layer_defn,src_layer_geom,src_layer_sref,ref_layer_name)
            
            dst_layer.StartTransaction()
            
            #add/copy features
            #src_layer.ResetReading()
            src_feat = src_layer.GetNextFeature()
            '''since the characteristics of each feature wont change between layers we only need to define a new feature definition once'''
            if src_feat is not None:
                new_feat_def = self.partialCloneFeatureDef(src_feat)
                e = 0
                while 1:
                    '''identify the change in the WFS doc (INS,UPD,DEL)'''
                    change =  (src_feat.GetField(changecol) if changecol is not None and all(i in string.whitespace for i in changecol) else "insert").lower()
                    '''not just copy but possubly delete or update a feature on the DST layer'''
                    #self.copyFeature(change,src_feat,dst_layer,ref_pkey,new_feat_def,ref_gcol)
                    
                    try:
                        if change == 'insert': 
                            e = self.insertFeature(dst_layer,src_feat,new_feat_def)
                        elif change == 'delete': 
                            e = self.deleteFeature(dst_layer,src_feat,             ref_pkey)
                        elif change == 'update': 
                            e = self.updateFeature(dst_layer,src_feat,new_feat_def,ref_pkey)
                        else:
                            ldslog.error("Error with Key "+str(change)+" !E {ins,del,upd}")
                        #    raise KeyError("Error with Key "+str(change)+" !E {ins,del,upd}",exc_info=1)
                    except InvalidFeatureException as ife:
                        ldslog.error("Invalid Feature Exception during "+change+" operation on dest. "+str(ife),exc_info=1)
                        
                    if e != 0:                  
                        ldslog.error("Driver Error ["+str(e)+"] on "+change,exc_info=1)
                        if change=='update':
                            ldslog.warn('Update failed on SetFeature, attempting delete/insert')
                            #let delete and insert error handlers take care of any further exceptions
                            e1 = self.deleteFeature(dst_layer,src_feat,ref_pkey)
                            e2 = self.insertFeature(dst_layer,src_feat,new_feat_def)
                            if e1+e2 != 0:
                                raise InvalidFeatureException("Driver Error [d="+str(e1)+",i="+str(e2)+"] on "+change)
                    
                    
                    next_feat = src_layer.GetNextFeature()
                    #On no new features grab the last primary key index and break
                    if next_feat is None:
                        if hasattr(self.src_link, 'pkey'):
                            max_index = src_feat.GetField(self.src_link.pkey)
                        break
                    else:
                        src_feat = next_feat
                    

            #self._showLayerData(dst_layer)
            
            '''Builds an index on a newly created layer'''
            #May need to be pushed out to subclasses depending on syntax differences
            if new_layer and ref_index is not None and ref_pkey is not None:
                self.buildIndex(ref_index,ref_pkey,ref_gcol,dst_layer_name)
            
            dst_layer.CommitTransaction()
            src_layer.ResetReading()
            dst_layer.ResetReading()
            
            
        return max_index          

    def transformSRS(self,src_layer_sref):
        '''Defines the transform from one SRS to another. Doesn't actually do the transformation, just defines the transformation needed.
        Requires the supplied EPSG be correct and coordinates can be transformed'''
        self.transform = None
        selected_sref = self.getSRS()
        if selected_sref is not None and not all(i in string.whitespace for i in selected_sref):
            #if the selected SRS fails to validate assume error and flag but dont silently drop back to default
            validated_sref = Projection.validateEPSG(selected_sref)
            if validated_sref is not None:
                self.transform = osr.CoordinateTransformation(src_layer_sref, validated_sref)
            else:
                ldslog.warn("Unable to validate selected SRS, epsg="+str(selected_sref))
        else:
            return src_layer_sref
                    
    
    def insertFeature(self,dst_layer,src_feat,new_feat_def):
        '''insert a new feature'''
        new_feat = self.partialCloneFeature(src_feat,new_feat_def)
        
        e = dst_layer.CreateFeature(new_feat)

        #dst_fid = new_feat.GetFID()
        #ldslog.debug("INSERT: "+str(dst_fid))
        return e
    
    def updateFeature(self,dst_layer,src_feat,new_feat_def,ref_pkey):
        '''build new feature, assign it the looked-up matching fid and overwrite on dst'''
        if ref_pkey is None:
            ref_pkey = self.getFieldNames(src_feat)
            src_pkey = self.getFieldValues(src_feat)
        else:
            src_pkey = src_feat.GetFieldAsInteger(ref_pkey)
        
        #ldslog.debug("UPDATE: "+str(src_pkey))
        #if not new_layer_flag: 
        new_feat = self.partialCloneFeature(src_feat,new_feat_def)
        dst_fid = self._findMatchingFID(dst_layer, ref_pkey, src_pkey)
        if dst_fid is not None:
            new_feat.SetFID(dst_fid)
            e = dst_layer.SetFeature(new_feat)
            
        else:
            ldslog.error("No match for FID with ID="+str(src_pkey)+" on update",exc_info=1)
            raise InvalidFeatureException("No match for FID with ID="+str(src_pkey)+" on update")
        
        return e
    
    def deleteFeature(self,dst_layer,src_feat,ref_pkey): 
        '''lookup and delete using fid matching ID of feature being deleted'''
        #naive first implementation, might/will be slow 
        if ref_pkey is None:
            ref_pkey = self.getFieldNames(src_feat)
            src_pkey = self.getFieldValues(src_feat)
        else:
            src_pkey = src_feat.GetFieldAsInteger(ref_pkey)
            
        #ldslog.debug("DELETE: "+str(src_pkey))
        dst_fid = self._findMatchingFID(dst_layer, ref_pkey, src_pkey)
        if dst_fid is not None:
            e = dst_layer.DeleteFeature(dst_fid)
        else:
            ldslog.error("No match for FID with ID="+str(src_pkey)+" on delete",exc_info=1)
            raise InvalidFeatureException("No match for FID with ID="+str(src_pkey)+" on delete")
        
        return e
        
    def getFieldNames(self,feature):  
        '''Returns the names of fields in a feature'''
        fnlist = ()
        fdr = feature.GetDefnRef()
        for i in range(0,fdr.GetFieldCount()):
            fnlist += (fdr.GetFieldDefn(i).GetName(),)
        return fnlist
    
    def getFieldValues(self,feature):  
        '''Returns field values for a feature'''
        fvlist = ()
        for i in range(0,feature.GetFieldCount()):
            fvlist += (feature.GetFieldAsString(i),)
        return fvlist
 
                      
    def buildNewDataLayer(self,dst_layer_name,dst_ds,dst_sref,src_layer_defn,src_layer_geom,src_layer_sref,ref_layer_name):        
        '''Constructs a new layer using another source layer as a template. This does not populate that layer'''
        #read defns of each field
        fdef_list = []
        for fi in range(0,src_layer_defn.GetFieldCount()):
            fdef_list.append(src_layer_defn.GetFieldDefn(fi))
        
        #use the field defns to build a schema since this needs to be loaded as a create_layer option
        opts = self.getOptions(ref_layer_name)
        #NB wkbPolygon = 3, wkbMultiPolygon = 6
        dst_layer_geom = ogr.wkbMultiPolygon if src_layer_geom is ogr.wkbPolygon else self.selectValidGeom(src_layer_geom)
        
        '''build layer replacing poly with multi and revert to def if that doesn't work'''
        try:
            #gs = 'GEOGCS'
            #sr = osr.SpatialReference('EPSG:4167')
            #ac = sr.GetAuthorityCode(None)
            dst_layer = dst_ds.CreateLayer(dst_layer_name, dst_sref, dst_layer_geom,opts)
        except RuntimeError as re:
            ldslog.error("Cannot create layer. "+str(re))
            if 'already exists' in str(re):
                '''indicates the table has been created previously but was not returned with the getlayer command, SL does this with null geom tables'''
                #raise ASpatialFailureException('SpatiaLite driver cannot be used to update ASpatial layers')
                #NB. DeleteLayer also wont work since the layer can't be found.
                #dst_ds.DeleteLayer(dst_layer_name)
                #dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,src_layer_geom,opts)
                #Option 2. Deleting the layer with SQL
                self.executeSQL('drop table '+dst_layer_name)
                dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,src_layer_geom,opts)
            elif 'General function failure' in str(re):
                ldslog.error('Possible SR problem, continuing. '+str(re))
                dst_layer = None
            
        #if we fail through to this point most commonly the problem is SpatialRef
        if dst_layer is None:
            #overwrite the dst_sref if its causing trouble (ie GDAL general function errors)
            dst_sref = Projection.getDefaultSpatialRef()
            ldslog.warning("Could not initialise Layer with specified SRID {"+str(src_layer_sref)+"}.\n\nUsing Default {"+str(dst_sref)+"} instead")
            dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,dst_layer_geom,opts)
                
        #if still failing, give up
        if dst_layer is None:
            ldslog.error(dst_layer_name+" cannot be created")
            raise LayerCreateException(dst_layer_name+" cannot be created")
    
        
        '''if the dst_layer isn't empty it's probably not a new layer and we shouldn't be adding stuff to it'''
        if len(dst_layer.schema)>0:
            return (dst_layer,False)
        
        '''setup layer headers for new layer etc'''
        for fdef in fdef_list:
            #print "field:",fi
            name = fdef.GetName()
            if name not in self.optcols and name not in [field.name for field in dst_layer.schema]:
                #dst_layer.CreateField(fdef)
                '''post create alter column type'''
                if self.identify64Bit(name):
                    #self.changeColumnIntToString(dst_layer_name,name)
                    new_field_def = ogr.FieldDefn(name,ogr.OFTString)
                    dst_layer.CreateField(new_field_def)
                else:
                    dst_layer.CreateField(fdef)
                    
                #could check for any change tags and throw exception if none
                
        return (dst_layer,True)
    
    def selectValidGeom(self,geom):
        '''To be overridden, eliminates geometry types that cause trouble for certain driver types'''
        return geom
                           
    def changeColumnIntToString(self,table,column):
        '''Default column type changer, to be overriden but works on PG. Used to change 64 bit integer columns to string''' 
        '''NOTE. No longer used! column change done at build time'''
        self.executeSQL('alter table '+table+' alter '+column+' type character varying')
        
    def identify64Bit(self,name):
        '''Common 64bit column identification function (just picks out the key text 'sufi' in the column name since the sufi-id is the only 64 bit data type in use)'''
        return 'sufi' in name     
                                           
    def partialCloneFeature(self,fin,fout_def):
        '''Builds a feature using a passed in feature definition. Must still ignore discarded columns since they will be in the source'''

        fout = ogr.Feature(fout_def)

        '''Modify input geometry from P to MP'''
        fin_geom = fin.GetGeometryRef()
        if fin_geom is not None:
            #absent geom attribute indicates aspatial
            if fin_geom.GetGeometryType() == ogr.wkbPolygon:
                fin_geom = ogr.ForceToMultiPolygon(fin_geom)
                fin.SetGeometryDirectly(fin_geom)
      
            '''set Geometry transforming if needed'''
            if hasattr(self,'transform') and self.transform is not None:
                #TODO check whether this fin_geom needs to be cloned first
                try:
                    fin_geom.Transform(self.transform)
                except RuntimeError as re:
                    if 'OGR Error' in str(re):
                        ldslog.error('Cannot convert to requested SR. '+str(re))
                        raise
                
            '''and then set the output geometry'''
            fout.SetGeometry(fin_geom)

        #DataStore._showFeatureData(fin)
        #DataStore._showFeatureData(fout)
        '''prepopulate any 64 replacement lists. this is done once per 64bit inclusive layer so not too intensive'''
        if self.sixtyfour and not hasattr(self,'sufi_list'): 
            self.sufi_list = {}
            doc = None
            for fin_no in range(0,fin.GetFieldCount()):
                fin_field_name = fin.GetFieldDefnRef(fin_no).GetName()
                if self.identify64Bit(fin_field_name) and fin_field_name not in self.sufi_list:
                    if doc is None:
                        doc = LDSUtilities.readDocument(self.src_link.getURI())
                    self.sufi_list[fin_field_name] = SUFIExtractor.readURI(doc,fin_field_name)
            
        '''populate non geometric fields'''
        fout_no = 0
        for fin_no in range(0,fin.GetFieldCount()):
            fin_field_name = fin.GetFieldDefnRef(fin_no).GetName()
            #assumes id is the PK, TODO, change to pkey reference
            if fin_field_name == 'id':
                current_id =  fin.GetField(fin_no)
            if self.sixtyfour and self.identify64Bit(fin_field_name): #in self.sixtyfour
                #assumes id occurs before sufi in the document
                #sixtyfour test first since identify could be time consuming. Luckily sufi-containing tables are small and process quite quickly                
                copy_field = self.sufi_list[fin_field_name][current_id]
                fout.SetField(fout_no, str(copy_field))
                fout_no += 1
            elif fin_field_name not in self.optcols:
                copy_field = fin.GetField(fin_no)
                fout.SetField(fout_no, copy_field)
                fout_no += 1
            
        return fout 
 
    def partialCloneFeatureDef(self,fin):
        '''Builds a feature definition ignoring optcols i.e. {gml_id, __change__} and any other discarded columns'''
        #create blank feat defn
        fout_def = ogr.FeatureDefn()
        #read input feat defn
        #fin_feat_def = fin.GetDefnRef()
        
        #loop existing feature defn ignoring column X
        for fin_no in range(0,fin.GetFieldCount()):
            fin_field_def = fin.GetFieldDefnRef(fin_no)
            fin_field_name = fin_field_def.GetName()
            if self.identify64Bit(fin_field_name): 
                new_field_def = ogr.FieldDefn(fin_field_name,ogr.OFTString)
                fout_def.AddFieldDefn(new_field_def)
            elif fin_field_name not in self.optcols:
                #print "n={}, typ={}, wd={}, prc={}, tnm={}".format(fin_fld_def.GetName(),fin_fld_def.GetType(),fin_fld_def.GetWidth(),fin_fld_def.GetPrecision(),fin_fld_def.GetTypeName())
                fout_def.AddFieldDefn(fin_field_def)
                
        return fout_def
      
    def getLastModified(self,layer):
        '''Gets the last modification time of a layer to use for incremental "fromdate" calls. This is intended to be run 
        as a destination method since the destination is the DS being modified i.e. dst.getLastModified'''
        lmd = self.layerconf.readLastModified(layer)
        if lmd is None or lmd == '':
            lmd = self.EARLIEST_INIT_DATE
        return lmd
        #return lm.strftime(self.DATE_FORMAT)
        
    def setLastModified(self,layer,newdate):
        '''Sets the last modification time of a layer following a successful incremental copy operation'''
        self.layerconf.writeLayerProperty(layer, 'lastmodified', newdate)  
        
    def clearLastModified(self,layer):
        '''Clears the last modification time of a layer following a successful clean operation'''
        self.layerconf.writeLayerProperty(layer, 'lastmodified', None)  

    def getCurrent(self):
        '''Gets the current timestamp for incremental todate calls. 
        Time format is UTC for LDS compatibility.
        NB. Because the current date is generated to build the LDS URI the lastmodified time will reflect the request time and not the layer creation time'''
        dpo = datetime.utcnow()
        return dpo.strftime(self.DATE_FORMAT)  
    
    def buildIndex(self,ref_index,ref_pkey,ref_gcol,dst_layer_name):
        '''Default index string builder for new fully replicated layers'''
        ref_index = DataStore.parseStringList(ref_index)
        if ref_index.intersection(set(('spatial','s'))) and ref_gcol is not None:
            cmd = 'CREATE INDEX {}_SK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+ref_gcol,dst_layer_name,ref_gcol)
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

    
    # private methods
        
    def executeSQL(self,sql):
        '''Executes arbitrary SQL on the datasource'''
        '''Tagged private since we only want it called from well controlled methods'''
        '''TODO. step through multi line queries?'''
        retval = None
        #ogr.UseExceptions()
        ldslog.debug("SQL: "+sql)
        '''validating sql as a block acts as a sort of transaction mechanism and means we can execute the entire statement which is faster'''
        if self._validateSQL(sql):
            try:
                #cast to STR since unicode raises exception in driver 
                retval = self.ds.ExecuteSQL(str(sql))
            except RuntimeError as rex:
                ldslog.error("Runtime Error. Unable to execute SQL:"+sql+". Get Error "+str(rex),exc_info=1)
                #this can be a bad thing so we want to stop if this occurs e.g. no lds_config -> no layer list etc
                #but also indicate no problem, e.g. deleting a layer already deleted
                raise
            except Exception as ex:
                ldslog.error("Exception. Unable to execute SQL:"+sql+". Exception: "+str(ex),exc_info=1)
                #raise#often misreported, halting may be unnecessary
                
        return retval
            
    def _validateSQL(self,sql):
        '''Validates SQL against a list of allowed queries. Not trying to restrict queries here, rather catch invalid SQL'''
        '''TODO. Better validation.'''
        sql = sql.lower()
        for line in sql.split('\n'):
            #ignore comments/blanks
            if re.match('^(?:#|--)|^\s*$',line):
                continue
            #first match 'create/drop index'
            if re.match('(?:create|drop)(?:\s+spatial)?\s+index',line):
                continue
            #match 'create/drop index/table'
            if re.match('(?:create|drop)\s+(?:index|table)',line):
                continue
            #match 'select'
            if re.match('select\s+(?:\w+|\*)\s+from',line):
                continue
            #match 'insert'
            if re.match('(?:update|insert)\s+(?:\w+|\*)\s+',line):
                continue
            if re.match('if\s+object_id\(',line):
                continue
            #MSSQL insert identity flag
            if re.match('set\s+identity_insert',line):
                continue
            #match 'alter table'
            if re.match('alter\s+table',line):
                continue
            
            ldslog.error("Line in SQL failed to validate. "+line)
            return False
        
        return True
        
    def _cleanLayerByIndex(self,ds,layer_i):
        '''Deletes a layer from the DS using the DS sequence number. Not tested!'''
        ldslog.info("DS clean (ds_seq)")
        try:
            ds.DeleteLayer(layer_i)
        except ValueError as ve:
            ldslog.error('Error deleting layer with index '+str(layer_i)+'. '+str(ve))
            #since we dont want to alter lastmodified on failure
            return False
        return True
    
    def _cleanLayerByRef(self,ds,layer):
        '''Deletes a layer from the DS using the layer reference ie. v:x###'''

        #when the DS is created it uses (PG) the active_schema which is the same as the layername schema.
        #since getlayerX returns all layers in all schemas we ignore the ones with schema prepended since they wont be 'active'
        name = self.generateLayerName(self.layerconf.readLayerProperty(layer,'name')).split('.')[-1]
        try:
            for li in range(0,self.ds.GetLayerCount()):
                lref = ds.GetLayerByIndex(li)
                lname= lref.GetName()
                if lname == name:
                    ds.DeleteLayer(li)
                    ldslog.info("DS clean "+str(lname))
                    #since we only want to alter lastmodified on success return flag=True
                    #we return here too since we assume user only wants to delete one layer, re-indexing issues occur for more than one deletion
                    return True
            ldslog.warning('Matching layer name not found, '+name+'. Attempting base level delete.')
            if self._baseDeleteLayer(name):
                ldslog.error('Unable to clean layer, '+str(self.layer))
                raise DatasourceOpenException('Unable to clean layer, '+str(self.layer))
            return True
                
                    
        except ValueError as ve:
            ldslog.error('Error deleting layer '+str(layer)+'. '+str(ve))
            raise
        except Exception as e:
            ldslog.error("Generic error in layer "+str(layer)+' delete. '+str(e))
            raise
        return False
    
    def _baseDeleteLayer(self,table):
        '''Basic layer delete function intended for aspatial tables which are not returned by queries to the DS. Should work on most DS types'''
        #TODO. Implement for all DS types
        sql_str = "drop table "+table
        return self.executeSQL(sql_str)    
    
    def _baseDeleteColumn(self,table,column):
        '''Basic column delete function for when regular deletes fail. Intended for aspatial tables which are not returned by queries to the DS'''
        #TODO. Implement for all DS types
        sql_str = "alter table "+table+" drop column "+column
        return self.executeSQL(sql_str)
        
    def _clean(self):
        '''Deletes the entire DS layer by layer'''
        #for PG, indices decrement as layers are deleted so delete i=0, N times
        for li in range(0,self.ds.GetLayerCount()):
            if self._cleanLayerByIndex(self.ds,0):
                self.clearLastModified(li)
        
    def _findMatchingFID(self,search_layer,ref_pkey,key_val):
        '''Find the FID matching a primary key value'''
        if isinstance(ref_pkey,basestring):
            newf = self._findMatchingFeature(search_layer,ref_pkey,key_val)
        else:
            newf = self._findMatchingFeature_AllFields(search_layer,ref_pkey,key_val)
        if newf is None:
            return None
        return newf.GetFID()
    
    def _findMatchingFeature_AllFields(self,search_layer,col_list,row_vals):
        '''
        find a feature for a layer with no PK, to do this generically we have to query all fields'''
        qt = ()
        for col,val in zip(col_list,row_vals):
            if col not in self.optcols and val is not '':
                qt += (str(col)+" = '"+str(val)+"'",)        
        search_layer.SetAttributeFilter(' and '.join(qt).replace("''","'"))
        #ResetReading to fix MSSQL ODBC bug, "Function Sequence Error"  
        search_layer.ResetReading()
        return search_layer.GetNextFeature()
           
    def _findMatchingFeature(self,search_layer,ref_pkey,key_val):
        '''Find the Feature matching a primary key value'''
        qry = ref_pkey+" = '"+str(key_val)+"'"
        search_layer.SetAttributeFilter(qry)
        #ResetReading to fix MSSQL ODBC bug, "Function Sequence Error"  
        search_layer.ResetReading()
        return search_layer.GetNextFeature()
            

# static utility methods
    
    @staticmethod
    def sanitise(name):
        '''Manually substitute potential table naming errors implemented as a common function to retain naming convention across all outputs.
        No guarantees are made that this feature won't cause naming conflicts e.g. A-B-C -> a_b_c <- a::{b}::c'''
        #append _ to name beginning with a number
        if re.match('\A\d',name):
            name = "_"+name
        #replace unwanted chars with _ and compress multiple and remove trailing
        sanitised = re.sub('_+','_',re.sub('[ \-,.\\\\/:;{}()\[\]]','_',name.lower())).rstrip('_')
        #unexpected name substitutions can be a source of bugs, log as debug
        ldslog.debug("Sanitise: raw="+name+" name="+sanitised)
        return sanitised
    
    @staticmethod
    def parseStringList(st):
        '''QaD List-as-String to List parser'''
        return set(st.lower().rstrip(')]}').lstrip('{[(').split(','))
    
    # debugging methods
    
    @staticmethod
    def _showFeatureData(feature):
        '''Prints feature/fid info. Useful for debugging'''
        ldslog.debug("Feat:FID:"+str(feature.GetFID()))
        ldslog.debug("Feat:Geom:"+str(feature.GetGeometryRef().GetGeometryType()))
        for field_no in range(0,feature.GetFieldCount()):
            ldslog.debug("fid={},fld_no={},fld_data={}".format(feature.GetFID(),field_no,feature.GetFieldAsString(field_no)))
            
    @staticmethod
    def _showLayerData(layer):
        '''Prints layer and embedded feature data. Useful for debugging'''
        ldslog.debug("Layer:Name:"+layer.GetName())
        layer.ResetReading()
        feat = layer.GetNextFeature()
        while feat is not None:
            DataStore._showFeatureData(feat)
            feat = layer.GetNextFeature()                
                
# INTERNAL CONFIG SECTION. The config section is written as part of the datastore to take advantage  
# of its connection features when the internal config options is chosen. Consider peeling this off into a subclass

    def setupLayerConfig(self):
        '''Read internal OR external from main config file and set, default to internal'''
        
        if 'external' in map(lambda x: x.lower() if type(x) is str else x,self.mainconf.readDSParameters(self.DRIVER_NAME)):
            self.setConfExternal()
        else:
            self.setConfInternal()

    def buildConfigLayer(self,config_array):
        '''Builds the config table into and using the active DS'''
        #TODO check initds for conf table name
        if not hasattr(self,'ds') or self.ds is None:
            self.ds = self.initDS(self.destinationURI(DataStore.LDS_CONFIG_TABLE))  
            
        #bypass (probably not needed) if external (alternatively set [layerconf = self or layerconf = self.mainconf])
        if not self.isConfInternal():
            return self.layerconf.buildConfigLayer()

        try:
            self.ds.DeleteLayer(DataStore.LDS_CONFIG_TABLE)
        except Exception as e:
            ldslog.warn("Exception deleting config layer: "+str(e))
        
        config_layer = self.ds.CreateLayer(DataStore.LDS_CONFIG_TABLE,None,self.getConfigGeometry(),['OVERWRITE=YES'])

        
        feat_def = ogr.FeatureDefn()
        for name in self.CONFIG_COLUMNS:
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
            #    pass
            config_feat.SetField(self.CONFIG_COLUMNS[0],str(row[0]))
            config_feat.SetField(self.CONFIG_COLUMNS[1],str(row[1]))
            config_feat.SetField(self.CONFIG_COLUMNS[2],str(row[2]))
            config_feat.SetField(self.CONFIG_COLUMNS[3],str(','.join(row[3])))
            config_feat.SetField(self.CONFIG_COLUMNS[4],str(row[4]))
            config_feat.SetField(self.CONFIG_COLUMNS[5],str(row[5]))
            config_feat.SetField(self.CONFIG_COLUMNS[6],str(row[6]))
            config_feat.SetField(self.CONFIG_COLUMNS[7],str(row[7]))
            config_feat.SetField(self.CONFIG_COLUMNS[8],None if row[8] is None else str(','.join(row[8])))
            config_feat.SetField(self.CONFIG_COLUMNS[9],str(row[9]))
            
            config_layer.CreateFeature(config_feat)
            
        config_layer.ResetReading()
        config_layer.SyncToDisk()

        
    def getConfigGeometry(self):
        return ogr.wkbNone;
        
    def getLayerNames(self):
        '''Returns configured layers for respective layer properties file'''
        namelist = ()
        layer = self.ds.GetLayer(DataStore.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = layer.GetNextFeature() 
        while feat is not None:
            namelist += (feat.GetField('id'),)
            feat = layer.GetNextFeature()
        
        return namelist
     
    def readLayerParameters(self,pkey):
        '''Full Layer config reader'''
        layer = self.ds.GetLayer(DataStore.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = self._findMatchingFeature(layer, 'id', pkey)
        if feat is None:
            InaccessibleFeatureException('Cannot access feature with id='+str(pkey)+' in layer '+str(layer.GetName()))
        return LDSUtilities.extractFields(feat)
         
    def readLayerProperty(self,pkey,field):
        '''Single property reader'''
        layer = self.ds.GetLayer(DataStore.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = self._findMatchingFeature(layer, 'id', pkey)
        if feat is None:
            return None
        prop = feat.GetField(field)
        return None if prop == 'None' or all(i in string.whitespace for i in prop) else prop

    def writeLayerProperty(self,pkey,field,value):
        '''Write changes to layer config table'''
        #ogr.UseExceptions()
        try:
            layer = self.ds.GetLayer(DataStore.LDS_CONFIG_TABLE)
            feat = self._findMatchingFeature(layer, 'id', pkey)
            feat.SetField(field,value)
            layer.SetFeature(feat)
            ldslog.debug("Check "+field+" for layer "+pkey+" is set to "+value+" : GetField="+feat.GetField(field))
        except Exception as e:
            ldslog.error(e)


        
