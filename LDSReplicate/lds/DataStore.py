'''
v.0.0.1

LDSReplicate -  DataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

DataStore is the base Datasource wrapper object 

Created on 9/08/2012

@author: jramsay
'''

import ogr
import osr
import gdal
import re
import logging
import string

#from osr import CoordinateTransformation
from datetime import datetime
from abc import ABCMeta, abstractmethod

from LDSUtilities import LDSUtilities,SUFIExtractor
from ProjectionReference import Projection
from ConfigWrapper import ConfigWrapper
#from TransferProcessor import CONF_EXT, CONF_INT


ldslog = logging.getLogger('LDS')
#Enabling exceptions halts program on non critical errors i.e. create DS throws exception but builds valid DS anyway 
ogr.UseExceptions()

#exceptions
class DSReaderException(Exception): pass
class LDSReaderException(DSReaderException): pass
class IncompleteWFSRequestException(LDSReaderException): pass
class DriverInitialisationException(LDSReaderException): pass
class DatasourceCopyException(LDSReaderException): pass
class DatasourceCreateException(LDSReaderException): pass
class DatasourceOpenException(DSReaderException): pass
class LayerCreateException(LDSReaderException): pass
class FeatureCopyException(LDSReaderException): pass
class InvalidLayerException(LDSReaderException): pass
class InvalidFeatureException(LDSReaderException): pass
class InvalidSQLException(LDSReaderException): pass
class ASpatialFailureException(LDSReaderException): pass
class UnknownTemporaryDSType(LDSReaderException): pass
class MalformedConnectionString(DSReaderException): pass
class InaccessibleLayerException(DSReaderException): pass
class InaccessibleFeatureException(DSReaderException): pass
class DatasourcePrivilegeException(DSReaderException): pass



class DataStore(object):
    '''
    DataStore superclasses PostgreSQL, LDS(WFS), FileGDB and SpatiaLite datastores.
    This class contains the main copy functions for each datasource and sets up default connection parameters. Common options are also set up in this class 
    but variations are implemented in the appropriate subclasses
    '''
    __metaclass__ = ABCMeta


    DRIVER_NAMES = {'pg':'PostgreSQL','ms':'MSSQLSpatial','sl':'SQLite','fg':'FileGDB'}
    
    LDS_CONFIG_TABLE = 'lds_config'
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
    EARLIEST_INIT_DATE = '2000-01-01T00:00:00'
    #Number of retry attempts before abandoning replication completely
    MAXIMUM_WFS_ATTEMPTS = 5
    #Number of retry attempts before abandoning transactions (slower but more likely to succeed)
    TRANSACTION_THRESHOLD_WFS_ATTEMPTS = 3
    
    DRIVER_NAME = '<init in subclass>'
    
    DEFAULT_IE = 'external'
    
    CONFIG_COLUMNS = ('id','pkey','name','category','lastmodified','geocolumn','index','epsg','discard','cql')
    #TEMP_DS_TYPES = ('Memory','ESRI Shapefile','Mapinfo File','GeoJSON','GMT','DXF')
    
    ValidGeometryTypes = (ogr.wkbUnknown, ogr.wkbPoint, ogr.wkbLineString,
                      ogr.wkbPolygon, ogr.wkbMultiPoint, ogr.wkbMultiLineString, 
                      ogr.wkbMultiPolygon, ogr.wkbGeometryCollection, ogr.wkbNone, 
                      ogr.wkbLinearRing, ogr.wkbPoint25D, ogr.wkbLineString25D,
                      ogr.wkbPolygon25D, ogr.wkbMultiPoint25D, ogr.wkbMultiLineString25D, 
                      ogr.wkbMultiPolygon25D, ogr.wkbGeometryCollection25D)
    
    CONF_INT = 'internal'
    CONF_EXT = 'external'
    DEFAULT_CONF = CONF_EXT #Definitive default declaration
    
    ITYPES = LDSUtilities.enum('QUERYONLY','QUERYMETHOD','METHODONLY')
    
    def __init__(self,conn_str=None,user_config=None):
        '''
        Constructor inits driver and some date specific settings. Arguments are for config overrides 
        '''

        #PYLINT. Set by TP but defined here. Not sure I agree with this requirement since it enforces specific instantiation order
        self.layer = None
        self.layerconf = None
        self.OVERWRITE = None
        self.incremental = None
        self.driver = None
        self.uri = None
        self.cql = None
        self.srs = None
        self.config = None
        self.src_link = None
        self.sufi_list = None # should be unique per layer
        self.ds = None
        self.transform = None
        self.sixtyfour = None
        self.conn_str = None
        
        #self.CONFIG_XSL = "getcapabilities."+self.DRIVER_NAME.lower()+".xsl"#we use just 'file' or 'json' now
         
        if LDSUtilities.mightAsWellBeNone(conn_str) is not None:
            self.conn_str = conn_str
        
        self.setSRS(None)
        self.setFilter(None)     

        self.setOverwrite()
        
        self.getDriver(self.DRIVER_NAME)
        #NB. mainconf here isnt the same as the main/user distinction in the ConfigWrapper    
        self.mainconf = ConfigWrapper(user_config)
        
        self.params = self.mainconf.readDSParameters(self.DRIVER_NAME)
        
        '''set of <potential> columns not needed in final output, global'''
        self.optcols = set(['__change__','gml_id'])
        

    #incr flag copied straight from Datastore
    #def setIncremental(self,itype):
    #    if itype in self.ITYPES:
    #        self.incremental = itype
            
    def clearIncremental(self):
        self.incremental = False
        gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED','OFF')
            
    def setIncremental(self):
        self.incremental = True
        gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED','ON')
         
    def getIncremental(self):
        return self.incremental 
     
    def applyConfigOptions(self):
        for opt in self.getConfigOptions():
            ldslog.info('Applying '+self.DRIVER_NAME+' option '+opt)
            k,v = str(opt).split('=')
            gdal.SetConfigOption(k.strip(),v.strip())
             
    def getDriver(self,driver_name):

        self.driver = ogr.GetDriverByName(driver_name)
        if self.driver == None:
            raise DriverInitialisationException, "Driver cannot be initialised for type "+driver_name
            
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
    
    def setConfInternal(self,config):
        self.config = config
         
    def getConfInternal(self):
        return self.config       
    
    #--------------------------  
    
    def setOverwrite(self):
        self.OVERWRITE = "YES"
         
    def clearOverwrite(self):
        self.OVERWRITE = "NO"
         
    def getOverwrite(self):
        return self.OVERWRITE   
    
    def getLayerConf(self):
        return self.layerconf
    
    def setLayerConf(self,layerconf):
        self.layerconf = layerconf
    
    #options sections ----------------------------------------
    def getConfigOptions(self):
        '''Returns common gdal operating options, overridden in subclasses for source specifc options'''
        return []  
    
    def getDBOptions(self):
        '''Returns database creation options (used by spatialite)'''
        return []    
    
    def getLayerOptions(self,layer_id):
        '''Returns common layer options, overridden in subclasses for source specifc options'''
        #layer_id used in some subclasses
        return ['OVERWRITE='+self.getOverwrite()]#,'OGR_ENABLE_PARTIAL_REPROJECTION=True']
    
    
    @abstractmethod
    def sourceURI(self,layer):
        '''Abstract URI method for returning source. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method sourceURI not implemented")
    
#    @abstractmethod
#    def sourceURI_incrd(self,layer):
#        '''Abstract URI method for returning source. Raises NotImplementedError if accessed directly'''
#        raise NotImplementedError("Abstract method sourceURI_incrd not implemented")
    
    @abstractmethod
    def destinationURI(self,layer):
        '''Abstract URI method for returning destination. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method destinationURI not implemented")
    
    
    @abstractmethod
    def validateConnStr(self,conn_str):
        '''Abstract method to check user supplied connection strings. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method destinationURI not implemented")
    
    def testConnection(self):
        '''Test connection to a (typically database) source'''
        return True
    
    def initDS(self,dsn=None,create=True):
        '''Initialise the data source calling a provided DSN or self.dsn and a flag to indicate whether we should try and create a DS if none found'''
        #Notes
        #1. FGDB.driver will Open() a directory if its a valid FGDB dir, it will not Open() an empty directory. It will not CreateDataSource() an existing directory, valid or empty
        from WFSDataStore import WFSDataStore
        ds = None
        '''initialise a DS for writing'''
        try:
            #we turn ogr exceptions off here so reported errors don't kill DS initialisation 
            ogr.DontUseExceptions()
            ds = self.driver.Open(LDSUtilities.percentEncode(dsn) if self.DRIVER_NAME==WFSDataStore.DRIVER_NAME else dsn, update = 1 if self.getOverwrite()=='YES' else 0)       
            if ds is None:
                raise DSReaderException("Error opening DS "+str(dsn)+(', attempting DS create.' if create else '.'))
        except (RuntimeError,DSReaderException) as dsre1:
            #print "DSReaderException",dsre1 
            ldslog.error('Open '+str(dsn)+' throws '+str(dsre1),exc_info=1)
            if create:
                try:
                    ds = self.driver.CreateDataSource(dsn, self.getDBOptions())
                    if ds is None:
                        raise DSReaderException("Error opening/creating DS "+str(dsn))
                except DSReaderException as dsre2:
                    #print "DSReaderException, Cannot create DS.",dsre2
                    ldslog.error(dsre2,exc_info=1)
                    raise
                except RuntimeError as rte:
                    '''this is only caught if ogr.UseExceptions() is enabled (which we done enable since RunErrs thrown even when DS completes)'''
                    #print "GDAL RuntimeError. Error creating DS.",rte
                    ldslog.error(rte,exc_info=1)
                    raise
            else:
                raise dsre1
        finally:
            ogr.UseExceptions()
        return ds
        
    def read(self,dsn,create=True):
        '''Main DS read method'''
        ldslog.info("DS read "+dsn)#.split(":")[0])
        #5050 initDS for consistency and utilise if-ds-is-none check OR quick open and overwrite
        self.ds = self.initDS(dsn,create)
        #self.ds = self.driver.Open(dsn)
    
    def write(self,src,dsn,sixtyfour):
        '''Main DS write method. Attempts to open or alternatively, create a datasource'''
        #mild hack. src_link created so we can re-query the source as a doc to get 64bit ints as strings
        self.src_link = src
        #we need to store 64 beyond fC/dC flag to identify need for sufi-to-str conversion
        self.sixtyfour = sixtyfour
        #Clear sufi list between consecutive calls to reinit on different layers
        self.sufi_list = None
        self.attempts = 0
        
        while self.attempts < self.MAXIMUM_WFS_ATTEMPTS:
            try:
                ldslog.info('PAGING1 = '+str(gdal.GetConfigOption('OGR_WFS_PAGING_ALLOWED')))
                #if incr&haspk then fCi
                if self.getIncremental():
                    # standard incremental featureCopyIncremental. change_col used in delete list and as change (INS/DEL/UPD) indicator
                    #gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED','ON')
                    self.featureCopyIncremental(self.src_link.ds,self.ds,self.src_link.CHANGE_COL)
                else:
                    #gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED','OFF') 
                    self.featureCopy(self.src_link.ds,self.ds)
                
            except (FeatureCopyException, InaccessibleFeatureException, RuntimeError) as rte:
                em = gdal.GetLastErrorMsg()
                en = gdal.GetLastErrorNo()
                ldslog.warn("ErrorMsg: "+str(em))
                ldslog.warn("ErrorNo: "+str(en))
                #Errors below seem to all indicate server load problems, so we try again
                if self.attempts < self.MAXIMUM_WFS_ATTEMPTS-1 and ( \
                    re.search(   'Function sequence error',str(rte)) \
                    or re.search('HTTP error code : 504',str(rte)) \
                    or re.search('HTTP error code : 502',str(rte)) \
                    or re.search('HTTP error code : 404',str(rte)) \
                    or re.search('General Error',str(rte)) \
                    or re.search('Empty content returned by server',str(rte)) \
                    or re.search('Feature count mismatch',str(rte)) \
                    or re.search('Cannot access any Features',str(rte))):
                    self.attempts += 1
                    attcount = str(self.attempts)+"/"+str(self.MAXIMUM_WFS_ATTEMPTS)
                    ldslog.warn("Failed LDS fetch attempt "+attcount+". "+str(rte))
                    print '*** Att '+attcount+'  *** '+str(datetime.now().isoformat())
                    #re-initialise one/all of the datasources
                    src.read(src.getURI(),False)
                    #self.read(self.getURI(),False)
                    
                else: 
                    ldslog.error(rte,exc_info=1)
                    raise
            else:
                break

        
    def closeDS(self):
        '''close a DS with sync and destroy'''
        ldslog.info("Sync DS and Close")
        self.ds.SyncToDisk()
        #FileGDB locks up on destroy, do we even need this? Supposedly for backward compatibility
        #self.ds.Destroy()  
        self.ds = None
    
        
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
        and PostgreSQL implements an "active_schema" option bypassing the need for a schema declaration'''
        return self.sanitise(ref_name)
        
    #--------------------------------------------------------------------------            
    
    def featureCopy(self,src_ds,dst_ds):
        '''Feature copy without the change column (and other incremental) overhead. Replacement for driverCopy(cloneDS).''' 
        for li in range(0,src_ds.GetLayerCount()):
            is_new = False
            transaction_flag = True
            src_layer = src_ds.GetLayer(li)
            src_feat_count = None
            
            src_info = LayerInfo(LDSUtilities.cropChangeset(src_layer.GetName()))
            
            '''retrieve per-layer settings from props'''
            #(ref_pkey,ref_name,ref_group,ref_gcol,ref_index,ref_epsg,ref_lmod,ref_disc,ref_cql) = self.layerconf.readLayerParameters(src_layer_name)
            layerconfentry = self.layerconf.readLayerParameters(src_info.layer_id)
            
            dst_info = LayerInfo(src_info.layer_id,self.generateLayerName(layerconfentry.name))
            
            ldslog.info("Dest layer: "+dst_info.layer_id)
            
            '''parse discard columns'''
            self.optcols |= set(layerconfentry.disc.strip('[]{}()').split(',') if layerconfentry.disc is not None else [])

            try:
                dst_layer = dst_ds.GetLayer(dst_info.layer_name)
            except RuntimeError as rer:
                '''Instead of returning none, runtime errors sometimes occur if the layer doesn't exist and needs to be created or has no data'''
                ldslog.warning("Runtime Error fetching layer. "+str(rer))
                dst_layer = None
  
            #NB. this has been modified since replacing 'clean' with 'truncate' since a layer may now exists when creating a layer from scratch
            if dst_layer is None:
                ldslog.warning("Non-Incremental layer ["+dst_info.layer_id+"] request. (re)Creating layer")
                '''create a new layer if a similarly named existing layer can't be found on the dst'''
                src_info.spatial_ref = src_layer.GetSpatialRef()
                src_info.geometry = src_layer.GetGeomType()
                src_info.layer_defn = src_layer.GetLayerDefn()
                #transforms from SRC to DST sref if user requests a different EPSG, otherwise SRC returned unchanged
                dst_info.spatial_ref = self.transformSRS(src_info.spatial_ref)
                
                (dst_layer,is_new) = self.buildNewDestinationLayer(dst_info,src_info,dst_ds)
                

            if  dst_layer.TestCapability('Transactions') and self.attempts < self.TRANSACTION_THRESHOLD_WFS_ATTEMPTS:
                dst_layer.StartTransaction()
            else:
                transaction_flag = False
                ldslog.warn('Transactions Disabled')
                
            #add/copy features
            #src_layer.ResetReading()
            dst_change_count = 0
            src_feat_count = src_layer.GetFeatureCount()
            ldslog.info('Features available = '+str(src_feat_count))

            '''since the characteristics of each feature wont change between layers we only need to define a new feature definition once'''
            if src_feat_count>0:
                src_feat = src_layer.GetNextFeature()
                new_feat_def = self.partialCloneFeatureDef(src_feat)
            elif src_feat_count>0:
                raise InaccessibleFeatureException('Cannot access any Features of '+str(src_feat_count)+' available')
                
            while src_feat is not None:
                dst_change_count += 1
                #slowest part of this copy operation is the insert since we have to build a new feature from defn and check fields for discards and sufis
                self.insertFeature(dst_layer,src_feat,new_feat_def,layerconfentry.pkey)
                
                src_feat = src_layer.GetNextFeature()
            
            if src_feat_count is not None and src_feat_count != dst_change_count:
                if self.attempts < self.TRANSACTION_THRESHOLD_WFS_ATTEMPTS:
                    dst_layer.RollbackTransaction()
                raise FeatureCopyException('Feature count mismatch. Source count['+str(src_feat_count)+'] <> Change count['+str(dst_change_count)+']')
            
            
            '''Builds an index on a newly created layer if; 
            1) new layer flag is true, 2) index p|s is asked for, 3) we have a pk to use and 4) the layer has replicated at least 1 feat'''
            #May need to be pushed out to subclasses depending on syntax differences
            if is_new and (layerconfentry.gcol or layerconfentry.pkey) and dst_change_count>0:
                self.buildIndex(layerconfentry,dst_info.layer_name)
                
            if transaction_flag:
                try:
                    ogr.DontUseExceptions()
                    dst_layer.CommitTransaction()
                except RuntimeError as rte:
                    #HACK
                    if re.search('General Error',str(rte)):
                        ldslog.warn('CommitTransaction raising OGR General Error. [ '+str(rte)+']')
                    #else:
                        raise
                finally:
                    ogr.UseExceptions()

            
            src_layer.ResetReading()
            dst_layer.ResetReading()    

    
    def featureCopyIncremental(self,src_ds,dst_ds,changecol):
        #TDOD. decide whether C_C is better as an arg or a src.prop
        '''DataStore feature-by-feature replication for incremental queries'''
        #build new layer by duplicating source layers  

        ldslog.info("Using featureCopyIncremental. Per-feature copy")
        for li in range(0,src_ds.GetLayerCount()):
            is_new = False
            transaction_flag = True
            src_layer = src_ds.GetLayer(li)

            #TODO. resolve conflict between lastmodified and fdate
            src_info = LayerInfo(LDSUtilities.cropChangeset(src_layer.GetName()))
            
            '''retrieve per-layer settings from props'''
            #(ref_pkey,ref_name,ref_group,ref_gcol,ref_index,ref_epsg,ref_lmod,ref_disc,ref_cql) = self.layerconf.readLayerParameters(src_layer_name)
            layerconfentry = self.layerconf.readLayerParameters(src_info.layer_id)
            
            dst_info = LayerInfo(src_info.layer_id,self.generateLayerName(layerconfentry.name))

            ldslog.info("Dest layer: "+dst_info.layer_id)
            
            '''parse discard columns'''
            self.optcols |= set(layerconfentry.disc.strip('[]{}()').split(',') if layerconfentry.disc is not None else [])
            
            try:
                if layerconfentry.lmod:
                    #if the layer conf had a lastmodified don't overwrite
                    dst_layer = dst_ds.GetLayer(dst_info.layer_name)
                else:
                    #with no lastmodified can assume the layer doesnt exist
                    src_info.spatial_ref = src_layer.GetSpatialRef()
                    src_info.geometry = src_layer.GetGeomType()
                    src_info.layer_defn = src_layer.GetLayerDefn()
                    dst_info.spatial_ref = self.transformSRS(src_info.spatial_ref)
                    (dst_layer,is_new) = self.buildNewDestinationLayer(dst_info, src_info, dst_ds)
            except RuntimeError as rer:
                '''Instead of returning none, runtime errors sometimes occur if the layer doesn't exist and needs to be created or has no data'''
                ldslog.warning("Runtime Error fetching layer. "+str(rer))
                dst_layer = None
                
            if dst_layer is None:
                #with or without a lmod its still possible the layer doesn't exist or cannot be read
                ldslog.warning(dst_info.layer_id+" does not exist. Creating new layer")
                '''create a new layer if a similarly named existing layer can't be found on the dst'''
                src_info.spatial_ref = src_layer.GetSpatialRef()
                src_info.geometry = src_layer.GetGeomType()
                src_info.layer_defn = src_layer.GetLayerDefn()
                dst_info.spatial_ref = self.transformSRS(src_info.spatial_ref)
                
                (dst_layer,is_new) = self.buildNewDestinationLayer(dst_info,src_info,dst_ds)
                
                if dst_layer is None:
                    #if its still none, bail (and don't bother with re-attempt)
                    raise LayerCreateException('Unable to initialise a new Layer on destination')
                
            #dont bother with transactions if they're failing > N times
            if dst_layer.TestCapability('Transactions') and self.attempts < self.TRANSACTION_THRESHOLD_WFS_ATTEMPTS:
                dst_layer.StartTransaction()
            else:
                transaction_flag = False
                ldslog.warn('Attempting replicate without transactions')

            
            #add/copy features
            insert_count, delete_count, update_count = 0,0,0
            dst_change_count = 0
            src_feat_count = src_layer.GetFeatureCount()
            ldslog.info('Features available = '+str(src_feat_count))
            src_feat = src_layer.GetNextFeature()
            #since the characteristics of each feature wont change between layers we only need to define a new feature definition once
            if src_feat is not None:
                new_feat_def = self.partialCloneFeatureDef(src_feat)
                dst_change_count = 1
                e = 0
                while 1:
                    '''identify the change in the WFS doc (INS,UPD,DEL)'''
                    change =  (src_feat.GetField(changecol) if LDSUtilities.mightAsWellBeNone(changecol) is not None else "insert").lower()
                    '''not just copy but possubly delete or update a feature on the DST layer'''
                    #self.copyFeature(change,src_feat,dst_layer,ref_pkey,new_feat_def,ref_gcol)
                    
                    try:
                        if change == 'insert': 
                            e = self.insertFeature(dst_layer,src_feat,new_feat_def,layerconfentry.pkey)
                            insert_count += 1
                        elif change == 'delete': 
                            e = self.deleteFeature(dst_layer,src_feat,             layerconfentry.pkey)
                            delete_count += 1
                        elif change == 'update': 
                            e = self.updateFeature(dst_layer,src_feat,new_feat_def,layerconfentry.pkey)
                            update_count += 1
                        else:
                            ldslog.error("Error with Key "+str(change)+" !E {ins,del,upd}")
                        #    raise KeyError("Error with Key "+str(change)+" !E {ins,del,upd}",exc_info=1)
                    except InvalidFeatureException as ife:
                        ldslog.error("Invalid Feature Exception during "+change+" operation on dest. "+str(ife),exc_info=1)
                    #except Exception as e:
                    #    ldslog.error('trap new errors here... '+str(e))
                        
                    if e != 0:                  
                        ldslog.error("Driver Error ["+str(e)+"] on "+change,exc_info=1)
                        if change == 'update':
                            ldslog.warn('Update failed on SetFeature, attempting delete+insert')
                            #let delete and insert error handlers take care of any further exceptions
                            e1 = self.deleteFeature(dst_layer,src_feat,             layerconfentry.pkey)
                            e2 = self.insertFeature(dst_layer,src_feat,new_feat_def,layerconfentry.pkey)
                            if e1+e2 != 0:
                                raise InvalidFeatureException("Driver Error [d="+str(e1)+",i="+str(e2)+"] on "+change)
                    

                    next_feat = src_layer.GetNextFeature()
                    #On no-new-features grab the last primary key index and break
                    if next_feat is None:
                        if hasattr(self.src_link, 'pkey'):
                            #this of course assumes the layer is correctly sorted in pkey
                            src_feat.GetField(layerconfentry.pkey)
                        break
                    else:
                        src_feat = next_feat
                        dst_change_count += 1
                    
            if src_feat_count != dst_change_count:
                if transaction_flag:
                    dst_layer.RollbackTransaction()
                raise FeatureCopyException('Feature count mismatch. Source count['+str(src_feat_count)+'] <> Change count['+str(dst_change_count)+']')
                
            #self._showLayerData(dst_layer)
            
            '''Builds an index on a newly created layer if; 
            1) new layer flag is true, 2) index p|s is asked for, 3) we have a pk to use and 4) the layer has at least 1 feat'''
            #Ordinarily pushed out to subclasses depending on syntax differences
            if is_new and (layerconfentry.gcol or layerconfentry.pkey) and dst_change_count>0:
                self.buildIndex(layerconfentry,dst_info.layer_name)
                
            if transaction_flag:
                try:
                    ogr.DontUseExceptions()
                    dst_layer.CommitTransaction()
                except RuntimeError as rte:
                    #HACK
                    if re.search('General Error',str(rte)):
                        ldslog.warn('CommitTransaction raising OGR General Error. [ '+str(rte)+'] Ignoring!')
                    #else:
                        raise
                finally:
                    ogr.UseExceptions()
                
            ldslog.info('Inserts={0}, Deletes={1}, Updates={2}'.format(insert_count,delete_count,update_count))
            
            src_layer.ResetReading()
            dst_layer.ResetReading()
            
        #returning nothing disables manual paging    
        #return max_index          

    def transformSRS(self,src_layer_sref):
        '''Defines the transform from one SRS to another. Doesn't actually do the transformation, just defines the transformation needed.
        Requires the supplied EPSG be correct and coordinates that can be transformed'''
        self.transform = None
        selected_sref = self.getSRS()
        if LDSUtilities.mightAsWellBeNone(selected_sref) is not None:
            #if the selected SRS fails to validate assume error and flag but dont silently drop back to default
            validated_sref = Projection.validateEPSG(selected_sref)
            if validated_sref is not None:
                self.transform = osr.CoordinateTransformation(src_layer_sref, validated_sref)
                if self.transform == None:
                    ldslog.warn('Can\'t init coordinatetransformation object with SRS:'+str(validated_sref))
                return validated_sref
            else:
                ldslog.warn("Unable to validate selected SRS, epsg="+str(selected_sref))
        else:
            return src_layer_sref
                    
    
    def insertFeature(self,dst_layer,src_feat,new_feat_def,ref_pkey):
        '''insert a new feature'''
        new_feat = self.partialCloneFeature(src_feat,new_feat_def,ref_pkey)
        
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
        new_feat = self.partialCloneFeature(src_feat,new_feat_def,ref_pkey)
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
 
                      
    def buildNewDestinationLayer(self,dst_info,src_info,dst_ds):
        '''Constructs a new layer using another source layer as a template. This does not populate that layer'''
        #read defns of each field
        fdef_list = []
        dst_layer = None
        for fi in range(0,src_info.layer_defn.GetFieldCount()):
            fdef_list.append(src_info.layer_defn.GetFieldDefn(fi))
        
        #use the field defns to build a schema since this needs to be loaded as a create_layer option
        opts = self.getLayerOptions(src_info.layer_id)
        #NB wkbPolygon = 3, wkbMultiPolygon = 6
        dst_info.geometry = ogr.wkbMultiPolygon if src_info.geometry is ogr.wkbPolygon else self.selectValidGeom(src_info.geometry)
        
        '''build layer replacing poly with multi and revert to def if that doesn't work'''
        try:
            dst_layer = dst_ds.CreateLayer(dst_info.layer_name, dst_info.spatial_ref, dst_info.geometry, opts)
        except RuntimeError as rer:
            ldslog.error("Cannot create layer. "+str(rer))
            if 'already exists' in str(rer):
                '''indicates the table has been created previously but may not have been returned with the Getlayer command, SL does this with null geom tables'''
                #raise ASpatialFailureException('SpatiaLite driver cannot be used to update ASpatial layers')
                #NB. DeleteLayer also wont work since the layer can't be found.
                #dst_ds.DeleteLayer(dst_layer_name)
                #dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,src_layer_geom,opts)
                #Option 2. Deleting the layer with SQL
                self.executeSQL('drop table '+dst_info.layer_name)
                #TODO check this works on non DB DS
                dst_layer = dst_ds.CreateLayer(dst_info.layer_name,dst_info.spatial_ref,src_info.geometry,opts)
            elif 'General function failure' in str(rer):
                ldslog.error('Possible SR problem, continuing. '+str(rer))
                dst_layer = None
            
        #if we fail through to this point most commonly the problem is SpatialRef
        if dst_layer is None:
            #overwrite the dst_sref if its causing trouble (ie GDAL general function errors)
            dst_info.spatial_ref = Projection.getDefaultSpatialRef()
            ldslog.warning("Could not initialise Layer with specified SRID {"+str(src_info.spatial_ref)+"}.\n\nUsing Default {"+str(dst_info.spatial_ref)+"} instead")
            dst_layer = dst_ds.CreateLayer(dst_info.layer_name,dst_info.spatial_ref,dst_info.geometry,opts)
                
        #if still failing, give up
        if dst_layer is None:
            ldslog.error(dst_info.layer_name+" cannot be created")
            raise LayerCreateException(dst_info.layer_name+" cannot be created")
    
        
        #Some drivers return an existing layer on CreateLayer even though GetLayer returned nothing. 
        #If the dst_layer isn't empty it's probably not a new layer and we shouldn't be adding stuff to it
        #if len(dst_layer.schema)>0:
        #Because layer.schema can throw an exception on SQLServer use GetFeatureCount?
        if dst_layer.GetFeatureCount()>0:
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
        '''To be overridden, eliminates geometry types that cause trouble for certain drivers'''
        return geom
                           
    def changeColumnIntToString(self,table,column):
        '''Default column type changer, to be overriden but works on PG. Used to change 64 bit integer columns to string''' 
        #NOTE. No longer used! column change done at build time
        self.executeSQL('alter table '+table+' alter '+column+' type character varying')
        
    def identify64Bit(self,name):
        '''Common 64bit column identification function (just picks out the key text 'sufi' in the column name since the 
        sufi-id is the only 64 bit data type in use. This is due to change soon with some new hydro layers being added
        that have sufi-ids which aren't 64bit...)'''
        return 'sufi' in name     
                                           
    def partialCloneFeature(self,fin,fout_def,pkey):
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
                except RuntimeError as rer:
                    if 'OGR Error' in str(rer):
                        ldslog.error('Cannot convert to requested SR. '+str(rer))
                        raise
                
            '''and then set the output geometry'''
            fout.SetGeometry(fin_geom)

        #DataStore._showFeatureData(fin)
        #DataStore._showFeatureData(fout)
        '''prepopulate any 64 replacement lists. this is done once per 64bit inclusive layer so not too intensive'''
        if self.sixtyfour and (not hasattr(self,'sufi_list') or self.sufi_list is None): 
            self.sufi_list = {}
            doc = None
            for fin_no in range(0,fin.GetFieldCount()):
                fin_field_name = fin.GetFieldDefnRef(fin_no).GetName()
                if self.identify64Bit(fin_field_name) and fin_field_name not in self.sufi_list:
                    if doc is None:
                        #fetch the GC document in GML2 format for column extraction. #TODO JSON extractor
                        doc = LDSUtilities.readDocument(re.sub('JSON|GML3','GML2',self.src_link.getURI()),self.src_link.pxy)
                    self.sufi_list[fin_field_name] = SUFIExtractor.readURI(doc,fin_field_name)
            
        '''populate non geometric fields'''
        fout_no = 0
        for fin_no in range(0,fin.GetFieldCount()):
            fin_field_name = fin.GetFieldDefnRef(fin_no).GetName()
            #assumes id is the PK
            if fin_field_name == pkey:#'id':
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
        
    def setLastModified(self,layer,newdate=None):
        '''Sets the last modification time of a layer following a successful incremental copy operation'''
        if newdate is None:
            newdate=self.getCurrent()
        self.layerconf.writeLayerProperty(layer, 'lastmodified', newdate)  
        
    def clearLastModified(self,layer):
        '''Clears the last modification time of a layer following a successful clean operation'''
        self.layerconf.writeLayerProperty(layer, 'lastmodified', None)  
    
    @classmethod
    def getCurrent(cls):
        '''Gets the current timestamp for incremental todate calls. 
        Time format is UTC for LDS compatibility.
        NB. Because the current date is generated to build the LDS URI the lastmodified time will reflect the request time and not the layer creation time'''
        dpo = datetime.utcnow()
        return dpo.strftime(cls.DATE_FORMAT)  
    
    @abstractmethod
    def buildIndex(self,lce,dst_layer_name):
        '''Default index string builder for new fully replicated layers'''
        raise NotImplementedError("Abstract method buildIndex not implemented")
#        #TODO. This isn't meant to be run, subclasses only. Left here for reference!
#        ref_index = DataStore.parseStringList(lce.index)
#        if ref_index.intersection(set(('spatial','s'))) and lce.gcol is not None:
#            #cmd = 'CREATE INDEX {}_SK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+lce.gcol,dst_layer_name,lce.gcol)
#            cmd = 'ALTER TABLE {} ADD CONSTRAINT UNIQUE({})'.format(dst_layer_name,lce.gcol)
#        elif ref_index.intersection(set(('primary','pkey','p'))):
#            #cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+lce.pkey,dst_layer_name,lce.pkey)
#            cmd = 'ALTER TABLE {} ADD CONSTRAINT UNIQUE({})'.format(dst_layer_name,lce.pkey)
#        elif ref_index is not None:
#            #maybe the user wants a non pk/spatial index? Try to filter the string
#            clst = ','.join(ref_index)
#            #cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+DataStore.sanitise(clst),dst_layer_name,clst)
#            cmd = 'ALTER TABLE {} ADD CONSTRAINT UNIQUE({})'.format(dst_layer_name,clst)
#        else:
#            return
#        ldslog.info("Index="+','.join(ref_index)+". Execute "+cmd)
#        
#        try:
#            self.executeSQL(cmd)
#        except RuntimeError as rte:
#            if re.search('already exists', str(rte)): 
#                ldslog.warn(rte)
#            else:
#                raise

    
    # private methods
        
    def executeSQL(self,sql):
        '''Executes arbitrary SQL on the datasource'''
        '''Tagged? private since we only want it called from well controlled methods'''
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
                if re.search('does not exist',str(rex)):
                    ldslog.error("Attempt to delete unrecognised table. "+str(rex))
                    return retval
                raise
            except InvalidSQLException as ise:
                #Caused by query not matching valid entry
                ldslog.error("Error executing SQL Command. "+str(ise))
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
            #SL index function'
            if re.match('select\s+createspatialindex',line):
                continue
            #match 'select'
            if re.match('select\s+(?:\w+|\*)\s+from',line):
                continue
            if re.match('select\s+(has_table_privilege|has_schema_privilege|version|postgis_full_version|@@version)',line):
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
            raise InvalidSQLException('SQL '+str(sql)+' failed to validate')
        
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
    
    def _cleanLayerByRef(self,ds,layer,truncate):
        '''Deletes a layer from the DS using the layer reference ie. v:x###'''
        msg = 'truncate' if truncate else 'clean'
        #when the DS is created it uses (PG) the active_schema which is the same as the layername schema.
        #since getlayerX returns all layers in all schemas we ignore the ones with schema prepended since they wont be 'active'
        name = self.generateLayerName(self.layerconf.readLayerProperty(layer,'name')).split('.')[-1]
        try:
            for li in range(0,self.ds.GetLayerCount()):
                lref = ds.GetLayerByIndex(li)
                lname = lref.GetName().split('.')[-1] #strip schema
                if lname == name:
                    if truncate and lref.TestCapability('DeleteFeature'):
                        for fi in range(1,lref.GetFeatureCount()+1):
                            try:
                                lref.DeleteFeature(fi)
                            except RuntimeError as re:
                                ldslog.error("RuntimeError deleting feature {} on layer {}. {}".format(fi,layer,re))
                                ds.DeleteLayer(li)
                                break   
                    else:
                        ds.DeleteLayer(li)
                    ldslog.info("DS {} {}".format(msg,str(lname)))
                    #since we only want to alter lastmodified on success return flag=True
                    #we return here too since we assume user only wants to delete one layer, re-indexing issues occur for more than one deletion
                    return True
            ldslog.warning('Matching layer name not found, '+name+'. Attempting base level delete.')
            try:
                self._baseDeleteLayer(name)
            except:
                raise DatasourceOpenException('Unable to {} layer, {}'.format(msg,str(layer)))
            return True
                
                    
        except ValueError as ve:
            ldslog.error('Value Error doing {} on layer {}. {}'.format(msg,str(layer),str(ve)))
            raise
        except RuntimeError as re:
            ldslog.error("RuntimeError deleting features on layer "+str(layer)+'. '+str(re))
            raise
        except Exception as e:
            ldslog.error("Generic error in layer "+str(layer)+'. '+str(e))
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
           
    def formatWhereClause(self,ref_pkey,key_val):
        return "{0} = '{1}'".format(ref_pkey,key_val)
    
    def _findMatchingFeature(self,search_layer,ref_pkey,key_val):
        '''Find the Feature matching a primary key value'''
        matching_feature = None
        try:
            where = self.formatWhereClause(ref_pkey, key_val)
            search_layer.SetAttributeFilter(where)
            #ResetReading to fix MSSQL ODBC bug, "Function Sequence Error". 
            #NB. Since we're resetting the DST layer it has no affect on the SRC read order, just starts the FID search from the beginning  
            search_layer.ResetReading()
            matching_feature = search_layer.GetNextFeature()
        except RuntimeError as rte:
            ldslog.error('Cant find matching feature using '+str(where)+'. '+str(rte))
            raise

        finally:
            #Once you have a matching feature clear the attribute filter or postgres gets upset
            search_layer.SetAttributeFilter(None)
            
        return matching_feature
        
            
            
            
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
                
#    def transferIETernal(self,override_int_ext):
#        '''Read internal OR external from main config file and set, default to internal'''
#        if override_int_ext is None:
#            #look for 'external' in all the params returned by the mainconf <driver> section (converting to lower case...)
#            #this is because each DS has different parameters in different positions
#            plist = [str(x).lower() for x in self.mainconf.readDSParameters(self.DRIVER_NAME)]
#            if self.CONF_EXT in plist:
#                self.setConfInternal(self.CONF_EXT)
#            elif self.CONF_INT in plist:
#                self.setConfInternal(self.CONF_INT)
#            else:
#                self.setConfInternal(self.DEFAULT_IE)
#        else:
#            #if overriding
#            self.setConfInternal(override_int_ext)
            
    @abstractmethod
    def versionCheck(self):
        '''A version check to be used once the DS have been initialised... if normal checks cant be established eg psql on w32'''
        #Obviously this returns a default True for any subclasses that dont support it
        return True


class LayerInfo(object):
    '''Simple class for layer attributes'''
    def __init__(self,layer_id,layer_name=None,layer_defn=None,spatial_ref=None,geometry=None):
        #to clarify name confusion, id here refers to the layer 'name' read by the layer.GetName fuinction i.e v:xNNNN
        self.layer_id = layer_id
        #but name here refers to the descriptive name e.g. NZ Primary Parcels
        self.layer_name = layer_name
        self.layer_defn = layer_defn
        self.spatial_ref = spatial_ref
        self.geometry = geometry
        
        self.lce = None
        
    def setLCE(self,lce):
        self.lce = lce
        
