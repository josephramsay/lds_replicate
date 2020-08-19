'''
v.0.0.9

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
import os

#from osr import CoordinateTransformation
from datetime import datetime
from abc import ABCMeta, abstractmethod
from difflib import SequenceMatcher

from lds.LDSUtilities import LDSUtilities as LU, Debugging as DB, DirectDownload, SUFIExtractor, FeatureCounter
from lds.ProjectionReference import Projection
from lds.ConfigWrapper import ConfigWrapper
#from TransferProcessor import CONF_EXT, CONF_INT

ldslog = LU.setupLogging()
timerlog = LU.setupLogging(lf='TIMER',ff=3)

#Enabling exceptions halts program on non critical errors i.e. create DS throws exception but builds valid DS anyway 
ogr.UseExceptions()

#exceptions
class DSReaderException(Exception): 
    def __init__(self,em,ll=ldslog.error): ll('{} - {}'.format(type(self).__name__,em))
class DSConversionException(Exception): pass
class LDSReaderException(DSReaderException): pass

class IncompleteWFSRequestException(LDSReaderException): pass
class DriverInitialisationException(LDSReaderException): pass
class DatasourceCopyException(LDSReaderException): pass
class DatasourceCreateException(LDSReaderException): pass
class DatasourceConnectException(DSReaderException): pass
class DatasourceOpenException(DSReaderException): pass
class LayerCreateException(LDSReaderException): pass
class FeatureCopyException(LDSReaderException): pass
class InvalidLayerException(LDSReaderException): pass
class InvalidFeatureException(LDSReaderException): pass
class InvalidSQLException(LDSReaderException): pass
class ASpatialFailureException(LDSReaderException): pass
class UnknownDSVersionException(DSReaderException): pass
class UnknownDSTypeException(DSReaderException): pass
class UnknownTemporaryDSTypeException(DSReaderException): pass
class MalformedConnectionString(DSReaderException): pass
class InaccessibleLayerException(DSReaderException): pass
class InaccessibleFeatureException(DSReaderException): pass    
class OversizeLayerException(DSReaderException): pass   
class DatasourcePrivilegeException(DSReaderException): pass
class UnsupportedServiceException(LDSReaderException): pass

class UnrecognisableGeometryTypeException(DSConversionException): pass

CREATE_DS_FLAG = True

#Triggers feature geometry forcing setting Poly-> MultiPoly and Poly3D -> Poly
ENABLE_FORCE_GEOMETRY = False

#Constant to add/sub when converting between 2D and 25D
DIM_CONST = 2147483648

RUN_ENV = LU.getRuntimeEnvironment()

class DataStore(object):
    '''
    DataStore superclasses PostgreSQL, LDS(WFS), FileGDB(ESRI) and SpatiaLite datastores.
    This class contains the main copy functions for each datasource and sets up default connection parameters. Common options are also set up in this class 
    but variations are implemented in the appropriate subclasses
    '''
    __metaclass__ = ABCMeta


    DRIVER_NAMES = {'pg':'PostgreSQL','ms':'MSSQLSpatial','sl':'SQLite','fg':'FileGDB'}
    
    LDS_CONFIG_TABLE = 'lds_config'
    TEMP_FILE = os.path.join(os.path.dirname(__file__), '../conf/tempfile.xml')
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
    EARLIEST_INIT_DATE = '2000-01-01T00:00:00'
    #Number of retry attempts before abandoning replication completely
    MAXIMUM_WFS_ATTEMPTS = 5
    #Number of retry attempts before abandoning transactions (slower but more likely to succeed)
    TRANSACTION_THRESHOLD_WFS_ATTEMPTS = 3
    #Number of records to prefetch before writing output
    MAX_PREFETCH = 100000
    
    DRIVER_NAME = '<init in subclass>'
    
    DEFAULT_FETCH_METHOD = 'direct'
    
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
    
    #ITYPES = LU.enum('QUERYONLY','QUERYMETHOD','METHODONLY')
    
    CPL_DEBUG = 'OFF'
    GDAL_CACHEMAX = 2047
    GML_INVERT = 'YES'
    GML_URN = 'YES'
    GDAL_IGNORE = 'Function sequence error','HTTP error code : 50[234]','HTTP error code : 404',\
        'General Error','Empty content returned by server','Feature count mismatch','Cannot access any Features'
    
    
    def __init__(self,conn_str=None,user_config=None):
        '''
        Constructor inits driver and some date specific settings. Arguments are for config overrides 
        '''

        self.name = 'DS{}'.format(datetime.utcnow().strftime('%y%m%d%H%M%S'))
        
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
        
        self.prefetchsize = None
        
        #self.CONFIG_XSL = "getcapabilities."+self.DRIVER_NAME.lower()+".xsl"#we use just 'file' or 'json' now
         
        if LU.assessNone(conn_str):
            self.conn_str = conn_str
        
        self.setSRS(None)
        self.setFilter(None)     

        self.setOverwrite()
        
        self.getDriver(self.DRIVER_NAME)
        #NB. confwrap here isnt the same as the main/user distinction in the ConfigWrapper    
        self.confwrap = ConfigWrapper(user_config)
        
        self.params = self.confwrap.readDSParameters(self.DRIVER_NAME)
        
        '''the set of columns we dont want to copy into the output layer, global'''
        self.optcols = set(['__change__','gml_id','ogc_fid','__pk__'])
        ###added ogc_fid so it doesnt get copied from src but will this prevent it being build by ogr as a serial?
        
        self.src_feat_count = 0
        self.refcount = 0
        self.feat_field_names = None
        
        self.change_ct = {'delete':0,'update':0,'insert':0}
        self.change_op = {'delete':self.deleteFeature,'update':self.updateFeature,'insert':self.insertFeature}
        
        self.offline = False

    #incr flag copied straight from Datastore
    #def setIncremental(self,itype):
    #    if itype in self.ITYPES:
    #        self.incremental = itype
            
    def __str__(self):
        return '{name}: URI:{uri}, Layer:{layer}, CQL:{cql} '.format(name=self.name,uri=self.uri,layer=self.layer,cql=self.cql)
        
    def setDS(self,ds):
        self.ds = ds
        
    def getDS(self):
        return self.ds
    
    def closeDS(self):
        '''close a DS with sync and destroy'''
        ldslog.info("Sync DS and Close")
        if self.ds:
            self.ds.SyncToDisk()
            self.ds.Release()
                    
    def rebuildDS(self):
        '''Re read the DS in case there is a failure. Implemented for WFS. Not really necessary here'''
        self.read(self.getURI(),False)
    
    def clearIncremental(self):
        self.incremental = False
            
    def setIncremental(self,prefetchsize=None):
        self.prefetchsize = prefetchsize
        self.incremental = True
         
    def getIncremental(self):
        return self.incremental 
    
    def setPrefetchSize(self,prefetchsize=None):
        self.prefetchsize = prefetchsize
        
    def getPrefetchSize(self):
        '''returns prefetch is available but defaults to partitionsize which is set in ReadConfig'''
        return self.prefetchsize if self.prefetchsize else self.MAX_PREFETCH
            
    def getPrefetchMethod(self):
        if self.DRIVER_NAME==DataStore.DRIVER_NAMES['fg']:
            return 'prefetch'
        else:
            return self.DEFAULT_FETCH_METHOD

    def applyConfigOptions(self):
        for opt in self.getConfigOptions():
            self.applyConfigOptionSingle(opt)
            
    def applyConfigOptionSingle(self,opt):
        k,v = str(opt).split('=',1)
        res = gdal.SetConfigOption(k.strip(),v.strip())
        ldslog.info('Applying {} option {} -> {}'.format(self.DRIVER_NAME,opt,str(res)))
            
    def getDriver(self,driver_name):
        '''Load the specified driver'''
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
        global_opts = ['CPL_DEBUG='+str(self.CPL_DEBUG)]
        global_opts += ['GDAL_CACHEMAX='+str(self.GDAL_CACHEMAX)]
        global_opts += ['GML_INVERT_AXIS_ORDER_IF_LAT_LONG='+str(self.GML_INVERT)]
        global_opts += ['GML_CONSIDER_EPSG_AS_URN='+str(self.GML_URN)]
        #global_opts += ['CPL_CURL_VERBOSE=YES']
        return global_opts  
    
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
        #raise NotImplementedError("Abstract method sourceURI not implemented")
    
#    @abstractmethod
#    def sourceURI_incrd(self,layer):
#        '''Abstract URI method for returning source. Raises NotImplementedError if accessed directly'''
#        raise NotImplementedError("Abstract method sourceURI_incrd not implemented")
    
    @abstractmethod
    def destinationURI(self,layer):
        '''Abstract URI method for returning destination. Raises NotImplementedError if accessed directly'''
        #raise NotImplementedError("Abstract method destinationURI not implemented")
    
    
    @abstractmethod
    def validateConnStr(self,conn_str):
        '''Abstract method to check user supplied connection strings. Raises NotImplementedError if accessed directly'''
        #raise NotImplementedError("Abstract method destinationURI not implemented")
    
#     def testConnection(self):
#         '''Test connection to a (typically database) source'''
#         return True
    
    def testURL(self,url):
        '''Empty testURL super classing for WFSDS'''
        pass
    
    def initDS(self,dsn=None,create=CREATE_DS_FLAG):
        '''Initialise the data source calling a provided DSN or self.dsn and a flag to indicate whether we should try and create a DS if none found'''
        #Notes
        #1. FGDB.driver will Open() a directory if its a valid FGDB dir, it will not Open() an empty directory. It will not CreateDataSource() an existing directory, valid or empty
        from WFSDataStore import WFSDataStore
        ds = None
        self.setURI(dsn)
        '''initialise a DS for writing'''
        try:
            #we can turn OGR exceptions off here so reported but recoverable errors don't kill DS initialisation 
            #ogr.UseExceptions()  #see finally below  
            ds = self.driver.Open(dsn, update = 1 if self.getOverwrite()=='YES' else 0)
            if ds is None:
                raise DatasourceOpenException('Null DS returned attempting to open {}'.format(dsn))
        except (RuntimeError, DSReaderException) as re1:
            #Check for common causes, fail or retry
            if re.search('HTTP error code : 404',str(re1)):
                return None
                
            #Try to create a DS
            if create: 
                ldslog.info('Create DS from '+str(dsn))
                try:
                    ds = self.createDS(dsn)
                except RuntimeError as re2:
                    e2 = 'Cannot CREATE DS with {}. {}'.format(dsn,re2)
                    ldslog.error(e2)
                    raise DatasourceCreateException(e2)
            else:
                e1 = 'Cannot OPEN DS with {}. {}'.format(dsn,re1)
                ldslog.error(e1)
                raise re1
        finally:
            pass
            #ogr.UseExceptions()
        return ds
    
    def createDS(self,dsn):
        try:
            ds = self.driver.CreateDataSource(dsn, self.getDBOptions())
            if ds is None:
                raise DSReaderException("Error opening/creating DS "+str(dsn))
        except DSReaderException as ds1:
            #print "DSReaderException, Cannot create DS.",dsre2
            ldslog.error(ds1,exc_info=1)
            raise
        except RuntimeError as re2:
            '''this is only caught if ogr.UseExceptions() is enabled (which we dont enable since RunErrs thrown even when DS completes)'''
            #print "GDAL RuntimeError. Error creating DS.",rte
            ldslog.error(re2,exc_info=1)
            raise
        return ds if ds else None
       
    def read(self,dsn,create=True):
        '''Main DS read method'''
        ldslog.info("DS read "+dsn)#.split(":")[0])
        newds = self.initDS(dsn,create)
        if newds:
            self.setDS(newds)
            return True
        return False
        
    def write(self,src,dsn,layername,sixtyfour):
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
                    self.featureCopyIncremental(self.src_link.getDS(),self.getDS(),layername,self.src_link.CHANGE_COL)
                else:
                    #gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED','OFF') 
                    self.featureCopy(self.src_link.getDS(),self.getDS(),layername)
                
            except (FeatureCopyException, InaccessibleFeatureException, RuntimeError) as rte:
                em = gdal.GetLastErrorMsg()
                en = gdal.GetLastErrorNo()
                ldslog.warn("GDAL ErrorMsg: "+str(em))
                ldslog.warn("GDAL ErrorNo: "+str(en))
                #Errors below seem to all indicate server load problems, so we try again
                if self.attempts < self.MAXIMUM_WFS_ATTEMPTS-1 and \
                    re.search('|'.join(self.GDAL_IGNORE),str(rte)):
                    self.attempts += 1
                    attcount = str(self.attempts)+"/"+str(self.MAXIMUM_WFS_ATTEMPTS)
                    ldslog.warn("Failed LDS fetch attempt "+attcount+". "+str(rte))
                    print '*** Att '+attcount+'  *** '+str(datetime.now().isoformat())
                    #re-initialise one/all of the datasources
                    src.read(src.getURI(),False)
                    #self.read(self.getURI(),False)
                    
                    #For 504's also reduce page size
                    if re.search('HTTP error code : 50[234]',str(rte)):
                        ps = src.getPartitionSize() if src.getPartitionSize() else src.OGR_WFS_PAGE_SIZE
                        reduction = int(ps*src.PAGE_REDUCTION_STEP)
                        ldslog.warn('Reducing Page Size to '+str(reduction))
                        src.setPartitionSize(reduction)
                        src.applyConfigOptionSingle('OGR_WFS_PAGE_SIZE='+str(reduction))
                else: 
                    #for all other errors, quit
                    ldslog.error('Traceback {}'.format(rte),exc_info=1)
                    raise
                
            except OversizeLayerException as ole:
                #HACK7. Layer is too big to fetch over ogr so we have to pre-download and using urllib read local file
                ldslog.warn('OGR layer download failure {}. Implementing manual download workaround'.format(ole))
                DirectDownload(self.src_link.getDS().name,self.TEMP_FILE).download()
                self.src_link.offline = True
                self.src_link.getDriver('GML')
                if not self.src_link.read(self.TEMP_FILE):
                    raise
                self.attempts += 1
                
            else:
                #break if no exceptions
                break
            
            finally:
                if os.path.exists(self.TEMP_FILE): os.remove(self.TEMP_FILE)
          
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
            if fdef and fdef_nm in self.optcols:
                self.deleteFieldFromLayer(dst_layer, fi-offset,fdef_nm)
                offset += 1
                
    def deleteFieldFromLayer(self,layer,field_id,fdef_nm):
        '''per DS delete field since some do not support this'''
        layer.DeleteField(field_id)

    def constructLayerName(self,ref_name):
        '''Generic layer name constructor'''
        '''Doesn't use schema prefix since its not used in FileGDB, SpatiaLite 
        and PostgreSQL implements an "active_schema" option bypassing the need for a schema declaration'''
        return LU.sanitise(ref_name)
        
    #--------------------------------------------------------------------------
        
    def getFeatureCount(self,layername):
        '''Alternate feature counter by hacking the uri (using wfs version 1.1.0) and asking for a hits result'''
        import WFSDataStore
        from RequestBuilder import RequestBuilder as RB
        if self.src_link.offline:
            try:
                return self.src_link.GetLayer(LU.recode(layername,uflag='compat')).GetFeatureCount()
            except: 
                return self.src_feat_count
        
        newurl = LU.reVersionURL(self.src_link.getURI(), WFSDataStore.WFSDataStore.VERSION_COUNT)
        doc = self.processExternal(LU.readLDS, (RB.hitsAppend(newurl),self.src_link.pxy))
        fc = FeatureCounter.readCount(doc)
        ldslog.info('Alt FeatureCount '+str(fc))
        return fc
    
    def getFirstFeature(self,src_layer):
        '''fetch first source feature. useful for reading feeature. features ie dimensions'''
        try:
            src_feat = src_layer.GetNextFeature()
        except Exception as e:
            m = LU.errorMessageTranslate(e.message)
            if not re.search('1.Unable',m): raise InaccessibleFeatureException('Error calling layer.GetNextFeature. {}'.format(m))
            raise OversizeLayerException('Error writing during layer.GetNextFeature. {}'.format(m))

        if src_feat:
            self.src_info.feat_info.setGeometryRef(src_feat)
            new_feat_def = self.partialCloneFeatureDef(src_feat)          
        else:
            print src_feat
            raise InaccessibleFeatureException('Cannot access first Feature. ({} available)'.format(self.src_feat_count))
        
        return src_feat,new_feat_def
                
    @DB.dmesg(prefix='fC')
    def featureCopy(self,src_ds,dst_ds,layername):
        '''Feature copy without the change column (and other incremental) overhead. Replacement for driverCopy(cloneDS).''' 
        #Since driver update ds.GetFeatureCount will return all layers available in the DS not just those in the original request
        #so now we have to specify which layer it is we wanted access to. 
        
        #for li in range(0,src_ds.GetLayerCount()): Use this when using GML2 and expecting multiple layers per query

        is_new = False
        transaction_flag = True
        src_layer = src_ds.GetLayer(LU.recode(layername,uflag='compat'))
        
        lc_id = LU.standardiseLayername(src_layer.GetName())
        '''retrieve per-layer settings from props'''
        layerconfentry = self.layerconf.readLayerParameters(lc_id)
        lc_name = self.constructLayerName(layerconfentry.name)
        
        self.src_info = LayerInfo(lc_id,lc_name)
        self.src_info.setLCE(layerconfentry)
        
        self.dst_info = LayerInfo(lc_id,lc_name)
        self.dst_info.setLCE(layerconfentry)
        
        self.src_info.geometry = src_layer.GetGeomType()
        self.dst_info.geometry = self.selectValidGeom(self.src_info.geometry)
        self.src_info.spatial_ref = src_layer.GetSpatialRef()
        self.dst_info.spatial_ref = self.transformSRS(self.src_info.spatial_ref)
        self.src_info.parseLayerDefn(src_layer.GetLayerDefn())
        self.dst_info.parseLayerDefn(self.src_info.layer_defn)
        '''parse discard columns'''
        self.optcols |= set(self.dst_info.discard.strip('[]{}()').split(',') if LU.assessNone(self.dst_info.discard) else [])
        
        ldslog.info("Dest layer: "+self.dst_info.layer_id)
        try:
            #self.src_feat_count = src_layer.GetFeatureCount()
            self.src_feat_count = self.getFeatureCount(layername)
        except Exception:
            '''Try to get a feat count one more time'''
            ldslog.warn('Failed getting feature count. Rebuilding SDS')
            self.src_link.rebuildDS()
            src_ds = self.src_link.ds
            src_layer = src_ds.GetLayer(LU.recode(layername,uflag='compat'))
            self.src_feat_count = self.getFeatureCount(layername)
            
        ldslog.info('Features available = '+str(self.src_feat_count))
        '''since the characteristics of each feature wont change between layers we only need to define a new feature definition once'''
        if self.src_feat_count>0:
            src_feat,new_feat_def = self.getFirstFeature(src_layer)
        else:
            #if there are no features (likely with small incr)
            ldslog.info('No features available, returning')
            src_layer.ResetReading()
            #dst_layer.ResetReading()#hasn't been created yet
            return
            #no need to raise exception, there are no feats (kinda unlikely) so just return

        #MSSQL doesn't like schema specifiers
        try:
            tableonly = LU.recode(self.dst_info.layer_id,uflag='compat')
            dst_layer = dst_ds.GetLayer(tableonly)
        except RuntimeError as rer:
            '''Instead of returning none, runtime errors occur if the layer doesn't exist and needs to be created or has no data'''
            ldslog.warning("Runtime Error fetching layer. "+str(rer))
            dst_layer = None
        
        if dst_layer is None:
            ldslog.warning("Non-Incremental layer ["+self.dst_info.layer_id+"] request. Creating layer")
            '''create a new layer if a similarly named existing layer can't be found on the dst'''
            dst_layer,is_new = self.buildNewDestinationLayer(dst_ds)
            
        #Start Transaction
        if  self.attempts < self.TRANSACTION_THRESHOLD_WFS_ATTEMPTS and dst_layer.TestCapability(ogr.OLCTransactions):
            dst_layer.StartTransaction()
            ldslog.debug('FC Start Transaction '+str(self.attempts))
        else:
            transaction_flag = False
            ldslog.warn('FC Transactions Disabled '+str(self.attempts))
            
        self.change_ct['insert'] = 0
        #Loop feats
        while src_feat:
            #slowest part of this copy operation is the insert since we have to build a new feature from defn and check fields for discards and sufis
            self.change_op['insert'](dst_layer,src_feat,new_feat_def)
            self.change_ct['insert'] += 1
            #Produces a lot of output and slows things down
            #ldslog.debug('sref diff {}'.format(SequenceMatcher(None,str(src_layer.GetSpatialRef()),str(dst_layer.GetSpatialRef())).ratio()))
            try:
                src_feat = src_layer.GetNextFeature()
                #print 'GNF3',src_feat.GetFID(),self.change_ct['insert']
            except RuntimeError as re1:
                ldslog.warn('GNF Failure 1 {}'.format(re1))
                src_feat = self._retryGNF(src_layer)
            except Exception as e:
                raise InaccessibleFeatureException('Unable to GetNextFeature 2. {}'.format(LU.errorMessageTranslate(e.message)))
        
        if self.src_feat_count and self.src_feat_count != sum(self.change_ct.values()):
            if transaction_flag:
                dst_layer.RollbackTransaction()
            raise FeatureCopyException('Feature count mismatch. Source count['+str(self.src_feat_count)+'] <> Change count['+str(sum(self.change_ct.values()))+']')
        
        
        '''Builds an index on a newly created layer if; 
        1) new layer flag is true, 2) index p|s is asked for, 3) we have a pk to use and 4) the layer has replicated at least 1 feat'''
        #May need to be pushed out to subclasses depending on syntax differences
        if is_new and (self.dst_info.geocolumn or self.dst_info.pkey) and sum(self.change_ct.values())>0: self.buildIndex()
            
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
                
    @DB.dmesg(prefix='fCI')
    def featureCopyIncremental(self,src_ds,dst_ds,layername,changecol):
        #TDOD. decide whether C_C is better as an arg or a src.prop
        '''DataStore feature-by-feature replication for incremental queries'''
        #build new layer by duplicating source layers  

        ldslog.info("Using featureCopyIncremental. Per-feature copy")
        #for li in range(0,src_ds.GetLayerCount()):
        is_new = False
        transaction_flag = True
        src_layer = src_ds.GetLayer(LU.recode(layername,uflag='compat'))

        lc_id = LU.standardiseLayername(src_layer.GetName())
        '''retrieve per-layer settings from props'''
        layerconfentry = self.layerconf.readLayerParameters(lc_id)
        lc_name = self.constructLayerName(layerconfentry.name)
        
        self.src_info = LayerInfo(lc_id,lc_name)
        self.src_info.setLCE(layerconfentry)
        
        self.dst_info = LayerInfo(lc_id,lc_name)
        self.dst_info.setLCE(layerconfentry)
        
        ldslog.info("Dest layer: "+self.dst_info.layer_id)
        
        '''parse discard columns'''
        self.optcols |= set(self.dst_info.discard.strip('[]{}()').split(',') if LU.assessNone(self.dst_info.discard) else [])
        
        try:
            tableonly = LU.recode(self.dst_info.layer_id,uflag='compat')
            if self.dst_info.lastmodified:
                #if the layer conf had a lastmodified don't overwrite
                dst_layer = dst_ds.GetLayer(tableonly)#dst_info.layer_name)
            else:
                #with no lastmodified can assume the layer doesnt exist in the dst
                #self.src_info.spatial_ref = src_layer.GetSpatialRef()
                #self.src_info.geometry = src_layer.GetGeomType()
                self.src_info.layer_defn = src_layer.GetLayerDefn()
                #self.dst_info.spatial_ref = self.transformSRS(self.src_info.spatial_ref)
                (dst_layer,is_new) = self.buildNewDestinationLayer(dst_ds)
        except RuntimeError as rer:
            '''Instead of returning none, runtime errors sometimes occur if the layer doesn't exist and needs to be created or has no data'''
            ldslog.warning("Runtime Error fetching layer. "+str(rer))
            dst_layer = None
            
        self.src_info.geometry = src_layer.GetGeomType()
        self.dst_info.geometry = self.selectValidGeom(self.src_info.geometry)
        self.src_info.spatial_ref = src_layer.GetSpatialRef()
        self.dst_info.spatial_ref = self.transformSRS(self.src_info.spatial_ref)
        self.src_info.parseLayerDefn(src_layer.GetLayerDefn())
        self.dst_info.parseLayerDefn(self.src_info.layer_defn)
        
        if dst_layer is None:
            #with or without a lmod its still possible the layer doesn't exist or cannot be read
            ldslog.warning(self.dst_info.layer_id+" does not exist. Creating new layer")
            '''create a new layer if a similarly named existing layer can't be found on the dst'''
            (dst_layer,is_new) = self.buildNewDestinationLayer(dst_ds)
            
            if dst_layer is None:
                #if its still none, bail (and don't bother with re-attempt)
                raise LayerCreateException('Unable to initialise a new Layer on destination')
        
        #add/copy features
        try:
            #self.src_feat_count = src_layer.GetFeatureCount()
            self.src_feat_count = self.getFeatureCount(layername)
        except Exception:         
            '''Try to get a feat count one more time'''
            ldslog.warn('Failed getting feature count. Rebuilding SDS')
            self.src_link.rebuildDS()
            src_ds = self.src_link.ds
            src_layer = src_ds.GetLayer(LU.recode(layername,uflag='compat'))
            self.src_feat_count = self.getFeatureCount(layername)
            
        ldslog.info('Features available = '+str(self.src_feat_count))
        
        if self.src_feat_count>0:
            #src_feat = self.processExternal(LU.wrapWorker,(src_layer.GetNextFeature,))
            try:
                #print 'GNFi1'
                src_feat = src_layer.GetNextFeature()
            except Exception as e:
                raise InaccessibleFeatureException('Unable to GetNextFeature 1i. {}'.format(LU.errorMessageTranslate(e.message)))
            
            if src_feat:
                new_feat_def = self.partialCloneFeatureDef(src_feat)
            else:
                em = 'Cannot access first Feature. ('+str(self.src_feat_count)+' available)'
                ldslog.warn(em)
                raise InaccessibleFeatureException(em)
        else:
            #if there are no features (likely with small incr)
            ldslog.info('No features available, returning')
            src_layer.ResetReading()
            dst_layer.ResetReading()
            return
            #raise InaccessibleFeatureException('Error attempting to access Feature count, ('+str(self.src_feat_count)+' available)')


        #dont bother with transactions if they're failing > N times
        if self.attempts < self.TRANSACTION_THRESHOLD_WFS_ATTEMPTS and dst_layer.TestCapability(ogr.OLCTransactions):
            #NB. Jeremy. TestCap for transactions is needed for FileGDB since rollback throws exception if attempted
            dst_layer.StartTransaction()
            ldslog.debug('FCI Start Transaction '+str(self.attempts))
        else:
            transaction_flag = False
            ldslog.warn('FCI Transactions Disabled '+str(self.attempts))
        

        #prefetch vs direct  
        if self.getPrefetchMethod()=='direct':
            ldslog.info('Direct')

            e = 0
            feat_count = 0
            self.change_ct = {'delete':0,'update':0,'insert':0}

            while src_feat:
                feat_count += 1
                #(src_feat.GetField(changecol) if LU.assessNone(changecol) else "insert").lower()
                change =  (src_feat.GetField(changecol) if LU.assessNone(changecol) else 'insert').lower()
                
                try:
                    e = self.change_op[change](dst_layer, src_feat, new_feat_def)
                    # raise KeyError("Error with Key "+str(change)+" !E {ins,del,upd}",exc_info=1)
                    self.change_ct[change] += 1
                except InvalidFeatureException as ife:
                    ldslog.error("Invalid Feature Exception during "+change+" operation on dest. "+str(ife),exc_info=1)
                #except Exception as e:
                # ldslog.error('trap new errors here... '+str(e))
                if e != 0:
                    ldslog.error("Driver Error ["+str(e)+"] on "+change,exc_info=1)
                    if change == 'update':
                        ldslog.warn('Update failed on SetFeature, attempting delete+insert')
                        #let delete and insert error handlers take care of any further exceptions
                        e1 = self.change_op['delete'](dst_layer,src_feat, None)
                        e2 = self.change_op['insert'](dst_layer,src_feat,new_feat_def)
                        if e1+e2 != 0:
                            raise InvalidFeatureException("Driver Error [d="+str(e1)+",i="+str(e2)+"] on "+change)
                #testing
                #ldslog.info(feat_count) 
                try:
                    #print 'GNFi2'
                    src_feat = src_layer.GetNextFeature()
                except Exception as e:
                    raise InaccessibleFeatureException('Unable to GetNextFeature 2i. {}'.format(LU.errorMessageTranslate(e.message))) 
                 
            ##if self.src_feat_count != self.dst_change_ct:
            if self.src_feat_count != sum(self.change_ct.values()):
                if transaction_flag:
                    dst_layer.RollbackTransaction()
                #raise FeatureCopyException('Feature count mismatch. Source count['+str(self.src_feat_count)+'] <> Change count['+str(self.dst_change_ct)+']')
                raise FeatureCopyException('Feature count mismatch. Source count['+str(self.src_feat_count)+'] <> Change count['+str(sum(self.change_ct.values()))+']')

        #prefetch results (mandatory for fgdb)
        
        elif self.getPrefetchMethod()=='prefetch':
            ldslog.info('Pre-Fetch')

            e = 0
            #key order here is important
            src_array = {'delete':(),'update':(),'insert':()}    
            self.change_ct = {'delete':0,'update':0,'insert':0}
            
            ldslog.info('Begin Pre-Fetch with {} Features'.format(self.getPrefetchSize()))
            feat_count = 0
            proc_count = 0

            while src_feat:
                feat_count += 1
                change =  (src_feat.GetField(changecol) if LU.assessNone(changecol) else "insert").lower()
  
                src_array[change] += (src_feat,)
                if feat_count>=self.getPrefetchSize():
                    ldslog.info('Loading Features {}-{}'.format(self.getPrefetchSize()*proc_count,self.getPrefetchSize()*(proc_count+1)))
                    self.processFetchedIncrement(src_array,dst_layer,new_feat_def)
                    feat_count = 0
                    proc_count += 1
                    src_array = {'delete':(),'update':(),'insert':()}
                #testing
                #ldslog.info(feat_count) 
                #print 'GNFi3'
                try:
                    src_feat = src_layer.GetNextFeature()            
                except RuntimeError as re1:
                    ldslog.warn('GNFi Failure 1 {}'.format(re1))
                    src_feat = self._retryGNF(src_layer)
                except Exception as e:
                    raise InaccessibleFeatureException('Unable to GetNextFeature 3i. {}'.format(LU.errorMessageTranslate(e.message))) 
                
                 
            ldslog.info('Loading remaining Features {}-{}'.format(self.getPrefetchSize()*proc_count,self.src_feat_count))
            self.processFetchedIncrement(src_array,dst_layer,new_feat_def)
             
            if self.src_feat_count != sum(self.change_ct.values()):
                if transaction_flag:
                    dst_layer.RollbackTransaction()
                raise FeatureCopyException('Feature count mismatch. Source count['+str(self.src_feat_count)+'] <> Change count['+str(sum(self.change_ct.values()))+']')

        else:
            ldslog.error('Unknown Fetch Method') 
            raise FeatureCopyException('Unknown Fetch Method')        

        #self._showLayerData(dst_layer)
        
        '''Builds an index on a newly created layer if; 
        1) new layer flag is true, 2) index p|s is asked for, 3) we have a pk to use and 4) the layer has at least 1 feat'''
        #Ordinarily pushed out to subclasses depending on syntax differences
        if is_new and (self.dst_info.geocolumn or self.dst_info.pkey) and sum(self.change_ct.values())>0: self.buildIndex()
            
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
            
        ldslog.info('Inserts={0}, Deletes={1}, Updates={2}'.format(self.change_ct['insert'],self.change_ct['delete'],self.change_ct['update']))
        
        src_layer.ResetReading()
        dst_layer.ResetReading()
            
        #returning nothing disables manual paging    
        #return max_index    
              
    def _retryGNF(self,src_layer):
        '''Get trying to get features if the error surfaceMember'''
        fault = 1
        #src_layer.ResetReading()
        while fault:
            try: 
                src_feat = src_layer.GetNextFeature()
            except RuntimeError as re2: 
                fault += 1
                ldslog.warn('Retry GNF Failure {} {}'.format(fault,re2))
            else: 
                ldslog.info('Found FID {}'.format(src_feat.GetFID()))
                fault = 0
        return src_feat
    
    def processFetchedIncrement(self, src_array, dst_layer, new_feat_def):
        '''Process current feature pool'''          
        for change in src_array.keys():
            for src_feat in src_array[change]:
                try:
                    e = self.change_op[change](dst_layer, src_feat, new_feat_def, self.dst_info.pkey) 
                    self.change_ct[change] += 1

                except InvalidFeatureException as ife:
                    ldslog.error("Invalid Feature Exception during " + change + " operation on dest. " + str(ife), exc_info=1)
                # except Exception as e:
                #    ldslog.error('trap new errors here... '+str(e))
                    
                if e != 0:                  
                    ldslog.error("Driver Error [" + str(e) + "] on " + change, exc_info=1)
                    if change == 'update':
                        ldslog.warn('Update failed on SetFeature, attempting delete+insert')
                        # let delete and insert error handlers take care of any further exceptions
                        e1 = self.change_op['delete'](dst_layer, src_feat, None)
                        e2 = self.change_op['insert'](dst_layer, src_feat, new_feat_def)
                        if e1 + e2 != 0:
                            raise InvalidFeatureException("Driver Error [d=" + str(e1) + ",i=" + str(e2) + "] on " + change)
        
        
    def transformSRS(self,src_layer_sref):
        '''Defines the transform from one SRS to another. Doesn't actually do the transformation, just defines the transformation needed.
        Requires the supplied EPSG be correct and coordinates that can be transformed'''
        self.transform = None
        selected_sref = self.getSRS()
        if LU.assessNone(selected_sref):
            #if the selected SRS fails to validate assume error and flag but dont silently drop back to default
            validated_sref = Projection.validateEPSG(selected_sref)
            if validated_sref:
                self.transform = osr.CoordinateTransformation(src_layer_sref, validated_sref)
                if self.transform == None:
                    ldslog.warn('Can\'t init coordinatetransformation object with SRS:'+str(validated_sref))
                return validated_sref
            else:
                ldslog.warn("Unable to validate selected SRS, epsg="+str(selected_sref))
        else:
            return src_layer_sref
                      
    def insertFeature(self,dst_layer,src_feat,new_feat_def):
        '''insert a new feature'''
        st = datetime.now()
        new_feat = self.partialCloneFeature(src_feat,new_feat_def)
        #print 'GR','\n'.join(str(new_feat.GetGeometryRef()).split(','))
        #print 'VR',len(str(new_feat.GetGeometryRef()).split(','))
        #print 'SR',new_feat.GetGeometryRef().GetSpatialReference()
        e = dst_layer.CreateFeature(new_feat)
        #dst_fid = new_feat.GetFID()
        #ldslog.debug("INSERT: "+str(dst_fid))
        timerlog.info('INSERT,{}'.format(1000*(datetime.now()-st).total_seconds()))
        
        return e
    
    def updateFeature(self,dst_layer,src_feat,new_feat_def):
        '''build new feature, assign it the looked-up matching fid and overwrite on dst'''
        st = datetime.now()
        ref_pkey = self.dst_info.pkey
        if not ref_pkey:
            if not self.feat_field_names:
                self.feat_field_names = self.getFieldNames(src_feat)
            ref_pkey = self.feat_field_names
            src_pkey = self.getFieldValues(src_feat)
        else:
            src_pkey = src_feat.GetFieldAsInteger(ref_pkey)
            
        #ldslog.debug("UPDATE: "+str(src_pkey))
        #if not new_layer_flag: 
        new_feat = self.partialCloneFeature(src_feat,new_feat_def,ref_pkey)
        dst_fid = self._findMatchingFID(dst_layer, ref_pkey, src_pkey)
        if dst_fid:
            new_feat.SetFID(dst_fid)
            e = dst_layer.SetFeature(new_feat)
            
        else:
            ldslog.error("No match for FID with ID="+str(src_pkey)+" on update",exc_info=1)
            raise InvalidFeatureException("No match for FID with ID="+str(src_pkey)+" on update")
        
        timerlog.info('UPDATE,{}'.format(src_pkey,1000*(datetime.now()-st).total_seconds()))
        
        return e
    
    def deleteFeature(self,dst_layer,src_feat,_): 
        '''lookup and delete using fid matching ID of feature being deleted'''
        #naive first implementation, might/will be slow 
        st = datetime.now()
        ref_pkey = self.dst_info.pkey
        if not ref_pkey:
            if not self.feat_field_names:
                self.feat_field_names = self.getFieldNames(src_feat)
            ref_pkey = self.feat_field_names
            src_pkey = self.getFieldValues(src_feat)
        else:
            src_pkey = src_feat.GetFieldAsInteger(ref_pkey)
            
        #ldslog.debug("DELETE: "+str(src_pkey))
        dst_fid = self._findMatchingFID(dst_layer, ref_pkey, src_pkey)
        if dst_fid:
            e = dst_layer.DeleteFeature(dst_fid)
        else:
            ldslog.error("No match for FID with ID="+str(src_pkey)+" on delete",exc_info=1)
            raise InvalidFeatureException("No match for FID with ID="+str(src_pkey)+" on delete")
        
        timerlog.info('DELETE,{},{}'.format(src_pkey,1000*(datetime.now()-st).total_seconds()))
        
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
 
                      
    def buildNewDestinationLayer(self,dst_ds):
        '''Constructs a new layer using another source layer as a template. This does not populate that layer'''
        #read defns of each field
        dst_layer = None        
        #use the field defns to build a schema since this needs to be loaded as a create_layer option
        opts = self.getLayerOptions(self.src_info.layer_id)
        #NB wkbPolygon = 3, wkbMultiPolygon = 6, wkbPolygon25D = -2147483645
        #override to force multipoly sometimes needed but also breaks 3D poly inserts which cant be detected from layer def
        #self.dst_info.geometry = ogr.wkbMultiPolygon if src_info.geometry is ogr.wkbPolygon and ENABLE_FORCE_GEOMETRY else self.selectValidGeom(src_info.geometry)
        #above is commented out because hydro are providing 3D polys
        #self.dst_info.geometry = self.selectValidGeom(src_info.geometry) 
        '''Test coordinate dimensions for 25D. If so set layer geom to 25D'''
        if hasattr(self.src_info.feat_info,'feat_dims') \
        and hasattr(self.src_info.feat_info,'feat_cdims') \
        and max(self.src_info.feat_info.feat_dims,self.src_info.feat_info.feat_cdims)>2:
            self.dst_info.geometry = self._convertDim(self.dst_info.geometry)
        '''build layer replacing poly with multi and revert to def if that doesn't work'''
        try:
            dst_layer = dst_ds.CreateLayer(self.dst_info.ascii_name, self.dst_info.spatial_ref, self.dst_info.geometry, opts)
        except RuntimeError as rer:
            ldslog.error("Cannot create layer. "+str(rer))
            if 'already exists' in str(rer):
                '''indicates the table has been created previously but may not have been returned with the Getlayer command, SL does this with null geom tables'''
                #raise ASpatialFailureException('SpatiaLite driver cannot be used to update ASpatial layers')
                #NB. DeleteLayer also wont work since the layer can't be found.
                #dst_ds.DeleteLayer(dst_layer_name)
                #dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,src_layer_geom,opts)
                #Option 2. Deleting the layer with SQL
                self.executeSQL('drop table '+self.dst_info.ascii_name)
                #TODO check this works on non DB DS
                dst_layer = dst_ds.CreateLayer(self.dst_info.ascii_name,self.dst_info.spatial_ref,self.src_info.geometry,opts)
            elif 'General function failure' in str(rer):
                ldslog.error('Possible SR problem, continuing. '+str(rer))
                dst_layer = None
            elif 'database is locked' in str(rer):
                ldslog.error('Locked database - SpatiaLite problem, cannot continue. '+str(rer))
                raise
            
        #if we fail through to this point most commonly the problem is SpatialRef
        if dst_layer is None:
            #overwrite the dst_sref if its causing trouble (ie GDAL general function errors)
            self.dst_info.spatial_ref = Projection.getDefaultSpatialRef()
            ldslog.warning("Could not initialise Layer with specified SRID {"+str(self.src_info.spatial_ref)+"}.\n\nUsing Default {"+str(self.dst_info.spatial_ref)+"} instead")
            dst_layer = dst_ds.CreateLayer(self.dst_info.ascii_name,self.dst_info.spatial_ref,self.dst_info.geometry,opts)
                
        #if still failing, give up
        if dst_layer is None:
            ldslog.error(self.dst_info.layer_name+" cannot be created")
            raise LayerCreateException(self.dst_info.layer_name+" cannot be created")
    
        
        #Some drivers return an existing layer on CreateLayer even though GetLayer returned nothing. 
        #If the dst_layer isn't empty it's probably not a new layer and we shouldn't be adding stuff to it
        #if len(dst_layer.schema)>0:
        #Because layer.schema can throw an exception on SQLServer use GetFeatureCount?
        if dst_layer.GetFeatureCount()>0:
            return (dst_layer,False)
        
        '''setup layer headers for new layer etc'''
        for fdef in self.src_info.feat_info.fdef_list:
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
                
    @staticmethod
    def _compareGeometries(f1,f2):
        '''For debugging to test whether src/dst geometries match. Also very slow'''
        f1g = f1.GetGeometryRef()
        f2g = f2.GetGeometryRef()
        smr = SequenceMatcher(None,str(f1g),str(f2g)).ratio()
        f1i = FeatureInfo()
        f1i.setGeometryRef(f1g)
        f2i = FeatureInfo()
        f2i.setGeometryRef(f2g)
        if smr<0.8:
            ldslog.debug('geom diff {} - sd{}/nd{} - scd{}/ncd{}'.format(smr,f1i.feat_dims,f2i.feat_dims,f1i.feat_cdims,f2i.feat_cdims))
            ldslog.debug('\nSRCgeo-{}\nNEWgeo-{}'.format(str(f1g)[:100],str(f2g)[:100]))
            
    def _repairGeometry(self,fin):
        '''Detect 3D geometries and attempt to force them into 2D if theyre not supported'''
        fclone = fin.Clone()
        fclone_geom = fclone.GetGeometryRef()
        if fclone_geom:
            #test for geom since absent geom attribute indicates aspatial
            if fclone_geom.GetGeometryType() < 0:#eg -2147483642, then this is an OGR 3D value, replace with layer geom type
                msg = 'Forcing Geometry - {}/{}'.format(fclone.GetFID(),self.dst_info.layer_name)
                fclone_geom = self.force2D(fclone_geom)            
            
            #last resort, try to flatten the geom (can kill process for certain drivers)
            if fclone_geom.GetGeometryType()<0:
                msg = 'Flattening Geometry - {}/{}'.format(fclone.GetFID(),self.dst_info.layer_name)
                fclone_geom.FlattenTo2D()
                
            fclone.SetGeometry(fclone_geom)
            ldslog.warn(msg)
                
        return fclone      
    
    def _convertDim(self,geoval,twothree=3):
        '''converts a 2D to 25D to 2D''' 
        if twothree==3 and geoval>0:
            return geoval-DIM_CONST
        elif twothree==2 and geoval<0:
            return geoval+DIM_CONST
        return geoval
    
    def force2D(self,geom):
        '''force a geometry from its 2D/25D form into a 2D equivalent. ie flatten'''
        #gtype = self._convertDim(geom.GetGeometryType()) #convert to 2D equiv
        #gtype = self.dst_info.geometry                   #convert to layer specified geotype
        clone = None
        gtype = geom.GetGeometryType()
        if gtype == ogr.wkbPolygon or gtype == ogr.wkbPolygon25D:
            clone = ogr.ForceToPolygon(geom)
        elif gtype == ogr.wkbMultiPolygon or gtype == ogr.wkbMultiPolygon25D:
            clone = ogr.ForceToMultiPolygon(geom)
        elif gtype == ogr.wkbLineString or gtype == ogr.wkbMultiLineString:
            clone = ogr.ForceToMultiLineString(geom)
        elif gtype == ogr.wkbPoint or gtype == ogr.wkbMultiPoint:
            clone = ogr.ForceToMultiPoint(geom) 
        return clone
                    
    def partialCloneFeature(self,fin,fout_def):
        '''Builds a feature using a passed in feature definition. Must still ignore discarded columns since they will be in the source'''
        fout = ogr.Feature(fout_def)
        fin_geom = fin.GetGeometryRef()
        if fin_geom:
            #absent geom attribute indicates aspatial
            '''Modify input geometry from P to MP'''
            if ENABLE_FORCE_GEOMETRY:
                fin_geotype = fin_geom.GetGeometryType()
                if fin_geotype == ogr.wkbPolygon:
                    fin_geom = ogr.ForceToMultiPolygon(fin_geom)
                    fin.SetGeometryDirectly(fin_geom)
                elif fin_geotype == ogr.wkbPolygon25D:
                    fin_geom = ogr.ForceToPolygon(fin_geom)
                    fin.SetGeometryDirectly(fin_geom)
      
            '''set Geometry transforming if needed'''
            if hasattr(self,'transform') and self.transform:
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
                        up = (re.sub('JSON|GML3','GML2',self.src_link.getURI()),self.src_link.pxy)
                        doc = self.processExternal(LU.readLDS, up)
                        #doc = LU.readDocument(re.sub('JSON|GML3','GML2',self.src_link.getURI()),self.src_link.pxy)
                    self.sufi_list[fin_field_name] = SUFIExtractor.readURI(doc,fin_field_name)
            
        '''populate non geometric fields'''
        fout_no = 0
        for fin_no in range(0,fin.GetFieldCount()):
            fin_field_name = fin.GetFieldDefnRef(fin_no).GetName()
            #assumes id is the PK
            if fin_field_name == self.dst_info.pkey:#'id':
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
 
    @DB.dmesg(prefix='pCFD')
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
                #print "n={}, typ={}, wd={}, prc={}, tnm={}".format(fin_field_def.GetName(),fin_field_def.GetType(),fin_field_def.GetWidth(),fin_field_def.GetPrecision(),fin_field_def.GetTypeName())
                fout_def.AddFieldDefn(fin_field_def)
                
        return fout_def
    
    #----------------------------------------------------------------------------------------------
    
    def getLastModified(self,layer):
        '''Gets the last modification time of a layer to use for incremental "fromdate" calls. This is intended to be run 
        as a destination method since the destination is the DS being modified i.e. dst.getLastModified'''
        lmd = self.layerconf.readLayerProperty(layer,'lastmodified')
        #if not LU.assessNone(lmd):
        #    lmd = self.EARLIEST_INIT_DATE
        return LU.assessNone(lmd)
        #return lm.strftime(self.DATE_FORMAT)
        
    def setLastModified(self,layer,newdate=None):
        '''Sets the last modification time of a layer following a successful copy operation'''
        if newdate is None:
            newdate=self.getCurrent()
        self.layerconf.writeLayerProperty(layer, 'lastmodified', newdate)  
        ldslog.debug('Setting LM layer={} date={}'.format(layer,newdate))
        
    def clearLastModified(self,layer):
        '''Clears the last modification time of a layer following a successful clean operation'''
        self.layerconf.writeLayerProperty(layer, 'lastmodified', None)
        
    #----------------------------------------------------------------------------------------------
    
    def getEPSGConversion(self,layer):
        '''Gets the saved EPSG for the layer'''
        return self.layerconf.readLayerProperty(layer,'epsg')
    
    def saveEPSGConversion(self,layer,newepsg=None):
        '''Sets the user requested EPSG for this layer following a successful copy operation'''
        self.layerconf.writeLayerProperty(layer, 'epsg', newepsg)  
        
    def clearEPSGConversion(self,layer):
        '''Clears the last modification time of a layer following a successful clean operation'''
        self.layerconf.writeLayerProperty(layer, 'epsg', None)
        
    #----------------------------------------------------------------------------------------------
    
    @classmethod
    def getCurrent(cls):
        '''Gets the current timestamp for incremental todate calls. 
        Time format is UTC for LDS compatibility.
        NB. Because the current date is generated to build the LDS URI the lastmodified time will reflect the request time and not the layer creation time'''
        dpo = datetime.utcnow()
        return dpo.strftime(cls.DATE_FORMAT)  
    
    @abstractmethod
    def buildIndex(self):
        '''Default index string builder for new fully replicated layers'''
        raise NotImplementedError("Abstract method buildIndex not implemented")
    
    # private methods
        
    def executeSQL(self,sql):
        '''Executes arbitrary SQL on the datasource'''
        retval = None
        #ogr.UseExceptions()
        ldslog.debug("SQL: "+sql)
        '''validating sql as a block acts as a sort of transaction mechanism and means we can execute the entire statement which is faster'''
        if self._validateSQL(sql):
            try:
                #cast to STR since unicode raises exception in driver 
                dds = self.getDS()
                retval = dds.ExecuteSQL(LU.recode(sql,uflag='encode'))
                #self.closeDS()
            except RuntimeError as rex:
                ldslog.error("Runtime Error. Unable to execute SQL:"+sql+". Get Error "+str(rex),exc_info=1)
                #this can be a bad thing so we want to stop if this occurs e.g. no lds_config -> no layer list etc
                #but also indicate no problem, e.g. deleting a layer already deleted
                if re.search('does not exist|no such table',str(rex)):
                    ldslog.error("Attempt to delete unrecognised table. "+str(rex))
                    return retval
                #if self.DRIVER_NAME=='SQLite' and re.search('SQL logic error or missing database',str(rex)):
                #    ldslog.error("SQLite error evecuting pragma? ignoring. "+str(rex))
                #    return retval
                raise
            except InvalidSQLException as ise:
                #Caused by query not matching valid entry
                ldslog.error("Error executing SQL Command. "+str(ise))
            except Exception as ex:
                ldslog.error(u"Exception. Unable to execute SQL:"+sql+". Exception: "+str(ex),exc_info=1)
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
            if re.match('select\s+',line):
                continue
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
            if re.match('delete\s+\*?\s*from',line):
                continue
            #pragma commands, needed to turn on sqlite journal_mode=WAL
            if re.match('pragma\s+',line):
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
    
    
    def _cleanLayerByRef(self,ds,layerid,truncate):
        '''Deletes a layer from the DS using the layer reference ie. v:x###'''
        msg = 'truncate' if truncate else 'clean'
        #when the DS is created it uses (PG) the active_schema which is the same as the layername schema.
        #since getlayerX returns all layers in all schemas we ignore the ones with schema prepended since they wont be 'active'
        name = LU.recode(self.constructLayerName(self.layerconf.readLayerProperty(layerid,'name')).split('.')[-1],uflag='compat')
        try:
            lref = ds.GetLayerByName(name)
            if lref: 
                feat_delete_capable = lref.TestCapability(ogr.OLCDeleteFeature)
                layer_delete_capable = ds.TestCapability(ogr.ODsCDeleteLayer)
            
                if truncate:
                    if feat_delete_capable and lref.GetFeatureCount():
                        begun = False
                        try:
                            fid = 0
                            f = lref.GetNextFeature()
                            if f: 
                                begun = True
                                lref.StartTransaction()
                            while f:
                                fid = f.GetFID()
                                lref.DeleteFeature(fid)
                                f = lref.GetNextFeature()  
                        except Exception as e:
                            ldslog.error("Error deleting feature {} on layer {}. {}".format(fid,layerid,e))
                            lref.RollbackTransaction()     
                            
                        try:
                            #OGR General Errors occur on commit even though it succeeds, ignore these
                            if begun: lref.CommitTransaction()
                        except Exception as e:
                            ldslog.error("Error during commit on delete. {}".format(e))
                            if re.search('OGR Error: General Error',str(e)):
                                ldslog.warn('General error on Delete/Commit, ignoring')
                            else:
                                lref.RollbackTransaction()
                                raise  
                    else:
                        #Attempt sql truncate, 'delete from'
                        self._baseDeleteFeature(name)   
                else:        
                    #delete
                    ds.SyncToDisk()
                    if layer_delete_capable:
                        #HACK. Attempt repeated deletes/re-inits until no longer locked
                        i=0
                        keep_trying = True
                        while keep_trying and i<5:
                            i+=1
                            try:
                                keep_trying = False
                                ds.DeleteLayer(name)
                            except Exception as e: 
                                '''for SL, trial-and-error shows that, if we get a db locked error the db unlocks on delete but then loses the
                                layer being deleted returning a not found error. This repeats until we re-init the ds and delete again''' 
                                if re.search('database table is locked',str(e)):
                                    pass
                                elif re.search('not found to delete',str(e)):
                                    pass
                                    #TODO. can no longer close/reinit on error. must be done in DREG
                                    #self.closeDS()
                                    #self.setDS(self.initDS(self.getURI(), create=False))
                                else:
                                    raise
                                keep_trying = True
                                ldslog.error('Delete attempt #{}. {}'.format(i,e))

                    else:
                        #Attempt sql drop, 'drop table'
                        self._baseDeleteLayer(name)
            else:
                ldslog.warn('Layer {} not found'.format(name))        
                
            ldslog.info(u'DS {} {}'.format(msg,name))
            return True                
            #-----------------------------                             
                   
        except ValueError as ve:
            ldslog.error('Value Error doing {} on layer {}. {}'.format(msg,str(layerid),str(ve)))
            raise
        except RuntimeError as rte:
            ldslog.error("RuntimeError performing {} on layer/feature {}. {}".format(msg,str(layerid),str(rte)))
            if re.search('No field definitions found for',str(rte)):
                ldslog.warn('Unable to {} layer, cannot get layer ref for field-less table using driver {}. {}'.format(msg,self.DRIVER_NAME,str(rte)))
                return True
            if re.search('database table is locked',str(rte)):
                ldslog.warn('Unable to {} layer, table may be open in another application. {}'+str(rte))
            raise
        except Exception as e:
            ldslog.error("Generic error in layer "+str(layerid)+'. '+str(e))
            raise
        return False
    
    def _baseDeleteLayer(self,table):
        '''Basic layer delete function intended for aspatial tables which are not returned by queries to the DS. Should work on most DS types'''
        #TODO. Implement for all DS types
        sql_tbd = "drop table "+table
        sql_gcd = "delete from geometry_columns where f_table_name = "+table
        self.executeSQL(sql_tbd)    
        self.executeSQL(sql_gcd)   
        
        #MSSQL = DELETE FROM geometry_columns WHERE f_table_schema = '%s' AND f_table_name = '%s'\n
    
    def _baseDeleteColumn(self,table,column):
        '''Basic column delete function for when regular deletes fail. Intended for aspatial tables which are not returned by queries to the DS'''
        #TODO. Implement for all DS types
        sql_str = "alter table "+table+" drop column "+column
        return self.executeSQL(sql_str)
        
    def _baseDeleteFeature(self,table,where=None):
        '''Deletion by feature using base methods but intended for truncate operations'''
        #works with PG, MS, SL FG? NB. Not Fully tested...
        sql_str = u"delete * from "+table + " where "+str(where) if where else "delete from "+table
        return self.executeSQL(sql_str)
        
    def _clean(self):
        '''Deletes the entire DS layer by layer'''
        #for PG, indices decrement as layers are deleted so delete i=0, N times
        for li in range(0,self.getDS().GetLayerCount()):
            if self._cleanLayerByIndex(self.getDS(),0):
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
        '''Find a feature for a layer with no PK, to do this generically we have to query all fields'''
        qt = ()
        for col,val in zip(col_list,row_vals):
            if col not in self.optcols and val is not '':
                qt += (str(col)+" = '"+str(val)+"'",)        
        search_layer.SetAttributeFilter(' and '.join(qt).replace("''","'"))
        #ResetReading to fix MSSQL ODBC bug, "Function Sequence Error"  
        search_layer.ResetReading()
        #HACK. Need to call GFC on win7 to prevent crash!?!
        search_layer.GetFeatureCount()
        return search_layer.GetNextFeature()
           
    def formatWhereClause(self,ref_pkey,key_val):
        fstr = "{0} = {1}" if isinstance(key_val,int) or re.search('^\d+$',str(key_val)) else "{0} = '{1}'"
        return fstr.format(ref_pkey,key_val)
    
    def _findMatchingFeature(self,search_layer,ref_pkey,key_val):
        '''Find the Feature matching a primary key value'''
        matching_feature = None
        try:
            where = self.formatWhereClause(ref_pkey, key_val)
            #print '>>>'+str(search_layer.GetName())+':'+str(where)
            search_layer.SetAttributeFilter(where)
            #ResetReading to fix MSSQL ODBC bug, "Function Sequence Error". 
            #NB. Since we're resetting the DST layer it has no affect on the SRC read order, just starts the FID search from the beginning  
            search_layer.ResetReading()#commenting this out seems to make no difference to fgdb speed
            #HACK. Need to call GFC on win7 to prevent crash!?!
            search_layer.GetFeatureCount()
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
        #HACK Win7
        layer.GetFeatureCount()
        feat = layer.GetNextFeature()
        while feat:
            DataStore._showFeatureData(feat)
            feat = layer.GetNextFeature()                
                
    @abstractmethod
    def versionCheck(self):
        '''A version check to be used once the DS have been initialised... if normal checks cant be established eg psql on w32'''
        #Obviously this returns a default True for any subclasses that dont support it
        #return True

    @staticmethod
    def processExternal(proc,args):
        '''Selects timed process or not based on runtime ie qgis'''
        if RUN_ENV == 'QGIS':
            from Queue import Queue
            q = Queue()
            proc(args,q)
            return q.get()
        else:
            return LU.timedProcessRunner(proc, args, T=None)
    
    
class LayerInfo(object):
    '''Simple class for layer attributes'''
    def __init__(self,layer_id,layer_name=None,layer_defn=None,spatial_ref=None,geometry=None):
        #to clarify name confusion, id here refers to the layer 'name' read by the layer.GetName fuinction i.e v:xNNNN
        self.layer_id = layer_id
        #but name here refers to the descriptive name e.g. NZ Primary Parcels
        self.layer_name = layer_name
        self.spatial_ref = spatial_ref
        self.geometry = geometry
        self.ascii_name = LU.recode(self.layer_name,uflag='subst')
        if layer_defn: self.parseLayerDefn(layer_defn)
        
    
    def setLCE(self,lce):
        '''If we have the LCE use it to set some LI params'''
        # the LI lce derived data will be the same for both src and dst
        self.cql = lce.cql
        self.epsg = lce.epsg
        self.geocolumn = lce.gcol
        self.category = lce.group
        #self.geometry = self.lce.gcol#geometry is geo type not c geo col name ie geometry/geography 
        self.lastmodified = lce.lmod
        self.pkey = lce.pkey
        self.config_name = lce.name#this is unsanitised!
        self.discard = lce.disc
        
        if not self.layer_name: 
            #this is not built by constructLN so doesn't pick up format specific adaptations
            self.layer_name = LU.sanitise(self.config_name)
            self.ascii_name = LU.recode(self.layer_name,uflag='subst')
            
    def parseLayerDefn(self,ld):
        self.layer_defn = ld
        ln = ld.GetName()
        self.feat_info = FeatureInfo(0,ln)
        self.feat_info.parseFeatureDefn(ld)
        

        
class FeatureInfo(object):    
    '''Simple convenience class used to store common feature info, to be embedded in LayerInfo'''
    def __init__(self,feat_id,feat_name):
        self.feat_id = feat_id
        self.feat_name = feat_name
        
    def parseFeatureDefn(self,fd):
        self.geometry = fd.GetGeomType()
        self.fdef_list = []
        for fi in range(0,fd.GetFieldCount()):
            self.fdef_list.append(fd.GetFieldDefn(fi))
            
    def setGeometryRef(self,feat):
        '''Save the geometry info for spatial layers'''
        sfg = feat.GetGeometryRef()
        if sfg:
            self.feat_dims = sfg.GetDimension()
            self.feat_cdims = sfg.GetCoordinateDimension()
        
        
        
