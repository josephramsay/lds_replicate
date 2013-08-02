'''
v.0.0.1

LDSReplicate -  TransferProcessor

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 26/07/2012

@author: jramsay
'''

import logging
import re
import gdal

from datetime import datetime 

from lds.DataStore import DataStore
from lds.DataStore import DatasourceOpenException

from lds.LDSDataStore import LDSDataStore
from lds.LDSUtilities import LDSUtilities, ConfigInitialiser
#from ArcSDEDataStore import ArcSDEDataStore
#from CSVDataStore import CSVDataStore
from lds.FileGDBDataStore import FileGDBDataStore
#from ShapefileDataStore import ShapefileDataStore
#from MapinfoDataStore import MapinfoDataStore
from lds.PostgreSQLDataStore import PostgreSQLDataStore
from lds.MSSQLSpatialDataStore import MSSQLSpatialDataStore
from lds.SpatiaLiteDataStore import SpatiaLiteDataStore

from lds.ReadConfig import LayerFileReader, LayerDSReader

ldslog = logging.getLogger('LDS')


class InputMisconfigurationException(Exception): 
    def __init__(self, msg):
        super(InputMisconfigurationException,self).__init__(msg)
        ldslog.error('InputMisconfigurationException :: Improperly formatter input argument, '+str(msg))
        
class PrimaryKeyUnavailableException(Exception): pass
class LayerConfigurationException(Exception): pass
class DatasourceInitialisationException(Exception): pass

#enum((L|G),lgval) to distinguish between v:x, GROUPNAME, nz_layer_name
LORG = LDSUtilities.enum('GROUP','LAYER')

class TransferProcessor(object):
    '''Primary class controlling data transfer objects and the parameters for these'''
    
    #Hack for testing, these layers that {are too big, dont have PKs} crash the program so we'll just avoid them. Its not definitive, just ones we've come across while testing
    #1029 has no PK though Koordinates are working on this
    ###layers_that_crash = map(lambda s: 'v:x'+s, ('772','839','1029','817'))
    
    
    #Hack. To read 64bit integers we have to translate tables without GDAL's driver copy mechanism. 
    #Step 1 is to flag using feature-by-feature copy (featureCopy* instead of driverCopy)
    #Step 2 identify tables where 64 bit ints are used
    #Step 3 intercept feature build and copy and overwrite with string values
    #The tables listed below are ASP tables using a sufi number which is 64bit 
    ###layers_with_64bit_ints = map(lambda s: 'v:x'+s, ('1203','1204','1205','1028','1029'))
    #Note. This won't work for any layers that don't have a primary key, i.e. Topo and Hydro. Since feature ids are only used in ASP this shouldnt be a problem
    
    LP_SUFFIX = ".layer.properties"
    DEF_IE = DataStore.CONF_EXT
        
    POLL_INTERVAL = 1
    
    def __init__(self,parent,lg=None,ep=None,fd=None,td=None,sc=None,dc=None,cql=None,uc=None):

        self.parent = parent
        self.CLEANCONF = None
        self.INITCONF = None
        
        self.src = None
        self.dst = None 
        self.lnl = None
        self.partitionlayers = None
        self.partitionsize = None
        self.sixtyfourlayers = None
        self.temptable = None
        
        self.layer_total = 0
        self.layer_count = 0
        
        #only do a config file rebuild if requested
        self.clearInitConfig()
        self.clearCleanConfig()
            
        self.setEPSG(ep)
        self.setFromDate(fd)
        self.setToDate(td)

        #splitting out group/layer and lgname
        self.lgval = None
        if LDSUtilities.mightAsWellBeNone(lg):
            self.setLayerGroupValue(lg)
            
        self.source_str = None
        if LDSUtilities.mightAsWellBeNone(sc):
            self.parseSourceConfig(sc)
            
        self.destination_str = LDSUtilities.mightAsWellBeNone(dc)
        self.cql = LDSUtilities.mightAsWellBeNone(cql)  
        
        self.setUserConf(uc)
        
        #initialise the data source
        self.src = self.initSource()

        
    def __str__(self):
        return 'Dst:{ds}, Layer/Group:{lgval}, CQL:{cql}, '.format(ds=self.destination_str,lgval=self.lgval,cql=self.cql)
    
    def idLayerOrGroup(self,lg):
        '''Identify whether being passed a layer or a group identifier'''
        #still need to decide the difference between a layer name and a group name e.g. nz_rock_polys vs GROUP_ABC. 
        #For now (and probably better that) we take care of this when we search/match group names 
        return LORG.LAYER if re.match('^v:x',lg) else LORG.GROUP
    
    def parseSourceConfig(self,sc):
        '''If a user supplied their own LDS connection string, parse out relevant values'''

        self.source_str = sc
        #check for dates to set incr  
        ufd = LDSUtilities.getDateStringFromURL('from',sc)
        if ufd is not None:
            ufds = ufd.group(1)
            ldslog.warn("Using 'from:' date string from supplied URL "+str(ufds))
            self.fromdate = ufds
        utd = LDSUtilities.getDateStringFromURL('to',sc)
        if utd is not None:
            utds = utd.group(1)
            ldslog.warn("Using 'to:' date string from supplied URL "+str(utds))
            self.todate = utds
            
        #if doing incremental we also need to check changeset
        if (utd is not None or ufd is not None) and not LDSUtilities.checkHasChangesetIdentifier(sc):
            raise InputMisconfigurationException("'changeset' identifier required for incremental LDS query")
        
        #all going well we can now get the layer string. This isn't optional so we just set it
        self.layer = LDSUtilities.getLayerNameFromURL(sc)
        ldslog.warn('Using layer selection from supplied URL '+str(self.layer))  
            
    def setUserConf(self,uc):
        self.user_config = LDSUtilities.mightAsWellBeNone(uc)
        
    def setLayerGroupValue(self,lgval):
        self.lgval = lgval    
        
    def getLayerGroupValue(self):
        return self.lgval
        
    def setEPSG(self,ep):
        self.epsg = LDSUtilities.mightAsWellBeNone(ep)
        
    def setFromDate(self,fd):
        self.fromdate = LDSUtilities.mightAsWellBeNone(fd)
        
    def setToDate(self,td):
        self.todate = LDSUtilities.mightAsWellBeNone(td)
    
    #initilaise config flags
    def setInitConfig(self):
        self.INITCONF = True
         
    def clearInitConfig(self):
        self.INITCONF = False
         
    def getInitConfig(self):
        return self.INITCONF 
    
    def setCleanConfig(self):
        self.CLEANCONF = True
         
    def clearCleanConfig(self):
        self.CLEANCONF = False
         
    def getCleanConfig(self):
        return self.CLEANCONF
    
    def getSixtyFour(self,testlayer):
        '''Pre check of named layers to see if they should be treated as 64bit integer containers needing int->string conversion'''
        #if self.layer in map(lambda a: 'v:x'+a, self.layers_with_64bit_ints):
        if testlayer in self.sixtyfourlayers:
            return True
        return False
    
    def doSRSConvert(self):
        '''Pre check of layer to see if an SRS conversion has been requested. NB Any entry here assumes conversion is needed, doesn't check against existing SRS'''
        return False if self.dst.getSRS() is None else True
    
    def hasPrimaryKey(self,testlayer):
        '''Reads layer conf pkey identifier. If PK is None or something, use this to decide processing type i.e. no PK = driverCopy'''
        return self.dst.getLayerConf().readLayerProperty(testlayer,'pkey') is not None
    
    def initDestination(self,dstname):
        '''Init a new destination using instantiated uconf (and dest str if provided)'''
        proc = {PostgreSQLDataStore.DRIVER_NAME:PostgreSQLDataStore,
                MSSQLSpatialDataStore.DRIVER_NAME:MSSQLSpatialDataStore,
                SpatiaLiteDataStore.DRIVER_NAME:SpatiaLiteDataStore,
                FileGDBDataStore.DRIVER_NAME:FileGDBDataStore
                }.get(LDSUtilities.standardiseDriverNames(dstname))
        return proc(self,self.destination_str,self.user_config)


    def initSource(self):
        '''Initialise a new source, LDS nominally'''
        src = LDSDataStore(self,self.source_str,self.user_config) 
        src.setPartitionSize(self.partitionsize)#partitionsize may not exist when this is called but thats okay!
        src.applyConfigOptions()
        return src                
        
    def processLDS(self,dst):
        '''Process with LDS as a source and the destination supplied as an argument'''

        #fname = dst.DRIVER_NAME.lower()+self.LP_SUFFIX
        
        self.dst = dst
        self.dst.applyConfigOptions()
        
        self.dst.setSRS(self.epsg)
        #might as well initds here, its going to be needed eventually
        self.dst.ds = self.dst.initDS(self.dst.destinationURI(None))#DataStore.LDS_CONFIG_TABLE))
        
        self.dst.versionCheck()
        
        (self.sixtyfourlayers,self.partitionlayers,self.partitionsize,self.temptable) = self.dst.mainconf.readDSParameters('Misc')
        
        capabilities = self.src.getCapabilities()
                
        self.dst.setLayerConf(TransferProcessor.getNewLayerConf(self.dst))        
        #still used on command line
        if self.getInitConfig():
            TransferProcessor.initLayerConfig(capabilities,self.dst,self.src.pxy)
        
        if self.dst.getLayerConf() is None:
            raise LayerConfigurationException("Cannot initialise Layer-Configuration file/table, "+str(dst.getConfInternal()))
        

        #------------------------------------------------------------------------------------------
        
        #Valid layers are those that exist in LDS and are also configured in the LC
        self.initCapsDoc(capabilities, self.src)
        lds_valid = [i[0] for i in self.assembleLayerList(self.dst)]
        
        
        #if layer provided, check that layer is in valid list
        #else if group then intersect valid and group members
        lgid = self.idLayerOrGroup(self.lgval)
        if lgid == LORG.GROUP:
            self.lnl = set()
            group = set(self.lgval.split(','))
            for lid in lds_valid:
                cats = self.dst.getLayerConf().readLayerProperty(lid,'category')
                if cats is not None and set(cats.split(',')).intersection(group):
                    self.lnl.update((lid,))
                
            if not len(self.lnl):
                ldslog.warn('Possible mis-identified Group, {}'.format(group))
                lgid = LORG.LAYER
                
        if lgid == LORG.LAYER:
            layer = LDSUtilities.checkLayerName(self.dst.getLayerConf(),self.lgval) 
            if layer in lds_valid:
                self.lnl = (layer,)
            else:
                raise InputMisconfigurationException('Layer '+str(layer)+' invalid')

        # ***HACK*** big layer bypass (address this with partitions)
        #self.lnl = filter(lambda each_layer: each_layer not in self.partitionlayers, self.lnl)
        
        #override config file dates with command line dates if provided
        ldslog.debug("SelectedLayers={}".format(len(self.lnl)))
        #ldslog.debug("Layer List:"+str(self.lnl))
        
        #------------------------------------------------------------------------------------------  
        
        #Before we go any further, if this is a cleaning job, no point doing anymore setup. Start deleting
        if self.getCleanConfig():
            for cleanlayer in self.lnl:
                self.cleanLayer(cleanlayer,truncate=False)
            return
                
        #build a list of layers with corresponding lastmodified/incremental flags
        fd = LDSUtilities.checkDateFormat(self.fromdate)#if date format wrong treated as None
        td = LDSUtilities.checkDateFormat(self.todate)
        today = self.dst.getCurrent()
        early = DataStore.EARLIEST_INIT_DATE
        
        self.layer_total = len(self.lnl)
        self.layer_count = 0
        
        for each_layer in self.lnl:
            lm = LDSUtilities.checkDateFormat(self.dst.getLayerConf().readLayerProperty(each_layer,'lastmodified'))
            pk = LDSUtilities.mightAsWellBeNone(self.dst.getLayerConf().readLayerProperty(each_layer,'pkey'))
            filt = self.dst.getLayerConf().readLayerProperty(each_layer,'cql')
            srs = self.dst.getLayerConf().readLayerProperty(each_layer,'epsg')
            
            #Set (cql) filters in URI call using layer picking the one with highest precedence            
            self.src.setFilter(LDSUtilities.precedence(self.cql,self.dst.getFilter(),filt))
        
            #SRS are set in the DST since the conversion takes place during the write process. Needed here to trigger bypass to featureCopy
            self.dst.setSRS(LDSUtilities.precedence(self.epsg,self.dst.getSRS(),srs))
            
            #Destination URI won't change because of incremental so set it here
            self.dst.setURI(self.dst.destinationURI(each_layer))
                
            #if PK is none do paging since page index uses pk (can't lookup matching FIDs for updates/deletes?)
            if pk:
                gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED','ON')
            else:
                gdal.SetConfigOption('OGR_WFS_PAGING_ALLOWED','OFF')
                
            #check dates -> check incr read -> incr or non
            
            nonincr = False                
            if any(i is not None for i in [lm, fd, td]):
                final_fd = (early if lm is None else lm) if fd is None else fd
                final_td = today if td is None else td
          
                if (datetime.strptime(final_td,'%Y-%m-%dT%H:%M:%S')-datetime.strptime(final_fd,'%Y-%m-%dT%H:%M:%S')).days>0:
                    self.src.setURI(self.src.sourceURI_incrd(each_layer,final_fd,final_td))
                    if self.readLayer():
                        self.dst.setIncremental()    
                        self.dst.write(self.src, self.dst.getURI(), self.getSixtyFour(each_layer))
                        self.dst.setLastModified(each_layer,final_td)
                    else:
                        ldslog.warn('Incremental Read failed. Switching to Non-Incremental')
                        nonincr = True
                else:
                    ldslog.warning("No update required for layer "+each_layer+" since [start:"+final_fd+" >= finish:"+final_td+"] by at least 1 day")
                    continue
            else:
                nonincr = True
            #--------------------------------------------------    
            if nonincr:                
                self.src.setURI(self.src.sourceURI(each_layer))
                if self.readLayer():
                    self.dst.clearIncremental()
                    self.cleanLayer(each_layer,truncate=True)
                    self.dst.write(self.src, self.dst.getURI(), self.getSixtyFour(each_layer))
                    #since no date provided defaults to current 
                    self.dst.setLastModified(each_layer)
                else:
                    ldslog.warn('Non-Incremental Read failed')
                    raise DatasourceInitialisationException('Unable to read from data source with URI '+self.src.getURI())
                
            self.layer_count += 1
            self.dst.src_feat_count = 0
            self.dst.dst_change_count = 0
                
        self.dst.closeDS()
        
    def initCapsDoc(self,capabilities,src):
        '''Fetch, format and store the capabilities document'''
        if not hasattr(self,'lds_full'):
            self.lds_full = LDSDataStore.fetchLayerInfo(capabilities,src.pxy)
        
    def assembleLayerList(self,dst,intersect=True):
        '''Match the capabilities layer list with the configured layer list'''
        #TODO Profiler reports slow
        #List of configured layers (from layer-config file/table)
        lds_read = dst.getLayerConf().getLayerNames() if dst else []
        
        #Valid layers are those that exist in LDS and are also configured in the LC
        #set(lds_full).intersection(set(lds_read))
        if intersect:
            return [i for i in self.lds_full if i[0] in lds_read]
        else: #union
            return self.lds_full+[i for i in lds_read if i[0] not in self.lds_full]

#--------------------------------------------------------------------------------------------------

    def readLayer(self):
        '''Attempt a read of the configured layer'''
        self.src.read(self.src.getURI(),False)
        return self.src.ds is not None
        
#--------------------------------------------------------------------------------------------------
        
    def cleanLayer(self,layer_i,truncate=False):
        '''clean a selected layer (once the layer conf file has been established)'''
        try:
            if self.dst._cleanLayerByRef(self.dst.ds,layer_i,truncate):
                self.dst.clearLastModified(layer_i)
        except DatasourceOpenException as dse:
            #if we can't clean it probably doesn't exist so continue with any replication jobs
            ldslog.warn('Attempt to clean a non-existent layer. '+str(dse))                
            
#--------------------------------------------------------------------------------------------------

    @classmethod
    def getNewLayerConf(cls,dst):
        '''Decide whether to use internal or external layer config and return the appropriate instantiation'''
        fn = dst.DRIVER_NAME.lower()+cls.LP_SUFFIX  
        return LayerDSReader(dst) if dst.getConfInternal()==DataStore.CONF_INT else LayerFileReader(fn)

            
            
    @classmethod
    def parseCapabilitiesDoc(cls,capabilitiesurl,file_json,pxy):
        '''Class method returning the capabilities doc as requested, in either JSON or CP format'''
        xml = LDSUtilities.readDocument(capabilitiesurl,pxy)
        return ConfigInitialiser.buildConfiguration(xml,file_json)
  
        
    @classmethod
    def initLayerConfig(cls,capabilitiesurl,dst,pxy):
        '''Class method initialising a layer config using the capabilities document'''
        file_json = 'json' if dst.getConfInternal()==DataStore.CONF_INT else 'file'
        res = cls.parseCapabilitiesDoc(capabilitiesurl,file_json,pxy)
        dst.getLayerConf().buildConfigLayer(str(res))
        

