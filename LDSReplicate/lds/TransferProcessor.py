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

from datetime import datetime 

from DataStore import DataStore
from DataStore import ASpatialFailureException

from LDSDataStore import LDSDataStore
from LDSUtilities import LDSUtilities, ConfigInitialiser
#from ArcSDEDataStore import ArcSDEDataStore
#from CSVDataStore import CSVDataStore
from FileGDBDataStore import FileGDBDataStore
#from ShapefileDataStore import ShapefileDataStore
#from MapinfoDataStore import MapinfoDataStore
from PostgreSQLDataStore import PostgreSQLDataStore
from MSSQLSpatialDataStore import MSSQLSpatialDataStore
from SpatiaLiteDataStore import SpatiaLiteDataStore

from ReadConfig import LayerFileReader, LayerDSReader

ldslog = logging.getLogger('LDS')


class InputMisconfigurationException(Exception): 
    def __init__(self, msg):
        super(InputMisconfigurationException,self).__init__(msg)
        ldslog.error('InputMisconfigurationException :: Improperly formatter input argument, '+str(msg))
        
class PrimaryKeyUnavailableException(Exception): pass
class LayerConfigurationException(Exception): pass
class DatasourceInitialisationException(Exception): pass


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
    
    def __init__(self,ly=None,gp=None,ep=None,fd=None,td=None,sc=None,dc=None,cql=None,uc=None,ie=None,fbf=None):

        self.CLEANCONF = None
        self.INITCONF = None
        
        self.src = None
        self.dst = None 
        self.lnl = None
        self.partitionlayers = None
        self.partitionsize = None
        self.sixtyfourlayers = None
        self.temptable = None
        
        #only do a config file rebuild if requested
        self.clearInitConfig()
        self.clearCleanConfig()
        
        self.group = None
        if gp != None:
            self.group = gp
            
        self.epsg = None
        if ep != None:
            self.epsg = ep
            
        self.fromdate = None
        if fd != None:
            self.fromdate = fd
        
        self.todate = None
        if td != None:
            self.todate = td     
        
        self.layer = None
        if ly != None:
            self.layer = ly
            
        self.source_str = None
        if sc != None:
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
            
            
        self.destination_str = None
        if dc != None:
            self.destination_str = dc   
            
        self.cql = None
        if cql != None:
            self.cql = cql     
            
        self.user_config = None
        if uc != None:
            self.user_config = uc   
            
        self.confinternal = None
        if ie in [DataStore.CONF_EXT,DataStore.CONF_INT]:
            self.setConfInternal(ie)


        
    def __str__(self):
        return 'Layer:{layer}, Group:{group}, CQL:{cql}, '.format(layer=self.layer,group=self.group,cql=self.cql)
    
    
    #Internal/External flag to override config set option
    def setConfInternal(self,confinternal):
        self.confinternal = confinternal
         
    def getConfInternal(self):
        return self.confinternal
    
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
        return proc(self.destination_str,self.user_config)


    def initSource(self):
        '''Initialise a new source, LDS nominally'''
        src = LDSDataStore(self.source_str,self.user_config) 
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
        
        self.src = self.initSource()
        
        capabilities = self.src.getCapabilities()
        
        #transfer internal/external from TP to DS
        self.dst.transferIETernal(self.getConfInternal())          
                
        self.dst.setLayerConf(TransferProcessor.getNewLayerConf(self.dst))        
        
        if self.getInitConfig():
            TransferProcessor.initLayerConfig(capabilities,self.dst) 
        
        if self.dst.getLayerConf() is None:
            raise LayerConfigurationException("Cannot initialise Layer-Configuration file/table, "+str(dst.getConfInternal()))
        

        #------------------------------------------------------------------------------------------
        
        #Full LDS layer name listv:x (from LDS WFS)
        lds_full = zip(*LDSDataStore.fetchLayerInfo(capabilities))[0]
        #List of configured layers (from layer-config file/table)
        lds_read = self.dst.getLayerConf().getLayerNames()
        
        #Valid layers are those that exist in LDS and are also configured in the LC
        lds_valid = set(lds_full).intersection(set(lds_read))
        
        #Filter by group designation

        if LDSUtilities.mightAsWellBeNone(self.group) is not None:
            #A group is provided. It could be an empty group in which case no layers are selected
            lds_group = set()
            lg = set(self.group.split(','))
            for lid in lds_valid:
                cats = self.dst.getLayerConf().readLayerProperty(lid,'category')
                if cats is not None and set(cats.split(',')).intersection(lg):
                    lds_group += (lid,)
        else:
            lds_group = lds_valid
                     
        #finally we check for a requested layer. NB Logic change 9/5/13. Layer decl depends on group decl
        layer = LDSUtilities.checkLayerName(self.dst.getLayerConf(),self.layer)
        if layer is None:
            #We shouldnt need to check layer name validity taken from a group since they're automatically 'valid' 
            self.lnl = lds_group
        elif layer in lds_group:
            self.lnl = (layer,)
        else:
            raise InputMisconfigurationException('Layer'+str(layer)+' invalid or not part of requested group')
                            
        # ***HACK*** big layer bypass (address this with partitions)
        #self.lnl = filter(lambda final_layer: final_layer not in self.partitionlayers, self.lnl)
        
        #override config file dates with command line dates if provided
        ldslog.debug("AllLayer={}, ConfLayers={}, GroupLayers={}, SelectedLayers={}".format(len(lds_full),len(lds_read),len(lds_group),len(self.lnl)))
        #ldslog.debug("Layer List:"+str(self.lnl))
        
        #------------------------------------------------------------------------------------------  
        
        #Before we go any further, if this is a cleaning job, no point doing anymore setup. Start deleting
        if self.getCleanConfig():
            for cleanlayer in self.lnl:
                self.cleanLayer(cleanlayer)
            return
                
        #build a list of layers with corresponding lastmodified/incremental flags
        fd = LDSUtilities.checkDateFormat(self.fromdate)#if date format wrong treated as None
        td = LDSUtilities.checkDateFormat(self.todate)
        today = self.dst.getCurrent()
        first = DataStore.EARLIEST_INIT_DATE

        for final_layer in self.lnl:
            lm = self.dst.getLayerConf().readLayerProperty(final_layer,'lastmodified')
            pk = self.dst.getLayerConf().readLayerProperty(final_layer,'pkey')
            filt = self.dst.getLayerConf().readLayerProperty(final_layer,'cql')
            srs = self.dst.getLayerConf().readLayerProperty(final_layer,'epsg')
            
            #Set (cql) filters in URI call using layer picking the one with highest precedence            
            self.src.setFilter(LDSUtilities.precedence(self.cql,self.dst.getFilter(),filt))
        
            #SRS are set in the DST since the conversion takes place during the write process. Needed here to trigger bypass to featureCopy
            self.dst.setSRS(LDSUtilities.precedence(self.epsg,self.dst.getSRS(),srs))
            
            #Destination URI won't change because of incremental so set it here
            self.dst.setURI(self.dst.destinationURI(final_layer))
                
            if all(i is None for i in [lm, fd, td]):#if there are no date values in this list its not incr
                self.src.setURI(self.src.sourceURI(final_layer))
                self.dst.clearIncremental()
                self.replicateLayer(final_layer, pk)
            else:
                final_fd = (first if lm is None else lm) if fd is None else fd
                final_td = today if td is None else td

                self.src.setURI(self.src.sourceURI_incrd(final_layer,final_fd,final_td))
                self.dst.setIncremental()            
                if (datetime.strptime(final_td,'%Y-%m-%dT%H:%M:%S')-datetime.strptime(final_fd,'%Y-%m-%dT%H:%M:%S')).days>0:
                    self.replicateLayer(final_layer, pk)
                else:
                    ldslog.warning("No update required for layer "+final_layer+" since [start:"+final_fd+" >= finish:"+final_td+"] by at least 1 day")
                

        self.dst.closeDS()
    
#--------------------------------------------------------------------------------------------------
    
    def replicateLayer(self,layer_i,pkey):
        '''Replicate the requested layer non-incrementally, ie Init a new layer overwriting any previous iteration of that layer'''

        #We dont try and create (=false) a DS on a LDS WFS connection since its RO
        self.src.read(self.src.getURI(),False)
        
        if self.src.ds is None:
            raise DatasourceInitialisationException('Unable to read from data source with URI '+self.src.getURI())
        
        self.dst.write(self.src, self.dst.getURI(), self.getSixtyFour(layer_i))

        self.dst.setLastModified(layer_i,self.dst.getCurrent())
        
#--------------------------------------------------------------------------------------------------
    
#    def setupPartitions(self,layer_i,pkey):
#        if layer_i in self.partitionlayers:
#            if pkey is None:
#                raise PrimaryKeyUnavailableException('Cannot partition layer '+str(layer_i)+'without a valid primary key')
#            self.src.setPrimaryKey(pkey)
#            self.src.setPartitionStart(0)
#            self.src.setPartitionSize(self.partitionsize)#redundant, set earlier
#            '''
#            self.setupPartitions(layer_i, pkey)
#            while 1:
#                maxkey = self.dst.write(...)                                )
#                if maxkey is not None:
#                    self.src.setPartitionStart(maxkey)
#                else:
#                    break
#            '''
            
    def cleanLayer(self,layer_i):
        '''clean a selected layer (once the layer conf file has been established)'''
        if self.dst._cleanLayerByRef(self.dst.ds,layer_i):
            self.dst.clearLastModified(layer_i)
            
#--------------------------------------------------------------------------------------------------

    @classmethod
    def getNewLayerConf(cls,dst):
        '''Decide whether to use internal or external layer config and return the appropriate instantiation'''
        fn = dst.DRIVER_NAME.lower()+cls.LP_SUFFIX  
        return LayerDSReader(dst) if dst.getConfInternal()==DataStore.CONF_INT else LayerFileReader(fn)

            
            
    @classmethod
    def parseCapabilitiesDoc(cls,capabilitiesurl,file_json):
        '''Class method returning the capabilities doc as requested, in either JSON or CP format'''
        xml = LDSUtilities.readDocument(capabilitiesurl)
        return ConfigInitialiser.buildConfiguration(xml,file_json)
  
        
    @classmethod
    def initLayerConfig(cls,capabilitiesurl,dst):
        '''Class method initialising a layer config using the capabilities document'''
        file_json = 'json' if dst.getConfInternal()==DataStore.CONF_INT else 'file'
        res = cls.parseCapabilitiesDoc(capabilitiesurl,file_json)
        dst.getLayerConf().buildConfigLayer(str(res))
        