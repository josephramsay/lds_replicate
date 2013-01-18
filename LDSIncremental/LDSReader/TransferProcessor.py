'''
v.0.0.1

LDSIncremental -  LDS Incremental Utilities

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 26/07/2012

@author: jramsay
'''

import logging
import os

from datetime import datetime 

from DataStore import DataStore
from DataStore import ASpatialFailureException
from DataStore import DatasourceOpenException

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

from ReadConfig import LayerFileReader

ldslog = logging.getLogger('LDS')


class InputMisconfigurationException(Exception): pass
class PrimaryKeyUnavailableException(Exception): pass


class TransferProcessor(object):
    '''Primary class controlling data transfer objects and the parameters for these'''
    
    #Hack for testing, these layers that {are too big, dont have PKs} crash the program so we'll just avoid them. Its not definitive, just ones we've come across while testing
    #1029 has no PK though Koordinates are working on this
    ###layers_that_crash = map(lambda s: 'v:x'+s, ('772','839','1029','817'))
    
    
    #Hack. To read 64bit integers we have to translate tables without GDAL's driver copy mechanism. 
    #Step 1 is to flag using feature-by-feature copy (copyDS instead of cloneDS)
    #Step 2 identify tables where 64 bit ints are used
    #Step 3 intercept feature build and copy and overwrite with string values
    #The tables listed below are ASP tables using a sufi number which is 64bit 
    ###layers_with_64bit_ints = map(lambda s: 'v:x'+s, ('1203','1204','1205','1028','1029'))
    #Note. This won't work for any layers that don't have a primary key, i.e. Topo and Hydro. Since feature ids are only used in ASP this shouldnt be a problem
    
    
    def __init__(self,ly=None,gp=None,ep=None,fd=None,td=None,sc=None,dc=None,cql=None,uc=None,fbf=None):
        #ldsu? lnl?
        #self.src = LDSDataStore() 
        #self.lnl = LDSDataStore.fetchLayerNames(self.src.getCapabilities())
        
        #do a driver copy unless valid dates have been provided indicating changeset
        self.clearIncremental()
        
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
            
        self.destination_str = None
        if dc != None:
            self.destination_str = dc   
            
        self.cql = None
        if cql != None:
            self.cql = cql     
            
        self.user_config = None
        if uc != None:
            self.user_config = uc   
            
        self.FBF = None
        if fbf != None and fbf is True:
            self.setFBF()
        elif fbf != None and fbf is False:
            self.clearFBF()

    #incr flag copied straight from Datastore
    def setIncremental(self):
        self.INCR = True
         
    def clearIncremental(self):
        self.INCR = False
         
    def getIncremental(self):
        return self.INCR
    
    #Feature-by-Feature flag to override incremental
    def setFBF(self):
        self.FBF = True
         
    def clearFBF(self):
        self.FBF = False
         
    def getFBF(self):
        return self.FBF
    
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
    
    def hasPrimaryKey(self,testlayer):
        '''Reads layer conf pkey identifier. If PK is None or something, use this to decide processing type i.e. no PK = driverCopy'''
        if self.dst.layerconf.readLayerProperty(testlayer,'pkey') is None:
            return False
        return True
    
    
    def processLDS2PG(self):
        '''process LDS to PG convenience method'''
        self.processLDS(PostgreSQLDataStore(self.destination_str,self.user_config))
        
    def processLDS2MSSQL(self):
        '''process LDS to PG convenience method'''
        self.processLDS(MSSQLSpatialDataStore(self.destination_str,self.user_config))
        
    def processLDS2SpatiaLite(self):
        '''process LDS to SpatiaLite convenience method'''
        self.processLDS(SpatiaLiteDataStore(self.destination_str,self.user_config))
        
    def processLDS2FileGDB(self):
        '''process LDS to FileGDB convenience method'''
        self.processLDS(FileGDBDataStore(self.destination_str,self.user_config))
        
#    def processLDS2Shape(self):
#        '''process LDS to ESRI Shapefile convenience method'''
#        self.processLDS(ShapefileDataStore())
#        
#    def processLDS2Mapinfo(self):
#        '''process LDS to Mapinfo MIF convenience method'''
#        self.processLDS(MapinfoDataStore())
#        
#    def processLDS2CSV(self):
#        print "*** testing only ***"
#        self.processLDS(CSVDataStore())
#           
#    def processLDS2ArcSDE(self):
#        print "*** testing only ***"
#        self.processLDS(ArcSDEDataStore())
        

        
    def processLDS(self,dst):
        '''Process with LDS as a source and the destination supplied as an argument.
        
        The logic here is:
        
        if layer is not specified, do them all {$layer = All}
        else if a group is specified do the layers in that group
        else if layer specified do that layer {$layer = L[i]} (provided its in the group)
        
        ie layer>group>all
        
        if dates specified as 'ALL' do full replication on $layer
        else if (both) dates are specified do incr on this range for $layer
        else do auto-increment on $layer (where auto picks last-mod and current dates as range)
        '''
        
        #NB self.cql <- commandline, self.src.cql <- ldsincr.conf, 
        
        fdate = None
        tdate = None
        
        fname = dst.DRIVER_NAME.lower()+".layer.properties"
        
        self.dst = dst
        self.dst.setSRS(self.epsg)
        #might as well initds here, its going to be needed eventually
        self.dst.ds = self.dst.initDS(self.dst.destinationURI(None))#DataStore.LDS_CONFIG_TABLE))
        
        (self.sixtyfourlayers,self.partitionlayers,self.partitionsize) = self.dst.mainconf.readDSParameters('Misc')
        
        self.src = LDSDataStore(self.source_str,self.user_config) 
        capabilities = self.src.getCapabilities()
        
        #init a new DS for the DST to read config table (not needed for config file...)
        #because we need to read a new config from the SRC and write it to the DST config both of these must be initialised
        self.dst.setupLayerConfig()
        if self.getInitConfig():
            xml = LDSDataStore.readDocument(capabilities)
            if dst.isConfInternal():
                res = ConfigInitialiser.buildConfiguration(xml,'json')
                #open the internal layer table and populate with res 
                self.dst.buildConfigLayer(str(res))
            else:
                res = ConfigInitialiser.buildConfiguration(xml,'file')
                #open and write res to the external layer config file
                open(os.path.join(os.path.dirname(__file__), '../',fname),'w').write(str(res))
                
        if dst.isConfInternal():
            #set the layerconf to access functions (which just happen to be in the DST)
            self.dst.layerconf = self.dst
        else:
            #set the layerconf to a reader that accesses the external file
            self.dst.layerconf = LayerFileReader(fname)
        
            
        if self.getCleanConfig():
            '''clean a selected layer (once the layer conf file has been established)'''
            if self.dst._cleanLayerByRef(self.dst.ds,self.layer):
                self.dst.clearLastModified(self.layer)
            '''once a layer is cleaned don't need to continue so quit'''
            return
            
        #full LDS layer name listv:x
        lds_full = LDSDataStore.fetchLayerNames(capabilities)
        #list of configured layers
        lds_read = self.dst.layerconf.getLayerNames()
        
        lds_valid = set(lds_full).intersection(set(lds_read))
        
        #Filter by group designation

        self.lnl = ()
        if self.group is not None:
            lg = set(self.group.split(','))
            for lid in lds_valid:
                cats = self.dst.layerconf.readLayerProperty(lid,'category')
                if cats is not None and set(cats.split(',')).intersection(lg):
                    self.lnl += (lid,)
        else:
            self.lnl = lds_valid
            
            
        # ***HACK*** big layer bypass (address this with partitions)
        #self.lnl = filter(lambda l: l not in self.partitionlayers, self.lnl)
        
        #override config file dates with command line dates if provided
        ldslog.debug("AllLayer={}, ConfLayers={}, GroupLayers={}".format(len(lds_full),len(lds_read),len(self.lnl)))
        #ldslog.debug("Layer List:"+str(self.lnl))
        
        
        '''if valid dates are provided we assume copyDS'''
        if self.todate is not None:
            tdate = LDSUtilities.checkDateFormat(self.todate)
            if tdate is None:
                raise InputMisconfigurationException("To-Date provided but format incorrect {-td yyyy-MM-dd[Thh:mm:ss]}")
            else:
                self.setIncremental()
        
        if self.fromdate is not None:
            fdate = LDSUtilities.checkDateFormat(self.fromdate)
            if fdate is None:
                raise InputMisconfigurationException("From-Date provided but format incorrect {-fd yyyy-MM-dd[Thh:mm:ss}")
            else:
                self.setIncremental()
        
        if self.layer is None:
            '''If layer is not specified the result is ALL layers though this selection list is still moderated by group and validity'''
            layer = 'ALL'  
        elif LDSUtilities.checkLayerName(self.layer):
            layer = self.layer
        else:
            raise InputMisconfigurationException("Layer name provided but format incorrect {-l "+LDSUtilities.LDS_TN_PREFIX+"###}")
        
              
        #this is the first time we use the incremental flag to do something (and it should only be needed once?)
        #if incremental is false we want a duplicate of the whole layer so fullreplicate
        if not self.getIncremental():
            ldslog.info("Full Replicate on "+str(layer)+" using group "+str(self.group))
            self.fullReplicate(layer)
        elif fdate is None or tdate is None:
            '''do auto incremental'''
            ldslog.info("Auto Incremental on "+str(layer)+" using group "+str(self.group)+" : "+str(fdate)+" to "+str(tdate)) 
            self.autoIncrement(layer,fdate,tdate)
        else:
            '''do requested date range'''
            ldslog.info("Selected Replicate on "+str(layer)+" : "+str(fdate)+" to "+str(tdate))
            self.definedIncrement(layer,fdate,tdate)

        self.dst.closeDS()
        #missing case is; if one date provided and other sg ? caught by elif (consider using the valid date?)
    
    #----------------------------------------------------------------------------------------------
    
    def fullReplicate(self,layer):
        '''Replicate across the whole date range'''
        if layer is 'ALL':
            #TODO consider driver reported layer list
            for layer_i in self.lnl:
                try:
                    self.fullReplicateLayer(str(layer_i))
                except (ASpatialFailureException, PrimaryKeyUnavailableException) as ee:
                    '''if we're processing a layer list, don't stop on an aspatial-only fault, the spatial layers might just work'''
                    ldslog.error(str(ee))
        elif layer in self.lnl:
            self.fullReplicateLayer(layer)
        else:
            ldslog.warn('Invalid layer selected, '+str(layer))


    def fullReplicateLayer(self,layer_i):
        '''Replicate the requested layer non-incrementally'''
        
        #Set filters in URI call using layer            
        self.src.setFilter(LDSUtilities.precedence(self.cql,self.dst.getFilter(),self.dst.layerconf.readLayerProperty(layer_i,'cql')))

        #while (True):
        self.src.setURI(self.src.sourceURI(layer_i))
        self.dst.setURI(self.dst.destinationURI(layer_i))
                
        self.src.read(self.src.getURI())
        self.dst.write(self.src,self.dst.getURI(),self.getIncremental() and self.hasPrimaryKey(layer_i),self.getFBF(),self.getSixtyFour(layer_i))
#            if maxkey is not None:
#                self.src.setPartitionStart(maxkey)
#            else:
#                break
            
            
        '''repeated calls to getcurrent is kinda inefficient but depending on processing time may vary by layer
        Retained since dates may change between successive calls depending on the start time of the process'''
        self.dst.setLastModified(layer_i,self.dst.getCurrent())
        
    
    
    def autoIncrement(self,layer,fdate,tdate):
        if layer is 'ALL':
            for layer_i in self.lnl:
                try:
                    self.autoIncrementLayer(str(layer_i),fdate,tdate)
                except ASpatialFailureException as afe:
                    '''if we're processing a layer list, don't stop on an aspatial-only fault'''
                    ldslog.error(str(afe))
        elif layer in self.lnl:
            self.autoIncrementLayer(layer,fdate,tdate)
        else:
            ldslog.warn('Invalid layer selected, '+str(layer))
            
    def autoIncrementLayer(self,layer_i,fdate,tdate):
        '''For a specified layer read provided date ranges and call incremental'''
        if fdate is None or fdate =='':    
            fdate = self.dst.layerconf.readLayerProperty(layer_i,'lastmodified')
            if fdate is None or fdate == '':
                fdate = DataStore.EARLIEST_INIT_DATE
                
        if tdate is None or tdate =='':         
            tdate = self.dst.getCurrent()
        
        self.definedIncrementLayer(layer_i,fdate,tdate)

    def definedIncrement(self,layer,fdate,tdate):
        '''Final check on layer validity with provided dates'''
        if layer is 'ALL':
            for layer_i in self.lnl:
                try:
                    self.definedIncrementLayer(str(layer_i),fdate,tdate)
                except (ASpatialFailureException, PrimaryKeyUnavailableException) as ee:
                    '''if we're processing a layer list, don-t stop on an aspatial-only fault'''
                    ldslog.error(str(ee))
        elif layer in self.lnl:
            self.definedIncrementLayer(layer,fdate,tdate)
        else:
            ldslog.warn('Invalid layer selected, '+str(layer))
    
        
    def definedIncrementLayer(self,layer_i,fdate,tdate):
        '''Making sure the date ranges are sequential, read/write and set last modified'''
        #Once an individual layer has been defined...
        #croplayer = LDSUtilities.cropChangeset(layer_i)
        #Filters are set on the SRC since they're built into the URL, they are however specified per DST    
        self.src.setFilter(LDSUtilities.precedence(self.cql,self.dst.getFilter(),self.dst.layerconf.readLayerProperty(layer_i,'cql')))
        #SRS are set in the DST since the conversion takes place during the write process. These are only needed for Incremental
        self.dst.setSRS(LDSUtilities.precedence(self.epsg,self.dst.getSRS(),self.dst.layerconf.readLayerProperty(layer_i,'epsg')))
        
        td = datetime.strptime(tdate,'%Y-%m-%dT%H:%M:%S')
        fd = datetime.strptime(fdate,'%Y-%m-%dT%H:%M:%S')
        if (td-fd).days>0:
            
            #TODO optimise
            haspk = self.hasPrimaryKey(layer_i)
            
            if layer_i in self.partitionlayers:
                if not haspk:
                    raise PrimaryKeyUnavailableException('Cannot partition layer '+str(layer_i)+'without a valid primary key')
                self.src.setPrimaryKey(self.dst.layerconf.readLayerProperty(layer_i,'pkey'))
                self.src.setPartitionStart(0)
                self.src.setPartitionSize(self.partitionsize)
                self.setFBF()
                
            while 1:
                #set up URI
                self.src.setURI(self.src.sourceURI_incrd(layer_i,fdate,tdate) if haspk else self.src.sourceURI(layer_i))
                self.dst.setURI(self.dst.destinationURI(layer_i))
            
                #source read from URI
                self.src.read(self.src.getURI())
                #destination write the SRC to the dest URI
                maxkey = self.dst.write(self.src,self.dst.getURI(),self.getIncremental() and haspk,self.getFBF(),self.getSixtyFour(layer_i))
                if maxkey is not None:
                    self.src.setPartitionStart(maxkey)
                else:
                    break
                
#            else:
#                
#                self.src.setURI(self.src.sourceURI_incrd(layer_i,fdate,tdate))
#                self.dst.setURI(self.dst.destinationURI(layer_i))
#            
#                #source read from URI
#                self.src.read(self.src.getURI())
#                #destination write the SRC to the dest URI
#                self.dst.write(self.src,self.dst.getURI(),self.getIncremental(),self.getFBF(),self.getSixtyFour(layer_i))
                
            self.dst.setLastModified(layer_i,tdate)
            
        else:
            ldslog.info("No update required for layer "+layer_i+" since [start:"+fd.isoformat()+" >= finish:"+td.isoformat()+"] by at least 1 day")
        return
    
    

