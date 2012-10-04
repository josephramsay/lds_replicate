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

from datetime import datetime 

from LDSDataStore import LDSDataStore,LDSUtilities
#from ArcSDEDataStore import ArcSDEDataStore
#from CSVDataStore import CSVDataStore
from FileGDBDataStore import FileGDBDataStore
#from ShapefileDataStore import ShapefileDataStore
#from MapinfoDataStore import MapinfoDataStore
from PostgreSQLDataStore import PostgreSQLDataStore
from MSSQLSpatialDataStore import MSSQLSpatialDataStore
from SpatiaLiteDataStore import SpatiaLiteDataStore


ldslog = logging.getLogger('LDS')


class InputMisconfigurationException(Exception): pass


class TransferProcessor(object):
    '''Primary class controlling data transfer objects and the parameters for these'''

    def __init__(self,ly=None,gp=None,fd=None,td=None,sc=None,dc=None,cql=None,uc=None):
        #ldsu? lnl?
        #self.src = LDSDataStore() 
        #self.lnl = LDSDataStore.fetchLayerNames(self.src.getCapabilities())
        
        self.group = None
        if gp != None:
            self.group = gp
            
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
            self.ldslog.info("CQL:"+str(cql))
            
        self.user_config = None
        if uc != None:
            self.user_config = uc   

    #incr flag copied straight from Datastore
    def setIncremental(self):
        self.INCR = True
         
    def clearIncremental(self):
        self.INCR = False
         
    def getIncremental(self):
        return self.INCR
    
    
    
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
        else if layer specified do that layer {$layer = L[i]}
        
        if dates specified as 'ALL' do full replication on $layer
        else if (both) dates are specified do incr on this range for $layer
        else do auto-increment on $layer (where auto picks last-mod and current dates as range)
        '''
        
        #NB self.cql <- commandline, self.src.cql <- ldsincr.conf, 
        
        fdate = None
        tdate = None
        
        self.dst = dst
        self.src = LDSDataStore(self.source_str,self.user_config)       
        
        #full LDS layer name list
        lds_full = LDSDataStore.fetchLayerNames(self.src.getCapabilities())
        #list of configured layers
        lds_config = self.dst.mlr.getLayerNames()
        
        self.lnl = map(lambda x: x.lstrip('v:x'),set(lds_full).intersection(set(lds_config)))
        
        #Filter by group designation
        
        ldslog.debug("Layer List:"+str(self.lnl))
        
        #override config file dates with command line dates if provided
        
        
        if self.todate is not None:
            if LDSUtilities.checkDateFormat(self.todate):
                tdate = self.todate
            else:
                raise InputMisconfigurationException("To-Date provided but format incorrect {-td yyyy-MM-dd[Thh:mm:ss]}")
        
        if self.fromdate is not None:
            if LDSUtilities.checkDateFormat(self.fromdate):
                fdate = self.fromdate
            else:
                raise InputMisconfigurationException("From-Date provided but format incorrect {-fd yyyy-MM-dd[Thh:mm:ss}")
        
        if self.layer is None:
            layer = 'ALL'  
        elif LDSUtilities.checkLayerName(self.layer):
            layer = self.layer
        else:
            raise InputMisconfigurationException("Layer name provided but format incorrect {-l v:x###}")
        
        
        '''if any date is 'ALL' full rep otherwise do auto unless we have proper dates'''
        if self.getIncremental():#fdate=='ALL' or tdate=='ALL': 
            ldslog.info("Full Replicate on "+str(layer)) 
            self.fullReplicate(layer)      
        elif fdate is None or tdate is None:
            '''do auto incremental'''
            ldslog.info("Auto Incremental on "+str(layer)) 
            self.autoIncrement(layer)
        else:
            '''do requested date range'''
            ldslog.info("Selected Replicate on "+str(layer)+" : "+str(fdate)+" to "+str(tdate)) 
            self.definedIncremental(layer,fdate,tdate)

        #missing case is; if one date provided and other sg ? caught by elif (consider using the valid date?)
    
    #----------------------------------------------------------------------------------------------
    
    def fullReplicate(self,layer):
        '''Replicate across the whole date range'''
        if layer is 'ALL':
            #layer should never be none... 'ALL' needed
            #TODO consider driver reported layer list
            for layer_i in self.lnl:
                self.fullReplicateLayer(layer_i)
        else:
            self.fullReplicateLayer(layer)


    def fullReplicateLayer(self,layer):
        '''Replicate the requested layer non-incrementally'''
        self.src.read(self.src.sourceURI(layer))
        self.dst.write(self.src,self.dst.destinationURI(layer))
        '''repeated calls to getcurrent is kinda inefficient but depending on processing time may vary by layer
        i.e. retained since dates may change between successive calls depending on the start time of the process'''
        self.dst.setLastModified(layer,self.dst.getCurrent())
    
    
    
    def autoIncrement(self,layer):
        '''Auto-Increment reads last-mod and current time to construct incremental date ranges'''
        if layer is 'ALL':
            for layer_i in self.lnl:
                self.autoIncrementLayer(layer_i)
        else:
            self.autoIncrementLayer(layer)
            
                      
    def autoIncrementLayer(self,layer_i):
        '''For a specified layer read date ranges and call incremental'''
        fdate = self.dst.getLastModified(layer_i)
        tdate = self.dst.getCurrent()
        
        self.definedIncremental(layer_i,fdate,tdate)

    
    def definedIncremental(self,layer_i,fdate,tdate):
        '''Making sure the date ranges are sequential, read/write and set last modified'''
        #Once an individual layer has been defined...
        
        self.src.setFilter(self.establishCQLPrecedence(self.cql,self.src.getFilter(),self.dst.mlr.readCQLFilter(LDSUtilities.cropChangeset(layer_i))))
        
        if datetime.strptime(tdate,'%Y-%m-%d') > datetime.strptime(fdate,'%Y-%m-%d'):
            #Set Incremental determines whether we use the incremental or full endpoint construction
            self.src.setIncremental()
            self.src.read(self.src.sourceURI_incrd(layer_i,fdate,tdate))
            self.dst.write(self.src,self.dst.destinationURI(layer_i))
            self.dst.setLastModified(layer_i,tdate)
        else:
            ldslog.info("No update required for layer "+layer_i+" since [start:"+fdate+" > finish:"+tdate+"]")
        return tdate
    
    
    def establishCQLPrecedence(self,cmdline_cql,config_cql,layer_cql):
        '''
        Decide which CQL filter to apply.
        CommandLine > Config-File > Layer-Properties
        '''
        if cmdline_cql is not None:
            return cmdline_cql
        elif config_cql is not None and config_cql != '':
            return config_cql
        elif layer_cql is not None and layer_cql != '':
            return layer_cql
        return None
