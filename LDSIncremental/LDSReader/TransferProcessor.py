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


import sys, re
from datetime import datetime 

from WFSDataStore import WFSDataStore
from LDSDataStore import LDSDataStore,LDSUtilities
#from ArcSDEDataStore import ArcSDEDataStore
#from CSVDataStore import CSVDataStore
from FileGDBDataStore import FileGDBDataStore
#from ShapefileDataStore import ShapefileDataStore
#from MapinfoDataStore import MapinfoDataStore
from PostgreSQLDataStore import PostgreSQLDataStore
from MSSQLSpatialDataStore import MSSQLSpatialDataStore
from SpatiaLiteDataStore import SpatiaLiteDataStore

from ReadConfig import Reader
from DataStore import InvalidLayerException

class InputMisconfigurationException(Exception): pass


class TransferProcessor(object):
    '''primary class controlling data transfer objects and parameters for these'''

    def __init__(self,ly,fd=None,td=None,sc=None,dc=None,cql=None):
        #ldsu? lnl?
        #self.src = LDSDataStore() 
        #self.lnl = LDSDataStore.fetchLayerNames(self.src.getCapabilities())
        
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

    
    def processLDS2PG(self):
        '''process LDS to PG convenience method'''
        self.processLDS(PostgreSQLDataStore(self.destination_str))
        
    def processLDS2MSSQL(self):
        '''process LDS to PG convenience method'''
        self.processLDS(MSSQLSpatialDataStore(self.destination_str))
        
    def processLDS2SpatiaLite(self):
        '''process LDS to SpatiaLite convenience method'''
        self.processLDS(SpatiaLiteDataStore(self.destination_str))    
        
    def processLDS2FileGDB(self):
        '''process LDS to FileGDB convenience method'''
        self.processLDS(FileGDBDataStore(self.destination_str))
        
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
        '''process with LDS as a source and the dest supplied as arg'''
        '''
        the logic here is:
        if layer not specified do them all {$layer = All}
        else if layer specified do it {$layer = L[i]}
        
        if dates specified as 'Full' do full replication on $layer
        else if dates specified do incr on this range for $layer
        else do auto-incr on $layer (where auto picks last-mod and current dates as range)
        '''
        
        #NB self.cql <- commandline, self.src.cql <- ldsincr.conf, 
        
        fdate = None
        tdate = None
        
        self.dst = dst
        self.src = LDSDataStore(self.source_str)        
        
        self.lnl = LDSDataStore.fetchLayerNames(self.src.getCapabilities())
        
        #override config file dates with command line dates if provided
        
        
        if self.todate is not None:
            if LDSUtilities.checkDateFormat(self.todate):
                tdate = self.todate
            else:
                raise InputMisconfigurationException("To-Date provided but format incorrect {-td yyyy-MM-dd | ALL}")
        
        if self.fromdate is not None:
            if LDSUtilities.checkDateFormat(self.fromdate):
                fdate = self.fromdate
            else:
                raise InputMisconfigurationException("From-Date provided but format incorrect {-fd yyyy-MM-dd | ALL}")
            
        if LDSUtilities.checkLayerName(self.layer):
            layer = self.layer
        else:
            raise InputMisconfigurationException("Layer name required {-l v:xNNN | ALL}")
        
        
        
        #choose to replicate All layers (or just one)       
        if fdate is None or tdate is None:
            '''do auto incremental'''
            self.autoIncrement(layer)
        elif fdate=='ALL' or tdate=='ALL':
            '''do full replication. require 'ALL' explicitly since full replicate could be an expensive operation'''
            self.fullReplicate(layer) 
        else:
            '''do requested date range'''
            self.definedIncremental(layer,fdate,tdate)
        #missing case is; if one date provided and other sg else. caught by elif
    
    #----------------------------------------------------------------------------------------------
    
    def fullReplicate(self,layer):
        if layer is 'ALL':
            #layer should never be none... 'ALL' needed
            #TODO consider driver reported layer list
            for layer_i in self.lnl:
                self.fullReplicateLayer(layer_i)
        else:
            self.fullReplicateLayer(layer)


    def fullReplicateLayer(self,layer):
        self.src.read(self.src.sourceURI(layer))
        self.dst.write(self.src,self.dst.destinationURI(layer))
        '''repeated calls to getcurrent is kinda inefficient but depending on processing time may vary by layer'''
        self.dst.setLastModified(layer,self.dst.getCurrent(None))
    
    
    
    def autoIncrement(self,layer):
        if layer is 'ALL':
            for layer_i in self.lnl:
                self.autoIncrementLayer(layer_i)
        else:
            self.autoIncrementLayer(layer) 
            
                      
    def autoIncrementLayer(self,layer_i):
        offset = None
        fdate = self.dst.getLastModified(layer_i)
        tdate = self.dst.getCurrent(offset)
        
        self.definedIncremental(layer_i,fdate,tdate)

    
    def definedIncremental(self,layer_i,fdate,tdate):
        '''making sure the date ranges are sequential read/write and set last modified'''
        #Once an individual layer has been defined...
        #though it seems a bit of a hack it makes sense that we're stealing the DST MetaLayer to get its CQL and use it in the SRC query 
        
        self.src.setFilter(self.establishCQLPrecedence(self.cql,self.src.getFilter(),self.dst.mlr.readCQLFilter(LDSUtilities.cropChangeset(layer_i))))
        
        if datetime.strptime(tdate,'%Y-%M-%d') > datetime.strptime(fdate,'%Y-%M-%d'):
            self.src.setIncremental()
            self.src.read(self.src.sourceURI_incrd(layer_i,fdate,tdate))
            self.dst.write(self.src,self.dst.destinationURI(layer_i))
            self.dst.setLastModified(layer_i,tdate)
        else:
            print "No update required for layer "+layer_i
        return tdate
    
    
    def establishCQLPrecedence(self,cmdline_cql,config_cql,layer_cql):
        if cmdline_cql is not None:
            return cmdline_cql
        elif config_cql is not None and config_cql != '':
            return config_cql
        elif layer_cql is not None and layer_cql != '':
            return layer_cql
        return None
    


        
           
    #----------------------------------------------------------------------------------------------
        
def main():
    '''temporarily used as test runner'''
    #tp = TransferProcessor('v:x787','ALL','ALLl')
    #set the desc field to something common to see what gets updated
    #tp = TransferProcessor('v:x787','2012-08-01','2012-09-11')
    #tp = TransferProcessor('v:x787',None,None)
    
    tp = TransferProcessor('v:x785','2012-09-01','2012-09-18')
    

    tp.processLDS2PG()
    #tp.processLDS2FileGDB()

    #testlayer = "v:x780"
    
    
    # SOURCES
    
    #src = LDSDataStore() 
    #src.read(src.sourceURI(testlayer))
    
    #src = ShapefileDataStore()
    #src.read(src.sourceURI("nz-primary-parcels.shp"))    
    
    #src = MapinfoDataStore()
    #src.read(src.sourceURI("nz-primary-parcels.mif"))
    
    #src = CSVDataStore()
    #src.read(src.sourceURI("nz-primary-parcels.csv"))
    
    # DESTINATIONS
    
    #dst1 = PostgreSQLDataStore()
    #dst1.write(src,dst1.destinationURI(layer))
    
    #dst2 = CSVDataStore()
    #dst2.write(src,dst2.destinationURI(basename+".csv"))
    
    #dst3 = ShapefileDataStore()
    #dst3.write(src,dst3.destinationURI(basename+".shp"))
    
    #dst4 = MapinfoDataStore()
    #dst4.write(src,dst4.destinationURI(basename+".mif"))
    
    #dst5 = ArcSDEDataStore()
    #dst5.write(src,dst4.destinationURI("copy785_table"))
    #Need instance to test against
    
    #dst6 = FileGDBDataStore()
    #dst6.write(src,dst5.destinationURI("copy785.gdb"))
    #ERROR 1: Error: Failed at creating table for \v_x785 (General function failure.) 
    #[In this case its not an ESRI projection name problem]
    
    #print dst1.ds.GetName()
    #print dst2.ds.GetName()
    #print dst3.ds.GetName()


if __name__ == "__main__":
    main()    
    print "***FIN***"