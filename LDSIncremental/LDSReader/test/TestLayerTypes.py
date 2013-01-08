'''
Created on 17/08/2012

@author: jramsay
'''
import unittest, datetime


from LDSReader.PostgreSQLDataStore import PostgreSQLDataStore
from LDSReader.MSSQLSpatialDataStore import MSSQLSpatialDataStore
from LDSReader.LDSDataStore import LDSDataStore
from LDSReader.FileGDBDataStore import FileGDBDataStore

from LDSReader.TransferProcessor import TransferProcessor
        
# -----------------------------------------------------------------------------
        
class TestLayerTypes(unittest.TestCase):
    

    def test1WMSTopo50(self):    
        print "LDS WMS raster"
        
        tp = TransferProcessor(self.raster_layer1)
        tp.processLDS2PG()
        
        #this whole thing should fail
        self.assert_(False)
        
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()