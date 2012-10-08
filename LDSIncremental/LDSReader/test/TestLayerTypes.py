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
    
    @classmethod
    def setUpClass(self):        
        self.fd1 = "2001-01-01"
        self.td1 = "2012-03-31"
        
        self.fd2 = "2012-04-01"
        self.td2 = "2012-09-14"
        
        
        self.point_layer = "v:x787"     #NZ Geodetic Marks
        self.line_layer1 = "v:x781"     #NZ Rail Centrelines
        self.line_layer2 = "v:x818"     #NZ Road Centrelines
        self.poly_layer1 = "v:x785"     #NZ Land Districts
        self.poly_layer2 = "v:x772"     #NZ Primary Parcels
        self.raster_layer1 = "v:x767"   #NZ Mainland Topo50 Maps
        
        self.wfs_test_layers = (self.point_layer,self.line_layer1,self.line_layer2,self.poly_layer1,self.poly_layer2)
    
    def setUp(self):
        print "\n\n>>> TIMESTAMP START",datetime.datetime.now()

    def tearDown(self):
        print "\n>>> TIMESTAMP END",datetime.datetime.now(),"\n"
    

    def test1WMSTopo50(self):    
        print "LDS WMS raster"
        
        tp = TransferProcessor(self.raster_layer1)
        tp.processLDS2PG()
        
        #this whole thing should fail
        self.assert_(False)
        
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()