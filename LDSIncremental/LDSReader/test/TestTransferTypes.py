'''
Created on 17/08/2012

@author: jramsay
'''
import unittest
import datetime

import LDSIncrTestCase

from LDSReader.PostgreSQLDataStore import PostgreSQLDataStore
from LDSReader.MSSQLSpatialDataStore import MSSQLSpatialDataStore
from LDSReader.LDSDataStore import LDSDataStore
from LDSReader.FileGDBDataStore import FileGDBDataStore

from LDSReader.TransferProcessor import TransferProcessor

# -----------------------------------------------------------------------------


class TestTransfer(LDSIncrTestCase):

    @classmethod
    def setUpClass(self):
        self.layername = "v:x785"
        self.csvfilename = "x785.csv"
        self.shapefilename = "x785.shp"
        self.mapinfofilename = "x785.tab"
        self.tablename = "x785table"
        self.sdetablename = "x785table"
        
        self.src = LDSDataStore()
        self.src.read(self.src.sourceURI(self.layername))


    @classmethod
    def tearDownClass(self):
        #self.src.disconnect?
        self.src = None


    #File-1
#    def test1LDS2CSVTransfer(self):        
#        dst = CSVDataStore()
#        print "DST:",dst.destinationURI(self.csvfilename)
#        dst.write(self.src.ds,dst.destinationURI(self.csvfilename))
#        self.fail("LDS2CSV")
        
    #ESRIFile-2
    def test2LDS2FileGDBTransfer(self):       
        dst = FileGDBDataStore()
        dst.write(self.src.ds,dst.destinationURI(self.filegdbname))
        self.assertEqual(122, 123, "fail123")
    
    #GISFile-3
#    def test3LDS2ShapefileTransfer(self):
#        dst = ShapefileDataStore()
#        dst.write(self.src,dst.destinationURI(self.shapefilename))
#        
#    def test3LDS2MapinfoFileTransfer(self):
#        dst = MapinfoDataStore()
#        dst.write(self.src,dst.destinationURI(self.mapinfofilename))
    
    #DB-4  
    def test4LDS2PostgreSQLTransfer(self):
        dst = PostgreSQLDataStore()
        dst.write(self.src,dst.destinationURI(self.tablename))
        
#    def test4LDS2OracleTransfer(self):
#        dst = OracleSpatialDataStore()
#        dst.write(self.src,dst.destinationURI(self.tablename))
    
    def test4LDS2MSSQLTransfer(self):
        dst = MSSQLSpatialDataStore()
        dst.write(self.src,dst.destinationURI(self.tablename))
        
#    #ESRISomething-5
#    def test5LDS2ArcSDETransfer(self):
#        dst = ArcSDEDataStore()
#        dst.write(self.src,dst.destinationURI(self.sdetablename))


#-----------------------------------------------------------------------------

class TestIncrementalDates(unittest.TestCase):
    def __init__(self):
        self.fd1 = "2012-01-01"
        self.td1 = "2012-08-31"
        
        self.fd2 = "2012-09-01"
        self.td2 = "2012-09-14"
        
        
        self.point_layer = "v:x787"     #NZ Geodetic Marks
        self.line_layer1 = "v:x781"     #NZ Rail Centrelines
        self.line_layer2 = "v:x818"     #NZ Road Centrelines
        self.poly_layer1 = "v:x785"     #NZ Land Districts
        self.poly_layer2 = "v:x772"     #NZ Primary Parcels
        
    @classmethod
    def setUpClass(self):#        
        self.tp = TransferProcessor()
        
       


    @classmethod
    def tearDownClass(self):
        #self.src.disconnect?
        self.tp = None


    def test1LDSDefDate(self):    
        
        self.tp.fromdate = '2012-01-01'
        self.tp.todate = '2012-08-30'
        self.tp.layer = 'v:x787'
        self.tp.processLDS2PG()    
        
        self.fail("T1")
        
        
    def test2LDSNoDate(self):    
        
        self.tp.fromdate = None
        self.tp.todate = None
        self.tp.layer = 'v:x787'
        self.tp.processLDS2PG()
        
        self.fail("T2")
        
# -----------------------------------------------------------------------------
        
class TestIncrementalDestinations(unittest.TestCase):
    
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
        
        self.test_layers = (self.point_layer,self.line_layer1,self.line_layer2,self.poly_layer1,self.poly_layer2)
    
    def setUp(self):
        print "\n\n>>> TIMESTAMP START",datetime.datetime.now()

    def tearDown(self):
        print "\n>>> TIMESTAMP END",datetime.datetime.now(),"\n"
    
    
    
    
    def test1LDS2PostgreSQL(self):    
        print "LDS 2 PostgreSQL"
        for layer in self.test_layers:
            tp = TransferProcessor(layer)
            tp.fromdate = self.fd1
            tp.todate = self.td1

            tp.processLDS2PG()
        
            tp.fromdate = None
            tp.todate = None

            tp.processLDS2PG()
        
        self.assert_(True)
        
    def test2LDS2MSSQL(self):    
        print "LDS 2 MSSQL"
        #self.tp.layer = self.point_layer
        for layer in self.test_layers:
            tp = TransferProcessor(layer)
            tp.fromdate = self.fd1
            tp.todate = self.td1

            tp.processLDS2MSSQL()
        
            tp.fromdate = None
            tp.todate = None

            tp.processLDS2MSSQL()
        
        self.assert_(True)
        
    def test3LDS2SpatialLite(self):    
        print "LDS 2 SpatiaLite"
        #self.tp.layer = self.point_layer
        for layer in self.test_layers:
            tp = TransferProcessor(layer)
            tp.fromdate = self.fd1
            tp.todate = self.td1

            tp.processLDS2SpatiaLite()
        
            tp.fromdate = None
            tp.todate = None

            tp.processLDS2SpatiaLite()
        
        self.assert_(True)
        
    def test4LDS2FileGDB(self):    
        print "LDS 2 FileGDB"
        #self.tp.layer = self.point_layer
        for layer in self.test_layers:
            tp = TransferProcessor(layer)
            tp.fromdate = self.fd1
            tp.todate = self.td1

            tp.processLDS2FileGDB()
        
            tp.fromdate = None
            tp.todate = None

            tp.processLDS2FileGDB()

        self.assert_(True)
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()