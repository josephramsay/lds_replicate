'''
Created on 17/08/2012

@author: jramsay
'''
import unittest

from LDSReader.PostgreSQLDataStore import PostgreSQLDataStore
from LDSReader.OracleSpatialDataStore import OracleSpatialDataStore
from LDSReader.MSSQLSpatialDataStore import MSSQLSpatialDataStore
from LDSReader.LDSDataStore import LDSDataStore
from LDSReader.CSVDataStore import CSVDataStore
from LDSReader.FileGDBDataStore import FileGDBDataStore
from LDSReader.ShapefileDataStore import ShapefileDataStore
from LDSReader.MapinfoDataStore import MapinfoDataStore
from LDSReader.ArcSDEDataStore import ArcSDEDataStore

class TestConnect(unittest.TestCase):
    '''basic test of connectivity to configured destinations '''

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test1LDSRead(self):
        self.assertIsNotNone(LDSDataStore(),"LDS init fail")
    
    def test2CSVConnect(self):
        self.assertIsNotNone(CSVDataStore(),"CSV init fail")
    
    def test3ShapefileConnect(self):
        self.assertIsNotNone(ShapefileDataStore(),"Shapefile init fail")
    
    def test3MapinfoFileConnect(self):
        self.assertIsNotNone(MapinfoDataStore(),"Mapinfo init fail")
    
    def test4FileGDBConnect(self):
        self.assertIsNotNone(FileGDBDataStore(),"FileGDB init fail")
    
    def test5PostgreSQLConnect(self):
        self.assertIsNotNone(PostgreSQLDataStore(),"Postgres init fail")
    
    def test5OracleConnect(self):
        self.assertIsNotNone(OracleSpatialDataStore(),"Oracle init fail")
    
    def test5MSSQLConnect(self):
        self.assertIsNotNone(MSSQLSpatialDataStore(),"MSSQL init fail")
    
    def test6ArcSDEConnect(self):
        print "ASDE"
        self.assertIsNotNone(ArcSDEDataStore(),"ArcSDE init fail")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()