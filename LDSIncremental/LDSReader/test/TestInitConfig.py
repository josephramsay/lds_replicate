'''
Created on 17/08/2012

@author: jramsay
'''
import unittest


from LDSReader.LDSUtilities import ConfigInitialiser
from LDSReader.LDSDataStore import LDSDataStore
from LDSReader.PostgreSQLDataStore import PostgreSQLDataStore


class TestInitConfig(unittest.TestCase):
    '''basic test of connectivity to configured destinations '''

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test1PGInit(self):
        lds = LDSDataStore() 
        pgds = PostgreSQLDataStore()
        pgds.initDS(pgds.destinationURI(None))

        ConfigInitialiser.buildTransform(lds,pgds)
        
#    def test2PGReadTable(self):
#        #lds = LDSDataStore() 
#        pgds = PostgreSQLDataStore()
#        
#        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = pgds.mlr.readAllLayerParameters('v:x772')
#        
#        assert name == 'NZ Primary Parcels'
#        
#    def test3PGReadFile(self):
#        lds = LDSDataStore() 
#        pgds = PostgreSQLDataStore()
#        
#        (pkey,name,group,gcol,index,epsg,lmod,disc,cql) = pgds.mlr.readAllLayerParameters('v:x772')
#        
#        assert name == 'NZ Primary Parcels'
        
        


    



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()