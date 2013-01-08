'''
Created on 17/08/2012

@author: jramsay
'''
import unittest
import os
import logging


from LDSReader.PostgreSQLDataStore import PostgreSQLDataStore
from LDSReader.MSSQLSpatialDataStore import MSSQLSpatialDataStore


ldslog = logging.getLogger('LDS')
ldslog.setLevel(logging.DEBUG)

df = os.path.normpath(os.path.join(os.path.dirname(__file__), "../../debug.log"))
#df = '../debug.log'
fh = logging.FileHandler(df,'w')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('* %(asctime)s - %(levelname)s - %(module)s - %(message)s')
fh.setFormatter(formatter)
ldslog.addHandler(fh)


class TestConfigLayerBuild(unittest.TestCase):
    '''basic test of command line arguments and whether they work as expected'''

    @classmethod
    def setUpClass(self):
        self.ds = MSSQLSpatialDataStore()
        self.ds.initDS(self.ds.destinationURI(None))
        
        self.ds.buildConfigLayer(open('/home/jramsay/git/LDS/LDSIncremental/base.layer.json','r').read())

    @classmethod
    def tearDownClass(self):
        self.ds = None


    def test1MSConfigLayer_SelectedProperty(self):
        
        assert self.ds.readLayerProperty('v:x772', 'name') == 'NZ Primary Parcels', 'Layer to ID msimatch' 
        assert self.ds.readLayerProperty('v:x785', 'name') == 'NZ Land Districts', 'Layer to ID msimatch'     
        
    def test2MSConfigLayer_AllLayers(self):
        
        assert self.ds.getLayerNames()[0] == 'NZ Chatham Roads Centrelines', 'Layer name msimatch'  
        
    def test3MSConfigLayer_PropsFromLayer(self):
        
        assert self.ds.readLayerParameters('v:x772')[1] == 'NZ Primary Parcels', 'Full layer props faulire'  
        
        



if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()