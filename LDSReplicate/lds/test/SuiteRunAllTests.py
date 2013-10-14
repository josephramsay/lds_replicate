'''
v.0.0.9

LDSReplicate -  SuiteRunAllTests

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Test Suite runner

Created on 24/07/2012

@author: jramsay
'''
import unittest

from lds.test.ConfigConnector_Test import Test_1_DatasourceRegister as T1
from lds.test.ConfigConnector_Test import Test_2_ConfigConnector as T2
from lds.test.LDSDataStore_Test import Test_1_LDSDataStore as T3
from lds.test.DataStore_Test import Test_1_DataStore as T4
from lds.test.RequestBuilder_Test import Test_1_RequestBuilder as T5
from lds.test.LDSUtilities_Test import Test_1_LDSUtilities as T6

from lds.LDSUtilities import LDSUtilities

testlog = LDSUtilities.setupLogging()

class FullSuite(unittest.TestSuite):

    def __init__(self):
        pass
    
    def _suite(self):
        suite = unittest.TestSuite()

        suite.addTest(T1('test_1_openEndPoint'))
        suite.addTest(T1('test_2_closeEndPoint'))
        suite.addTest(T1('test_3_multipleReferences'))
        
        suite.addTest(T2('test_1_setupComplete'))
        suite.addTest(T2('test_2_setupReserved'))
        suite.addTest(T2('test_3_setupAssigned'))
        
        suite.addTest(T3('test_1_getLayerOptions'))
        suite.addTest(T3('test_2_getCapabilities'))
        suite.addTest(T3('test_3_fetchLayerInfo'))
        
        
        return suite
    
    def suite(self):
        suites = ()
        suites += unittest.makeSuite(T1)
        suites += unittest.makeSuite(T2)
        suites += unittest.makeSuite(T3)
        suites += unittest.makeSuite(T4)
        suites += unittest.makeSuite(T5)
        suites += unittest.makeSuite(T6)
        
        return unittest.TestSuite(suites)

    
def main():
    
    if True:
        suite = FullSuite().suite()  
    else:
        suite  = unittest.TestSuite()
    
    runner = unittest.TextTestRunner()
    runner.run(suite)
    
if __name__ == "__main__":
    main()

    