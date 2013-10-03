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

#from ConfigConnector_Test import Test_1_DatasourceRegister, Test_2_ConfigConnector
from LDSDataStore_Test import Test_1_LDSDataStore

from lds.LDSUtilities import LDSUtilities

testlog = LDSUtilities.setupLogging()

class FullSuite(unittest.TestSuite):

    def __init__(self):
        pass
    
#    def suite(self):
#        '''for greater control... if needed'''
#        suite = unittest.TestSuite()
#
#        suite.addTest(TestConnect('test1LDSRead'))
#        return suite

    
def main():
    #runner  = unittest.TextTestRunner()
    #f  = FullSuite()
    #suite = f.suite()    
    #runner.run(suite)
    
    runner  = unittest.TextTestRunner()
    #s1a = unittest.TestLoader().loadTestsFromTestCase(Test_1_DatasourceRegister)
    #s1b = unittest.TestLoader().loadTestsFromTestCase(Test_2_ConfigConnector)
    s2a = unittest.TestLoader().loadTestsFromTestCase(Test_1_LDSDataStore)


    
    ss = unittest.TestSuite([s2a])
    runner.run(ss)
    
if __name__ == "__main__":
    main()

    