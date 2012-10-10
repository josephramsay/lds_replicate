'''
v.0.0.1

LDSIncremental -  LDS Incremental Utilities

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 24/07/2012

@author: jramsay
'''
import unittest
import os
import logging


#from TestTransferTypes import TestIncrementalDates
#from TestTransferTypes import TestIncrementalDestinations
#from TestConnectivity import TestConnect
#from TestUI import TestUI
#from TestLayerTypes import TestLayerTypes
from TestInitConfig import TestInitConfig


ldslog = logging.getLogger('LDS')
ldslog.setLevel(logging.DEBUG)


df = os.path.normpath(os.path.join(os.path.dirname(__file__), "../../debug.log"))
#df = '../debug.log'
fh = logging.FileHandler(df,'w')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('* %(asctime)s - %(levelname)s - %(module)s - %(message)s')
fh.setFormatter(formatter)
ldslog.addHandler(fh)

class FullSuite(unittest.TestSuite):

    def __init__(self):
        pass
    
#    def suite(self):
#        '''for greater control... if needed'''
#        suite = unittest.TestSuite()
#
#        suite.addTest(TestConnect('test1LDSRead'))
#        suite.addTest(TestConnect('test2CSVConnect'))
#        suite.addTest(TestConnect('test3ShapefileConnect'))
#        suite.addTest(TestConnect('test3MapinfoFileConnect'))
#        suite.addTest(TestConnect('test4FileGDBConnect'))
#        suite.addTest(TestConnect('test5PostgreSQLConnect'))
#        suite.addTest(TestConnect('test5OracleConnect'))
#        suite.addTest(TestConnect('test5MSSQLConnect'))
#        suite.addTest(TestConnect('test6ArcSDEConnect'))
#        
#
#        suite.addTest(TestTransfer('test1LDS2CSVTransfer'))
#        suite.addTest(TestTransfer('test2LDS2FileGDBTransfer'))
#        suite.addTest(TestTransfer('test3LDS2ShapefileTransfer'))
#        suite.addTest(TestTransfer('test3LDS2MapinfoFileTransfer'))
#        suite.addTest(TestTransfer('test4LDS2PostgreSQLTransfer'))
#        suite.addTest(TestTransfer('test4LDS2MSSQLTransfer'))
#        suite.addTest(TestTransfer('test4LDS2OracleTransfer'))
#        suite.addTest(TestTransfer('test5LDS2ArcSDETransfer'))
#        
#        
#        return suite
    
#    def suite(self):
#        '''for greater control... if needed'''
#        suite = unittest.TestSuite()
#        suite.addTest(TestIncrementalDestinations('test1LDS2PostgreSQL'))
#        
#        return suite
    
def main():
    #runner  = unittest.TextTestRunner()
    #f  = FullSuite()
    #suite = f.suite()    
    #runner.run(suite)
    
    runner  = unittest.TextTestRunner()
    #s1a = unittest.TestLoader().loadTestsFromTestCase(TestConnect)
    #s2a = unittest.TestLoader().loadTestsFromTestCase(TestTransfer)
    #s3a = unittest.TestLoader().loadTestsFromTestCase(TestIncrementalDates)
    #s3b = unittest.TestLoader().loadTestsFromTestCase(TestIncrementalDestinations) 
    #s4a = unittest.TestLoader().loadTestsFromTestCase(TestUI)
    #s5a = unittest.TestLoader().loadTestsFromTestCase(TestLayerTypes)
    s6a = unittest.TestLoader().loadTestsFromTestCase(TestInitConfig)
    
    ss = unittest.TestSuite([s6a])
    runner.run(ss)
    
if __name__ == "__main__":
    main()

    