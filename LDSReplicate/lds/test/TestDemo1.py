'''
v.0.0.9

LDSReplicate -  TestDemo

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Tests for demo cases

Created on 17/09/2012

@author: jramsay
'''

import unittest
import os
import sys

from lds.LDSUtilities import LDSUtilities

ldslog = LDSUtilities.setupLogging()

#class TestUI(LDSIncrTestCase):
class TestUI(unittest.TestCase):
    '''Basic tests of ldsreplicate.py using command line arguments to see whether they work as expected'''
    
 
    
    def setUp(self):
        self.APP = '../../ldsreplicate.py'
        self.DB = 'pg'
        self.MS="MSSQL:server=LZ104588-VM\SQLExpress;database=LDSINCR;UID=mssqluser;PWD=mssqlpass;Driver=SQL Server Native Client 11.0;Schema=dbo"
        #super(TestUI,self).setUp()
        if 'inux' in sys.platform:
            self.UC = "../conf/ldsincr.lnx.conf"
            self.BB = "bbox\(shape,174.3118,-36.6355,175.2456,-37.0663\)"
        elif sys.platform == 'win32':
            self.UC = "../conf/ldsincr.win.conf"
            self.BB="bbox(shape,174.3118,-36.6355,175.2456,-37.0663)"
        else:
            sys.exit(1)
            
            
    def tearDown(self):
        #super(TestUI,self).tearDown()
        pass


    def test01BasicMS787(self):
        '''test init and full layer create'''
        st = "python "+self.APP+" -u "+self.UC+" -l v:x787 init "+self.DB
        print st
        self.assertEquals(os.system(st),0)
        
        
    def test02CleanMS787(self):
        '''test simple delete'''
        st = "python "+self.APP+" -u "+self.UC+" -l v:x787 clean "+self.DB
        print st
        self.assertEquals(os.system(st),0)
        
        
    def test03BBAKL772MS(self):
        '''test boundingbox command and sr conversion'''
        st = "python "+self.APP+" -u "+self.UC+" -l v:x772 -c "+self.BB+" -e 2193 "+self.DB
        print st
        self.assertEquals(os.system(st),0)


        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()