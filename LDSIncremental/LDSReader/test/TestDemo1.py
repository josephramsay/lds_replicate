'''
v.0.0.1

LDSIncremental -  TestDemo

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
import time
import logging

ldslog = logging.getLogger('LDS')
ldslog.setLevel(logging.DEBUG)

df = os.path.normpath(os.path.join(os.path.dirname(__file__), "../debug.log"))
#df = '../debug.log'
fh = logging.FileHandler(df,'a')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
fh.setFormatter(formatter)
ldslog.addHandler(fh)


#class TestUI(LDSIncrTestCase):
class TestUI(unittest.TestCase):
    '''Basic tests of ldsreplicate.py using command line arguments to see whether they work as expected'''
    
 
    
    def setUp(self):
        self.MS="MSSQL:server=LZ104588-VM\SQLExpress;database=LDSINCR;UID=mssqluser;PWD=mssqlpass;Driver=SQL Server Native Client 11.0;Schema=dbo"
        #super(TestUI,self).setUp()
        if 'inux' in sys.platform:
            self.UC = "ldsincr.lnx.conf"
            self.BB = "bbox\(shape,174.3118,-36.6355,175.2456,-37.0663\)"
        elif sys.platform == 'win32':
            self.UC = "ldsincr.win.conf"
            self.BB="bbox(shape,174.3118,-36.6355,175.2456,-37.0663)"
        else:
            sys.exit(1)
            
            
    def tearDown(self):
        #super(TestUI,self).tearDown()
        pass


    def test01BasicMS787(self):
        '''test init and full layer create'''
        st = "python ../ldsreplicate.py -u "+self.UC+" -l v:x787 init ms"
        print st
        self.assertEquals(os.system(st),0)
        
        
    def test02CleanMS787(self):
        '''test simple delete'''
        st = "python ../ldsreplicate.py -u "+self.UC+" -l v:x787 clean ms"
        print st
        self.assertEquals(os.system(st),0)
        
        
    def test03BBAKL772MS(self):
        '''test boundingbox command and sr conversion'''
        st = "python ../ldsreplicate.py -u "+self.UC+" -l v:x772 -c "+self.BB+" -e 2193 ms"
        print st
        self.assertEquals(os.system(st),0)


        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()