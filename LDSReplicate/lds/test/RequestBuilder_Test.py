'''
v.0.0.9

LDSReplicate -  RequestBuilder_Test

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Tests for command line URLs

Created on 04/10/2013

@author: jramsay
'''
import unittest
import os
import sys
import time
import subprocess

sys.path.append('..')

from lds.LDSUtilities import LDSUtilities

from lds.RequestBuilder import RequestBuilder

testlog = LDSUtilities.setupLogging(ff=2)


class Test_1_RequestBuilder(unittest.TestCase):
    
    UCONF = 'TEST'
    LGVAL = 'v:x100'
    DESTNAME = 'PostgreSQL'
    
    PARAMS100 = ['http://wfs.data.linz.govt.nz/', 'aaaa1111bbbb2222cccc3333dddd4444', 'WFS', '1.0.0', 'GML2', '']
    PARAMS110 = ['http://wfs.data.linz.govt.nz/', '1111bbbb2222cccc3333dddd4444eeee', 'WFS', '1.1.0', 'GML2', '']
    PARAMS200 = ['http://wfs.data.linz.govt.nz/', 'bbbb2222cccc3333dddd4444eeee5555', 'WFS', '2.0.0', 'GML2', '']
    
    def setUp(self):
        testlog.debug('LDSDataStore_Test.setUp') 
        
    def tearDown(self):
        testlog.debug('LDSDataStore_Test.tearDown')
        
    def test_1_getInstance(self):
        w100 = RequestBuilder.getInstance(self.PARAMS100,None)
        w110 = RequestBuilder.getInstance(self.PARAMS110,None)
        w200 = RequestBuilder.getInstance(self.PARAMS200,None)
        
        #since RB is incomplete just test for name string
        #self.assertEqual(w100.__str__(),'RequestBuilder_WFS-1.0.0','str cmp 100')
        self.assertEqual(w100.__str__(),'RequestBuilder_WFS-1.1.0','str cmp 100 (subst)')
        self.assertEqual(w110.__str__(),'RequestBuilder_WFS-1.1.0','str cmp 110')
        self.assertEqual(w200.__str__(),'RequestBuilder_WFS-2.0.0','str cmp 200')



        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()