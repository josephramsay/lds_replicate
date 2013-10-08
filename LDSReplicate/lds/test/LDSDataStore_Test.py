'''
v.0.0.9

LDSReplicate -  LDSDataStore_Test

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Tests for command line URLs

Created on 03/10/2013

@author: jramsay
'''
import unittest
import os
import sys
import time
import subprocess

sys.path.append('..')

from lds.LDSUtilities import LDSUtilities

from lds.LDSDataStore import LDSDataStore

testlog = LDSUtilities.setupLogging(ff=2)


class Test_1_LDSDataStore(unittest.TestCase):
    
    UCONF = 'TEST'
    LGVAL = 'v:x100'
    DESTNAME = 'PostgreSQL'
    
    def setUp(self):
        testlog.debug('LDSDataStore_Test.setUp')
        self.ldsdatastore = LDSDataStore(None,self.UCONF)    
        
    def tearDown(self):
        testlog.debug('LDSDataStore_Test.tearDown')
        

    def test_1_getLayerOptions(self):
        cfl = ['CPL_DEBUG=OFF', 'GDAL_HTTP_USERAGENT=LDSReplicate/0.0.9.0', 'OGR_WFS_PAGING_ALLOWED=OFF', 'OGR_WFS_PAGE_SIZE=10000', 'OGR_WFS_USE_STREAMING=NO', 'OGR_WFS_LOAD_MULTIPLE_LAYER_DEFN=OFF', 'OGR_WFS_BASE_START_INDEX=0']   
        self.assertEqual(self.ldsdatastore.getConfigOptions()[1],cfl[1],'config options 1')
        self.assertEqual(self.ldsdatastore.getConfigOptions()[2],cfl[2],'config options 2')
        self.assertEqual(self.ldsdatastore.getConfigOptions()[3],cfl[3],'config options 3')
        self.assertEqual(self.ldsdatastore.getConfigOptions()[4],cfl[4],'config options 4')
        self.assertEqual(self.ldsdatastore.getConfigOptions()[5],cfl[5],'config options 5')
        

    def test_2_getCapabilities(self):
        self.assertEqual(self.ldsdatastore.getCapabilities()[74:77],'WFS','uri service')
        self.assertEqual(self.ldsdatastore.getCapabilities()[86:91],'1.1.0','uri version')
        self.assertEqual(self.ldsdatastore.getCapabilities()[100:115],'GetCapabilities','uri getcapabilities')
        

    def test_3_fetchLayerInfo(self):
        rsl = [('v:x845', '12 Mile Territorial Sea Limit Basepoints', ['New Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries']), ('v:x846', '12 Mile Territorial Sea Outer Limit', ['New Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries'])]
        gcu = self.ldsdatastore.getCapabilities()
        res = self.ldsdatastore.fetchLayerInfo(gcu)
        self.assertEqual(res[0][0],rsl[0][0],'res 00')
        self.assertEqual(res[1][1],rsl[1][1],'res 11')
        self.assertEqual(res[0][2][0],rsl[0][2][0],'res 020')
        self.assertEqual(res[1][2][1],rsl[1][2][1],'res 121')




        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()