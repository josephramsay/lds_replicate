'''
v.0.0.9

LDSReplicate -  ConfigConnector_Test

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

from lds.DataStore import MalformedConnectionString
from lds.LDSUtilities import LDSUtilities

from lds.ConfigConnector import ConfigConnector, DatasourceRegister

testlog = LDSUtilities.setupLogging(ff=2)


class Test_2_ConfigConnector(unittest.TestCase):
    
    UCONF = 'TEST'
    LGVAL = 'v:x100'
    DESTNAME = 'PostgreSQL'
    
    def setUp(self):
        testlog.debug('ConfigConnector_Test.setUp')
        self.configconnector = ConfigConnector(None,self.UCONF,self.LGVAL,self.DESTNAME)    
        self.sep,self.dep = self._fetchEPs()
        self.configconnector.reg.setupLayerConfig(self.configconnector.tp,self.sep,self.dep)
        
    def tearDown(self):
        testlog.debug('ConfigConnector_Test.tearDown')
        self._dropEPs()

    def _fetchEPs(self):
        sep = self.configconnector.reg.openEndPoint('WFS', self.UCONF)
        dep = self.configconnector.reg.openEndPoint(self.DESTNAME, self.UCONF)
        return sep,dep
        
    def _dropEPs(self):
        self.configconnector.reg.closeEndPoint('WFS')
        self.configconnector.reg.closeEndPoint(self.DESTNAME)
        self.sep, self.dep = None, None
        
        
    def test_2_setupComplete(self):     
        self.assertIsNotNone(self.configconnector.complete,'complete init')
        self.configconnector.setupComplete(self.dep)
        self.assertIsNotNone(self.configconnector.complete,'complete indep')
    
    def test_1_setupReserved(self): 
        self.assertIsNotNone(self.configconnector.reserved,'reserved init')
        self.configconnector.setupReserved()
        self.assertIsNotNone(self.configconnector.reserved,'reserved indep')
            
    def test_3_setupAssigned(self):
        self.assertIsNotNone(self.configconnector.assigned,'assigned init')
        self.configconnector.setupAssigned()
        self.assertIsNotNone(self.configconnector.assigned,'assigned indep')
        

class Test_1_DatasourceRegister(unittest.TestCase):
    
    UCONF = 'TEST'
    
    def setUp(self):
        testlog.debug('DatasourceRegister_Test.setUp')
        self.datasourceregister = DatasourceRegister()    
    
    def tearDown(self):
        testlog.debug('DatasourceRegister_Test.tearDown')
    
    
    def test_1_openEndPoint(self):
        ep_w = self.datasourceregister.openEndPoint('WFS', self.UCONF)
        ep_f = self.datasourceregister.openEndPoint('FileGDB', self.UCONF)
        ep_s = self.datasourceregister.openEndPoint('SpatiaLite', self.UCONF)
        ep_p = self.datasourceregister.openEndPoint('PostgreSQL', self.UCONF)
        #ep_m = self.datasourceregister.openEndPoint('MSSQLServer', self.UCONF)
        
        self.assertIsNotNone(ep_w)
        self.assertIsNotNone(ep_f)
        self.assertIsNotNone(ep_s)
        self.assertIsNotNone(ep_p)
        #self.assertIsNotNone(self.ep_m)
        
        self.datasourceregister.closeEndPoint('WFS')
        self.datasourceregister.closeEndPoint('FileGDB')
        self.datasourceregister.closeEndPoint('SpatiaLite')
        self.datasourceregister.closeEndPoint('PostgreSQL')
        #self.datasourceregister.closeEndPoint('MSSQLServer')
        
    def test_2_closeEndPoint(self):
        self.datasourceregister.openEndPoint('WFS', self.UCONF)
        self.datasourceregister.openEndPoint('FileGDB', self.UCONF)
        self.datasourceregister.openEndPoint('SpatiaLite', self.UCONF)
        self.datasourceregister.openEndPoint('PostgreSQL', self.UCONF)
        #self.datasourceregister.openEndPoint('MSSQLServer', self.UCONF)
        
        self.datasourceregister.closeEndPoint('WFS')
        self.datasourceregister.closeEndPoint('FileGDB')
        self.datasourceregister.closeEndPoint('SpatiaLite')
        self.datasourceregister.closeEndPoint('PostgreSQL')
        #self.datasourceregister.closeEndPoint('MSSQLServer')
        
        self.assertEquals(len(self.datasourceregister.register),0)

        
    def test_3_multipleReferences(self):
        '''Test to make sure register incr/decr works as advertised, returning a single object but counting references to it'''
        ep_1 = self.datasourceregister.openEndPoint('WFS', self.UCONF)
        ep_2 = self.datasourceregister.openEndPoint('WFS', self.UCONF)
        ep_3 = self.datasourceregister.openEndPoint('WFS', self.UCONF)
        
        self.assertEquals(ep_1, ep_2, 'object 1 == object 2')
        self.assertEquals(ep_2, ep_3, 'object 2 == object 3')
        
        self.assertEqual(self.datasourceregister.refCount('WFS'), 3, 'refcount 3')
        self.datasourceregister.closeEndPoint('WFS')
        self.assertEqual(self.datasourceregister.refCount('WFS'), 2, 'refcount 2')
        self.datasourceregister.closeEndPoint('WFS')
        self.assertEqual(self.datasourceregister.refCount('WFS'), 1, 'refcount 1')
        self.datasourceregister.closeEndPoint('WFS')
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()