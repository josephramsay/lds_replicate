'''
v.0.0.1

LDSReplicate -  TestURL

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Tests for command line URLs

Created on 17/09/2012

@author: jramsay
'''
import unittest
import os
import sys
import time
import logging
import subprocess

from lds.DataStore import MalformedConnectionString
from lds.LDSUtilities import LDSUtilities

ldslog = LDSUtilities.setupLogging()

#ldslog = logging.getLogger('LDS')
#ldslog.setLevel(logging.DEBUG)
#
#path = os.path.normpath(os.path.join(os.path.dirname(__file__), "../../log/"))
#if not os.path.exists(path):
#    os.mkdir(path)
#df = os.path.join(path,"debug.log")
#
#fh = logging.FileHandler(df,'a')
#fh.setLevel(logging.DEBUG)
#
#formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
#fh.setFormatter(formatter)
#ldslog.addHandler(fh)


class TestURL(unittest.TestCase):
    '''Basic tests of ldsreplicate.py using command line arguments to see whether they work as expected'''
    
    #        WACA,    land-dist, name-assoc, antarctic (non-NZ sref, RSRGD)
    LAYER = ('v:x836','v:x785',  'v:x1203',  'v:x789')
    LAYER_GEODETIC = ('v:x784','v:x786','v:x787','v:x788','v:x789','v:x817','v:x839','v:x1029')
    LAYER_ASPATIAL = ('v:x1203','v:x1209','v:x1204','v:x1208','v:x1211','v:x1210','v:x1199')
    LAYER_PROBLEM = ('v:x772',)
    
    #2113=Wellington NZGD2000, 3788=AKL islands WGS84,2759=NAD83/HARN Alabama
    EPSG = (2113,3788,2759)
    DATE = '2012-03-20'
    
    DATE1 = '2012-02-25'
    DATE2 = '2012-09-17'
    
    PATH_L = '/home/jramsay/git/LDS/LDSReplicate/LDSReader/'
    PATH_W = 'F:\\git\\LDS\\LDSReplicate\\LDSReader\\'
    PATH_C = PATH_W.replace('\\','/').replace('C:','/cygdrive/c')
    OUTP_L = ('pg',)#'fg','sl')
    OUTP_W = ('ms',)#'sl')
    CONF_I = 'ldsincr.internal.conf'
    CONF_E = 'ldsincr.external.conf'
    CONF_WI = 'ldsincr.windows.internal.conf' 
    CONF_WE = 'ldsincr.windows.external.conf' 
    
    _CONN_STR_L = "PG:dbname='jrdb' host='144.66.6.86' port='5432' user='pguser' password='pgpass'"
    _CONN_STR_W = "MSSQL:server=LZ104588-VM\SQLExpress;database=LDSINCR;UID=mssqluser;PWD=mssqlpass"
    
    URL = 'http://wfs.data.linz.govt.nz/'
    
    
    
#    @classmethod
#    def setUpClass(cls):
#        some_other_layers = ('v:x772',  'v:x1203')
#        geodetic_layers = ('v:x784','v:x786','v:x787','v:x788','v:x789','v:x817','v:x839','v:x1029')
#        for o in TestUI.OUTP:
#            for l in TestUI.LAYER+TestUI.LAYER_ASPATIAL+TestUI.LAYER_GEODETIC+TestUI.LAYER_PROBLEM:
#                TestUI.prepLayer(l,o)
                
    
    def setUp(self):
        self.key = 'blahblah'#import RK; RK().key
        
        self.host = '144.66.6.86'
        
        self.mssvr = 'LZ104588-VM\SQLExpress'
        self.msdvr = 'SQL Server Native Client 11.0'
        self.msdb = 'LDSINCR'
        self.pgdb = 'ldsincr'
        self.pgprt = '5432'
        self.pgsch = 'lds'
        
        self.msusr = 'mssqluser'
        self.mspwd = 'mssqlpass'
        self.pgusr = 'pguser'
        self.pgpwd = 'pgpass'
        
        
        self.pg_str = "\"PG:dbname='{}' host='{}' port='{}' user='{}' password='{}' active_schema={}\"".format(self.pgdb,self.host,self.pgprt,self.pgusr,self.pgpwd,self.pgsch)
        self.ms_str = "MSSQL:server={};database={};UID={};PWD={};Driver={}".format(self.mssvr, self.msdb, self.msusr, self.mspwd,self.msdvr)
        
        #super(TestUI,self).setUp()
        if 'inux' in sys.platform:
            self.PATH = self.PATH_L
            self.CONN_STR = self._CONN_STR_L
            self.CONF = self.CONF_E            
            self.CONF2 = self.CONF_I
            self.OUTP = self.OUTP_L
        elif sys.platform == 'win32':
            self.PATH = self.PATH_W
            self.CONN_STR = self._CONN_STR_W
            self.CONF = self.CONF_WE
            self.CONF2 = self.CONF_WI
            self.OUTP = self.OUTP_W
        elif sys.platform == 'cygwin':
            self.PATH = self.PATH_C
            self.CONN_STR = self._CONN_STR_W
            self.CONF = self.CONF_WE
            self.CONF2 = self.CONF_WI
            self.OUTP = self.OUTP_W
            sys.path.append('/cygdrive/c/progra~1/GDAL/python/gdal/osgeo')
        #elif os.name in ('os2', 'mac', 'ce','riscos')
        #    sys.exit()
        else:
            sys.exit(1)
            
            
    def tearDown(self):
        #super(TestUI,self).tearDown()
        pass


#    def test00CleanDatabase(self):
#        '''prep by cleaning used layers'''
#        for o in self.OUTP:
#            for l in TestUI.LAYER+TestUI.LAYER_ASPATIAL+TestUI.LAYER_GEODETIC: #+TestUI.LAYER_PROBLEM:
#                self.prepLayer(l,o)
        
        
    def test01RequestHelpPage(self):
        st = 'python '+self.PATH+'ldsreplicate.py -h'
        print st
        self.assertEquals(os.system(st),0)
        
        
    def test02BasicIncremental(self):
        '''Simple incremental URL test'''
        for o in self.OUTP:
            cmd = "python "+self.PATH+"ldsreplicate.py"

            st = cmd+" -s "+self.strSelector(0)+" -d "+self.pg_str if 'inux' in sys.platform else self.ms_str+" "+o
            print 0,st
            rv = subprocess.call(self.escapeCL(st),shell=True)
            self.assertEquals(rv,0)
            
    def test03LDSStringErrorCatch(self):
        '''Check for common URL errors and whether they're caught or not'''
        for o in self.OUTP:
            cmd = "python "+self.PATH+"ldsreplicate.py"
            for i in range(1,5):
                
                st = cmd+" -s "+self.strSelector(i)+" -d "+self.pg_str if 'inux' in sys.platform else self.ms_str+" "+o
                print i,st
                #os.system(self.escapeCL(st))
                with self.assertRaises(MalformedConnectionString):
                    subprocess.check_call(self.escapeCL(st),shell=True)
            

#        

    def countdown(self):
        '''simple timer to allow user inspection/interrupt'''
        print 'STOPPING Process'
        print 'RESUMING Process in 60s'
        time.sleep(50)
        print 'RESUMING Process in 10s'
        time.sleep(5)
        print 'RESUMING Process in 5s'
        time.sleep(1)
        print 'RESUMING Process in 4s'
        time.sleep(1)
        print 'RESUMING Process in 3s'
        time.sleep(1)
        print 'RESUMING Process in 2s'
        time.sleep(1)
        print 'RESUMING Process in 1s'
        time.sleep(1)
        print 'RESUMING Process NOW'
            
    def prepLayerGeodetic(self,o):
        self.prepLayer('v:x1029',o)    
        self.prepLayer('v:x839',o)    
        self.prepLayer('v:x784',o)    
        self.prepLayer('v:x787',o)    
        self.prepLayer('v:x786',o)    
        self.prepLayer('v:x789',o)    
        self.prepLayer('v:x788',o) 
        
    def prepLayer(self,l,o):
        '''Common layer clean function'''
        stc = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l '+l+' clean '+o
        print stc
        os.system(stc)
        
    def escapeCL(self,cl):
        return cl.replace('&','\&').replace(';','\;')
    
    def strSelector(self,i):
        #errors to catch; 0 ok, 1 layer mismatch, 2 no changeset, 3 d1>d2?, 4 wf|ms
        d = {
         0:"/v/x785-changeset/wfs?service=WFS&request=GetFeature&typeName=v:x785-changeset&viewparams=from:2010-01-01;to:2012-12-01&MAXFEATURES=3",
         1:"/v/x785-changeset/wfs?service=WFS&request=GetFeature&typeName=v:x700-changeset&viewparams=from:2010-01-01;to:2012-12-01&MAXFEATURES=3",
         2:"/v/x785-change5et/wfs?service=WFS&request=GetFeature&typeName=v:x785-changeset&viewparams=from:2010-01-01;to:2012-12-01&MAXFEATURES=3",
         3:"/v/x785-changeset/wfs?service=WFS&request=GetFeature&typeName=v:x785-changeset&viewparams=from:2012-01-01;to:2010-12-01&MAXFEATURES=3",
         4:"/v/x785-changeset/wms?service=WFS&request=GetFeature&typeName=v:x785-changeset&viewparams=from:2010-01-01;to:2012-12-01&MAXFEATURES=3"
         }
        
        return self.URL+self.key+d.get(i)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()