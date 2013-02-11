'''
v.0.0.1

LDSIncremental -  TestUI

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Tests for typical user input

Created on 17/09/2012

@author: jramsay
'''

import unittest
import os
import sys
import time
import logging
import ConfigParser

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
    

    PATH_L = '/home/jramsay/git/LDS/LDSIncremental/LDSReader/'
    PATH_W = 'F:\\git\\LDS\\LDSIncremental\\LDSReader\\'
    PATH_C = PATH_W.replace('\\','/').replace('C:','/cygdrive/c')
    OUTP_L = ('pg','fg','sl')
    OUTP_W = ('ms',)#'sl','fg')
    CONF_I = 'ldsincr.internal.conf'
    CONF_E = 'ldsincr.external.conf'
    CONF_B = 'ldsincr.broken.conf'
    CONF_WI = 'ldsincr.windows.internal.conf' 
    CONF_WE = 'ldsincr.windows.external.conf' 
    
    _CONN_STR_L = "PG:dbname='jrdb' host='144.66.6.86' port='5432' user='pguser' password='pgpass'"
    _CONN_STR_W = "MSSQL:server=LZ104588-VM\SQLExpress;database=LDSINCR;UID=mssqluser;PWD=mssqlpass"
    


#    @classmethod
#    def setUpClass(cls):
#        some_other_layers = ('v:x772',  'v:x1203')
#        geodetic_layers = ('v:x784','v:x786','v:x787','v:x788','v:x789','v:x817','v:x839','v:x1029')
#        for o in TestUI.OUTP:
#            for l in TestUI.LAYER+TestUI.LAYER_ASPATIAL+TestUI.LAYER_GEODETIC+TestUI.LAYER_PROBLEM:
#                TestUI.prepLayer(l,o)
                
    
    def setUp(self):
        self.cp = None
        self.fname = None
        self.sec = None
        self.prop = None
        self.real_val = None
        self.bogus_val = None
        

        
        #super(TestUI,self).setUp()
        if 'inux' in sys.platform:
            self.PATH = self.PATH_L
            self.CONN_STR = self._CONN_STR_L
            self.CONF = self.CONF_B            
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


   
                
    def test00BasicConfigSubstutionMechanism(self):
        '''Test different SR conversions. Doesnt really affect processing since this is serverside'''
        try:
            self.substituteConf(self.CONF,'LDS','key','aaabbbcccdddeeefffggghhhiiijjjkk')
            st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 pg'
            print st
            self.assertEquals(os.system(st),0)
            
        finally:
            self.restoreConf()
            

            
    def test01PGFailingConfSubst(self):
        '''wrong ip'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 pg'
        sb = (self.CONF,'PostgreSQL','host','144.66.6.6')
        self.runCommand(sb,st)
    def test02PGFailingConfSubst(self):
        '''wrong port'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 pg'
        sb = (self.CONF,'PostgreSQL','port','8080')
        self.runCommand(sb,st)
    def test03PGFailingConfSubst(self):
        '''wrong schema'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 pg'
        sb = (self.CONF,'PostgreSQL','schema','fakeschema')
        self.runCommand(sb,st)
    def test04PGFailingConfSubst(self):
        '''fake user'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 pg'
        sb = (self.CONF,'PostgreSQL','user','fakeuser')
        self.runCommand(sb,st)
    def test05PGFailingConfSubst(self):
        '''fake password'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 pg'
        sb = (self.CONF,'PostgreSQL','pass','fakepass')
        #self.runCommand(sb,st)
        self.assertFalse(False)
        
    
    
    
    def test11FGFailingConfSubst(self):
        '''non existant path. actual path will create a file there'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 fg'
        sb = (self.CONF,'FileGDB','path','/home/somewherefictional')
        self.runCommand(sb,st)
    def test12FGFailingConfSubst(self):
        '''non existant name. probably build a new db file'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 fg'
        sb = (self.CONF,'FileGDB','name','fakefgname')
        #self.runCommand(sb,st)
        self.assertFalse(False)
        
        
        
    def test21SLFailingConfSubst(self):
        '''non existant path'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 sl'
        sb = (self.CONF,'SQLite','path','/home/somewherefictional')
        self.runCommand(sb,st)
    def test22SLFailingConfSubst(self):
        '''non existant name'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 sl'
        sb = (self.CONF,'SQLite','name','fakeslname')
        #self.runCommand(sb,st)
        self.assertFalse(False)
        
        
    #its okay to run the MS stuff since its intended to fail anyway... test whether connect is possible
    def test31MSFGFailingConfSubst(self):
        '''wrong server'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 ms'
        sb = (self.CONF,'MSSQLSpatial','server','host\instance')
        self.runCommand(sb,st)
    def test32MSFGFailingConfSubst(self):
        '''wrong db name'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 ms'
        sb = (self.CONF,'MSSQLSpatial','dbname','fakedbname')
        self.runCommand(sb,st)
    def test33MSFGFailingConfSubst(self):
        '''missing schema... new schema created?'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 ms'
        sb = (self.CONF,'MSSQLSpatial','schema','noschema')
        self.runCommand(sb,st)
    def test34MSFGFailingConfSubst(self):
        '''no user'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 ms'
        sb = (self.CONF,'MSSQLSpatial','user','fakeuser')
        self.runCommand(sb,st)
    def test35MSFGFailingConfSubst(self):
        '''wrong password'''
        st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 ms'
        sb = (self.CONF,'MSSQLSpatial','pass','fakepass')
        self.runCommand(sb,st)






    def runCommand(self,subs,cmd):
        '''run the requested command with the request config substitutions'''
        
        print subs,cmd
        try:
            self.substituteConf(subs[0],subs[1],subs[2],subs[3])
            self.assertFalse(os.system(cmd),0)
        finally:
            self.restoreConf()

    def substituteConf(self,fname,sec,prop,bogus_val):
        '''Reads named config file'''
        self.fname = self.PATH+'../'+fname
        self.sec= sec
        self.prop = prop
        self.bogus_val = bogus_val
        
        #read current val
        self.cp = ConfigParser.ConfigParser()
        self.cp.read(self.fname)
        self.real_val = self.cp.get(self.sec,self.prop)
        
        
        #replace with fake
        self.cp.set(self.sec,self.prop,self.bogus_val)
        with open(self.fname, 'w') as configfile:
            self.cp.write(configfile)
        ldslog.debug('['+self.sec+']'+self.prop+'='+self.real_val+' substituted')    


    def restoreConf(self):
        if self.fname is not None and self.sec is not None and self.prop is not None:
            try:            
                self.cp.set(self.sec,self.prop,self.real_val)
                with open(self.fname, 'w') as configfile:
                    self.cp.write(configfile)
                ldslog.debug('['+self.sec+']'+self.prop+'='+self.real_val+' restored')    
                #now forget all thesefake values
                self.fname
                self.sec = None
                self.prop = None
                self.real_val = None                                                                                    
            except Exception as e:
                ldslog.error('Problem writing LM date to layer config file. '+str(e))
        else:
            print 'Cannot restore conf'
            
        
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
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()