'''
v.0.0.1

LDSReplicate -  TestUI

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
import re
import sys
import time
import logging

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
    
    PATH_L = '../../'
    PATH_W = '..\\..\\'
    PATH_C = re.sub(r'(\w):',r'/cygdrive/\1',PATH_W.replace('\\','/'))

    OUTP_L = ('pg',)#'fg','sl')
    OUTP_W = ('ms',)#'sl','fg')
    CONF_L = 'ldsincr.lnx.conf'
    CONF_W = 'ldsincr.win.conf' 
    
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
        #super(TestUI,self).setUp()
        if 'inux' in sys.platform:
            self.PATH = self.PATH_L
            self.CONN_STR = self._CONN_STR_L
            self.CONF = self.CONF_L            
            self.OUTP = self.OUTP_L
        elif sys.platform == 'win32':
            self.PATH = self.PATH_W
            self.CONN_STR = self._CONN_STR_W
            self.CONF = self.CONF_W
            self.OUTP = self.OUTP_W
        elif sys.platform == 'cygwin':
            self.PATH = self.PATH_C
            self.CONN_STR = self._CONN_STR_W
            self.CONF = self.CONF_W
            self.OUTP = self.OUTP_W
            sys.path.append('/cygdrive/c/progra~1/GDAL/python/gdal/osgeo')
        #elif os.name in ('os2', 'mac', 'ce','riscos')
        #    sys.exit()
        else:
            sys.exit(1)
            
            
    def tearDown(self):
        #super(TestUI,self).tearDown()
        pass


    def test00CleanDatabase(self):
        '''prep by cleaning used layers'''
        for o in self.OUTP:
            for l in TestUI.LAYER+TestUI.LAYER_ASPATIAL+TestUI.LAYER_GEODETIC: #+TestUI.LAYER_PROBLEM:
                self.prepLayer(l,o)
        
        
    def test01RequestHelpPage(self):
        st = 'python '+self.PATH+'ldsreplicate.py -h'
        print st
        self.assertEquals(os.system(st),0)
        
        
    def test02AutoFillLayer(self):
        '''Simple layer populate'''
        for o in self.OUTP:
            st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 '+o
            print st
            self.assertEquals(os.system(st),0)
        
    def test03InternalExternal(self):
        '''Tests for any differences in the use of internal vs external config files'''
        for o in self.OUTP:
            for l in self.LAYER:
                self.prepLayer(l, o)
                ste = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+'-x -l '+l+' '+o
                print ste
                self.assertEquals(os.system(ste),0)
                
                self.prepLayer(l, o)
                sti = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+'-i -l '+l+' '+o
                print sti
                self.assertEquals(os.system(sti),0)    
                
    def test04LayerByName(self):
        '''Tests for any differences in the use of internal vs external config files'''
        for o in self.OUTP:
            for l in self.LAYER:
                self.prepLayer(l, o)
                
                if 'inux' in sys.platform:
                    ste = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l 'NZ Land Districts' "+o
                elif sys.platform == 'win32':
                    ste = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l "NZ Land Districts" '+o
                print ste
                self.assertEquals(os.system(ste),0)
                
            
#    def test03IncrementalFillLayer(self):
#        '''Layer clean and populate using supplied dates (following test 02 tests pop-clean-pop)'''
#        for o in self.OUTP:
#            st1 = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 clean '+o
#            print st1
#            self.assertEquals(os.system(st1),0)
#            
#            st2 = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x787 -f 2010-01-01 -t 2012-07-01 '+o
#            print st2
#            self.assertEquals(os.system(st2),0)

        
    def test05ProblemLayer(self):
        '''Attempts to get 772. Tests data partitioning solution... This takes a while to run! return to bypass'''
        return
        for o in self.OUTP:
            for l in self.LAYER_PROBLEM:
                st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l '+l+' '+o
                print st
                self.assertEquals(os.system(st),0)


    def test06InitClone(self):
        '''Test INIT of layer config file and layer CLEAN functions'''
        for o in self.OUTP:
            for l in self.LAYER:
                self.prepLayer(l,o)
                #INIT: clone a layer from LDS
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' init '+o
                print st
                self.assertEquals(os.system(st),0)
                
                
    def test07IncrCopy2Part(self):
        '''Test incremental functionality using an intermediate date, "data up To DATE" and "data From DATE to present"'''
        for o in self.OUTP:          
            for l in self.LAYER:
                self.prepLayer(l,o)
                #TO: incremental copy from first day to DATE
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -t '+self.DATE+' '+o
                print st
                self.assertEquals(os.system(st),0)
                
                #FROM: incremental from DATE to latest
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -f '+self.DATE+' '+o
                print st
                self.assertEquals(os.system(st),0)
                
                
    def test08IncrCopy2Auto(self):
        '''Test incremental functionality by "get data up To DATE" and "Incremental AUTO fill last DATE to present"'''
        for o in self.OUTP:
            for l in self.LAYER:
                self.prepLayer(l,o)
                #TO: incremental copy from first day to DATE
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -t '+self.DATE+' '+o
                print st
                self.assertEquals(os.system(st),0)
                #FROM: incremental from DATE to latest
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' '+o
                print st
                self.assertEquals(os.system(st),0)
                
                
    def test09AutoConnOverride(self):
        '''Test -d option overriding connection string''' 
        for l in self.LAYER:
            self.prepLayer(l,self.CONN_STR.lower()[0:2])
            #auto fill using a command line configured connection string
            st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -d "'+self.CONN_STR+'" '+self.CONN_STR.lower()[0:2]
            print st
            self.assertEquals(os.system(st),0)    
            
            
    def test10AutoFilterGeodetic(self):
        '''Test group selection using -g option'''
        for o in self.OUTP:
            self.prepLayerGeodetic(o)              
            #auto fill using a command line configured connection string
            st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -g Geodetic '+o
            print st
            self.assertEquals(os.system(st),0)
        
        
    def test11AutoFilterWithDates(self):
        '''Test group selection using -g option and defined date ranges'''
        for o in self.OUTP:
            self.prepLayerGeodetic(o)        
            st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -g Geodetic -t '+self.DATE1+' -f '+self.DATE2+' '+o
            print st
            self.assertEquals(os.system(st),0)    
            
 
            
    def test12AspatialClone(self):
        '''Test an A-Spatial layer. (Layer 1203 also contains sufi's so this also tests the 64 bit workaround)'''
        for o in self.OUTP:    
            self.prepLayer('v:x1203',o)
            st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x1203 '+o
            print st
            self.assertEquals(os.system(st),0)
    
    
    def test13EPSGChange(self):
        '''Test different SR conversions'''
        for o in self.OUTP:    
            for e in self.EPSG: 
                
                self.prepLayer('v:x785',o)
                st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x785 -e '+str(e)+' '+o
                print st
                self.assertEquals(os.system(st),0)
      
                
    def test14CQLSelection(self):
        '''Test different SR conversions. Doesnt really affect processing since this is serverside'''

        for o in self.OUTP:    
            
            if 'inux' in sys.platform:
                st1 = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l v:x785 -c 'id=1001' "+o
                '''NB. Need to take care single quoting alphabetic values'''
                st2 = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l v:x785 -c name=\\'Southland\\' "+o
                st3 = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l v:x785 -c bbox\(shape,164.88,-47.46,169.45,-43.85\) "+o
            elif sys.platform == 'win32':
                st1 = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l v:x785 -c id=1001 "+o
                st2 = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l v:x785 -c name='Southland' "+o
                st3 = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l v:x785 -c bbox(shape,164.88,-47.46,169.45,-43.85) "+o
                
            else:
                return       
            
                    
            self.prepLayer('v:x785',o)
            print st1
            self.assertEquals(os.system(st1),0) 
            
            self.prepLayer('v:x785',o)
            print st2
            self.assertEquals(os.system(st2),0) 
            
            self.prepLayer('v:x785',o)
            print st3
            self.assertEquals(os.system(st3),0)
        

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