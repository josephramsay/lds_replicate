'''
Created on 17/08/2012

@author: jramsay
'''
import unittest
import os
import sys
import time

#class TestUI(LDSIncrTestCase):
class TestUI(unittest.TestCase):
    '''Basic tests of ldsreplicate.py using command line arguments to see whether they work as expected'''
    
    #        WACA,    land-dist, name-assoc, antarctic (non-NZ sref, RSRGD)
    LAYR = ('v:x836','v:x785',  'v:x1203',  'v:x789')
    #2113=Wellington NZGD2000, 3788=AKL islands WGS84,2759=NAD83/HARN Alabama
    EPSG = (2113,3788,2759)
    DATE = '2012-03-20'
    
    DATE1 = '2012-02-25'
    DATE2 = '2012-09-17'
    
    PATH_L = '/home/jramsay/git/LDS/LDSIncremental/LDSReader/'
    PATH_W = 'C:\\data\\workspace\\LDS\\LDSIncremental\\LDSReader\\'
    PATH_C = '/cygdrive/c/data/workspace/LDS/LDSIncremental/LDSReader/'
    OUTP_L = ('fg',)#'fg','sl')
    OUTP_W = ('ms',)
    CONF_L = 'ldsincr.external.conf'
    CONF_W = 'ldsincr.windows.conf' 
    
    _CONN_STR_L = "PG:dbname='jrdb' host='144.66.6.86' port='5432' user='pguser' password='pgpass'"
    _CONN_STR_W = "MSSQL:server={LZ104588-VM\SQLExpress};database={LDSINCR};UID={mssqluser};PWD={mssqlpass};Driver={MSSQLSpatial}"
    
    
#    @classmethod
#    def setUpClass(cls):
#        some_other_layers = ('v:x772',  'v:x1203')
#        geodetic_layers = ('v:x784','v:x786','v:x787','v:x788','v:x789','v:x817','v:x839','v:x1029')
#        for o in TestUI.OUTP:
#            for l in TestUI.LAYR+some_other_layers+geodetic_layers:
#                st = 'python '+TestUI.PATH+'ldsreplicate.py -l '+l+' -u '+TestUI.CONF+' clean '+o
#                print st
#                TestUI.assertEquals(os.system(st),0)
                
    
    def setUp(self):
        print sys.platform
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
        some_other_layers = ('v:x772',  'v:x1203')
        geodetic_layers = ('v:x784','v:x786','v:x787','v:x788','v:x789','v:x817','v:x839','v:x1029')
        for o in self.OUTP:
            for l in TestUI.LAYR+some_other_layers+geodetic_layers:
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
        
    def test04HugeLayer(self):
        '''Attempts to get 772. This is supposed to fail since 772 is in the "problemlayers" list'''
        for o in self.OUTP:
            st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x772 '+o
            print st
            self.assertEquals(os.system(st),0)


    def test05InitClone(self):
        '''Test INIT of layer config file and layer CLEAN functions'''
        for o in self.OUTP:
            for l in self.LAYR:
                #INIT: clone a layer from LDS
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' init '+o
                print st
                self.assertEquals(os.system(st),0)
                #CLEAN the layer from the DST and reset the conf  
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' clean '+o
                print st
                self.assertEquals(os.system(st),0)
                
                
    def test06IncrCopy2Part(self):
        '''Test incremental functionality using an intermediate date, "data up To DATE" and "data From DATE to present"'''
        for o in self.OUTP:          
            for l in self.LAYR:
                self.prepLayer(l,o)
                #TO: incremental copy from first day to DATE
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -t '+self.DATE+' '+o
                print st
                self.assertEquals(os.system(st),0)
                
                #FROM: incremental from DATE to latest
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -f '+self.DATE+' '+o
                print st
                self.assertEquals(os.system(st),0)
                
                
    def test07IncrCopy2Auto(self):
        '''Test incremental functionality by "get data up To DATE" and "Incremental AUTO fill last DATE to present"'''
        for o in self.OUTP:
            for l in self.LAYR:
                self.prepLayer(l,o)
                #TO: incremental copy from first day to DATE
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -t '+self.DATE+' '+o
                print st
                self.assertEquals(os.system(st),0)
                #FROM: incremental from DATE to latest
                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' '+o
                print st
                self.assertEquals(os.system(st),0)
                
                
    def test08AutoConnOverride(self):
        '''Test -d option overriding connection string''' 
        for l in self.LAYR:
            self.prepLayer(l,self.CONN_STR.lower()[0:2])
            #auto fill using a command line configured connection string
            st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -d "'+self.CONN_STR+'" '+self.CONN_STR.lower()[0:2]
            print st
            self.assertEquals(os.system(st),0)    
            
            
    def test09AutoFilterGeodetic(self):
        '''Test group selection using -g option'''
        for o in self.OUTP:
            self.prepLayerGeodetic(o)              
            #auto fill using a command line configured connection string
            st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -g Geodetic '+o
            print st
            self.assertEquals(os.system(st),0)
        
        
    def test10AutoFilterWithDates(self):
        '''TTest group selection using -g option and defined date ranges'''
        for o in self.OUTP:
            self.prepLayerGeodetic(o)        
            st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -g Geodetic -t '+self.DATE1+' -f '+self.DATE2+' '+o
            print st
            self.assertEquals(os.system(st),0)    
            
            
    def test11AspatialClone(self):
        '''Test an A-Spatial layer. (Layer 1203 also contains sufi's so this also tests the 64 bit workaround)'''
        for o in self.OUTP:    
            self.prepLayer('v:x1203',o)
            st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x1203 '+o
            print st
            self.assertEquals(os.system(st),0)
    
    
    def test12EPSGChange(self):
        '''Test different SR conversions'''
        for o in self.OUTP:    
            for e in self.EPSG: 
                
                self.prepLayer('v:x785',o)
                st = 'python '+self.PATH+'ldsreplicate.py -u '+self.CONF+' -l v:x785 -e '+str(e)+' '+o
                print st
                self.assertEquals(os.system(st),0)
                
    def test13CQLSelection(self):
        '''Test different SR conversions. Doesnt really affect processing since this is serverside'''
        for o in self.OUTP:    
                
            self.prepLayer('v:x785',o)
            st = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l v:x785 -c 'id=1001' "+o
            print st
            self.assertEquals(os.system(st),0) 
            
            '''NB. Need to take care single quoting alphabetic values'''
            self.prepLayer('v:x785',o)
            st = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l v:x785 -c name=\\'Southland\\' "+o
            print st
            self.assertEquals(os.system(st),0) 
            
            self.prepLayer('v:x785',o)
            st = "python "+self.PATH+"ldsreplicate.py -u "+self.CONF+" -l v:x785 -c 'bbox(shape,164.88,-47.46,169.45,-43.85)' "+o
            print st
            self.assertEquals(os.system(st),0)
        

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
        os.system(stc)
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()