'''
Created on 17/08/2012

@author: jramsay
'''
import unittest
import os
import sys
import logging
from datetime import datetime

ldslog = logging.getLogger('LDS')
ldslog.setLevel(logging.DEBUG)

df = os.path.normpath(os.path.join(os.path.dirname(__file__), "../debug.log"))
#df = '../debug.log'
fh = logging.FileHandler(df,'a')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
fh.setFormatter(formatter)
ldslog.addHandler(fh)


from LDSReader.TransferProcessor import TransferProcessor

#class TestUI(LDSIncrTestCase):

class TestCaseRunner(unittest.TestCase):
    def run(self,result=None):
        '''Class to override and bypass stop-on-fail assertions'''
        if result.failures or result.errors:
            print "aborted"
        else:
            super(TestCaseRunner, self).run(result)
            
class TestSpeed(unittest.TestCase):
    '''Basic tests of ldsreplicate.py using command line arguments to see whether they work as expected'''
    
    #        WACA,    land-dist, name-assoc, antarctic (non-NZ sref, RSRGD)
    LAYER = ('v:x836','v:x785',  'v:x1203')
    LAYER_GEODETIC = ('v:x784','v:x786','v:x787','v:x788','v:x789','v:x817','v:x839','v:x1029')
    LAYER_ASPATIAL = ('v:x1203','v:x1209','v:x1204','v:x1208','v:x1211','v:x1210','v:x1199')
    LAYER_PROBLEM = ('v:x772','v:x293')
    #cables,coast
    LAYER_HYDRO = ('v:x1091','v:x384')
    #road-cl,lake
    LAYER_TOPO = ('v:x329','v:x293')
    #2113=Wellington NZGD2000, 3788=AKL islands WGS84,2759=NAD83/HARN Alabama
    EPSG = (2113,3788,2759)
    DATE = '2012-03-20'
    
    DATE1 = '2012-02-25'
    DATE2 = '2012-09-17'
    
    PATH_L = '/home/jramsay/git/LDS/LDSIncremental/LDSReader/'
    PATH_W = 'C:\\data\\workspace\\LDS\\LDSIncremental\\LDSReader\\'
    PATH_C = PATH_W.replace('\\','/').replace('C:','/cygdrive/c')
    OUTP_L = ('pg',)#'fg','sl')
    OUTP_W = ('ms',)#'sl')
    CONF_I = 'ldsincr.internal.conf'
    CONF_E = 'ldsincr.external.conf'
    CONF_WI = 'ldsincr.windows.internal.conf' 
    CONF_WE = 'ldsincr.windows.external.conf' 
    
    _CONN_STR_L = "PG:dbname='jrdb' host='144.66.6.86' port='5432' user='pguser' password='pgpass'"
    _CONN_STR_W = "MSSQL:server=LZ104588-VM\SQLExpress;database=LDSINCR;UID=mssqluser;PWD=mssqlpass"
    
    
    def setUp(self):
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

        
    def testBasic(self):
        '''Simple PG layer compare'''
        timing = ()

        for o in self.OUTP:
            for l in self.LAYER_TOPO:#+self.LAYER_ASPATIAL+self.LAYER_GEODETIC+self.LAYER_HYDRO+self.LAYER_TOPO:
                print 'layer',l,'output',o
                
                #tp = TransferProcessor(ly,gp,   ep,   fd,   td,   sc,   dc,   cq,   uc,        fbf)
                tp1 = TransferProcessor(l, None, None, None, None, None, None, None, self.CONF, '1') #driver
                tp2 = TransferProcessor(l, None, None, None, None, None, None, None, self.CONF, '2') #feature
                d1 = self.execute(self.selectProcess(tp1,o)) 
                d2 = self.execute(self.selectProcess(tp2,o))
                
                print 'layer::',l,'featureCopy::',d1,'driverCopy::',d2
                timing += ((l,d1,d2),)

        s1=0
        s2=0
        handle = open('res.txt','w')
        handle.write('layer,fC,dC\n')
        for ln,t1,t2 in timing:
            s1 += t1
            s2 += t2
            handle.write(ln+','+str(t1)+','+str(t2)+'\n')
        handle.close()
        self.assertGreater(s1,s2)
        
        
            
    def selectProcess(self,processor,procname):
        return {
         'pg':processor.processLDS2PG,
         'ms':processor.processLDS2MSSQL,
         'sl':processor.processLDS2SpatiaLite,
         'fg':processor.processLDS2FileGDB,
         }.get(procname)   
    
    
    def executeComparison(self,p1,p2):

        return self.execute(p1),self.execute(p2)

    def execute(self,proc):
        st = datetime.now()
        proc()
        et = datetime.now()
        return (et-st).total_seconds()
   
            
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