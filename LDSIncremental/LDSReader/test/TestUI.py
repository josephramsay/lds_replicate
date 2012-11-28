'''
Created on 17/08/2012

@author: jramsay
'''
import unittest
import os,sys


class TestUI(unittest.TestCase):
    '''basic test of command line arguments and whether they work as expected'''

    LPATH = '/home/jramsay/git/LDS/LDSIncremental/LDSReader/'
    WPATH = 'C:\\Data\\workspace\\LDS\\LDS\\LDSIncremental\\LDSReader\\'
    OUTP = ('fg','sl')
    #        WACA,    land-dist, street-asp
    LAYR = ('v:x836','v:x785',  'v:x1208')
    DATE = '2012-03-20'
    CONF = 'ldsincr.external.conf' 
    
    _CONN_STR1 = "PG:dbname='ldsincr' host='144.66.6.86' port='5432' user='pguser' password='pgpass' active_schema=lds"
    _CONN_STR2 = "PG:dbname='jrdb' host='144.66.6.86' port='5432' user='pguser' password='pgpass'"
    
    def setUp(self):
        if os.name == 'posix':
            self.PATH = self.LPATH
        elif os.name == 'nt':
            self.PATH = self.WPATH
        #elif os.name in ('os2', 'mac', 'ce','riscos')
        #    sys.exit()
        else:
            sys.exit(1)
            
    def tearDown(self):
        pass


#    def test1CmdLine_Help(self):
#        os.system('python main.py -h')
#        
#    def test2CmdLine_AutoLayer(self):
#        os.system('python main.py -l v:x787 pg')
#        
#    def test3CmdLine_DefLayer(self):
#        os.system('python main.py -l v:x787 -f 2010-01-01 -t 2012-07-01 pg')
#        
#    def test4CmdLine_AllFD(self):
#        os.system('python main.py -l v:x787 -f ALL pg')
        
    def test5InitClone(self):
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
                
##    def test6IncrCopy2Part(self):
##        for o in self.OUTP:          
##            for l in self.LAYR:
##
##                #TO: incremental copy from first day to DATE
##                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u ldsincr.external.conf -t '+self.DATE+' '+o
##                print st
##                self.assertEquals(os.system(st),0)
##                #FROM: incremental from DATE to latest
##                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u ldsincr.external.conf -f '+self.DATE+' '+o
##                print st
##                self.assertEquals(os.system(st),0)
#                
#    def test7IncrCopy2Auto(self):
#        for o in self.OUTP:          
#            for l in self.LAYR:
#
#                #TO: incremental copy from first day to DATE
#                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -t '+self.DATE+' '+o
#                print st
#                self.assertEquals(os.system(st),0)
#                #FROM: incremental from DATE to latest
#                st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' '+o
#                print st
#                self.assertEquals(os.system(st),0)
                
                
    def test8AutoConnOverride(self):    
        for l in self.LAYR:

            #auto fill using a command line configured connection string
            st = 'python '+self.PATH+'ldsreplicate.py -l '+l+' -u '+self.CONF+' -d "'+self._CONN_STR2+'" pg'
            print st
            self.assertEquals(os.system(st),0)
    
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()