'''
v.0.0.1

LDSReplicate -  TestSize

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Speed comparisons between driverCopy and featureCopy using -1 -2 forcing flags

Created on 17/09/2012

@author: jramsay
'''

import unittest
import sys
import pcapy

sys.path.append('..')

from lds.LDSUtilities import LDSUtilities

ldslog = LDSUtilities.setupLogging()

from lds.TransferProcessor import TransferProcessor


#class TestUI(LDSIncrTestCase):

class TestCaseRunner(unittest.TestCase):
    def run(self,result=None):
        '''Class to override and bypass stop-on-fail assertions'''
        if result.failures or result.errors:
            print "aborted"
        else:
            super(TestCaseRunner, self).run(result)
            
class TestSize(unittest.TestCase):
    '''Data Size test of ldsreplicate.py. Must be run as admin/root to get access to interfaces'''
    
    #        WACA,    land-dist, name-assoc, antarctic (non-NZ sref, RSRGD)
    LAYER = ('v:x787',)#('v:x772','v:x836','v:x785', 'v:x787', 'v:x1203')
    LAYER_GEODETIC = ('v:x784','v:x786','v:x787','v:x788','v:x789','v:x817','v:x839','v:x1029')
    LAYER_ASPATIAL = ('v:x1203','v:x1209','v:x1204','v:x1208','v:x1211','v:x1210','v:x1199')
    LAYER_PROBLEM = ('v:x772','v:x293')
    #cables,coast
    LAYER_HYDRO = ('v:x1091','v:x384')
    #road-cl,lake
    LAYER_TOPO = ('v:x329','v:x293')
    
    LAYER_ALL = LAYER_GEODETIC+LAYER_ASPATIAL+LAYER_HYDRO+LAYER_TOPO
    #2113=Wellington NZGD2000, 3788=AKL islands WGS84,2759=NAD83/HARN Alabama
    
    PATH_L = '/home/jramsay/git/LDS/LDSReplicate/'
    PATH_W = 'F:\\git\\LDS\\LDSReplicate\\'
    PATH_C = PATH_W.replace('\\','/').replace('C:','/cygdrive/c')
    OUTP_L = ('pg',)#'fg','sl')
    OUTP_W = ('ms',)#'sl')
    CONF_2 = 'ldsincr.gml2.conf'
    CONF_3 = 'ldsincr.gml3.conf'
    CONF_J = 'ldsincr.json.conf'
    
    _CONN_STR_L = "PG:dbname='jrdb' host='144.66.6.86' port='5432' user='pguser' password='pgpass'"
    _CONN_STR_W = "MSSQL:server=LZ104588-VM\SQLExpress;database=LDSINCR;UID=mssqluser;PWD=mssqlpass"
    
    
    def setUp(self):
        #super(TestUI,self).setUp()
        if 'inux' in sys.platform:
            self.PATH = self.PATH_L
            self.CONN_STR = self._CONN_STR_L
            self.OUTP = self.OUTP_L
        elif sys.platform == 'win32':
            self.PATH = self.PATH_W
            self.CONN_STR = self._CONN_STR_W
            self.OUTP = self.OUTP_W
        elif sys.platform == 'cygwin':
            self.PATH = self.PATH_C
            self.CONN_STR = self._CONN_STR_W
            self.OUTP = self.OUTP_W
            sys.path.append('/cygdrive/c/progra~1/GDAL/python/gdal/osgeo')
        #elif os.name in ('os2', 'mac', 'ce','riscos')
        #    sys.exit()
        else:
            sys.exit(1)
            
            
            
    def tearDown(self):
        #super(TestUI,self).tearDown()
        pass

        
    def testBasicCompare(self):
        '''Simple PG layer compare'''

        for o in self.OUTP:
            for l in self.LAYER:#+self.LAYER_ASPATIAL+self.LAYER_GEODETIC+self.LAYER_HYDRO+self.LAYER_TOPO:
                
                #tp = TransferProcessor(ly,gp,   ep,   fd,   td,   sc,   dc,   cq,   uc,        fbf)

                tp2 = TransferProcessor(l, None, None, None, None, None, None, None, self.CONF_2, None)
                self.prepLayer(tp2,o)
                d2t,d2r,d2c = self.monitor(self.selectProcess(tp2,o))
                print 'GML2::layer::',l,' data='+str(d2t/1000)+'kb.','rate='+str(d2r)+'b/s.','count='+str(d2c)
                
                tp3 = TransferProcessor(l, None, None, None, None, None, None, None, self.CONF_3, None)
                self.prepLayer(tp3,o)
                d3t,d3r,d3c = self.monitor(self.selectProcess(tp3,o))
                print 'GML3::layer::',l,' data='+str(d3t/1000)+'kb.','rate='+str(d3r)+'b/s.','count='+str(d3c)

                tpj = TransferProcessor(l, None, None, None, None, None, None, None, self.CONF_J, None)
                self.prepLayer(tpj, o) 
                djt,djr,djc = self.monitor(self.selectProcess(tpj,o))
                print 'JSON::layer::',l,' data='+str(djt/1000)+'kb.','rate='+str(djr)+'b/s.','count='+str(djc)
                
        self.assertTrue(True)
        
        
            
    def selectProcess(self,processor,procname):
        return processor.processLDS(processor.initDestination(procname)) 
    
    def executeComparison(self,p1,p2):

        return self.execute(p1),self.execute(p2)

    def execute(self,proc):
        try:
            proc()
        except Exception as e:
            print e
            pass

    
    def monitor(self,proc):
        dev = pcapy.findalldevs()[0]
        nm = NetMonitor(dev,None)
        nm.start()
        proc()
        nm.stop()
        nm.join()
        
        return nm.totaltransfer,nm.getAverageRate(),nm.count
    
        
    def prepLayerGeodetic(self,o):
        self.prepLayer('v:x1029',o)    
        self.prepLayer('v:x839',o)    
        self.prepLayer('v:x784',o)    
        self.prepLayer('v:x787',o)    
        self.prepLayer('v:x786',o)    
        self.prepLayer('v:x789',o)    
        self.prepLayer('v:x788',o) 
        
    def prepLayer(self,tp,o):
        '''Common layer clean function'''
        tp.setCleanConfig()
        self.execute(self.selectProcess(tp,o))
        tp.clearCleanConfig()

        
###----------------------------------------------------------------------------  
import threading
import time
from pcapy import PcapError

class NetMonitor(threading.Thread):

    _timeout = 0.001
    _snaplen = 65535#4096

    @classmethod
    def getNetInterfaces(cls):
        return pcapy.findalldevs()

    def __init__(self, device, bpf_filter):

        super(NetMonitor,self).__init__()

        self.active = True
        try:
            self.netmon = pcapy.open_live(device, self._snaplen, True, int(self._timeout * 1000))
            self.netmon.setfilter('host wfs.data.linz.govt.nz')
            self.dumper = self.netmon.dump_open("../log/pktdump.log")
        except PcapError as pce:
            ldslog.error("Must be root/admin to access network traffic data. "+str(pce))
            raise pce
            
        self.currentrate = 0
        self.totaltransfer = 0 # total number of Bytes transfered

        #<--- this is to calc average transfer B/s
        self.temp_bytes_per_sec = 0 # sums up B/s values from each dispatch iteration (eventually used to calc average value)
        self.count = 0 # number of dispatch iterations (eventually used to calc average B/s value)
        #--->

        self.dispatch_bytes = 0 # sums up packets size for one dispatch call


    def handlePacket(self, header, data):
        # method is called for each packet by dispatch call (pcapy)
        self.dispatch_bytes += len(data) #header.getlen() #len(data)
        #ldslog.debug("h: (len:{}, clen:{}, ts:{}), d:{}".format(header.getlen(), header.getcaplen(), header.getts(), len(data)))
        self.dumper.dump(header, data)


    def update(self):
        self.dispatch_bytes = 0
        # process packets
        packets_nr = self.netmon.dispatch(-1, self.handlePacket)
        self.totaltransfer += self.dispatch_bytes

        self.count += 1
        self.currentrate = self.dispatch_bytes / self._timeout  # add single dispatch B/s -> timeout is 1 s
        self.temp_bytes_per_sec += self.currentrate

        #ldslog.debug('Count:{},\tCurrent Rate: {}B/s,\tAverage Rate: {}B/s,\tTotal:{}B, Pkts NR: {}'.format(self.count, self.currentrate, self.getAverageRate(), self.totaltransfer, packets_nr))

        return self.currentrate, packets_nr



    def getAverageRate(self):
        if self.count:
            return self.temp_bytes_per_sec / self.count
        else:
            return 0

    def getCurrentRate(self):
        return self.currentrate


    def run(self):
        while(self.active):
            #call update() every timeout (1s) till no longer active
            self.update()
            time.sleep(self._timeout)

    def stop(self):
        self.active = False
        

    # average B/s rate
    #avg_rate = property(getAverageRate)
    # current B/s rate 
    #current_rate = property(getCurrentRate)

###----------------------------------------------------------------------------        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()