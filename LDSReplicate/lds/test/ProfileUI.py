'''
v.0.0.9

LDSReplicate -  ProfileUI

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Tests for typical user input

Created on 17/09/2012

@author: jramsay
'''

import os
import re
import sys
import time

import cProfile
import pstats

from lds.LDSUtilities import LDSUtilities
from lds.TransferProcessor import TransferProcessor
from lds.gui.LayerConfigSelector import LayerConfigSelector

ldslog = LDSUtilities.setupLogging()
    
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

OUTP_L = ('PostgreSQL',)#'fg','sl')
OUTP_W = ('MSSQLSpatial',)#'sl','fg')
CONF_L = 'ldsincr.lnx.conf'
CONF_W = 'ldsincr.win.conf' 

_CONN_STR_L = "PG:dbname='jrdb' host='144.66.6.86' port='5432' user='pguser' password='pgpass'"
_CONN_STR_W = "MSSQL:server=LZ104588-VM\SQLExpress;database=LDSINCR;UID=mssqluser;PWD=mssqlpass"

    

def setUp():
    if 'inux' in sys.platform:
        PATH = PATH_L
        CONN_STR = _CONN_STR_L
        CONF = CONF_L            
        OUTP = OUTP_L
    elif sys.platform == 'win32':
        PATH = PATH_W
        CONN_STR = _CONN_STR_W
        CONF = CONF_W
        OUTP = OUTP_W
    elif sys.platform == 'cygwin':
        PATH = PATH_C
        CONN_STR = _CONN_STR_W
        CONF = CONF_W
        OUTP = OUTP_W
        sys.path.append('/cygdrive/c/progra~1/GDAL/python/gdal/osgeo')
    else:
        sys.exit(1)
            
            

        
def selectProcess(processor,procname):
    return processor.processLDS(processor.initDestination(procname)) 
         
        
        

CONN_STR = ''
CONF = 'fglint.conf'
PATH = '.'
OUTP = ['pg']

def profile01AutoFillLayer(conf=CONF):
    '''Simple layer populate'''
     
    for o in OUTP:
        #tp = TransferProcessor(ly,gp,   ep,   fd,   td,   sc,   dc,   cq,   uc)
        tp1 = TransferProcessor(None,'v:x785', None, None, None, None, None, None, None, conf)
        selectProcess(tp1,o)
        
def profile02FGDBUpdateSpeedTest(conf=CONF):
    '''Simple layer populate'''
    todate = None
    tp = TransferProcessor(None,'v:x784',None,None,todate,None,None,None,'fgx.conf')
    selectProcess(tp,'FileGDB')
    
def profile03FGDBLayerConfigSpeed(conf=CONF):
    '''Simple layer populate'''
    lcs = LayerConfigSelector()
    lcs.main()
        
def main():
    setUp()
    cProfile.run('profile03FGDBLayerConfigSpeed()','temp')

    
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    main()