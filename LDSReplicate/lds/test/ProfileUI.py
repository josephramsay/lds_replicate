'''
v.0.0.1

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

from lds.TransferProcessor import TransferProcessor

    
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
CONF = 'ldsincr.lnx.conf'
PATH = '.'
OUTP = ['pg']

def profile01AutoFillLayer(conf=CONF):
    '''Simple layer populate'''
    
    for o in OUTP:
        #tp = TransferProcessor(ly,gp,   ep,   fd,   td,   sc,   dc,   cq,   uc)
        tp1 = TransferProcessor('v:x785', None, None, None, None, None, None, None, conf)
        selectProcess(tp1,o)
        
        
def main():
    setUp()
    cProfile.run('profile01AutoFillLayer()','temp')

    
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    main()