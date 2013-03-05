'''
v.0.0.3

LDSIncremental -  ldsreplicate

| Copyright 2011 Crown copyright (c)
| Land Information New Zealand and the New Zealand Government.
| All rights reserved

This program is released under the terms of the new BSD license. See the LICENSE file for more information.

Created on 23/07/2012

@author: jramsay

Python script to translate fetch an LDS update between two dates. Intended to be used in a batch process rather than interactively. 

Usage:

python LDSReader/ldsreplicate.py -l <layer_id>
    [-f <from date>|-t <to date>|-c <cql filter>|-s <src conn str>|-d <dst conn str>|-v|-h] 
    <output> [full]
    
    -f (--fromdate) Date in yyyy-mm-dd format start of incremental range (omission assumes auto incremental bounds)
    -t (--todate) Date in yyyy-mm-dd format for end of incremental range (omission assumes auto incremental bounds)
    -l (--layer) Layer name/id in format v:x### (IMPORTANT. Omission assumes all layers)
    -g (--group) Layer sub group list for layer selection, comma separated
    -e (--epsg) Destination EPSG. Layers will be converted to this SRS
    -s (--source) Connection string for source DS
    -d (--destination) Connection string for destination DS
    -c (--cql) Filter definition in CQL format
    -h (--help) Display this message
    -v (--version) Display the version number"

'''

import sys
import os
import getopt
import logging
#import traceback

from datetime import datetime
from urllib2 import HTTPError

from TransferProcessor import TransferProcessor
from TransferProcessor import InputMisconfigurationException
from VersionChecker import VersionChecker, UnsupportedVersionException
from DataStore import DSReaderException

ldslog = logging.getLogger('LDS')
ldslog.setLevel(logging.DEBUG)


df = os.path.normpath(os.path.join(os.path.dirname(__file__), "../debug.log"))
#df = '../debug.log'
fh = logging.FileHandler(df,'a')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s %(lineno)d - %(message)s')
fh.setFormatter(formatter)
ldslog.addHandler(fh)

__version__ = '0.0.4'


def usage():
    print "Usage: python LDSReader/ldsreplicate.py -l <layer_id> [-f <from date>|-t <to date>|-c <cql filter>|-s <src conn str>|-d <dst conn str>|-v|-h] <output> [full]"
    print "For help use --help"

def main():
    '''Main entrypoint if the LDS incremental replication script
    
    usage: python LDSReader/ldsreplicate.py -l <layer_id>
        [-f <from date>|-t <to date>|-c <cql filter>|-s <src conn str>|-d <dst conn str>|-u <user_config>|-g <group keyword>|-e <conversion-epsg>|-h (help)] 
        <output>
    '''

    
    td = None
    fd = None
    ly = None
    gp = None
    ep = None
    sc = None
    dc = None
    cq = None
    uc = None
    ie = None
    
    fbf = None
    
    gdal_ver = VersionChecker.getGDALVersion()   
    #pgis_ver = VersionChecker.getPostGISVersion()   
    #pg_ver = VersionChecker.getPostgreSQLVersion()
    
    
    if VersionChecker.compareVersions(VersionChecker.GDAL_MIN,gdal_ver.get('GDAL') if gdal_ver.get('GDAL') is not None else VersionChecker.GDAL_MIN):
        raise UnsupportedVersionException('PostgreSQL version '+str(gdal_ver.get('GDAL'))+' does not meet required minumum '+str(VersionChecker.GDAL_MIN))
     
#do the datasource checks in object and once initialised
#        message += 'GDAL '+pgis_ver.get('GDAL')+'<'+VersionChecker.GDAL_MIN+'(reqd) \n'
#    if VersionChecker.compareVersions(VersionChecker.GDAL_MIN,pgis_ver.get('GDAL') if pgis_ver.get('GDAL') is not None else VersionChecker.GDAL_MIN): 
#        message += 'GDAL(pgis) '+pgis_ver.get('GDAL')+'<'+VersionChecker.GDAL_MIN+'(reqd) \n'
#    if VersionChecker.compareVersions(VersionChecker.PostgreSQL_MIN,pg_ver.get('PostgreSQL') if pgis_ver.get('PostgreSQL') is not None else VersionChecker.PostgreSQL_MIN): 
#        message += 'PostgreSQL '+pg_ver.get('PostgreSQL')+'<'+VersionChecker.PostgreSQL_MIN+' (reqd)\n'

    
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hv12ixf:t:l:g:e:s:d:c:u:", ["help","version","drivercopy","featurecopy","internal","external","fromdate=","todate=","layer=","group=","epsg=","source=","destination=","cql=","userconf="])
        ldslog.info("OPTS:"+str(opts))
        ldslog.info("ARGS:"+str(args))
    except getopt.error, msg:
        print msg
        usage()
        sys.exit(2)
        
    # process options
    for opt, val in opts:
        if opt in ("-h", "--help"):
            print __doc__
            sys.exit(0)
        elif opt in ("-v", "--version"):
            print __version__
            sys.exit(0)
        elif opt in ("-1","--drivercopy"):
            ldslog.info("Forcing DriverCopy")
            fbf = False
        elif opt in ("-2","--featurecopy"):
            ldslog.info("Forcing FeatureCopy")
            fbf = True
        elif opt in ("-f","--fromdate"):
            fd = val 
        elif opt in ("-t","--todate"):
            td = val
        elif opt in ("-l","--layer"):
            ly = val
        elif opt in ("-g","--group"):
            gp = val
        elif opt in ("-e","--epsg"):
            ep = val
        elif opt in ("-s","--source"):
            sc = val
        elif opt in ("-d","--destination"):
            dc = val
        elif opt in ("-c","--cql"):
            cq = val
        elif opt in ("-i","--internal"):
            ie = True
        elif opt in ("-x","--external"):
            ie = False
        elif opt in ("-u","--userconf"):
            uc = val
        else:
            print "unrecognised option:\n" \
            "-f (--fromdate) Date in yyyy-mm-dd format start of incremental range (omission assumes auto incremental bounds)," \
            "-t (--todate) Date in yyyy-mm-dd format for end of incremental range (omission assumes auto incremental bounds)," \
            "-l (--layer) Layer name/id in format v:x### (IMPORTANT. Omission assumes all layers)," \
            "-g (--group) Layer sub group list for layer selection, comma separated" \
            "-e (--epsg) Destination EPSG. Layers will be converted to this SRS" \
            "-s (--source) Connection string for source DS," \
            "-d (--destination) Connection string for destination DS," \
            "-c (--cql) Filter definition in CQL format," \
            "-u (--user) User defined config file used as partial override for ldsincr.conf," \
            "-1 (--drivercopy) Testing option to force driver level copy (sometimes faster method used for layer duplication ignoring data modifications)" \
            "-2 (--featurecopy) Testing option to force feature level copy (used for incremental updates)" \
            "-i (--internal) Override internal/external setting in main config, use internal" \
            "-x (--external) Override internal/external setting in main config, use external" \
            "-h (--help) Display this message"
            sys.exit(2)

#    #TODO consider ly argument to specify a file name containing a list of layers? 
#    if ly is None:
#        raise InputMisconfigurationException("Layer name required (-l)")
#        sys.exit(1)
    st = datetime.now()
    m1 = '*** Begin    *** '+str(st.isoformat())
    print m1
    ldslog.info(m1)
    tp = TransferProcessor(ly,gp,ep,fd,td,sc,dc,cq,uc,ie,fbf)
    
    proc = None
    #output format
    if len(args)==0:
        print __doc__
        sys.exit(0)
    else: 
        '''since we're not breaking the switch the last arg read will be the DST used, ie proc gets overwritten'''
        for arg in args:
            if arg.lower() in ("init", "initialise", "initalize"):
                ldslog.info("Initialisation of configuration files/tables requested. Implies FULL rebuild")
                tp.setInitConfig()
            elif arg in ("clean"):
                ldslog.info("Cleaning named layer")
                tp.setCleanConfig()
            elif arg.lower() in ("pg", "postgres","postgresql"):
                proc = tp.processLDS2PG
            elif arg.lower() in ("ms", "mssql","mssqlserver"):
                proc = tp.processLDS2MSSQL
    #        elif arg in ("mi", "mapinfo"):
    #            tp.processLDS2Mapinfo()
    #        elif arg in ("shp", "shapefile"):
    #            tp.processLDS2Shape() 
    #        elif arg in ("csv", "csvfile"):
    #            tp.processLDS2CSV()
            elif arg.lower() in ("sl","slite", "spatialite"):
                proc = tp.processLDS2SpatiaLite
            elif arg.lower() in ("fg","fgdb", "filegdb"):
                proc = tp.processLDS2FileGDB
    #        elif arg in ("arc", "sde", "arcsde"):
    #            tp.processLDS2ArcSDE()
            else:
                print __doc__
                raise InputMisconfigurationException("Unrecognised command; output type (pg,ms,slite,fgdb) declaration required")
            

    #aggregation point for LDS errors
    try:
        proc()
    except HTTPError as he:
        ldslog.error('Error connecting to LDS. '+str(he))
        print str(he)
        sys.exit(1)
    except DSReaderException as dse:
        ldslog.error('Error creating DataSource. '+str(dse))
        print str(dse)
        sys.exit(1)
    except Exception as e:
        #if errors are getting through we catch/report them
        ldslog.warn("Error! "+str(e))
        sys.exit(1)
        
    
    et = datetime.now()
    
    m2 = '*** Complete *** '+str(et.isoformat())
    print m2
    ldslog.info(m2)
    
    dur = et-st
    m3 = '*** Duration *** '+str(dur)
    print m3
    ldslog.info(m3)
    
    return 1000*dur.total_seconds()


if __name__ == "__main__":
    #main()
    
    try:
        main()
    except Exception as e:        
        exc_type, exc_value, exc_traceback = sys.exc_info()
        ldslog.error('LDSReplicate Error.',exc_info=(exc_type,exc_value,exc_traceback))
        print str(e)+'\n(see debug.log for full stack trace)'
