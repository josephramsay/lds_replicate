'''
v.0.0.3

LDSIncremental -  LDS Incremental Utilities

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

from TransferProcessor import TransferProcessor
from TransferProcessor import InputMisconfigurationException
from VersionChecker import VersionChecker

ldslog = logging.getLogger('LDS')
ldslog.setLevel(logging.DEBUG)


df = os.path.normpath(os.path.join(os.path.dirname(__file__), "../debug.log"))
#df = '../debug.log'
fh = logging.FileHandler(df,'w')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
fh.setFormatter(formatter)
ldslog.addHandler(fh)

__version__ = '0.0.4'


def usage():
    print "Usage: python LDSReader/ldsreplicate.py -l <layer_id> [-f <from date>|-t <to date>|-c <cql filter>|-s <src conn str>|-d <dst conn str>|-v|-h] <output> [full]"
    print "For help use --help"

def main():
    '''Main entrypoint if the LDS incremental replication script
    
    usage: python LDSReader/ldsreplicate.py -l <layer_id>
        [-f <from date>|-t <to date>|-c <cql filter>|-s <src conn str>|-d <dst conn str>|-v|-h] 
        <output> [full]
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
    
    fbf = None
    
    
    #first check required libs
    #versionCheck('GDAL','gdal-config','1.9.1') 
    #versionCheck('PostgreSQL','psql','9.0.0')      
    
    GDAL_MIN = '1.9.1'
    PostgreSQL_MIN = '9.0'
    
    message = ''
    gdal_ver = VersionChecker.getGDALVersion()   
    pgis_ver = VersionChecker.getPostGISVersion()   
    pg_ver = VersionChecker.getPostgreSQLVersion()
       
    if VersionChecker.compareVersions(GDAL_MIN,gdal_ver.get('GDAL') if gdal_ver.get('GDAL') is not None else GDAL_MIN): 
        message += 'GDAL '+pgis_ver.get('GDAL')+'<'+GDAL_MIN+'(reqd) \n'
    if VersionChecker.compareVersions(GDAL_MIN,pgis_ver.get('GDAL') if pgis_ver.get('GDAL') is not None else GDAL_MIN): 
        message += 'GDAL(pgis) '+pgis_ver.get('GDAL')+'<'+GDAL_MIN+'(reqd) \n'
    if VersionChecker.compareVersions(PostgreSQL_MIN,pg_ver.get('PostgreSQL') if pgis_ver.get('PostgreSQL') is not None else PostgreSQL_MIN): 
        message += 'PostgreSQL '+pg_ver.get('PostgreSQL')+'<'+PostgreSQL_MIN+' (reqd)\n'
    
    if message != '':
        print 'Version checks failed:\n',message
        ldslog.warn('Version checks failed:\n'+message)

        if raw_input("Y to quit N to continue [y|N] : ").lower() == 'y':
            sys.exit(1)

    
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvbif:t:l:g:e:s:d:c:u:", ["help","version","block","item","fromdate=","todate=","layer=","group=","epsg=","source=","destination=","cql=","userconf="])
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
        elif opt in ("-b","--block"):
            ldslog.info("Forcing Block (Driver controlled) Copy")
            fbf = False
        elif opt in ("-i","--item"):
            ldslog.info("Forcing Itemised (Feature-By-Feature) Copy")
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
            fbf = True
        elif opt in ("-s","--source"):
            sc = val
        elif opt in ("-d","--destination"):
            dc = val
        elif opt in ("-c","--cql"):
            cq = val
            #fbf = True#TODO. check this is required
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
            "-b (--block) Force driver level copy (faster method used for layer duplication ignoring data modifications)" \
            "-i (--item) Force feature level copy (used for incremental updates)" \
            "-h (--help) Display this message"
            sys.exit(2)

#    #TODO consider ly argument to specify a file name containing a list of layers? 
#    if ly is None:
#        raise InputMisconfigurationException("Layer name required (-l)")
#        sys.exit(1)
        
    tp = TransferProcessor(ly,gp,ep,fd,td,sc,dc,cq,uc,fbf)
        
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
            elif arg.lower() in ("pg", "postgres"):
                proc = tp.processLDS2PG
            elif arg.lower() in ("ms", "mssql"):
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
            
        #now run the selected func
        proc()
        print '*** Complete ***'


if __name__ == "__main__":
    main()
