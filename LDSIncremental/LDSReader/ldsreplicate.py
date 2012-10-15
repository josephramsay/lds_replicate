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
import subprocess
import re

from TransferProcessor import TransferProcessor
from TransferProcessor import InputMisconfigurationException

ldslog = logging.getLogger('LDS')
ldslog.setLevel(logging.DEBUG)


df = os.path.normpath(os.path.join(os.path.dirname(__file__), "../debug.log"))
#df = '../debug.log'
fh = logging.FileHandler(df,'w')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
fh.setFormatter(formatter)
ldslog.addHandler(fh)

__version__ = '0.0.3'


def usage():
    print "for help use --help"

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
    
    
    #first check required libs
    versionCheck('GDAL','gdal-config','1.9.1') 
    versionCheck('PostgreSQL','psql','9.0.0')         
    
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvf:t:l:g:e:s:d:c:", ["help","version","fromdate=","todate=","layer=","group=","epsg=","source=","destination=","cql="])
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
            "-h (--help) Display this message"
            sys.exit(2)

#    #TODO consider ly argument to specify a file name containing a list of layers? 
#    if ly is None:
#        raise InputMisconfigurationException("Layer name required (-l)")
#        sys.exit(1)
        
    tp = TransferProcessor(ly,gp,ep,fd,td,sc,dc,cq,uc)
    proc = None
    #output format
    if len(args)==0:
        print __doc__
        sys.exit(0)
    else: 
        for arg in args:
            if arg in ("init", "initialise", "initalize"):
                ldslog.info("Initialisation of configuration files/tables requested. Implies FULL rebuild")
                tp.clearIncremental()
                tp.setInitConfig()
            elif arg in ("full", "full_replicate"):
                ldslog.info("FULL Replication of layer requested")
                tp.clearIncremental()
            elif arg in ("pg", "postgres"):
                proc = tp.processLDS2PG
                break
            elif arg in ("ms", "mssql"):
                proc = tp.processLDS2MSSQL
                break
    #        elif arg in ("mi", "mapinfo"):
    #            tp.processLDS2Mapinfo()
    #        elif arg in ("shp", "shapefile"):
    #            tp.processLDS2Shape() 
    #        elif arg in ("csv", "csvfile"):
    #            tp.processLDS2CSV()
            elif arg in ("slite", "spatialite"):
                proc = tp.processLDS2SpatiaLite
                break
            elif arg in ("fgdb", "filegdb"):
                proc = tp.processLDS2FileGDB
                break
    #        elif arg in ("arc", "sde", "arcsde"):
    #            tp.processLDS2ArcSDE()
            else:
                print __doc__
                raise InputMisconfigurationException("Unrecognised command; output type (pg,ms,slite,fgdb) and optional 'full' declaration required")
            
        #now run the selected func
        proc()
        print '*** FIN ***'
            

def versionCheck(name,cmd,mnm):
    out = subprocess.Popen(cmd+' --version',shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    ver = re.search('\d.\d.\d',str(out.stdout.readlines())).group(0)
    #alpha compare
    if ver<mnm: 
        print name,'version',ver,"is earlier than",mnm
        ldslog.error(name+' version '+ver+' is earlier than '+mnm)
        sys.exit(1)
    ldslog.info(name+' version '+ver+' is later than '+mnm)


if __name__ == "__main__":
    main()
