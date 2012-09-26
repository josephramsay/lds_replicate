'''
v.0.0.1

LDSIncremental -  LDS Incremental Utilities

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 23/07/2012

@author: jramsay

    Python script to translate fetch an LDS update between two dates. Intended to be used in a batch process rather than interactively. 
    
    Usage: 
        python main.py pg   | postgres
        python main.py ms   | mssql
        python main.py slite| spatialite
        python main.py fgdb | filegdb
        
        *** coming soon ***
        python main.py arc  | sde | arcsde
        python main.py mi   | mapinfo
        python main.py shp  | shapefile
        python main.py csv  | csvfile
        
    Options:
        -l --layer           v:xNNN | ALL
        -f --fromdate        yyyy-mm-dd | ALL
        -t --todate          yyyy-mm-dd | ALL
        -s --source          Connection string for data source (eg. WFS:http://path/to/WFS/service or http://path/to/WFS/service?SERVICE=WFS)   
        -d --destination     Connection string for data destination (eg. PG:"dbname='databasename' host='addr' port='5432' user='x' password='y'")
        -c --cql             CQL flter string (eg. "land_district = 'Otago'")
        -h --help            This help screen
        
    Note 
    1. Connection options can be set up in the config file "ldsincr.conf" but command line options can be used instead and will override 
    configuration file settings. There is no checking done on command line connection strings
    2. A valid layer name or the keyword ALL is required on the command line prefixed by the -l option flag
    3. Dates are not required and omitting them will trigger an auto incremental request based on the lastmodified parameter in the respective 
    destination layer properties file
    4. Dates when provided must be in the format yyyy-mm-dd or specify the keyword ALL
    5. CQL strings are only minimally checked for tokens indicating a predicate. The user is responsible for ensuring they are constructing their
    filter using the correct parameter names, formats and bounds.
    6. CQL filters will be override one another according to; Command Line > configfile (ldsincr.conf) > layer config (___.layer.properties) 

'''

import sys, getopt

from TransferProcessor import TransferProcessor
from TransferProcessor import InputMisconfigurationException


def usage():
    print "for help use --help"
    
def main():
    '''Main entrypoint if the LDS incremental replication script
    usage: python LDSReader/main.py -l <layer_id> { -f <fromdate> | -t <todate> | -c <cql_filter> } <output>
    '''
    td = None
    fd = None
    ly = None
    sc = None
    dc = None
    cq = None
    
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf:t:l:s:d:c:", ["help","fromdate=","todate=","layer=","source=","destination=","cql="])
    except getopt.error, msg:
        print msg
        usage()
        sys.exit(2)
        
    # process options
    for opt, val in opts:
        if opt in ("-h", "--help"):
            print __doc__
            sys.exit(0)
        elif opt in ("-f","--fromdate"):
            fd = val 
        elif opt in ("-t","--todate"):
            td = val
        elif opt in ("-l","--layer"):
            ly = val
        elif opt in ("-s","--source"):
            sc = val
        elif opt in ("-d","--destination"):
            dc = val
        elif opt in ("-c","--cql"):
            cq = val
        else:
            print "unrecognised option:\n" \
            "-f (--fromdate) Date in yyyy-mm-dd format start of incremental range (omission assumes auto incremental bounds)," \
            "-t (--todate) Date in yyyy-mm-dd format for end of incremental range (omission assumes auto incremental bounds)," \
            "-l (--layer) Mandatory. Layer name/id in format v:x### (omission assumes all layers)," \
            "-s (--source) Connection string for source DS," \
            "-d (--destination) Connection string for destination DS," \
            "-c (--cql) Filter definition in CQL format," \
            "-h (--help) Display this message" \
            "Note. Setting both -f and -t to 'ALL' will load the entire layer ignoring incremental bounds. Omitting the -l option loads all layers. Using these together will replicate the entire LDS"
            sys.exit(2)

    #TODO consider ly argument to specify a file name containing a list of layers? 
    if ly is None:
        raise InputMisconfigurationException("Layer name required (-l)")
        sys.exit(1)
        
    tp = TransferProcessor(ly,fd,td,sc,dc,cq)
    
    #output format
    for arg in args:
        if arg in ("pg", "postgres"):
            tp.processLDS2PG()
        elif arg in ("ms", "mssql"):
            tp.processLDS2MSSQL()
#        elif arg in ("mi", "mapinfo"):
#            tp.processLDS2Mapinfo()
#        elif arg in ("shp", "shapefile"):
#            tp.processLDS2Shape() 
#        elif arg in ("csv", "csvfile"):
#            tp.processLDS2CSV()
        elif arg in ("slite", "spatialite"):
            tp.processLDS2SpatiaLite()
        elif arg in ("fgdb", "filegdb"):
            tp.processLDS2FileGDB()
#        elif arg in ("arc", "sde", "arcsde"):
#            tp.processLDS2ArcSDE()
        else:
            raise InputMisconfigurationException("Output type required (pg,ms,slite,fgdb")




if __name__ == "__main__":
    main()
