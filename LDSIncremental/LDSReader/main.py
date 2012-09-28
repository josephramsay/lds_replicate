'''
v.0.0.2

LDSIncremental -  LDS Incremental Utilities

| Copyright 2011 Crown copyright (c)
| Land Information New Zealand and the New Zealand Government.
| All rights reserved

This program is released under the terms of the new BSD license. See the LICENSE file for more information.

Created on 23/07/2012

@author: jramsay

Python script to translate fetch an LDS update between two dates. Intended to be used in a batch process rather than interactively. 

| Usage: 
| python main.py pg   | postgres
| python main.py ms   | mssql
| python main.py slite| spatialite
| python main.py fgdb | filegdb

| python main.py arc  | sde | arcsde
| python main.py mi   | mapinfo
| python main.py shp  | shapefile
| python main.py csv  | csvfile
|
| Options:
| -l --layer           v:xNNN | ALL
| -f --fromdate        yyyy-mm-dd | ALL
| -t --todate          yyyy-mm-dd | ALL
| -s --source          Connection string for data source (eg. WFS:http://path/to/WFS/service or http://path/to/WFS/service?SERVICE=WFS)   
| -d --destination     Connection string for data destination (eg. PG:"dbname='databasename' host='addr' port='5432' user='x' password='y'")
| -c --cql             CQL flter string (eg. "land_district = 'Otago'")
| -h --help            This help screen

| Notes 
| 1. Connection options can be set up in the config file "ldsincr.conf" but command line options can be used instead and will override configuration file settings. There is no checking done on command line connection strings
| 2. A valid layer name or the keyword ALL is required on the command line prefixed by the -l option flag
| 3. Dates are not required and omitting them will trigger an auto incremental request based on the lastmodified parameter in the respective destination layer properties file
| 4. Dates when provided must be in the format yyyy-mm-dd or specify the keyword ALL to trigger full replication
| 5. Full replication uses a fast driver copy mechanism and ignores discards and filters. Adding discard columns to a fully replicated layer would be considered a schema change and potentially fail.
| 6. CQL strings are only minimally checked for tokens indicating a predicate. The user is responsible for ensuring they are constructing their filter using the correct parameter names, formats and bounds.
| 7. CQL filters will override one another (they do not stack) according to; Command Line > configfile (ldsincr.conf) > layer config (___.layer.properties) 
| 8. Output names (for DB tables/FileGDB directories etc) are defined in the layer config file, not LDS. Changing this name will trigger the creation of a new table.
| 9. Adding multiple output on the command line will trigger the copy process for these outputs 
| 10. The properties files are interchangeable and selected based on their name ie postgresql.layer.properties is the layer properties file for the PostgreSQL output. Copying this file to mssql.layer.properties would assign it as the same for a MSSQLSpatial output
| 11. In the properties files the sections are denoted by the layer id in square brackets eg [v:x111]
| 12. Beneath the section header property values include {pkey, name, lastmodified, geocolumn, epsg, cql}
| 12.1. Property 'pkey'. Defines the LDS primary key for this layer. Nominally set to 'id' globally this may very per layer.
| 12.2. Property 'name'. Used as the name for the output table/file these are not guaranteed to be unique. Nominally set to LDS layer name.
| 12.3. Property 'lastmodified'. This is a date string (yyyy-mm-dd) indicating the age of the data copied to output. It is used as a start point when doing auto-incremental updates.
| 12.4. Property 'geocolumn'. Used as the name for the output geometry column
| 12.5. Property 'epsg'. Specifies the required EPSG number to affect a projection change. If left blank the source projection will be retained
| 12.6. Property 'cql'. Sets a cql filter for the layer. Notes 1: the user is responsible for constructing well-formed CQL filters. 2: Layer filters will be overriden globally in the config file or on the command line.
'''

import sys
import getopt
import logging

from TransferProcessor import TransferProcessor
from TransferProcessor import InputMisconfigurationException

ldslog = logging.getLogger('LDS')
ldslog.setLevel(logging.DEBUG)

fh = logging.FileHandler('debug.log','w')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
fh.setFormatter(formatter)
ldslog.addHandler(fh)

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
