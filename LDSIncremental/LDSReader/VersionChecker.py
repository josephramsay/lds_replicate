'''
v.0.0.1

LDSIncremental -  VersionChecker

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 24/10/2012

@author: jramsay
'''
import osgeo.gdal
import subprocess
import re

from distutils.version import StrictVersion
#from verlib import NormalizedVersion
from ReadConfig import MainFileReader


class VersionChecker(object):
    '''
    classdocs
    '''


    def __init__(self,params):
        '''
        Constructor
        '''
        
    @staticmethod
    def getGDALVersion():
        return {'GDAL':re.search('[\d.]+',osgeo.gdal.__version__).group(0)}
    
    @staticmethod
    def getPostGISVersion():

        mfr = MainFileReader("../ldsincr.conf",True).readPostgreSQLConfig()
        cmd = "psql -c 'select postgis_full_version()' "+mfr[2]
        sp = subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in sp.stdout.readlines():
            m1 = re.search('POSTGIS=\"(\d.\d.\d)',line)
            postgis = m1.group(1) if m1 is not None else None
            
            m2 = re.search('GEOS=\"(\d.\d.\d)',line)
            geos = m2.group(1) if m2 is not None else None
            
            m3 = re.search('PROJ=\"Rel.\s+(\d.\d.\d)',line)
            proj = m3.group(1) if m3 is not None else None
            
            m4 = re.search('GDAL=\"GDAL\s+(\d.\d)',line)
            gdal = m4.group(1) if m4 is not None else None
            
            m5 = re.search('LIBXML=\"(\d.\d.\d)',line)
            libxml = m5.group(1) if m5 is not None else None
            
        return {'PostGIS':postgis,'GEOS':geos,'PROJ':proj,'GDAL':gdal,'LIBXML':libxml}
    
    @staticmethod
    def getPostgreSQLVersion():

        mfr = MainFileReader("../ldsincr.conf",True).readPostgreSQLConfig()
        cmd = "psql -c 'select version()' "+mfr[2]
        sp = subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in sp.stdout.readlines():
            m1 = re.search('PostgreSQL\s+(\d.\d.\d)',line)
            postgresql = m1.group(1) if m1 is not None else None
        return {'PostgreSQL':postgresql}
    
    @staticmethod
    def compareVersions(v1,v2):
        return StrictVersion(v1)>StrictVersion(v2)
    
def main():
    print VersionChecker.getPostGISVersion()
    print VersionChecker.getGDALVersion()
    print VersionChecker.getPostgreSQLVersion()

if __name__ == "__main__":
    main()