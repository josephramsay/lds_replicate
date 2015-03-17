'''
v.0.0.9

LDSReplicate -  VersionUtilities

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 24/10/2012

@author: jramsay
'''

'''
Version information

PostgreSQL versions since 8
8.0     2005-01-19     8.0.26     2010-10-04
8.1     2005-11-08     8.1.23     2010-12-16
8.2     2006-12-05     8.2.23     2011-09-26
8.3     2008-02-04     8.3.23     2013-02-07
8.4     2009-07-01     8.4.16     2013-02-07
9.0     2010-09-20     9.0.12     2013-02-07
9.1     2011-09-12     9.1.8      2013-02-07

MSSQL versions since 8
8.0      2000     SQL Server 2000                Shiloh
8.0      2003     SQL Server 2000 64-bit Edition Liberty
9.0      2005     SQL Server 2005                Yukon
10.0     2008     SQL Server 2008                Katmai
10.25    2010     SQL Azure DB                   CloudDatabase
10.5     2010     SQL Server 2008 R2             Kilimanjaro (aka KJ)
11.0     2012     SQL Server 2012                Denali

SpatiaLite
Spatialite 2.4.0RC3 uses SQLite 3.7.0 which has Write-Ahead-Logging (WAL)
    
'''

#__version__ = '0.0.9.0'


import osgeo.gdal
import subprocess
import re

from distutils.version import StrictVersion, LooseVersion
#from verlib import NormalizedVersion
from lds.ReadConfig import MainFileReader
from LDSUtilities import LDSUtilities

ldslog = LDSUtilities.setupLogging()

class UnsupportedVersionException(Exception): pass

class AppVersion(object):
    __version__ = '0.0.9.0'
    
    @staticmethod
    def getVersion():
        return AppVersion.__version__
    
    
class VersionChecker(object):


    GDAL_MIN = '1.9.1'
    PostgreSQL_MIN = '8.4'
    PostGIS_MIN = '2.0'
    MSSQL_MIN = '10.0.0.0'
    SpatiaLite_MIN = '3.0'
    FileGDB_MIN = '$1,000,000.00'
    
    def __init__(self,params):
        pass
        
    @staticmethod
    def getGDALVersion():
        return {'GDAL':re.search('[\d+.]+',osgeo.gdal.__version__).group(0)}
    
    @staticmethod
    def getPostGISVersion():

        mfr = MainFileReader().readPostgreSQLConfig()
        cmd = "psql -c 'select postgis_full_version()' "+mfr[2]
        with subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as sp:
            #sp = subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in sp.stdout.readlines():
                m1 = re.search('POSTGIS=\"(\d+\.\d+\.\d+)',line)
                postgis = m1.group(1) if m1 else None
                
                m2 = re.search('GEOS=\"(\d+\.\d+\.\d+)',line)
                geos = m2.group(1) if m2 else None
                
                m3 = re.search('PROJ=\"Rel.\s+(\d+\.\d+\.\d+)',line)
                proj = m3.group(1) if m3 else None
                
                m4 = re.search('GDAL=\"GDAL\s+(\d+\.\d+)',line)
                gdal = m4.group(1) if m4 else None
                
                m5 = re.search('LIBXML=\"(\d+\.\d+\.\d+)',line)
                lxml = m5.group(1) if m5 else None
            
        return {'PostGIS':postgis,'GEOS':geos,'PROJ':proj,'GDAL':gdal,'LIBXML':lxml}
    
    @staticmethod
    def getPostgreSQLVersion():

        mfr = MainFileReader().readPostgreSQLConfig()
        cmd = "psql -c 'select version()' "+mfr[2]
        postgresql = VersionChecker.getVersionFromShell(cmd,'PostgreSQL\s+(\d+\.*\d*\.*\d*)')

        return {'PostgreSQL':postgresql}
    
    @staticmethod
    def getVersionFromShell(command,searchstring):
        ret = None
        sp = subprocess.Popen(command,shell=True, stdout=subprocess.PIPE)
        for line in sp.stdout.readlines():
            match = re.search(searchstring,line)
            if match: 
                ret = match.group(1)
        try:
            sp.kill()
        except Exception as e:
            ldslog.warn('Exception on uubprocess kill, '+str(e))
            pass
        return ret
        
        #No context management for subprocess v 2.7!!!
#        with subprocess.Popen(command,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as sp:
#            for line in sp.stdout.readlines():
#                match = re.search(searchstring,line)
#                if match: return match.group(1)
#        return None
        
    
    
    @staticmethod
    def compareVersions_strict(v1,v2):
        #Spatialite has 3.x version numbers which break strictversion. This should pick up other odd #ver too eg 12.abc2.2b ->12.02.2b
        #There is however no way so logically discard a 4th part of a version numbers e.g. MSSQL 10.0.0.1
        v1 = re.sub('.[a-z]+','.0',v1)
        v2 = re.sub('.[a-z]+','.0',v2)
        return StrictVersion(v1)>StrictVersion(v2)
    
    @staticmethod
    def compareVersions(v1,v2):
        return LooseVersion(v1)>LooseVersion(v2)
    
def main():
    print VersionChecker.getPostGISVersion()
    print VersionChecker.getGDALVersion()
    print VersionChecker.getPostgreSQLVersion()

if __name__ == "__main__":
    main()