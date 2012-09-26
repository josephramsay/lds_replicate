'''
Created on 9/08/2012

@author: jramsay
'''
import gdal

from DataStore import DataStore
from ReadConfig import Reader
from MetaLayerInformation import MetaLayerReader


class PostgreSQLDataStore(DataStore):
    '''
    PostgreSQL DataStore
    '''

    def __init__(self,conn_str=None):
        '''
        cons init driver
        '''
        
        
        super(PostgreSQLDataStore,self).__init__(conn_str)
        
        self.DRIVER_NAME = "PostgreSQL"
        #doesnt work with createlayer... but not needed if we overwrite FID with PK
        #self.PGSQL_OGR_FID = "ID"    
        #gdal.SetConfigOption("PGSQL_OGR_FID",self.PGSQL_OGR_FID)
        
        #do not use PG_USE_COPY if you want FID preserved
        self.PG_USE_COPY = "NO"
        gdal.SetConfigOption("PG_USE_COPY",self.PG_USE_COPY)
        
        self.PG_USE_BASE64 = "YES"
        gdal.SetConfigOption("PG_USE_BASE64",self.PG_USE_BASE64)
        
        self.getDriver(self.DRIVER_NAME)
        
        self.mlr = MetaLayerReader("postgresql.layer.properties")

        (self.host,self.port,self.dbname,self.schema,self.usr,self.pwd, self.overwrite) = self.mlr.readDSSpecificParameters(self.DRIVER_NAME)

        
    def sourceURI(self,layer):
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        return self._commonURI(layer)
        
    def _commonURI(self,layer):
        '''refers to common connection instance for example in a DB where it doesn't matter whether your reading or writing'''
        '''layer not used since table isnt a valid initialisation parameter'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        #can't put schema in quotes, causes error but without quotes tables get created in public anyway, still need schema.table syntax
        sstr = " active_schema={}".format(self.schema) if self.schema is not None and self.schema !='' else ""
        return "PG:dbname='{}' host='{}' port='{}' user='{}' password='{}'".format(self.dbname, self.host, self.port, self.usr, self.pwd)+sstr

#    def read(self,dsn):
#        print "PG read"
#        self.ds = self.driver.Open(dsn)
#    
#    def write(self,src,dsn):
#        print "PG write",dsn
#        super.write(self,src,dsn)

    def getOptions(self,layer_id):
        '''add PG options for SCHEMA and GEO_NAME'''
        local_opts = []
        gname = self.mlr.readGeometryColumnName(layer_id)
        
        if gname is not None:
            local_opts += ['GEOMETRY_NAME='+gname]
        
        return super(PostgreSQLDataStore,self).getOptions() + local_opts
        
    def _cleanLayer(self,layer):
        print "PG clean"
        self.ds.DeleteLayer(layer)
        
    def _clean(self):
        for li in range(0,self.ds.GetLayerCount()):
            self.cleanLayer(li)
    
    
#    def buildExternalLayerDefinition(self,layer_id,flist):
#        '''build a predefined schema for the layer'''
#        sr = Reader('../lpk.properties')
#        (pkey,name,gcol,lmod,excl) = self.mlr.readAllLayerParameters(layer_id)
#        #sr.readLayerSchemaConfig(lname)
#        sname = self.sanitise(name) if name is not None else self.sanitise(layer_id)
#        pk = self.parseStringList(pkey)
#        #atlist = self.parseStringList(excl)
#        s = 'CREATE TABLE '+sname+"("
#        #s += pk+' INTEGER UNIQUE NOT NULL,'
#        #s += gcol+' GEOMETRY,'
#        for f in flist:
#            cname = f.GetName()
#            if cname not in self.optcols:
#                s += '"'+cname+'" '+self.convertToDestinationType(f.GetType())+','
#        s += 'CONSTRAINT pk_'+sname+' PRIMARY KEY ('+pkey+'),'
#        s += 'CONSTRAINT enforce_dims_shape CHECK (st_ndims(shape) = 2),'
#        s += 'CONSTRAINT enforce_srid_shape CHECK (st_srid(shape) = 2193));'
#        
#        return s
#        
#
#
#    '''
#    returned by ogr.GetFieldTypeName(i)
#    0 Integer
#    1 IntegerList
#    2 Real
#    3 RealList
#    4 String
#    5 StringList
#    6 (unknown)
#    7 (unknown)
#    8 Binary
#    9 Date
#    10 Time
#    11 DateTime
#    12 (unknown) ...
#    '''
#    def convertToDestinationType(self,key):
#        return {0: 'integer', 1: 'integer',
#                2: 'double precision', 3: 'double precision',
#                4: 'character varying', 5: 'character varying',
#                8: 'byte',
#                9: 'date', 10: 'time', 11: 'timestamp'
#         }.get(key,'character varying')  
#               

        