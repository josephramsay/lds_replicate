'''
Created on 9/08/2012

@author: jramsay
'''
import gdal
import logging

from DataStore import DataStore
from MetaLayerInformation import MetaLayerReader

ldslog = logging.getLogger('LDS')

class PostgreSQLDataStore(DataStore):
    '''
    PostgreSQL DataStore
    '''

    def __init__(self,conn_str=None,user_config=None):
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
        
        
        self.mlr = MetaLayerReader(user_config,"postgresql.layer.properties")
        
        self.params = self.mlr.readDSSpecificParameters(self.DRIVER_NAME)
        #use params to read layer config here or use driver on dst ds?
        #self.readLayerConfig()
        #self.mlr.setLayerConfig(params)

        (self.host,self.port,self.dbname,self.schema,self.usr,self.pwd, self.overwrite) = self.params

        
    def sourceURI(self,layer):
        '''URI method returns source file name'''
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        '''URI method returns destination file name'''
        return self._commonURI(layer)
        
    def _commonURI(self,layer):
        '''Refers to common connection instance for reading or writing'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        #can't put schema in quotes, causes error but without quotes tables get created in public anyway, still need schema.table syntax
        sstr = " active_schema={}".format(self.schema) if self.schema is not None and self.schema !='' else ""
        uri = "PG:dbname='{}' host='{}' port='{}' user='{}' password='{}'".format(self.dbname, self.host, self.port, self.usr, self.pwd)+sstr
        ldslog.debug(uri)
        return uri

#    def read(self,dsn):
#        print "PG read"
#        self.ds = self.driver.Open(dsn)
#    
#    def write(self,src,dsn):
#        print "PG write",dsn
#        super.write(self,src,dsn)

    def getOptions(self,layer_id):
        '''add PG options for SCHEMA and GEO_NAME'''
        #Should default to geometry but doesn't, creates bytea instead
        local_opts = ['GEOM_TYPE=GEOMETRY']
        gname = self.mlr.readGeometryColumnName(layer_id)
        
        if gname is not None:
            local_opts += ['GEOMETRY_NAME='+gname]
        
        return super(PostgreSQLDataStore,self).getOptions() + local_opts
    
    def buildIndex(self,ref_index,ref_pkey,ref_gcol,dst_layer_name):
        '''Builds an index creation string for a new full replicate in PG format'''
        ref_index = ref_index.lower()
        if ref_index == 'spatial' or ref_index == 's':
            cmd = 'CREATE INDEX {}_SK ON {} USING GIST({})'.format(dst_layer_name.split('.')[-1]+"_"+ref_gcol,dst_layer_name,ref_gcol)
        elif ref_index == 'pkey' or ref_index == 'p':
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+ref_pkey,dst_layer_name,ref_pkey)
        elif ref_index is not None:
            #maybe the user wants a non pk/spatial index? Try to filter the string. This wont work for spatial columns since GIST needed
            #TODO. Detect when gcol is in the col list and build a "mixed-spatial"? index...
            clst = ','.join(self.parseStringList(ref_index))
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+self.sanitise(clst),dst_layer_name,clst)
        else:
            return
        ldslog.info("Index="+ref_index+". Execute "+cmd)
        self._executeSQL(cmd)
        
        
        
    #Config stuff
    
    def readLayerConfig(self):
        sqlstr = 'select * from lds_config;'
        res = self._executeSQL(sqlstr)
        if res is None:
            res = self.initLayerConfig()
        return res
    
    def writeLayerConfig(self):
        pass
    
    def initLayerConfig(self):
        sqlstr = 'create table ldsincr (id int);'
        try:
            r = self._executeSQL(sqlstr)
        except:
            print 'wtf'
        return r
    
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

        