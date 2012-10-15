'''
Created on 9/08/2012

@author: jramsay
'''
import gdal
import logging

from DataStore import DataStore

ldslog = logging.getLogger('LDS')

class PostgreSQLDataStore(DataStore):
    '''
    PostgreSQL DataStore
    '''

    def __init__(self,conn_str=None,user_config=None):
        '''
        cons init driver
        '''
        
        self.DRIVER_NAME = "PostgreSQL"
        self.CONFIG_XSL = "getcapabilities.postgresql.xsl"
        
        super(PostgreSQLDataStore,self).__init__(conn_str,user_config)
        
        
        #doesnt work with createlayer... but not needed if we want to overwrite FID with PK
        #self.PGSQL_OGR_FID = "ID"    
        #gdal.SetConfigOption("PGSQL_OGR_FID",self.PGSQL_OGR_FID)
        
        #do not use PG_USE_COPY if you want FID preserved
        self.PG_USE_COPY = "NO"
        gdal.SetConfigOption("PG_USE_COPY",self.PG_USE_COPY)
        
        self.PG_USE_BASE64 = "YES"
        gdal.SetConfigOption("PG_USE_BASE64",self.PG_USE_BASE64)

        (self.host,self.port,self.dbname,self.schema,self.usr,self.pwd, self.overwrite,self.config,self.srs,self.cql) = self.params

        
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
        self.executeSQL(cmd)
        
        
    # testing: layers as config storage
    def buildConfigLayer(self,config_array):
        config_layer = self.ds.CreateLayer()
        for row in config_array:
            config_feat = config_layer.addFeature()
            config_feat.setField()
        
        pass
    def readLayerConfig(self,layer):
        pass
        
    
        