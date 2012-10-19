'''
Created on 9/08/2012

@author: jramsay
'''
import logging

from DataStore import DataStore
from ProjectionReference import Geometry

ldslog = logging.getLogger('LDS')

class MSSQLSpatialDataStore(DataStore):
    '''
    MS SQL DataStore
    MSSQL:server=.\MSSQLSERVER2008;database=dbname;trusted_connection=yes
    '''

    def __init__(self,conn_str=None,user_config=None):
        '''
        cons init driver
        '''
        self.DRIVER_NAME = "MSSQLSpatial"
        self.CONFIG_XSL = "getcapabilities.mssqlspatial.xsl"  
         
        super(MSSQLSpatialDataStore,self).__init__(conn_str,user_config)
        
        (self.odbc,self.server,self.dsn,self.trust,self.dbname,self.schema,self.usr,self.pwd,self.config,self.srs,self.cql) = self.params

        
    def sourceURI(self,layer):
        '''URI method returns source file name'''
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        '''URI method returns destination file name'''
        return self._commonURI(layer)
        
    def _commonURI(self,layer):
        '''Refers to common connection instance for example in a DB where it doesn't matter whether your reading or writing'''
        if hasattr(self,'conn_str') and self.conn_str is not None:
            return self.conn_str
        #return "MSSQL:server={};database={};trusted_connection={};".format(self.server, self.dbname, self.trust)
        sstr = ";Schema={}".format(self.schema) if self.schema is not None and self.schema !='' else ""
        uri = "MSSQL:server={};database={};UID={};PWD={};Driver={}".format(self.server, self.dbname, self.usr, self.pwd,self.odbc)+sstr
        ldslog.debug(uri)
        return uri
        

    def buildIndex(self,ref_index,ref_pkey,ref_gcol,dst_layer_name):
        '''Builds an index creation string for a new full replicate'''
        ref_index = ref_index.lower()
        if ref_index == 'spatial' or ref_index == 's':
            bb = Geometry.getBoundingBox()
            cmd1 = 'CREATE SPATIAL INDEX {}_SK ON {}({}) '.format(dst_layer_name.split('.')[-1]+"_"+ref_gcol,dst_layer_name,ref_gcol)
            cmd2 = 'USING GEOMETRY_GRID WITH ( BOUNDING_BOX = (XMIN = {}, YMIN = {}, XMAX = {}, YMAX = {}),' \
                    'GRIDS = (LEVEL_1 = MEDIUM, LEVEL_2 = MEDIUM, LEVEL_3 = MEDIUM, LEVEL_4 = MEDIUM),' \
                    'CELLS_PER_OBJECT = 256)'.format(str(bb[0]),str(bb[1]),str(bb[2]),str(bb[3]))
            cmd = cmd1+cmd2
            
            #magic command...
            cmd = 'create spatial index on '+dst_layer_name
        elif ref_index == 'pkey' or ref_index == 'p':
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+ref_pkey,dst_layer_name,ref_pkey)
        elif ref_index is not None:
            #maybe the user wants a non pk/spatial index? Try to filter the string
            clst = ','.join(DataStore.parseStringList(ref_index))
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+DataStore.sanitise(clst),dst_layer_name,clst)
        else:
            return
        self.executeSQL(cmd)
                

    def getOptions(self,layer_id):
        '''Get MS options for GEO_NAME'''
        local_opts = []
        gname = self.layerconf.readLayerProperty(layer_id,'geocolumn')
        
        if gname is not None:
            local_opts += ['GEOM_NAME='+gname]
        
        return super(MSSQLSpatialDataStore,self).getOptions() + local_opts
    