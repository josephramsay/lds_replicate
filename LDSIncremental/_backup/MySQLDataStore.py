#'''
#Created on 9/08/2012
#
#@author: jramsay
#'''
#
#from DataStore import DataStore
#from ReadConfig import Reader
#
#class MySQLDataStore(DataStore):
#    '''
#    MySQL DataStore
#    MYSQL:dbname[,options]
#    '''
#
#    def __init__(self,conn_str=None):
#        '''
#        cons init driver
#        '''
#        
#        if conn_str is not None:
#            self.conn_str = conn_str
#            
#        super(MySQLDataStore,self).__init__()
#        
#        
#        self.getDriver("MySQL")
#        rc = Reader(None)
#        (self.host,self.port,self.dbname,self.usr,self.pwd) = rc.readMySQLConfig()
#
#        
#    def sourceURI(self,layer):
#        return self._commonURI(layer)
#    
#    def destinationURI(self,layer):
#        return self._commonURI(layer)
#        
#    def _commonURI(self,layer):
#        '''refers to common connection instance for example in a DB where it doesn't matter whether your reading or writing'''
#        if hasattr(self,'conn_str') and self.conn_str is not None:
#            return self.conn_str
#        return "MYSQL:dbname=%s,host=%s,port=%s,user=%s,pass=%s" % (self.host, self.port, self.dbname, self.usr, self.pwd)
#        #return "PG:dbname=%s,host=%s,port=%s,user=%s,pass=%s,tablename=%s" % (self.dbname, self.host, self.port, self.usr, self.pwd, layer)
#        
#
#    def read(self,dsn):
#        self.ds = self.driver.Open(dsn)
#    
#    def write(self,src_ds,dsn):
#        self.ds = self.driver.CopyDataSource(src_ds, dsn)