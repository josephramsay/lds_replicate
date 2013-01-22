#'''
#Created on 9/08/2012
#
#@author: jramsay
#'''
#
#from ESRIDataStore import ESRIDataStore
#from ReadConfig import Reader
#
#class ArcSDEDataStore(ESRIDataStore):
#    '''
#    ArcSDE DataStore
#    '''
#
#    def __init__(self,conn_str=None):
#        '''
#        cons init driver
#        '''
#        super(ArcSDEDataStore,self).__init__(conn_str)
#        
#        
#        self.getDriver("SDE")
#        rc = Reader(None)
#        (self.server,self.instance,self.database,self.username,self.password) = rc.readArcSDEConfig()
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
#        return "SDE:%s,%s,%s,%s,%s,%s" % (self.server, self.instance, self.database, self.username, self.password, layer)
#        
#
##    def read(self,dsn):
##        self.ds = self.driver.Open(dsn)
##    
##    def write(self,src_ds,dsn):
##        self.convertDatasourceESRI(src_ds)
##        self.ds = self.driver.CopyDataSource(src_ds, dsn)