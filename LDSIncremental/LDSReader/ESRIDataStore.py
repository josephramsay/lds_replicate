'''
Created on 9/08/2012

@author: jramsay
'''

from DataStore import DataStore
from ProjectionReference import Projection

#from osr import SpatialReference 

class ESRIDataStore(DataStore):
    '''
    ESRI specific DS because ESRI is non-conforming and demands its own SRID
    '''

    def __init__(self,conn_str=None):
        '''
        cons init driver
        '''
        super(ESRIDataStore,self).__init__(conn_str)
        
        
    def sourceURI(self,layer):
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        return self._commonURI(layer)
        
    def _commonURI(self,layer):
        raise NotImplementedError("No common URI method for ESRI stack, implement at type level")
        

#    def read(self,dsn):
#        self.ds = self.driver.Open(dsn)
#    
    def write(self,src_ds,dsn):
        '''TODO. No need to do the poly to multi conversion but incremental __change__ removal still reqd'''
        #naive implementation? change SR per layer in place
        self.convertDatasourceESRI(src_ds.ds)
        super(ESRIDataStore,self).write(src_ds,dsn)
        #self.ds = self.driver.CopyDataSource(src_ds, dsn)
        
        
    #def convertDatasetESRI(self,dataset):
    #    '''morphs raster datasets, not so useful with datasources'''
    #    pr1 = dataset.GetProjectionRef()
    #    pr2 = SpatialReference(pr1).MorphToESRI()
    #    return dataset.SetProjectionRef(pr2)
    
    def convertDatasourceESRI(self,datasource):
        '''morphs datasource layer by layer, in place'''
        for li in range(0,datasource.GetLayerCount()):
            layer = datasource.GetLayer(li)
           
            sref = layer.GetSpatialRef()
            #print "Original DS SR :: \nname={}\nlayerdefn={}\ngeocolumn={}\nspatialref={}".format(layer.GetName(),layer.GetLayerDefn(),layer.GetGeometryColumn(),sref)
            #for f in range(0,l1.GetFeatureCount()):
            #    print "FEAT:",l1.GetFeature(f)
            sref.MorphToESRI()
            sref = Projection.modifyMorphedSpatialReference(sref)
            #HACK. morph strips node authority so add it back manually. TODO. What happens when we're reprojecting? Remove for now
            #sref.SetAuthority("GEOGCS","EPSG",4167)
            #print "Converted DS SR",sref 
            
        return datasource
        
    def rebuildSpatialReference(self,sr):
        '''de/re-construct faulty sref after morph'''
        #SetGeogCS(self, char pszGeogName, char pszDatumName, char pszEllipsoidName, 
        #double dfSemiMajor, double dfInvFlattening, 
        #char pszPMName = "Greenwich", double dfPMOffset = 0.0, 
        #char pszUnits = "degree", double dfConvertToRadians = 0.0174532925199433) -> OGRErr
        #srs.SetGeogCS("GCS_NZGD_2000","D_NZGD_2000","GRS_1980",6378137.0,298.257222101,"Greenwich",0.0,"Degree",0.0174532925199433)
        pass
        
        
        
    def getOptions(self,layer_id):
        '''no cross-esri specific options'''
        
        return super(ESRIDataStore,self).getOptions()
        