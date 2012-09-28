'''
ESRI specific DS class super classing ESRI based data formats including FileGDB, ShapeFile and ArcSDE

Created on 9/08/2012

@author: jramsay
'''

from DataStore import DataStore
from ProjectionReference import Projection

#from osr import SpatialReference 

class ESRIDataStore(DataStore):
    '''
    ESRI Specific superclass primarily used to do OSGEO to ESRI SpatialReference transformations
    '''

    def __init__(self,conn_str=None):

        super(ESRIDataStore,self).__init__(conn_str)
        
        
    def sourceURI(self,layer):
        '''URI method for returning source calls private subclass common URI method'''
        return self._commonURI(layer)
    
    def destinationURI(self,layer):
        '''URI method for returning destination calls private subclass common URI method'''
        return self._commonURI(layer)
        
    def _commonURI(self,layer):
        raise NotImplementedError("No common URI method for ESRI stack, implement at type level")
        

#    def read(self,dsn):
#        self.ds = self.driver.Open(dsn)
#    
    def write(self,src_ds,dsn):
        '''ESRI specific write method used as entry point for converDataSourceESRI'''
        '''TODO. No need to do the poly to multi conversion but incremental __change__ removal still reqd'''
        #naive implementation? change SR per layer in place
        self.convertDataSourceESRI(src_ds.ds)
        super(ESRIDataStore,self).write(src_ds,dsn)
        #self.ds = self.driver.CopyDataSource(src_ds, dsn)
        
        
    #def convertDatasetESRI(self,dataset):
    #    '''morphs raster datasets, not so useful with datasources'''
    #    pr1 = dataset.GetProjectionRef()
    #    pr2 = SpatialReference(pr1).MorphToESRI()
    #    return dataset.SetProjectionRef(pr2)
    
    def convertDataSourceESRI(self,datasource):
        '''Spatial Reference method to "Morph" datasource layer by layer, in place'''
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
        
        
    def getOptions(self,layer_id):
        '''Direct push through to super since no pan-ESRI specific options'''
        
        return super(ESRIDataStore,self).getOptions()
        