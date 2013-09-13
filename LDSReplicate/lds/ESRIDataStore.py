'''
v.0.0.9

LDSReplicate - ESRIDataStore

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

ESRI specific DS class super classing ESRI based data formats including FileGDB, ShapeFile and ArcSDE wrapping calls to DS to intercept
cases requiring special handling

Created on 9/08/2012

@author: jramsay

'''

from lds.DataStore import DataStore
from lds.ProjectionReference import Projection
from lds.LDSUtilities import LDSUtilities
#from osr import SpatialReference 

ldslog = LDSUtilities.setupLogging()

class ESRIDataStore(DataStore):
    '''
    ESRI Specific superclass primarily used to do OSGEO to ESRI SpatialReference transformations
    '''

    def __init__(self,conn_str=None,user_config=None):

        super(ESRIDataStore,self).__init__(conn_str,user_config)
        
        
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
    def write(self,src_ds,dsn,sixtyfour):
        '''ESRI specific write method used as entry point for convertDataSourceESRI'''
        '''TODO. No need to do the poly to multi conversion but incremental __change__ removal still reqd'''
        #naive implementation? change SR per layer in place. Conversion not needed with latest GDAL
        #self.convertDataSourceESRI(src_ds.ds)
        super(ESRIDataStore,self).write(src_ds,dsn,sixtyfour)
        #self.ds = self.driver.CopyDataSource(src_ds, dsn)
        
    
    def convertDataSourceESRI(self,datasource):
        #bypassed when using gdal 1.9.2 since FileGDB SREF handling, its supposed to be fixed now
        '''Spatial Reference method to "Morph" datasource layer by layer, in place'''
        for li in range(0,datasource.GetLayerCount()):
            layer = datasource.GetLayer(li)
           
            sref = layer.GetSpatialRef()
            ldslog.debug("Original DS SR ::\nname={}\nlayerdefn={}\ngeocolumn={}\nspatialref={}".format(layer.GetName(),layer.GetLayerDefn(),layer.GetGeometryColumn(),sref))


            #Method 1 +  repaste authority + rename to an esri spec
            ac = sref.GetAuthorityCode('GEOGCS')
            sref.MorphToESRI()
            sref.SetAuthority("GEOGCS","EPSG",int(ac))
            sref = Projection.modifyMorphedSpatialReference(sref)

            ##Method 2
            #sref = Projection.downloadESRISpatialReference(sref.GetAuthorityCode('GEOGCS'))
            
            ldslog.debug("Converted DS SR ::\n"+str(sref)) 
            
        return datasource
        
        
    def getLayerOptions(self,layer_id):
        '''Direct push through to super since no pan-ESRI specific options'''
        
        return super(ESRIDataStore,self).getLayerOptions(layer_id)
    
    def getConfigOptions(self):
        '''Direct push through to super'''
        
        return super(ESRIDataStore,self).getConfigOptions()
        