'''
DataStore is the base Datasource wrapper object 

Created on 9/08/2012

@author: jramsay
'''

import sys
import ogr
import osr
import re
import logging
import json

from datetime import datetime
from abc import ABCMeta, abstractmethod

from LDSUtilities import LDSUtilities, ConfigInitialiser
from ProjectionReference import Projection
from ConfigWrapper import ConfigWrapper
from ReadConfig import LayerFileReader

ldslog = logging.getLogger('LDS')
#Enabling exceptions halts program on non critical errors ie create DS throws exception but builds valid DS anyway 
#ogr.UseExceptions()

#exceptions
class DSReaderException(Exception): pass
class LDSReaderException(DSReaderException): pass
class IncompleteWFSRequestException(LDSReaderException): pass
class CannotInitialiseDriverType(LDSReaderException): pass
class DatasourceCopyException(LDSReaderException): pass
class DatasourceCreateException(LDSReaderException): pass
class DatasourceOpenException(DSReaderException): pass
class LayerCreateException(LDSReaderException): pass
class InvalidLayerException(LDSReaderException): pass
class InvalidFeatureException(LDSReaderException): pass
class ExecuteSQLException(LDSReaderException): pass


class DataStore(object):
    '''
    DataStore superclasses PostgreSQL, LDS(WFS), FileGDB and SpatiaLite datastores.
    This class contains the main copy functions for each datasource and sets up default connection parameters. Common options are also set up in this class 
    '''
    __metaclass__ = ABCMeta


    LDS_CONFIG_TABLE = 'lds_config'
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
    EARLIEST_INIT_DATE = '2000-01-01T00:00:00'
    
    
    def __init__(self,conn_str=None,user_config=None):
        '''
        Constructor inits driver and some date specific settings. Arguments are for config overrides 
        '''

        if conn_str is not None:
            self.conn_str = conn_str
        
        self.setSRS(None)
        self.setFilter(None)     

        #default clear the INCR flag
        self.setOverwrite()
        self.clearIncremental()
        
        self.getDriver(self.DRIVER_NAME)
            
        self.confwrapper = ConfigWrapper(user_config)
        
        self.params = self.confwrapper.readDSParameters(self.DRIVER_NAME)
        
        
        '''set of <potential> columns not needed in final output, global'''
        self.optcols = set(['__change__','gml_id'])
        
        
    def getDriver(self,driver_name):

        self.driver = ogr.GetDriverByName(driver_name)
        if self.driver==None:
            raise CannotInitialiseDriverType, "Driver cannot be initialised for type "+driver_name
            sys.exit(1)


    #filter/cql (applicable to source type)
    def setFilter(self,cql):
        self.cql = cql
         
    def getFilter(self):
        return self.cql
    
    #SRS
    def setSRS(self,srs):
        self.srs = srs
         
    def getSRS(self):
        return self.srs  
    
    #config (internal/external)
    def setConfInternal(self):
        self.config = True
            
    def setConfExternal(self):
        self.config = False
         
    def isConfInternal(self):
        return self.config       
    
    #--------------------------  
    
    #incr flag
    def setIncremental(self):
        self.INCR = True
         
    def clearIncremental(self):
        self.INCR = False
         
    def getIncremental(self):
        return self.INCR
    
    #overwrite flag
    def setOverwrite(self):
        self.OVERWRITE = "YES"
         
    def clearOverwrite(self):
        self.OVERWRITE = "NO"
         
    def getOverwrite(self):
        return self.OVERWRITE    
    
    def getOptions(self):
        '''Returns common options, overridden in subclasses for source specifc options'''
        return ['OVERWRITE='+self.getOverwrite()]    
    
    '''Both Source and Destination URI for the generic situation where we want to transfer between similar Ds formats. e.g. PG->PG'''
    
    @abstractmethod
    def sourceURI(self,layer):
        '''Abstract URI method for returning source. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method sourceURI not implemented")
    
    @abstractmethod
    def destinationURI(self,layer):
        '''Abstract URI method for returning destination. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method destinationURI not implemented")

    #@abstractmethod
    #def buildExternalLayerDefinition(self,name,fdef_list):
    #    raise NotImplementedError("Abstract method buildExternalLayerDefinition not implemented")
    
    def initDS(self,dsn):
        '''initialise a DS for writing'''
        try:
            ds = self.driver.Open(dsn, update = 1 if self.getOverwrite() else 0)
            if ds is None:
                raise DSReaderException("Error opening DS on Destination "+str(dsn)+", attempting DS Create")
        #catches DSReader (but not runtime) error, but don't fail since we'll try to init a new DS
        except (RuntimeError,DSReaderException) as dsre1:
            #print "DSReaderException",dsre1 
            ldslog.error(dsre1)
            try:
                ds = self.driver.CreateDataSource(dsn)
                if ds is None:
                    raise DSReaderException("Error creating DS on Destination "+str(dsn)+", quitting")
            except DSReaderException as dsre2:
                print "DSReaderException, Cannot create DS.",dsre2
                ldslog.error(dsre2,exc_info=1)
                raise
            except RuntimeError as rte:
                '''this is only caught if ogr.UseExceptions() is enabled'''
                print "GDAL RuntimeError. Error creating DS.",rte
                ldslog.error(rte,exc_info=1)
                raise
        return ds
        
    
    def read(self,dsn):
        '''Main DS read method'''
        ldslog.info("DS read "+dsn)#.split(":")[0])
        #5050 initDS for consistency and utilise if-ds-is-none check OR quick open and overwrite
        #self.initDS(dsn)
        self.ds = self.driver.Open(dsn)
    
    def write(self,src,dsn):
        '''Main DS write method. Attempts to open or alternatively, create a datasource'''
        
        ldslog.info("DS Write "+dsn)#.split(":")[0]
        if not hasattr(self,'ds') or self.ds is None:
            self.ds = self.initDS(dsn)
        
        if src.getIncremental():
            # change col in delete list and as change indicator
            self.copyDS(src.ds,self.ds,src.CHANGE_COL)
        else:
            # no cols to delete and no operational instructions
            self.cloneDS(src.ds,self.ds)
        

    def closeDS(self):
        ldslog.info("Sync DS and Close")
        self.ds.SyncToDisk()
        self.ds.Destroy()  
              
    def cloneDS(self,src_ds,dst_ds):
        '''Copy from source to destination using the driver copy and without manipulating data'''
        '''TODO. address problems with this approach if a user has changed a tablename, specified ignore columns etc (though this doesnt seem possible)'''

        ldslog.info("Using cloneDS. Non-Incremental driver copy")
        for li in range(0,src_ds.GetLayerCount()):
            src_layer = src_ds.GetLayer(li)
            src_layer_name = src_layer.GetName()
            
            #ref_name = self.layerconf.readConvertedLayerName(src_layer_name)
            (ref_pkey,ref_name,ref_group,ref_gcol,ref_index,ref_epsg,ref_lmod,ref_disc,ref_cql) = self.layerconf.readLayerParameters(src_layer_name)
            
            dst_layer_name = self.schema+"."+DataStore.sanitise(ref_name) if self.schema is not None else DataStore.sanitise(ref_name)
            dst_ds.CopyLayer(src_layer,dst_layer_name,self.getOptions(src_layer_name)) 
            if ref_index is not None:
                self.buildIndex(ref_index,ref_pkey,ref_gcol,dst_layer_name)
            
        src_layer.ResetReading()

        #Delete unwanted columns... wont work with some drivers and others under certain conditions only
        self.optcols |= set(ref_disc.strip('[]{}()').split(',') if ref_disc is not None else [])
        
        dst_layer = dst_ds.GetLayer(dst_layer_name)
        for del_fld in self.optcols:
            dst_layer.DeleteField(del_fld)

    
    
    def copyDS(self,src_ds,dst_ds,changecol):
        #TDOD. decide whether C_C is better as an arg or a src.prop
        '''DataStore feature-by-feature replication for incremental queries'''
        #build new layer by duplicating source layers  
            
        ldslog.info("Using copyDS. Per-feature copy")
        for li in range(0,src_ds.GetLayerCount()):
            new_layer = False
            src_layer = src_ds.GetLayer(li)
            src_layer_name = src_layer.GetName()
            src_layer_sref = src_layer.GetSpatialRef()
            src_layer_geom = src_layer.GetGeomType()
            src_layer_defn = src_layer.GetLayerDefn()
            
            #TODO. resolve conflict between lastmodified and fdate
            ref_layer_name = LDSUtilities.cropChangeset(src_layer_name)
            
            '''retrieve per-layer settings from props'''
            (ref_pkey,ref_name,ref_group,ref_gcol,ref_index,ref_epsg,ref_lmod,ref_disc,ref_cql) = self.layerconf.readLayerParameters(ref_layer_name)
            
            dst_layer_name = self.schema+"."+self.sanitise(ref_name) if hasattr(self,'schema') and self.schema is not None else self.sanitise(ref_name)
            ldslog.info("Dest layer: "+dst_layer_name)
            
            '''parse discard columns'''
            self.optcols |= set(ref_disc.strip('[]{}()').split(',') if ref_disc is not None else [])
            
            #check for user defined projections
            #this should be done in TP.defIncr now
            #if self.getSRS() is not None:
            #    ref_epsg = self.getSRS()
            dst_sref = self.transformSRS(src_layer_sref)
               
            #assuming output layer name will be the same... confusion if this isnt so
            dst_layer = dst_ds.GetLayer(dst_layer_name)
            if dst_layer is None:
                new_layer = True
                ldslog.warning(dst_layer_name+" does not exist. Creating new layer")
                '''create a new layer if a similarly named existing layer can't be found on the dst'''
                dst_layer = self.buildNewDataLayer(dst_layer_name,dst_ds,dst_sref,src_layer_defn,src_layer_geom,src_layer_sref,ref_layer_name)
            
            #add/copy features
            #src_layer.ResetReading()
            src_feat = src_layer.GetNextFeature()
            
            '''since the characteristics of each feature wont change between layers we only need to define a new feature definition once'''
            if src_feat is not None:
                new_feat_def = self.partialCloneFeatureDef(src_feat)
            
            while src_feat is not None:
                '''identify the change in the WFS doc (INS,UPD,DEL)'''
                change =  src_feat.GetField(changecol) if changecol is not None and len(changecol)>0 else "INSERT"
                '''not just copy but possubly delete or update a feature on the DST layer'''
                self.copyFeature(change,src_feat,dst_layer,ref_pkey,new_feat_def,ref_gcol)
                                               
                src_feat = src_layer.GetNextFeature()

            #self._showLayerData(dst_layer)
            
            '''Builds an index on a newly created layer'''
            #May need to be pushed out to subclasses depending on syntax differences
            if new_layer and ref_index is not None:
                self.buildIndex(ref_index,ref_pkey,ref_gcol,dst_layer_name)
            
            src_layer.ResetReading()
            dst_layer.ResetReading()

    def transformSRS(self,src_layer_sref):
        '''transform from one SRS to another provided supplied EPSG is correct and coordinates can be transformed'''
        self.transform = None
        dst_sref = src_layer_sref#not necessary but for clarity
        selected_sref = self.getSRS()
        if selected_sref is not None and selected_sref != '':
            #if the selected SRS fails to validate assume error and flag but dont silently drop back to default
            trans_sref = Projection.validateEPSG(selected_sref)
            if trans_sref is not None:
                self.transform = osr.CoordinateTransformation(src_layer_sref, trans_sref)
                if self.transform is not None:
                    dst_sref = trans_sref
            else:
                ldslog.warn("Unable to validate selected SRS, epsg="+str(selected_sref))
                    
        return dst_sref
    
    def copyFeature(self,change,src_feat,dst_layer,ref_pkey,new_feat_def,ref_gcol):
        '''Insert, Delete or Update a feature to match WFS change set'''
        #self._showFeatureData(new_feat)
        src_pkey = src_feat.GetFieldAsInteger(ref_pkey)
        ldslog.debug("CHANGE:"+change+" "+str(src_pkey))
        
        try:
            if change == "INSERT":
                '''build new feature from defn and insert'''
                new_feat = self.partialCloneFeature(src_feat,new_feat_def,ref_gcol)
                err = dst_layer.CreateFeature(new_feat)
                dst_fid = new_feat.GetFID()
            elif change == "DELETE":
                '''lookup and delete using fid matching ID of feature being deleted'''
                #if not new_layer_flag: 
                dst_fid = self._findMatchingFID(dst_layer, ref_pkey, src_pkey)
                if dst_fid is not None:
                    err = dst_layer.DeleteFeature(dst_fid)
                else:
                    ldslog.error("No match for FID with ID="+str(src_pkey)+" on "+change,exc_info=1)
                    raise InvalidFeatureException("No match for FID with ID="+str(src_pkey)+" on "+change)
            elif change == "UPDATE":
                '''build new feature, assign it the looked-up matching fid and overwrite on dst'''
                #if not new_layer_flag: 
                dst_fid = self._findMatchingFID(dst_layer, ref_pkey, src_pkey)
                new_feat = self.partialCloneFeature(src_feat,new_feat_def,ref_gcol)
                if dst_fid is not None:
                    new_feat.SetFID(dst_fid)
                    err = dst_layer.SetFeature(new_feat)
                else:
                    ldslog.error("No match for FID with ID="+str(src_pkey)+" on "+change,exc_info=1)
                    raise InvalidFeatureException("No match for FID with ID="+str(src_pkey)+" on "+change)
                
            if err!=0: 
                ldslog.error("Driver Error ["+str(err)+"] using FID="+str(dst_fid)+" on "+change,exc_info=1)
                raise InvalidFeatureException("Driver Error ["+str(err)+"] using FID="+str(dst_fid)+" on "+change)
            
        except InvalidFeatureException as ife:
            ldslog.error(ife,exc_info=1)
            #print "InvalidFeatureException",ife
    
                    
    def buildNewDataLayer(self,dst_layer_name,dst_ds,dst_sref,src_layer_defn,src_layer_geom,src_layer_sref,ref_layer_name):        
        #read defns of each field
        fdef_list = []
        for fi in range(0,src_layer_defn.GetFieldCount()):
            fdef_list.append(src_layer_defn.GetFieldDefn(fi))
        
        #use the field defns to build a schema since this needs to be loaded as a create_layer option
        opts = self.getOptions(ref_layer_name)
        
        '''build layer replacing poly with multi and revert to def if that doesn't work'''
        if src_layer_geom is ogr.wkbPolygon:
            dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,ogr.wkbMultiPolygon,opts)
        else:
            dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,src_layer_geom,opts)
            
        if dst_layer is None:
            #overwrite the dst_sref if its causing trouble (ie GDAL general function errors)
            dst_sref = Projection.getDefaultSpatialRef()
            ldslog.warning("Could not initialise Layer with specified SRID {"+str(src_layer_sref)+"}.\n\nUsing Default {"+str(dst_sref)+"} instead")
            if src_layer_geom is ogr.wkbPolygon:
                dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,ogr.wkbMultiPolygon,opts)
            else:
                dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,src_layer_geom,opts)
                
        if dst_layer is None:
            ldslog.error(dst_layer_name+" cannot be created")
            raise LayerCreateException(dst_layer_name+" cannot be created")
    
        '''setup layer headers for new layer etc'''                
        for fdef in fdef_list:
            #print "field:",fi
            if fdef.GetName() not in self.optcols:# and fdef.GetName() in reqdcols:
                dst_layer.CreateField(fdef)
                #could check for any change tags and throw exception if none
                
        return dst_layer
                                           
    def partialCloneFeature(self,fin,fout_def,ref_gcol):
        '''Builds a feature using a passed in feature definition. Must still ignore discarded columns since they will be in the source'''

        fout = ogr.Feature(fout_def)

        '''Set Geometry transforming if needed'''
        fin_geom = fin.GetGeometryRef()
        if self.transform is not None:
            #TODO check whether this fin_geom needs to be cloned first
            fin_geom.Transform(self.transform)
        fout.SetGeometry(fin_geom)

        #DataStore._showFeatureData(fin)
        #DataStore._showFeatureData(fout)
        
        '''populate feature'''
        fout_no = 0
        for fin_no in range(0,fin.GetFieldCount()):
            fin_field_name = fin.GetFieldDefnRef(fin_no).GetName()
            if fin_field_name not in self.optcols:
                copy_field = fin.GetField(fin_no)
                fout.SetField(fout_no, copy_field)
                fout_no += 1
            
        #DataStore._showFeatureData(fout)
        fout_geom = fout.GetGeometryRef()
   
        if fout_geom.ExportToWkt()[:7]=="POLYGON":
            fin.SetGeometryDirectly(ogr.ForceToMultiPolygon(fout_geom))
            
        return fout
    
    def partialCloneFeatureDef(self,fin):
        '''Builds a feature definition ignoring the __change__ and any discarded columns'''
        #create blank feat defn
        fout_def = ogr.FeatureDefn()
        #read input feat defn
        fin_feat_def = fin.GetDefnRef()
        
        #loop existing feature defn ignoring column X
        for fin_no in range(0,fin.GetFieldCount()):
            fin_field_def = fin.GetFieldDefnRef(fin_no)
            fin_field_name = fin_field_def.GetName()
            if fin_field_name not in self.optcols:
                fin_fld_def = fin_feat_def.GetFieldDefn(fin_no)
                #print "n={}, typ={}, wd={}, prc={}, tnm={}".format(fin_fld_def.GetName(),fin_fld_def.GetType(),fin_fld_def.GetWidth(),fin_fld_def.GetPrecision(),fin_fld_def.GetTypeName())
                fout_def.AddFieldDefn(fin_fld_def)
                
        return fout_def
    

    
    def getLastModified(self,layer):
        '''Gets the last modification time of a layer to use for incremental "fromdate" calls. This is intended to be run 
        as a destination method since the destination is the DS being modified i.e. dst.getLastModified'''
        lmd = self.layerconf.readLastModified(layer)
        if lmd is None or lmd == '':
            lmd = self.EARLIEST_INIT_DATE
        return lmd
        #return lm.strftime(self.DATE_FORMAT)
        
    def setLastModified(self,layer,newdate):
        '''Sets the last modification time of a layer following a successful incremental copy operation'''
        self.layerconf.writeLayerProperty(layer, 'lastmodified', newdate)  

    def getCurrent(self):
        '''Gets the current timestamp for incremental todate calls. 
        Time format is UTC for LDS compatibility.
        NB. Because the current date is generated to build the LDS URI the lastmodified time will reflect the request time and not the layer creation time'''
        dpo = datetime.utcnow()
        return dpo.strftime(self.DATE_FORMAT)
    
    
    def buildIndex(self,ref_index,ref_pkey,ref_gcol,dst_layer_name):
        '''Default index string builder for new fully replicated layers'''
        ref_index = ref_index.lower()
        if ref_index == 'spatial' or ref_index == 's':
            cmd = 'CREATE INDEX {}_SK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+ref_gcol,dst_layer_name,ref_gcol)
        elif ref_index == 'pkey' or ref_index == 'p':
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+ref_pkey,dst_layer_name,ref_pkey)
        elif ref_index is not None:
            #maybe the user wants a non pk/spatial index? Try to filter the string
            clst = ','.join(DataStore.parseStringList(ref_index))
            cmd = 'CREATE INDEX {}_PK ON {}({})'.format(dst_layer_name.split('.')[-1]+"_"+DataStore.sanitise(clst),dst_layer_name,clst)
        else:
            return
        ldslog.info("Index="+ref_index+". Execute "+cmd)
        self.executeSQL(cmd)

    
    # private methods
        
    def executeSQL(self,sql):
        '''Executes arbitrary SQL on the datasource'''
        '''Tagged private since we only want it called from well controlled methods'''
        '''TODO. step through multi line queries?'''
        retval = None
        #ogr.UseExceptions()
        ldslog.debug("SQL: "+sql)
        '''validating sql as a block acts as a sort of transaction mechanism and means we can execute the entire statement which is faster'''
        if self._validateSQL(sql):
            try:
                retval = self.ds.ExecuteSQL(sql)        
                #for r2 in sql.split('\n'):
                    #print "**********",r2
                    #self.ds.ExecuteSQL(r2)   
                if retval is None:
                    raise ExecuteSQLException("SQL block failed to execute. "+sql)
            except RuntimeError as rex:
                ldslog.error("Unable to execute SQL:"+sql+". Get Error "+str(rex),exc_info=1)
                #this is probably a bad thing so we want to stop if this occurs e.g. no lds_config -> no layer list etc
                raise
            except Exception as ex:
                ldslog.error("Unable to execute SQL:"+sql+". Catchall Error "+str(ex),exc_info=1)
                #raise
                
        return retval
            

    def _validateSQL(self,sql):
        '''Validates SQL against a list of allowed queries. Not trying to restrict queries here, rather catch invalid SQL'''
        '''TODO. Better validation.'''
        sql = sql.lower()
        for line in sql.split('\n'):
            #ignore comments/blanks
            if re.match('^(?:#|--)|^\s*$',line):
                continue
            #first match 'create/drop index'
            if re.match('(?:create|drop)(?:\s+spatial)?\s+index',line):
                continue
            #match 'create/drop index/table'
            if re.match('(?:create|drop)\s+(?:index|table)',line):
                continue
            #match 'select'
            if re.match('select\s+(?:\w+|\*)\s+from',line):
                continue
            #match 'insert'
            if re.match('(?:update|insert)\s+(?:\w+|\*)\s+',line):
                continue
            if re.match('if\s+object_id\(',line):
                continue
            
            ldslog.error("Line in SQL failed to validate. "+line)
            return False
        
        return True
        
    def _cleanLayer(self,ds,layer):
        '''Deletes a layer from the DS'''
        ldslog.info("DS clean")
        ds.DeleteLayer(layer)
        
    def _clean(self):
        '''Deletes the entire DS layer by layer'''
        for li in range(0,self.ds.GetLayerCount()):
            self._cleanLayer(self.ds,li)
    
    
    def _findMatchingFID(self,search_layer,ref_pkey,key):
        '''Find the FID matching a primary key value'''
        return self._findMatchingFeature(self,search_layer,ref_pkey,key).GetFID()
        
    
    def _findMatchingFeature(self,search_layer,ref_pkey,key):
        '''Find the Feature matching a primary key value'''
        qry = ref_pkey+" = '"+str(key)+"'"
        search_layer.SetAttributeFilter(qry)
        return search_layer.GetNextFeature()
            
    
    # utility methods
    
    @staticmethod
    def sanitise(name):
        '''Manually substitute potential table naming errors implemented as a common function to retain naming convention across all outputs.
        No guarantees are made that this feature won't cause naming conflicts e.g. A-B-C -> a_b_c <- a::{b}::c'''
        #append _ to name beginning with a number
        if re.match('\A\d',name):
            name = "_"+name
        #replace unwanted chars with _ and compress multiple and remove trailing
        sanitised = re.sub('_+','_',re.sub('[ \-,.\\\\/:;{}()\[\]]','_',name.lower())).rstrip('_')
        #unexpected name substitutions can be a source of bugs, log as debug
        ldslog.debug("Sanitise: raw="+name+" name="+sanitised)
        return sanitised
    

    @staticmethod
    def parseStringList(st):
        '''QaD List-as-String to List parser'''
        return st.rstrip(')]').lstrip('[(').split(',') if st.find(',')>-1 else st
    
    # debugging methods
    
    @staticmethod
    def _showFeatureData(feature):
        '''Prints feature/fid info. Useful for debugging'''
        ldslog.debug("Feat:FID:"+str(feature.GetFID()))
        for field_no in range(0,feature.GetFieldCount()):
            ldslog.debug("fid={},fld_no={},fld_data={}".format(feature.GetFID(),field_no,feature.GetFieldAsString(field_no)))
            
    @staticmethod
    def _showLayerData(layer):
        '''Prints layer and embedded feature data. Useful for debugging'''
        ldslog.debug("Layer:Name:"+layer.GetName())
        layer.ResetReading()
        feat = layer.GetNextFeature()
        while feat is not None:
            DataStore._showFeatureData(feat)
            feat = layer.GetNextFeature()                
                


#=======================TESTING!============================

    def setupLayerConfig(self):
        '''Read internal OR external from main config file and set, default to internal'''
        
        if 'external' in map(lambda x: x.lower() if type(x) is str else x,self.confwrapper.readDSParameters(self.DRIVER_NAME)):
            self.setConfExternal()
        else:
            self.setConfInternal()

    
    def buildConfigLayer(self,config_array):
        '''Builds the config table into and using the active DS'''
        #TODO check initds for conf table name
        if not hasattr(self,'ds') or self.ds is None:
            self.ds = self.initDS(self.destinationURI(DataStore.LDS_CONFIG_TABLE))
        #bypass (probably not needed) if external (alternatively set [layerconf = self or layerconf = self.confwrapper])
        if not self.isConfInternal():
            return self.layerconf.buildConfigLayer()
        #TODO unify the naming for the config tables

        
        #open('/home/jramsay/temp/pyary','w').write(json.dumps(cc))
        #json.loads(open('/home/jramsay/temp/pyary','r').read())

        #self.ds.DeleteLayer(DataStore.LDS_CONFIG_TABLE)

        cols = ('id','pkey','name','category','lastmodified','geocolumn','epsg','discard','cql')
        #HACK even though the config table is not a geometry table MSSQLSpatiaLite needs a physical geo type to build a layer

        if self.DRIVER_NAME == 'MSSQLSpatial':
            config_layer = self.ds.CreateLayer(DataStore.LDS_CONFIG_TABLE,None,ogr.wkbPoint,['OVERWRITE=YES'])
        else:
            config_layer = self.ds.CreateLayer(DataStore.LDS_CONFIG_TABLE,None,ogr.wkbNone,['OVERWRITE=YES'])
        
        
        feat_def = ogr.FeatureDefn()
        for name in cols:
            #create new field defn with name=name and type OFTString
            fld_def = ogr.FieldDefn(name,ogr.OFTString)
            #in the feature defn, define a new field
            feat_def.AddFieldDefn(fld_def)
            #also add a field to the table definition, i.e. column
            config_layer.CreateField(fld_def,True)                
        
        for row in json.loads(config_array):
            config_feat = ogr.Feature(feat_def)
            config_feat.SetField('id',str(row[0]))
            config_feat.SetField('pkey',str(row[1]))
            config_feat.SetField('name',str(row[2]))
            config_feat.SetField('category',str(','.join(row[3])))
            config_feat.SetField('lastmodified',str(row[4]))
            config_feat.SetField('geocolumn',str(row[5]))
            config_feat.SetField('epsg',str(row[6]))
            config_feat.SetField('discard',None if row[7] is None else str(','.join(row[7])))
            config_feat.SetField('cql',str(row[8]))
            
            config_layer.CreateFeature(config_feat)
            
        config_layer.ResetReading()
        config_layer.SyncToDisk()
        

    def getLayerNames(self):
        '''Returns configured layers for respective layer properties file'''
        namelist = ()
        layer = self.ds.GetLayer(DataStore.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = layer.GetNextFeature() 
        while feat is not None:
            namelist += (feat.GetField('id'),)
            feat = layer.GetNextFeature()
        
        return namelist

      
    def readLayerParameters(self,pkey):
        '''Full Layer config reader'''
        layer = self.ds.GetLayer(DataStore.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = self._findMatchingFeature(layer, 'id', pkey)
        return LDSUtilities.extractFields(feat)
        
        
    def readLayerProperty(self,pkey,field):

        layer = self.ds.GetLayer(DataStore.LDS_CONFIG_TABLE)
        layer.ResetReading()
        feat = self._findMatchingFeature(layer, 'id', pkey)
        if feat is None:
            return None
        prop = feat.GetField(field)
        return None if prop == 'None' else prop



    def writeLayerProperty(self,pkey,field,value):
        '''Write changes to layer config table'''
        #ogr.UseExceptions()
        try:
            layer = self.ds.GetLayer(DataStore.LDS_CONFIG_TABLE)
            feat = self._findMatchingFeature(layer, 'id', pkey)
            feat.SetField(field,value)
            layer.SetFeature(feat)
            ldslog.debug("Check "+field+" for layer "+pkey+" is set to "+value+" : GetField="+feat.GetField(field))
        except Exception as e:
            ldslog.error(e)


        
