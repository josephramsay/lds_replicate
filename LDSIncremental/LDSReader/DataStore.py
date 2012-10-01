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

from datetime import datetime, timedelta
from abc import ABCMeta, abstractmethod

from LDSUtilities import LDSUtilities
from ProjectionReference import Projection

ldslog = logging.getLogger('LDS')

#exceptions
class DSReaderException(Exception): pass
class LDSReaderException(DSReaderException): pass
class IncompleteWFSRequestException(LDSReaderException): pass
class CannotInitialiseDriverType(LDSReaderException): pass
class DatasourceCopyException(LDSReaderException): pass
class DatasourceCreateException(LDSReaderException): pass
class DatasourceOpenException(DSReaderException): pass
class InvalidLayerException(LDSReaderException): pass
class InvalidFeatureException(LDSReaderException): pass




class DataStore(object):
    '''
    DataStore superclasses PostgreSQL, LDS(WFS), FileGDB and SpatiaLite datastores.
    This class contains the main copy functions for each datasource and sets up default connection parameters. Common options are also set up in this class 
    '''
    __metaclass__ = ABCMeta



    def __init__(self,conn_str=None):
        '''
        cons init driver
        '''
        

        
        if conn_str is not None:
            self.conn_str = conn_str
            
            
        self.DATE_FORMAT='%Y-%m-%d'
        self.EARLIEST_INIT_DATE = '2000-01-01'

        #default clear the INCR flag
        self.setOverwrite()
        self.clearIncremental()
        
        '''set of <potential> columns not needed in final output. ogc_fid can be discarded if alternative PK is used'''
        self.optcols = set(['__change__','gml_id'])
        
        
    def getDriver(self,driver_name):

        self.driver = ogr.GetDriverByName(driver_name)
        if self.driver==None:
            raise CannotInitialiseDriverType, "Driver cannot be initialised for type "+driver_name
            sys.exit(1)


    #filter/cql (applicable for source type)
    def setFilter(self,cql):
        self.cql = cql
         
    def getFilter(self):
        if self.cql is not None and self.cql != '': 
            return self.cql
        return None
    
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
    def sourceURI(self):
        '''Abstract URI method for returning source. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method sourceURI not implemented")
    
    @abstractmethod
    def destinationURI(self):
        '''Abstract URI method for returning destination. Raises NotImplementedError if accessed directly'''
        raise NotImplementedError("Abstract method destinationURI not implemented")

    #@abstractmethod
    #def buildExternalLayerDefinition(self,name,fdef_list):
    #    raise NotImplementedError("Abstract method buildExternalLayerDefinition not implemented")
    
    def read(self,dsn):
        '''Main DS read method'''
        ldslog.info("DS read"+dsn.split(":")[0])
        self.ds = self.driver.Open(dsn)
    
    def write(self,src,dsn):
        '''Main DS write method. Attempts to open or alternatively, create a datasource'''
        
        ldslog.info("DS Write "+dsn)#.split(":")[0]
        try:
            self.ds = self.driver.Open(dsn, update = 1 if self.getOverwrite() else 0)
            if self.ds is None:
                raise DSReaderException("Error opening DS on Destination "+str(dsn)+", attempting DS Create")
        except DSReaderException as dsre1:
            print "DSReaderException",dsre1 
            ldslog.error(dsre1)
            try:
                self.ds = self.driver.CreateDataSource(dsn)
                if self.ds is None:
                    raise DSReaderException("Error creating DS on Destination "+str(dsn)+", quitting")
            except DSReaderException as dsre2:
                print "DSReaderException",dsre2
                ldslog.error(dsre2)
                raise
            
        if src.getIncremental():
            # change col in delete list and as change indicator
            self.copyDS(src.ds,self.ds,src.CHANGE_COL)
        else:
            # no cols to delete and no operational instructions
            self.cloneDS(src.ds,self.ds)
        
        ldslog.info("Sync output")
        self.ds.SyncToDisk()
        self.ds.Destroy()  

              
    def cloneDS(self,src_ds,dst_ds):
        '''Copy from source to destination using the driver copy and without manipulating data'''
        '''TODO. address problems with this approach if a user has changed a tablename, specified ignore columns etc'''

        ldslog.info("Using cloneDS. Non-Incremental driver copy")
        for li in range(0,src_ds.GetLayerCount()):
            src_layer = src_ds.GetLayer(li)
            src_layer_name = src_layer.GetName()
            ref_name = self.mlr.readConvertedLayerName(src_layer_name)
            dst_layer_name = self.schema+"."+self.sanitise(ref_name) if self.schema is not None else self.sanitise(ref_name)
            dst_ds.CopyLayer(src_layer,dst_layer_name,self.getOptions(src_layer_name))
            
        src_layer.ResetReading()

    
    
    def copyDS(self,src_ds,dst_ds,changecol):
        #TDOD. decide whether C_C is better as an arg or a src.prop
        '''DataStore feature-by-feature replication for incremental queries'''
        #build new layer by duplicating source layers  
            
        ldslog.info("Using copyDS. Per-feature copy")
        for li in range(0,src_ds.GetLayerCount()):

            src_layer = src_ds.GetLayer(li)
            src_layer_name = src_layer.GetName()
            src_layer_sref = src_layer.GetSpatialRef()
            src_layer_geom = src_layer.GetGeomType()
            src_layer_defn = src_layer.GetLayerDefn()
            
            #TODO. resolve conflict between lastmodified and fdate
            ref_layer_name = LDSUtilities.cropChangeset(src_layer_name)
            
            '''retrieve per-layer settings from props'''
            (ref_pkey,ref_name,ref_gcol,ref_index,ref_epsg,ref_lmod,ref_disc,ref_cql) = self.mlr.readAllLayerParameters(ref_layer_name)
            
            dst_layer_name = self.schema+"."+self.sanitise(ref_name) if hasattr(self,'schema') and self.schema is not None else self.sanitise(ref_name)
            ldslog.info("Dest layer: "+dst_layer_name)
            
            '''parse discard columns'''
            self.optcols |= set(ref_disc.strip('[]{}()').split(',') if ref_disc is not None else [])
            
            #check for user a defined projection
            self.transform = None
            dst_sref = src_layer_sref#not necessary but for clarity
            if ref_epsg is not None and ref_epsg != '':
                trans_sref = Projection.validateEPSG(ref_epsg)
                if trans_sref is not None:
                    self.transform = osr.CoordinateTransformation(src_layer_sref, trans_sref)
                    if self.transform is not None:
                        dst_sref = trans_sref
            
            #new_layer_flag = False # used if we del/ins features instead of setting them            
            
            #assuming output layer name will be the same... confusion if this isnt so
            dst_layer = dst_ds.GetLayer(dst_layer_name)
            if dst_layer is None:
                '''create a new layer if a similarly named existing layer can't be found on the dst'''
                ldslog.warning(dst_layer_name+" does not exist. Creating new layer")
                #new_layer_flag = True
                
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
                    ldslog.warning("Could not initialise Layer with specified SRID {",src_layer_sref,"}.\n\nUsing Default {",dst_sref,"} instead")
                    if src_layer_geom is ogr.wkbPolygon:
                        dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,ogr.wkbMultiPolygon,opts)
                    else:
                        dst_layer = dst_ds.CreateLayer(dst_layer_name,dst_sref,src_layer_geom,opts)

                #dst_layer.SetFID("id")
            
                '''setup layer headers for new layer etc'''                
                for fdef in fdef_list:
                    #print "field:",fi
                    if fdef.GetName() not in self.optcols:# and fdef.GetName() in reqdcols:
                        dst_layer.CreateField(fdef)
                        #could check for any change tags and throw exception if none
            
            #add/copy features
            #src_layer.ResetReading()
            src_feat = src_layer.GetNextFeature()
            
            '''since the characteristics of each feature wont change between layers we only need to define a new feature definition once'''
            if src_feat is not None:
                new_feat_def = self.partialCloneFeatureDef(src_feat)
            
            while src_feat is not None:
                #print src_feat.GetFID()
                change =  src_feat.GetField(changecol) if changecol is not None and len(changecol)>0 else "INSERT"
                #src_fid = src_feat.GetFieldAsInteger(ref_pkey)

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
                               
                src_feat = src_layer.GetNextFeature()

            #self._showLayerData(dst_layer)
            
            #Build index - May need to be pushed out to subclasses depending on syntax differences
            if ref_index == 'SPATIAL' or ref_index == 'S':
                self.buildIndex('CREATE SPATIAL INDEX {s}_SK ON {s}({s})'.format(ref_gcol,dst_layer_name,ref_gcol))
            elif ref_index == 'PKEY' or ref_index == 'P':
                self.buildIndex('CREATE INDEX {s}_PK ON {s}({s})'.format(ref_pkey,dst_layer_name,ref_pkey))
            elif ref_index is not None:
                #maybe the user wants a non pk/spatial index? Try to filter the string
                clst = ','.join(self.parseStringList(ref_index))
                self.buildIndex('CREATE INDEX {s}_PK ON {s}({s})'.format(self.sanitise(clst),dst_layer_name,clst))
            
            src_layer.ResetReading()
            dst_layer.ResetReading()



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
        lmd = self.mlr.readLastModified(layer)
        if lmd is None or lmd == '':
            lmd = self.EARLIEST_INIT_DATE
        return lmd
        #return lm.strftime(self.DATE_FORMAT)
        
    def setLastModified(self,layer,newdate):
        '''Sets the last modification time of a layer following a successful incremental copy operation'''
        self.mlr.writeLastModified(layer, newdate)    

    def getCurrent(self,offset):
        '''Gets the current timestamp plus any required offset for incremental todate calls. 
        Offsets are expected in dict form with {day=D, hour=H, minute=M}. 
        This may be useful if you need to synchronise layer dates when being processed across a day boundary'''
        if offset is None:
            offset = {'day':0,'hour':0,'minute':0}
        dpo = datetime.now()+timedelta(days=offset['day'],hours=offset['hour'],minutes=offset['minute'])
        return dpo.strftime(self.DATE_FORMAT)
    
    def buildIndex(self,command):
        '''Creates an index creation string for a new full replicate'''
        ldslog.info('Attempting SQL '+command)
        if self.validateSQL(command):
            self.executeSQL(command)
    
    # private methods
        
    def _executeSQL(self,sql):
        '''Executes arbitrary SQL on the datasource'''
        '''Tagged private since we only want it called from well controlled methods'''
        try:
            self.ds.executeSQL()
        except:
            ldslog.warning("Unable to execute SQL:"+sql,exc_info=1)
                
            
    def _validateSQL(self,sql):
        '''Validates SQL against a list of allowed queries'''
        
        sql = sql.lower()
        #first match 'create index'
        if re.match('create index on',sql) or re.match('create spatial index on',sql):
            return True
        
        #second match 'drop index'
        if re.match('drop index on',sql) or re.match('drop spatial index on',sql):
            return True
        
        return False
        
    def _cleanLayer(self,layer):
        '''Deletes a layer from the DS'''
        ldslog.info("DS clean")
        self.ds.DeleteLayer(layer)
        
    def _clean(self):
        '''Deletes the entire DS layer by layer'''
        for li in range(0,self.ds.GetLayerCount()):
            self.cleanLayer(li)
    
    
    def _findMatchingFID(self,dst_layer,ref_pkey,key):
        '''Find the FID matching a primary key value'''
        qry = ref_pkey+" = "+str(key)
        dst_layer.SetAttributeFilter(qry)
        found_feat = dst_layer.GetNextFeature()
        if found_feat is not None:
            return found_feat.GetFID()
        return None
            
    
    # utility methods
    
    @classmethod
    def sanitise(self,name):
        '''Manually substitute potential table naming errors implemented as a common function to retain naming convention across all outputs.
        No guarantees are made that this feature won't cause naming conflicts e.g. A-B-C -> a_b_c <- a::{b}::c'''
        #append _ to name beginning with a number
        if re.match('\A\d',name):
            name = "_"+name
        #replace unwanted chars with _ and compress multiple and remove trailing
        sani = re.sub('_+','_',re.sub('[ \-,.\\\\/:;{}()\[\]]','_',name.lower())).rstrip('_')
        #unexpected name subst source of a few bugs, log as debug
        ldslog.debug("Sanitise:raw="+name+" name="+sani)
        return sani
    

    @classmethod
    def parseStringList(self,st):
        '''QaD List-as-String to List parser'''
        return st.rstrip(')]').lstrip('[(').split(',') if st.find(',')>-1 else st
    
    # debugging methods
    
    @classmethod
    def _showFeatureData(self,feature):
        '''Prints feature/fid info. Useful for debugging'''
        ldslog.debug("Feat:FID:"+str(feature.GetFID()))
        for field_no in range(0,feature.GetFieldCount()):
            ldslog.debug("fid={},fld_no={},fld_data={}".format(feature.GetFID(),field_no,feature.GetFieldAsString(field_no)))
            
    @classmethod
    def _showLayerData(self,layer):
        '''Prints layer and embedded feature data. Useful for debugging'''
        ldslog.debug("Layer:Name:"+layer.GetName())
        layer.ResetReading()
        feat = layer.GetNextFeature()
        while feat is not None:
            self._showFeatureData(feat)
            feat = layer.GetNextFeature()                
                

