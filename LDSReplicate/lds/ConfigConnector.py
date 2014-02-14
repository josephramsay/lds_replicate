'''
v.0.0.9

LDSReplicate -  ConfigConnector

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 13/02/2013

@author: jramsay
'''

import re

from lds.DataStore import DataStore, UnknownDSTypeException
from lds.LDSUtilities import LDSUtilities, ConfigInitialiser
from lds.VersionUtilities import AppVersion

from lds.FileGDBDataStore import FileGDBDataStore
from lds.PostgreSQLDataStore import PostgreSQLDataStore
from lds.MSSQLSpatialDataStore import MSSQLSpatialDataStore
from lds.SpatiaLiteDataStore import SpatiaLiteDataStore
from lds.WFSDataStore import WFSDataStore
from lds.LDSDataStore import LDSDataStore


class EndpointConnectionException(Exception): pass
class ConnectionConfigurationException(Exception): pass

ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()

HCOLS = 2

#Notes:
#MS and PG settings entered in these dialogs are saved to config only
#When a new FGDB directory is set in the file dialog using the NewFolder button a new directory is created and a reference added to the user config
#When a new SLITE file is created by entering its name in the SL file dialog, it isnt created but a reference to it is put in the user config file
       
class ConfigConnector(object):

    def __init__(self,parent,uconf,lgval,destname):
        #HACK. Since we can't init an lg list without first intialising read connections must assume lgval is stored in v:x format. 
        #NOTE. This a controlled call to TP and we can't assume in general that TP will be called with a v:x layer/group (since names are allowed)
        #NOTE. Every time a new CC is created we call LDS for a caps doc even though this is mostly invariant
        self.parent = parent
        self.vlayers = None     
        self.lgval = None
        self.uconf = None
        self.destname = None   
        self.reg = DatasourceRegister()
        self.initConnections(uconf,lgval,destname)
        
    def initConnections(self,uconf,lgval,destname): 
        from lds.TransferProcessor import TransferProcessor
        #write a refresh CC instead of re initialising the whole object, for use when the dest menu changes  
        #lgval isn't used in CC itself but the gui's use it for menu indexing      
        self.lgval = lgval
        #Optimisation. If the uconf or dest type are unchanged don't go through the expensive task of updating TP and the layer lists
        if uconf and destname and ((uconf,destname)!=(self.uconf,self.destname)):
            #lgpair = (LORG.LAYER if re.match('^v:x',lgval) else LORG.GROUP,lgval) if lgval else (None,None)
            self.uconf = uconf
            self.destname = destname
            self.tp = TransferProcessor(self,lgval, None, None, None, None, None, None, uconf)
            sep = self.reg.openEndPoint('WFS', self.uconf)    
            #svp = Service, Version, Prefix
            self.svp = self.readProtocolVersion(sep)
            dep = self.reg.openEndPoint(self.destname, self.uconf)
            self.setupLayerConfig(self.tp,sep,dep)
            #print 'CCt',self.tp
            #print 'CCd',self.dep
            if not self.vlayers:
                self.vlayers = self.getValidLayers(sep,dep)
                self.setupReserved()
            self.setupComplete(dep)
            self.setupAssigned()
            self.buildLGList()
            self.inclayers = [self.svp['idp']+x[0] for x in ConfigInitialiser.readCSV()]
            self.reg.closeEndPoint(self.destname)
            self.reg.closeEndPoint('WFS')
            sep,dep = None,None
        elif destname and not uconf:
            raise ConnectionConfigurationException('Missing configuration, "{}.conf"'.format(uconf))
        elif uconf and not destname:
            raise ConnectionConfigurationException('No driver/dest specified "{}"'.format(destname))
    
            
    def readProtocolVersion(self,src):
        '''Get WFS/WMS, version and prefix from the Source'''
        return {'svc':src.svc,'ver':src.ver,'idp':src.idp}
    
    #----------------------------------------------------------------------------------
        
    @staticmethod
    def initLayerConfig(tp,src,dst=None):
        '''Wraps call to TP initlayerconf resetting LC for the selected dst'''
        #print 'src',src,'src.gc',src.getCapabilities(),'dst',dst,'src.pxy',src.pxy,'src.idp',src.idp
        tp.initLayerConfig(src.getCapabilities(),dst,src.pxy,src.idp)
        
    @staticmethod
    def setupLayerConfig(tp,sep,dep):
        '''Calls the TP LC setup function'''
        lc = tp.getNewLayerConf(dep)
        dep.setLayerConf(lc)
        ##if a lconf has not been created build a new one
        if not dep.getLayerConf().existsAndIsCurrent():
            ConfigConnector.initLayerConfig(tp,sep,dep)
            
    @staticmethod
    def closeLayerConfig(ep):
        '''Attempts to release DS resources used by LayerConfig'''
        lc = ep.getLayerConf()
        if lc:
            if hasattr(lc,'ds'):
                lc.ds.SyncToDisk()
                lc.ds = None
            lc = None
            
    #----------------------------------------------------------------------------------
    
    def setupComplete(self,dep):
        '''Reads a reduced lconf from file/table as a Nx3 array'''
        #these are all the keywords in the local file. if no dest has been set returns empty
        self.complete = dep.getLayerConf().getLConfAs3Array() if dep else []
    
    def setupReserved(self):
        '''Read the capabilities doc (as json) for reserved words'''
        #these are all the keywords LDS currently knows about            
        self.reserved = set()
        for i in [l[2] for l in self.vlayers]:
        #for i in [l[3] for l in json.loads(self.tp.parseCapabilitiesDoc(self.src.getCapabilities(),'json',self.src.pxy))]:
            self.reserved.update(set(i))
            
    def setupAssigned(self):
        '''Read the complete config doc for all keywords and diff out reserved. Requires init of complete and reserved'''   
        #these are the difference between saved and LDS-known keywords therefore, the ones the user has saved or LDS has forgotten about 
        assigned = set() 
        for i in [x[2] for x in self.complete]:
            assigned.update(set(i))
        assigned.difference_update(self.reserved)
        
        self.assigned = self.deleteForgotten(assigned)
        
    def deleteForgotten(self,alist,flt='v_\w{8}_wxs_\w{4}'):
        '''Removes keywords with the format v_########_wxs_#### since these are probably not user generated and 
        because they're used as temporary keys and will eventually be forgotten by LDS'''
        #HACK
        return set([a for a in alist if not re.search(flt,a)])
    
    def getValidLayers(self,src,dst):
        #if dest is true we should have a layerconf so use intersect(read_lc,lds_getcaps)
        capabilities = src.getCapabilities()
        self.tp.initCapsDoc(capabilities, src)
        vlayers = self.tp.assembleLayerList(dst,intersect=True)
        #In the case where we are setting up unconfigured (first init) populate the layer list with default/all layers
        return vlayers if vlayers else self.tp.assembleLayerList(dst,intersect=False) 
    
    def buildLGList(self,groups=None,layers=None):
        '''Sets the values displayed in the Layer/Group combo'''
        from lds.TransferProcessor import LORG
        #self.lgcombo.addItems(('',TransferProcessor.LG_PREFIX['g']))
        self.lglist = []
        #              lorg,value,display
        for g in sorted(groups if groups else self.assigned):
            self.lglist += ((LORG.GROUP,g.strip(),'{} (group)'.format(g.strip())),)
        for l in sorted(layers if layers else self.vlayers):
            self.lglist += ((LORG.LAYER,l[0],'{} ({})'.format(l[1],l[0])),)
        
    def getLGEntry(self,dispval):
        '''Finds a matching group/layer entry from its displayed name'''
        return self.lglist[self.getLGIndex(dispval)]
    
    def getLGIndex(self,dispval,col=2):
        '''Finds a matching group/layer entry from its displayed name'''
        #0=lorg,1=value,2=display 
        if not LDSUtilities.mightAsWellBeNone(dispval):
            ldslog.warn('No attempt made to find index for empty group/layer request, "{}"'.format(dispval))
            return None# or 0?
            
        try:
            index = [i[col] for i in self.lglist].index(str(dispval))
        except ValueError as ve:
            ldslog.warn('Cannot find an index in column {} for the requested group/layer, "{}", from {} layers. Returning None index'.format(col,dispval,len(self.lglist)))
            index = None
        return index
    
      
    
class DatasourceRegister(object):
    '''Wraps a dict of OGR DS references controlling access, instantiation and destruction'''
    #{'name':(rc=Ref Count,type={SOURCE|TRANSIENT|DESTINATION},uri={http://url/|file://path/file.gdb},ds=<ds_object>, ...}
    register = {}
    #sopposed to work something like; 
    #SOURCE=check ref hasn't changed, if it has update the object
    #TRANSIENT=free DB locks by closing as soon as not needed (FileGDB, SQLite)
    #DESTINATION=normal rw access with object kept open
    TYPE = LDSUtilities.enum('SOURCE','TRANSIENT','DESTINATION')
    REQ = LDSUtilities.enum('INCR','FEAT','FULL')

    def __init__(self):
        pass
        
    def _register(self,fn,uri):
        '''Registers a new DS under the provided name overwriting any previous entries'''
        self._assignRef(uri)
        if fn:
            type = self._type(fn)
            if type == self.TYPE.SOURCE:
                endpoint = self._newSRC()
            else:
                endpoint = self._newDST(fn)
            self.register[fn] = {}
            self.register[fn]['rc'] = 0
            self.register[fn]['type'] = type
            self.register[fn]['uri'] = uri
            self.register[fn]['ep'] = endpoint
            
        else:
            raise UnknownDSTypeException('Unknown DS requested, '+str(fn))
        
    def _deregister(self,name):
        fn = LDSUtilities.standardiseDriverNames(name)
        #sync/rel the DS
        self.register[fn]['ep'] = None
        del self.register[fn]
    
    def _assignRef(self,uri):
        '''mark ref value as either user config or a connection string based on the assumption conn strings contain certain suff/prefixes...'''#TODO Test different sl/fg cases
        self.cs,self.uc = (uri,None) if uri and re.search('PG:|MSSQL:|\.gdb|\.sqlite|\.db',uri,flags=re.IGNORECASE) else (None,uri) 

    
    def _type(self,fn):
        #fn = LDSUtilities.standardiseDriverNames(name)
        if fn == FileGDBDataStore.DRIVER_NAME:
            return self.TYPE.TRANSIENT
        elif fn == WFSDataStore.DRIVER_NAME:
            return self.TYPE.SOURCE
        return self.TYPE.DESTINATION
        
    def _connect(self,fn,req=None):
        '''Initialise an OGR datasource on the DST/SRC object or just return note the existing and increment count'''
        endpoint = self.register[fn]['ep']
        if not endpoint.getDS():
            if self.register[fn]['type']==self.TYPE.SOURCE:
                self._connectSRC(fn,req)
            else:
                self._connectDST(fn)
        
    def _connectDST(self,fn):
        endpoint = self.register[fn]['ep']
        uri = endpoint.destinationURI(None)
        conn = endpoint.initDS(uri)
        endpoint.setDS(conn)
        self.register[fn]['rc'] += 1
        
    def _connectSRC(self,fn,req=None):#layername=None,fromdate=None,todate=None): 
        endpoint = self.register[fn]['ep']
        if req and req['type']==self.REQ.INCR and (req['fromdate'] or req['todate']):
            uri = endpoint.sourceURIIncremental(req['layername'],req['fromdate'],req['todate'])
        elif req and req['type'] == self.REQ.FEAT:
            uri = endpoint.sourceURIFeatureCount(req['layername'])
        elif req and req['type'] == self.REQ.FULL:
            uri = endpoint.sourceURI(req['layername'])
        else:
            pass
        #conn = ep.initDS(uri)
        self.register[fn]['rc'] += 1
        
    def _disconnect(self,fn):
        '''Decrement reference to the DS and delete it entirely if its the last one'''
        endpoint = self.register[fn]['ep']
        self.register[fn]['rc'] -= 1
        if self.register[fn]['rc'] <= 0:
            ldslog.info('Release EP '+str(fn))
            ConfigConnector.closeLayerConfig(endpoint)
            endpoint.closeDS()  
        
    #---------------------
        
    def refCount(self,fn):
        return self.register[fn]['rc']
    
    def openEndPoint(self,name,uri=None,req=None):
        #print 'GEP',name,uri,req
        '''Gets a named EP incrementing a refcount or registers a new one as needed'''
        fn = LDSUtilities.standardiseDriverNames(name)
        #if fn+uri exist AND fn is valid AND (fn not prev registered OR the saved URI != uri) 
        if (fn and uri)\
        and (fn in DataStore.DRIVER_NAMES.values() or fn == WFSDataStore.DRIVER_NAME) \
        and (not self.register.has_key(fn) or self.register[fn]['uri']!=uri):
            self._register(fn,uri)
            self._connect(fn, req)
        elif fn in self.register: 
            self._connect(fn,req)
        else:
            raise EndpointConnectionException('Register Entry {} has not been initialised.'.format(fn))
        return self.register[fn]['ep']
    
    def closeEndPoint(self,name):
        '''Closes the DS is a named EP or deletes the EP completely if not needed'''
        fn = LDSUtilities.standardiseDriverNames(name)
        if self.register.has_key(fn):
            self._disconnect(fn)   
            if self.register[fn]['rc'] == 0:
                self._deregister(fn)     
    
    def _newDST(self,fn):
        '''Init a new destination using instantiated uconf (and dest str if provided)'''
        dest = {PostgreSQLDataStore.DRIVER_NAME:PostgreSQLDataStore,
                MSSQLSpatialDataStore.DRIVER_NAME:MSSQLSpatialDataStore,
                SpatiaLiteDataStore.DRIVER_NAME:SpatiaLiteDataStore,
                FileGDBDataStore.DRIVER_NAME:FileGDBDataStore
                }.get(fn)
        return dest(self.cs,self.uc)

    def _newSRC(self):
        '''Initialise a new source, LDS nominally'''
        src = LDSDataStore(self.cs,self.uc) 
        #src.setPartitionSize(partition_size)#partitionsize may not exist when this is called but thats okay!
        src.applyConfigOptions()
        return src    
 
    def __str__(self):
        s = ''
        for k in self.register.keys():
            r = self.register[k]
            s += 'key='+k+': rc='+str(r['rc'])+' uri='+str(r['uri'])+': type='+str(r['type'])+': ep='+str(r['ep'])+'\n'
        return s

#import pydevd
#pydevd.settrace(suspend=False)
#import threading
#threading.settrace(pydevd.GetGlobalDebugger().trace_dispatch)

#from threading import Thread

from PyQt4.QtCore import QThread, Qt
from PyQt4 import QtCore

class ProcessRunner(QThread):
    
    status = QtCore.pyqtSignal(int,str,str)
    enable = QtCore.pyqtSignal(bool)
    
    sn = 'WFS'
    #NB PR gets called from the 'controls' widget and keeps this link to report status etc
    #it also pulls   
    def __init__(self,controls,dd):
        QThread.__init__(self)
        self.controls = controls
        #original TP
        self.tp = controls.parent.confconn.tp
        #self.tp = controls.parent.confconn.tp.clone()
        
        #connect to dsregister
        self.reg = controls.parent.confconn.reg
        self.dn = controls.parent.confconn.destname
        self.uc = controls.parent.confconn.uconf
        #self._repthr()
        
        #error notification
        self.status.connect(self.controls.setStatus, Qt.QueuedConnection)
        self.enable.connect(self.controls.mainWindowEnable, Qt.QueuedConnection)
        
    def run(self):
        pt = ProgressTimer(self.controls,self.tp)
        try:
            try:
                pt.start()
                self.enable.emit(False)
                sep = self.reg.openEndPoint(self.sn,self.uc)
                dep = self.reg.openEndPoint(self.dn,self.uc)
                ConfigConnector.setupLayerConfig(self.tp,sep,dep)
                self.tp.setSRC(sep)
                self.tp.setDST(dep)
                self.tp.processLDS()
            except Exception as e1:
                ldslog.error('Error running PT thread, '+str(e1))
                raise
            finally:
                #die progress counter
                self.enable.emit(True)
                self.reg.closeEndPoint(self.dn)
                self.reg.closeEndPoint(self.sn)
                pt.join()
                pt = None
        except Exception as e:
            self.status.emit(0,'Error. Halting Processing',str(e))
        finally:
            self.join()
        
        
    def join(self,timeout=None):
        #QThread.join(self,timeout)
        self.quit()
        
        
#can't imprt enum directly!? have to refer to them by index
#'ERROR'=0,'IDLE'=1,'BUSY'=2,'CLEAN'=3
#from lds.gui.LDSGUI.LDSControls import STATUS

class ProgressTimer(QThread):
    
    pgbar = QtCore.pyqtSignal(int)
    status = QtCore.pyqtSignal(int,str,str)
    
    def __init__(self,controls,tp):
        QThread.__init__(self)
        self.stopped = False
        self.controls = controls
        self.tp = tp#controls.parent.confconn.tp
        #                              cc     gui
        self.pgbar.connect(self.controls.progressbar.setValue, Qt.QueuedConnection)
        self.status.connect(self.controls.setStatus, Qt.QueuedConnection)
        self.pct = 0
        
    def run(self):
        while not self.stopped:
            self.poll()
            self.sleep(self.tp.POLL_INTERVAL)

    def poll(self):
        '''Calculate progress. Bypass if denominators are zero'''
        feat_part = 0
        layer_part = 0
        layer_name = ''
        if self.tp.layer_total:
            layer_part = 100*float(self.tp.layer_count)/float(self.tp.layer_total)
            if hasattr(self.tp.dst,'src_feat_count') and self.tp.dst.src_feat_count:
                dst_count = sum(self.tp.dst.change_count.values())
                feat_part = 100*float(dst_count)/(float(self.tp.dst.src_feat_count)*float(self.tp.layer_total))
        if hasattr(self.tp.dst,'dst_info'):
            layer_name = self.tp.dst.dst_info.layer_name
        #ldslog.debug('fc={}/{} lc={}/{}'.format(str(dst_count),str(self.tp.dst.src_feat_count),str(self.tp.layer_count),str(self.tp.layer_total)))
        #ldslog.debug('fp={} lp={}'.format(str(feat_part),str(layer_part)))

        if int(feat_part+layer_part)!=self.pct:
            self.pct = int(feat_part+layer_part)
            self.report(self.pct,layer_name)

        
    def report(self,pct,lyr=None):
        #    tp cc     repl   con
        ldslog.info('Progress: '+str(pct)+'%')
        self.pgbar.emit(pct)
        if lyr: self.status.emit(2,'Replicating Layer '+str(lyr),'')
        
    def join(self,timeout=None):
        #QThread.join(self,timeout)
        self.stopped = True
        self.status.emit(1,'Finished','')
        #self.tp.dst.closeDS()
        self.quit()