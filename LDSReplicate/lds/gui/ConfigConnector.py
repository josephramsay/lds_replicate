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

from lds.TransferProcessor import TransferProcessor, LORG
from lds.DataStore import DataStore
from lds.LDSUtilities import LDSUtilities, ConfigInitialiser
from lds.VersionUtilities import AppVersion

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
        self.initConnections(uconf,lgval,destname)
        
    def initConnections(self,uconf,lgval,destname):
        #write a refresh CC instead of re initialising the whole object, for use when the dest menu changes  
        #lgval isn't used in CC itself but the gui's use it for menu indexing      
        self.lgval = lgval
        #Optimisation. If the uconf or dest type are unchanged don't go through the expensive task of updating TP and the layer lists
        if ((uconf,destname)!=(self.uconf,self.destname)):
            #lgpair = (LORG.LAYER if re.match('^v:x',lgval) else LORG.GROUP,lgval) if lgval else (None,None)
            self.uconf = uconf
            self.destname = destname
            self.tp = TransferProcessor(self,lgval, None, None, None, None, None, None, uconf)
            self.src = ConfigConnector.initSrc(self.tp)
            self.dst = ConfigConnector.initDst(self.tp,self.destname)
            #print 'CCt',self.tp
            #print 'CCd',self.dst
            if not self.vlayers:
                self.vlayers = self.getValidLayers(self.dst is not None)
                self.setupReserved()
            self.setupComplete()
            self.setupAssigned()
            self.buildLGList()
            self.inclayers = ['v:x'+x[0] for x in ConfigInitialiser.readCSV()]
  
    @staticmethod
    def initSrc(tp):
        '''aesthetics!?'''
        return tp.src
    
    @staticmethod
    def initDst(tp,destname):
        '''Initialises dst objects'''
        #initialise layer data using existing source otherwise use the capabilities doc
        #if doing a first run there is/may-be no destname
        if destname:
            dst = tp.initDestination(destname)
            #if internal lconf, need to init the DB
            if dst.getConfInternal() == DataStore.CONF_INT:
                dst.ds = dst.initDS(dst.destinationURI(None))
            dst.setLayerConf(tp.getNewLayerConf(dst))
            ##if a lconf has not been created build a new one
            if not dst.getLayerConf().exists():
                #self.tp.initLayerConfig(self.tp.src.getCapabilities(),dst,self.tp.src.pxy)
                ConfigConnector.initLayerConfig(tp,dst)
        else:
            dst = None
        return dst
    
    @staticmethod
    def initLayerConfig(tp,dst=None):
        '''Wraps call to TP initlayerconf resetting LC for the selected dst'''
        #self.tp.initLayerConfig(self.tp.src.getCapabilities(),dst if dst else self.dst,self.tp.src.pxy)
        tp.initLayerConfig(tp.src.getCapabilities(),dst,tp.src.pxy)
        
    def setupComplete(self):
        '''Reads a reduced lconf from file/table as a Nx3 array'''
        #these are all the keywords in the local file. if no dest has been set returns empty
        self.complete = self.dst.getLayerConf().getLConfAs3Array() if self.dst else []
    
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
    
    def getValidLayers(self,dest=True):
        #if dest is true we should have a layerconf so use intersect(read_lc,lds_getcaps)
        capabilities = self.src.getCapabilities()
        self.tp.initCapsDoc(capabilities, self.src)
        vlayers = self.tp.assembleLayerList(self.dst,intersect=dest)
        #In the case where we are setting up unconfigured (first init) populate the layer list with default/all layers
        return vlayers if vlayers else self.tp.assembleLayerList(self.dst,intersect=False) 
    
    def buildLGList(self,groups=None,layers=None):
        '''Sets the values displayed in the Layer/Group combo'''
        #self.lgcombo.addItems(('',TransferProcessor.LG_PREFIX['g']))
        self.lglist = []
        #              lorg,value,display
        for g in sorted(groups if groups else self.assigned):
            self.lglist += ((LORG.GROUP,g.strip(),'{} (group)'.format(g.strip())),)
        for l in sorted(layers if layers else self.vlayers):
            self.lglist += ((LORG.LAYER,l[0],'{} ({})'.format(l[1],l[0])),)
        
    
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
    
    def __init__(self,controls,dd):
        QThread.__init__(self)
        self.controls = controls
        #original TP
        self.tp = controls.parent.confconn.tp
        #self.tp = controls.parent.confconn.tp.clone()
        
        #original DST
        self.dst = controls.parent.confconn.dst
        #self.dst = controls.parent.confconn.dst.clone()
        #self.dst = ConfigConnector.initDst(self.tp,dd)
        #print 'PRt',self.tp
        #print 'PRd',self.dst
        
        #error notification
        self.status.connect(self.controls.setStatus, Qt.QueuedConnection)
        self.enable.connect(self.controls.mainWindowEnable, Qt.QueuedConnection)
        
    def run(self):
        pt = ProgressTimer(self.controls,self.tp)
        try:
            try:
                pt.start()
                self.enable.emit(False)
                self.tp.processLDS(self.dst)
            except Exception as e1:
                ldslog.error('Error running PT thread, '+str(e1))
                raise
            finally:
                #die progress counter
                self.enable.emit(True)
                pt.join()
                pt = None
        except Exception as e:
            self.status.emit(0,'Error. Halting Processing',str(e))
        
        self.quit()
        
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
                feat_part = 100*float(self.tp.dst.dst_change_count)/(float(self.tp.dst.src_feat_count)*float(self.tp.layer_total))
        if hasattr(self.tp.dst,'dst_info'):
            layer_name = self.tp.dst.dst_info.layer_name
        #ldslog.debug('fc={}/{} lc={}/{}'.format(str(self.tp.dst.dst_change_count),str(self.tp.dst.src_feat_count),str(self.tp.layer_count),str(self.tp.layer_total)))
        #ldslog.debug('fp={} lp={}'.format(str(feat_part),str(layer_part)))
        self.report(int(feat_part+layer_part),layer_name)

        
    def report(self,pct,lyr=None):
        #    tp cc     repl   con
        self.pgbar.emit(pct)
        if lyr: self.status.emit(2,'Replicating Layer '+str(lyr),'')
        
    def join(self,timeout=None):
        #QThread.join(self,timeout)
        self.stopped = True
        self.status.emit(1,'Finished','')
        
        self.quit()