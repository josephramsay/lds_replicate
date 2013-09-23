'''
v.0.0.9

LDSReplicate -  LayerConfigSelector

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 13/02/2013

@author: jramsay
'''

from PyQt4.QtGui import (QApplication, QLabel, QComboBox,
                         QVBoxLayout, QHBoxLayout, QGridLayout,QAbstractItemView,
                         QSizePolicy,QSortFilterProxyModel,
                         QMainWindow, QFrame, QStandardItemModel, 
                         QLineEdit,QToolTip, QFont, QHeaderView, 
                         QPushButton, QTableView,QMessageBox)
from PyQt4.QtCore import (Qt, QAbstractTableModel, QVariant)

import re
import sys
import copy

from lds.TransferProcessor import LORG
from lds.LDSUtilities import LDSUtilities
from lds.VersionUtilities import AppVersion


ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()

HCOLS = 2

#Notes:
#MS and PG settings entered in these dialogs are saved to config only
#When a new FGDB directory is set in the file dialog using the NewFolder button a new directory is created and a reference added to the user config
#When a new SLITE file is created by entering its name in the SL file dialog, it isnt created but a reference to it is put in the user config file
       

class LayerConfigSelector(QMainWindow):
    STEP = LDSUtilities.enum('PRE','POST')

    #def __init__(self,tp,uconf,group,dest='PostgreSQL',parent=None):
    def __init__(self,parent=None):
        '''Main entry point for the Layer selection dialog'''
        super(LayerConfigSelector, self).__init__(parent)
        
        self.parent = parent
        
        #Build models splitting by keyword if necessary 
        av_sl = self.splitData(str(self.parent.confconn.lgval),self.parent.confconn.complete)
        
        self.available_model = LayerTableModel('L::available',self)
        self.available_model.initData(av_sl[0],self.parent.confconn.inclayers)
        
        self.selection_model = LayerTableModel('R::selection',self)
        self.selection_model.initData(av_sl[1],self.parent.confconn.inclayers)
        
        self.page = LayerSelectionPage(self)
        self.setCentralWidget(self.page)

        self.setWindowTitle("LDS Layer Selection")
        self.resize(725,480)

        
    def resetLayers(self):
        '''Rebuilds lconf from scratch'''
        from lds.ConfigConnector import ConfigConnector
        #ConfigConnector.initLayerConfigWrapper(self.parent.confconn.tp,self.parent.confconn.dst)
        #self.parent.confconn.initLayerConfigWrapper()
        self.parent.confconn.initLayerConfig(self.parent.confconn.tp,self.parent.confconn.dst)
        self.refreshLayers()
        
    def refreshLayers(self,customkey=None):
        '''Refreshes from a reread of the lconf object'''
        self.parent.confconn.setupComplete()
        
        av_sl = self.splitData(customkey,self.parent.confconn.complete)
        self.signalModels(self.STEP.PRE)
        self.available_model.initData(av_sl[0],self.parent.confconn.inclayers)
        self.selection_model.initData(av_sl[1],self.parent.confconn.inclayers)
        self.signalModels(self.STEP.POST)
        
    def writeKeysToLayerConfig(self, customkey):
        '''Add custom key to the selection_model list of layers (assumes required av->sl transfer completed) not just the transferring entry'''
        layerlist = [ll[0] for ll in self.selection_model.mdata]
        replacementlist = ()
        ep = self.parent.confconn.reg.getEndPoint(self.parent.confconn.destname,self.parent.confconn.uconf)
        self.parent.confconn.setupLayerConfig(ep)
        #dst = self.parent.confconn.initDstWrapper()
        categorylist = ep.getLayerConf().readLayerProperty(layerlist, 'category')
        for cat in categorylist:
            replacementlist += (cat if re.search(customkey,cat) else cat+","+str(customkey),)
        ep.getLayerConf().writeLayerProperty(layerlist, 'category', replacementlist)
        #new keyword written so re-read complete (LC) and update assigned keys list
        self.parent.confconn.setupComplete(ep)
        self.parent.confconn.setupAssigned()
        self.parent.confconn.buildLGList()
        #self.refreshLayers(customkey)
        self.parent.confconn.reg.closeEndPoint(self.parent.confconn.destname)
        ep = None
    
    def deleteKeysFromLayerConfig(self,layerlist,customkey):
        replacementlist = ()
        ep = self.parent.confconn.reg.getEndPoint(self.parent.confconn.destname,self.parent.confconn.uconf)
        self.parent.confconn.setupLayerConfig(ep)
        categorylist = ep.getLayerConf().readLayerProperty(layerlist, 'category')
        for cat in categorylist:
            replacementlist += (re.sub(',+',',',''.join(cat.split(str(customkey))).strip(',')),)    
        ep.getLayerConf().writeLayerProperty(layerlist, 'category', replacementlist)
        self.parent.confconn.reg.closeEndPoint()
        #-----------------------------------
        self.parent.confconn.setupComplete()
        self.parent.confconn.setupAssigned()
        self.parent.confconn.buildLGList()   
        #self.refreshLayers(customkey)
        self.parent.confconn.reg.closeEndPoint(self.parent.confconn.destname)
        ep = None
        return self.selection_model.rowCount()
    
    @staticmethod
    def splitData(keyword,complete):
        '''Splits up the 'complete' layer list according to whether it has the selection keyword or not'''
        alist = []
        slist = []
        assert complete
        for dp in complete:
            if keyword in dp[2]:
                slist.append(dp)
            else:
                alist.append(dp)
        return alist,slist
    
    def signalModels(self,prepost):
        '''Convenience method to call the Layout Change signals when models are modified'''
        if prepost==self.STEP.PRE:        
            self.available_model.layoutAboutToBeChanged.emit()
            self.selection_model.layoutAboutToBeChanged.emit()
        elif prepost==self.STEP.POST:
            self.available_model.layoutChanged.emit()    
            self.selection_model.layoutChanged.emit()
        
    def closeEvent(self,event):
        '''Intercept close event to signal parent to update status'''
        self.parent.controls.setStatus(self.parent.controls.STATUS.IDLE,'Done')
        #return last group selection
        lastgroup = str(self.page.keywordcombo.lineEdit().text())
        #self.parent.controls.gpr.writeline('lgvalue',lastgroup)
        if LDSUtilities.mightAsWellBeNone(lastgroup) is not None:
            ep = self.parent.confconn.reg.getEndPoint(self.parent.confconn.destname,self.parent.confconn.uconf)
            self.parent.confconn.setupLayerConfig(ep)
            self.parent.confconn.setupComplete(ep)
            self.parent.confconn.setupAssigned()
            self.parent.confconn.buildLGList()
            lgindex = self.parent.confconn.getLGIndex(lastgroup,col=1)
            self.parent.controls.refreshLGCombo()
            self.parent.controls.lgcombo.setCurrentIndex(lgindex)
            self.parent.confconn.reg.closeEndPoint(self.parent.confconn.destname)
            ep = None

        ##super(LayerConfigSelector,self).closeEvent(event)
        #self.close()

    
class LayerTableModel(QAbstractTableModel):
    #NB. There dont need to be any row/col inserts but will need to add keyword (selecting a layer  = adding user-custom tag)
    #Data table is in the form
    #Name   |Title       |Keywords
    #-------+------------+--------
    #v:xNNNN|Topo Layer X|Topo,NZ,custom

    
    def __init__(self, name='',parent=None):    
        super(LayerTableModel, self).__init__(parent)
        self.parent = parent
        self.name = name
        self.mdata = []
        self.ilist = []
        self.ifont = QFont()
        self.ifont.setBold(True)
        
        #convenience link
        #self.confconn_link = self.parent.parent.confconn
        
        
    #abstract subclass funcs
    def rowCount(self, parent=None):
        return len(self.mdata)
    
    def columnCount(self, parent=None):
        return HCOLS#len(self.mdata[0])
    

    def data(self,index=None,role=None): 
        #print role
        ri = index.row()
        ci = index.column()
        if (role == Qt.DisplayRole):
            if ci==2:
                try:
                    d = '; '.join(self.mdata[ri][ci])
                    ldslog.debug(self.name,'r=',ri,'c=',ci,'mdata=',d)
                except Exception as e:
                    raise
                    #ldslog.error('Data fetch error on layer config at '+str(ri)+','+str(ci),e)
                return d
            try:
                d = self.mdata[ri][ci]
                #ldslog.debug(self.name,'r=',ri,'c=',ci,'mdata=',d)
            except Exception as e:
                ldslog.error('Data fetch error on layer config at '+str(ri)+','+str(ci),e)
            return self.mdata[ri][ci]
        if (role == Qt.FontRole):
            if self.mdata[ri][0] in self.ilist:
                return self.ifont
        return QVariant()        
    
    #editable datamodel subclass funcs
    
    def initData(self,mdata,ilist=None):
        self.mdata = copy.copy(mdata)
        if ilist is not None:
            self.ilist = ilist
        
    def addData(self,additions):
        point = len(self.mdata)#append to end
        index = self.createIndex(point, 0)
        self.layoutAboutToBeChanged.emit()
        self.beginInsertRows(index,point,point+len(additions)-1)
        for row in additions:
            self.mdata.append(row)
        
        self.endInsertRows()    
        self.layoutChanged.emit()
        
    def delData(self,indices):
        '''beginremoverows requires start and end markers which is fine for contiguous data but to meet this requirement 
        for non-contiguous data we have to split the data into contiguous blocks. This presents a problem since indices
        following a deletion become invalid
        1. call endremoverows only following all delete operations
        2. sort the indices and run deletions from tail-to-head'''
        #method 1
        removed = []
        for index in indices:
            point = index.row()
            self.layoutAboutToBeChanged.emit()
            self.beginRemoveRows(index,point,point)
            datarow = self.mdata[point]
            removed.append(datarow)
        for removal in removed:
            self.mdata.remove(removal)
        self.endRemoveRows()
        self.layoutChanged.emit()
        return removed
    
    def getData(self,index):
        point = index.row()
        datarow = self.mdata[point]
        return datarow
        
    def flags(self,index=None):
        return Qt.ItemIsSelectable | Qt.ItemIsEnabled
    
    
        
class LayerSelectionPage(QFrame):
    #TODO. Filtering, (visible) row selection, multi selection
    colparams = ((0,65,'Name'), (1,235,'Title'), (2,350,'Keywords'))
    XFER_BW = 40
    def __init__(self, parent=None):
        super(LayerSelectionPage, self).__init__(parent)
        self.parent = parent
        
        #convenience link
        self.confconn_link = self.parent.parent.confconn
        
        #flag top prevent read read action on keyword delete. New logic makes this redundant
        #self.keywordbypass = False

        QToolTip.setFont(QFont('SansSerif', 10))
        
        #label
        filterlabel = QLabel('Filter')
        availablelabel = QLabel('Available Layers')
        selectionlabel = QLabel('Layer Selections')
        keywordlabel = QLabel('Keyword')
        explainlabel = QLabel("Edit Group assignments using this dialog or to simply initialise the Layer-Config just click 'Finish'")
        
        #selection buttons
        chooseallbutton = QPushButton('>>')
        chooseallbutton.setFixedWidth(self.XFER_BW)
        chooseallbutton.clicked.connect(self.doChooseAllClickAction)
        
        choosebutton = QPushButton('>')
        choosebutton.setFixedWidth(self.XFER_BW)
        choosebutton.clicked.connect(self.doChooseClickAction)
        
        rejectbutton = QPushButton('<')
        rejectbutton.setFixedWidth(self.XFER_BW)
        rejectbutton.clicked.connect(self.doRejectClickAction)
        
        rejectallbutton = QPushButton('<<')
        rejectallbutton.setFixedWidth(self.XFER_BW)
        rejectallbutton.clicked.connect(self.doRejectAllClickAction)
        
        #operation buttons        
        finishbutton = QPushButton('Finish')
        finishbutton.setToolTip('Finish and Close layer selection dialog')
        finishbutton.clicked.connect(self.parent.close)
        
        resetbutton = QPushButton('Reset')
        resetbutton.font()
        resetbutton.setToolTip('Read Layer from LDS GetCapabilities request. Overwrites current Layer Config')       
        resetbutton.clicked.connect(self.doResetClickAction)
        
        self.available_sfpm = LDSSFPAvailableModel(self)
        self.selection_sfpm = LDSSFPSelectionModel(self)
        
        self.available_sfpm.setSourceModel(self.parent.available_model)
        self.selection_sfpm.setSourceModel(self.parent.selection_model)
        
        #textedits
        filteredit = QLineEdit('')
        filteredit.setToolTip('Filter Available-Layers pane (filter operates across Name and Title fields and accepts Regex expressions)')       
        filteredit.textChanged.connect(self.available_sfpm.setActiveFilter)
        
        self.keywordcombo = QComboBox()
        self.keywordcombo.setToolTip('Select or Add a unique identifier to be saved in layer config (keyword)')
        self.keywordcombo.addItems(list(self.confconn_link.assigned))
        self.keywordcombo.setEditable(True)
        self.keywordcombo.activated.connect(self.doKeyComboChangeAction)
        
        lgindex = self.confconn_link.getLGIndex(self.confconn_link.lgval,col=1)
        lgentry = self.confconn_link.lglist[lgindex] if LDSUtilities.mightAsWellBeNone(lgindex) is not None else None
        keywordedit = self.keywordcombo.lineEdit()
        #if no entry or layer indicated then blank 
        keywordedit.setText('' if lgentry is None or lgentry[0]==LORG.LAYER else lgentry[1])#self.confconn_link.lgval)#TODO. group only
        
        #header
        headmodel = QStandardItemModel()
        headmodel.setHorizontalHeaderLabels([i[2] for i in self.colparams][:self.parent.available_model.columnCount()])
        
        headview1 = QHeaderView(Qt.Horizontal)
        headview1.setModel(headmodel)
        headview1.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed) 
        
        headview2 = QHeaderView(Qt.Horizontal)
        headview2.setModel(headmodel)
        headview2.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)            

        #table
        self.available = QTableView()
        self.available.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.available.setSelectionMode(QAbstractItemView.MultiSelection)       
        
        self.selection = QTableView()
        self.selection.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.selection.setSelectionMode(QAbstractItemView.MultiSelection)
        
        #interesting, must set model after selection attributes but before headers else row selections/headers don't work properly
        self.available.setModel(self.available_sfpm)
        self.selection.setModel(self.selection_sfpm)
        
        self.available.setSortingEnabled(True)
        self.available.setHorizontalHeader(headview1)
        
        self.selection.setSortingEnabled(True)
        self.selection.setHorizontalHeader(headview2)

        for cp in self.colparams:
            self.available.setColumnWidth(cp[0],cp[1])
            self.selection.setColumnWidth(cp[0],cp[1])

        self.available.verticalHeader().setVisible(False)
        self.available.horizontalHeader().setVisible(True)
        
        self.selection.verticalHeader().setVisible(False)
        self.selection.horizontalHeader().setVisible(True)
        
        
        #layout  
        vbox00 = QVBoxLayout()
        vbox00.addWidget(availablelabel)
        vbox00.addWidget(self.available)
        
        vbox01 = QVBoxLayout()
        vbox01.addWidget(chooseallbutton)
        vbox01.addWidget(choosebutton)
        vbox01.addWidget(rejectbutton)
        vbox01.addWidget(rejectallbutton)
        
        vbox02 = QVBoxLayout()
        vbox02.addWidget(selectionlabel)
        vbox02.addWidget(self.selection)

        
        vbox10 = QVBoxLayout()
        vbox10.addWidget(filterlabel)
        vbox10.addWidget(filteredit)
        
        hbox12 = QHBoxLayout()
        hbox12.addWidget(keywordlabel)
        hbox12.addStretch(1)
        #hbox12.addWidget(inspbutton)
        #hbox12.addWidget(addbutton)
        #hbox12.addWidget(delbutton)
        
        vbox12 = QVBoxLayout()
        vbox12.addLayout(hbox12)
        vbox12.addWidget(self.keywordcombo)
                
        #00|01|02
        #10|11|12
        grid0 = QGridLayout()
        grid0.addLayout(vbox00,1,0)
        grid0.addLayout(vbox01,1,1)
        grid0.addLayout(vbox02,1,2)
        grid0.addLayout(vbox10,0,0)
        grid0.addLayout(vbox12,0,2)
        
        
        hbox2 = QHBoxLayout()
        hbox2.addWidget(resetbutton)
        hbox2.addStretch(1)
        hbox2.addWidget(explainlabel)
        hbox2.addWidget(finishbutton)
        #gbox1.setLayout(hbox2)
        
        
        
        vbox3 = QVBoxLayout()
        vbox3.addLayout(grid0)
        #vbox3.addLayout(hbox3)
        #vbox3.addWidget(line0)
        vbox3.addLayout(hbox2)
        
        self.setLayout(vbox3)


            
    def doChooseAllClickAction(self):
        '''Moves the lot to Selected'''
        ktext = str(self.keywordcombo.lineEdit().text())
        if not self.checkKeyword(ktext): return
        #------------------------------
        self.parent.signalModels(self.parent.STEP.PRE)
        #self.parent.selection_model.mdata += self.parent.available_model.mdata
        self.parent.selection_model.initData(self.confconn_link.complete)
        self.parent.available_model.initData([])
        self.parent.signalModels(self.parent.STEP.POST)
        #------------------------------
        self.parent.writeKeysToLayerConfig(ktext)
        #self.confconn_link.setupAssigned()
        if self.keywordcombo.findText(ktext) == -1:
            self.keywordcombo.addItem(ktext)
    
    def doChooseClickAction(self):
        '''Takes available selected and moves to selection'''
        ktext = str(self.keywordcombo.lineEdit().text())
        if not self.checkKeyword(ktext): return
        #------------------------------
        select = self.available.selectionModel()
        if select.hasSelection():
            self.transferSelectedRows(select.selectedRows(),self.available_sfpm,self.selection_sfpm)
            #------------------------------
            self.parent.writeKeysToLayerConfig(ktext)
            #self.confconn_link.assigned = self.confconn_link.setupAssigned()
            # -1 to indicate no index since 0,1,... are valid
            if self.keywordcombo.findText(ktext) == -1:
                self.keywordcombo.addItem(ktext)
        else:
            ldslog.warn('L2R > Transfer action without selection')
        self.available.clearSelection()
          
        
    def transferSelectedRows(self,indices,from_model,to_model):
        tlist = []
        for proxymodelindex in indices:
            transfer = from_model.getData(proxymodelindex)
            tlist.append((proxymodelindex,transfer),)

        to_model.addData([t[1] for t in tlist])
        from_model.delData([t[0] for t in tlist])
        return tlist
            
    def doRejectClickAction(self):
        '''Takes available selected and moves to selection'''
        ktext = str(self.keywordcombo.lineEdit().text())
        if not self.checkKeyword(ktext): return
        #------------------------------
        select = self.selection.selectionModel()
        if select.hasSelection():
            tlist = self.transferSelectedRows(select.selectedRows(),self.selection_sfpm,self.available_sfpm)
            #------------------------------
            kindex = self.keywordcombo.findText(ktext)
            remainder = self.parent.deleteKeysFromLayerConfig([ll[1][0] for ll in tlist],ktext)
            if remainder > 0 and kindex == -1:
                #items+newkey -> add
                self.parent.writeKeysToLayerConfig(ktext)
                self.keywordcombo.addItem(ktext)
            elif remainder == 0 and kindex > -1:
                #empty+oldkey -> del
                self.keywordcombo.removeItem(kindex)
                self.keywordcombo.clearEditText()
        else:
            ldslog.warn('R2L < Transfer action without selection')
        self.selection.clearSelection()
                
    def doRejectAllClickAction(self):
        ktext = str(self.keywordcombo.lineEdit().text())
        if not self.checkKeyword(ktext): return
        #------------------------------
        self.parent.deleteKeysFromLayerConfig([ll[0] for ll in self.parent.selection_model.mdata],ktext)
        #------------------------------
        self.parent.signalModels(self.parent.STEP.PRE)
        #self.parent.available_model.mdata += self.parent.selection_model.mdata
        self.parent.available_model.initData(self.confconn_link.complete)
        self.parent.selection_model.initData([])
        self.parent.signalModels(self.parent.STEP.POST)        
        #------------------------------
        #self.confconn_link.setupAssigned()
        #self.keywordbypass = True
        self.keywordcombo.removeItem(self.keywordcombo.findText(ktext))
        self.keywordcombo.clearEditText()
        
    def doKeyComboChangeAction(self):
        '''Reset the available pane and if there is anything in the keyword box use this to init the selection pane'''
        #HACK
        #if self.keywordbypass:
        #    self.keywordbypass = False
        #    return
        #------------------------------
        ktext = str(self.keywordcombo.lineEdit().text())
        #------------------------------
        av_sl = self.parent.splitData(ktext,self.confconn_link.complete)
        self.parent.signalModels(self.parent.STEP.PRE)
        self.parent.available_model.initData(av_sl[0])
        self.parent.selection_model.initData(av_sl[1])
        self.parent.signalModels(self.parent.STEP.POST)
    
    def doResetClickAction(self):
        '''Dumps the LC and rebuilds from a fresh read of the caps doc'''
        #int warning (QWidget parent, QString title, QString text, QString button0Text, QString button1Text = QString(), QString button2Text = QString(), int defaultButtonNumber = 0, int escapeButtonNumber = -1)
        ans = QMessageBox.warning(self, "Reset","This action will overwrite your Layer Configuration using the current LDS settings (potentially adding new or removing layers). Continue?","Continue","Cancel")
        if ans:
            #Cancel
            ldslog.warn('Cancelling Reset operation')
            return
        #Continue
        ldslog.warn('Reset Layer Config')
        self.parent.resetLayers()
        self.keywordcombo.clear()

    def checkKeyword(self,ktext):
        '''Checks keyword isn't null and isn't part of the LDS supplied keywords'''
        if LDSUtilities.mightAsWellBeNone(ktext) is None:
            QMessageBox.about(self, "Keyword Required","Please enter a Keyword to assign Layer(s) to")
            return False
        if ktext in self.confconn_link.reserved:
            QMessageBox.about(self, "Reserved Keyword","'{}' is a reserved keyword, please select again".format(ktext))
            return False
        return True
    
        
class LDSSortFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super(LDSSortFilterProxyModel, self).__init__(parent)
        self.parent = parent
        self.ftext = ''
        self.regexfilter = None
        
    def toggleFilter(self):
        self.direction = not self.direction
        
    def setActiveFilter(self,text):
        self.ftext = str(text)
        self.invalidateFilter()
        
    def addData(self,sourcedatalist):
        self.sourceModel().addData(sourcedatalist)
        
    def delData(self,proxyindexlist):
        sourceindexlist = self.translate(proxyindexlist)
        sourcedatalist = self.sourceModel().delData(sourceindexlist)
        return sourcedatalist
    
    def translate(self,pil):
        return [self.mapToSource(pi) for pi in pil]
        
    def getData(self,proxyindex):
        sourceindex = self.mapToSource(proxyindex)
        return self.sourceModel().getData(sourceindex)
    
    def filterAcceptsRow(self,row,parent):
        '''Override for row filter function'''
        for i in range(0,self.sourceModel().columnCount()):
            field = self.sourceModel().data(self.sourceModel().index(row, i, parent),Qt.DisplayRole)
            #print 'field="',field,'"    search_string="',self.ftext,'"'
            if re.search(self.ftext,field,re.IGNORECASE):
                return self.direction
        return not self.direction        
    

class LDSSFPSelectionModel(LDSSortFilterProxyModel):
    '''Selection model for selected layers, initialised with all layers but filters on keyword'''
    def __init__(self, parent=None):
        super(LDSSFPSelectionModel, self).__init__(parent)
        self.parent = parent
        self.ftext = ''
        self.regexfilter = None
        self.direction = True # True is 'normal' direction
        
    def setActiveFilter(self,text):
        self.ftext = str(text)
        self.invalidateFilter()
    
#enable this is we decide to also use the keyword box to filter results (problematic for invalid keywords, new or wrong)
#    def filterAcceptsRow(self,row,parent):
#        '''Override for row filter function, filters on keyword data'''      
#        
#        #for i in range(0,self.sourceModel().columnCount()):
#        keyfield = self.sourceModel().data(self.sourceModel().index(row, 2, parent),Qt.DisplayRole)
#        
#        for key in keyfield:
#            print 'S-SELECT :: field="',key,'"    search_string="',self.ftext,'"'
#            if re.search(self.ftext,key,re.IGNORECASE):
#                return self.direction
#        return not self.direction  
    
class LDSSFPAvailableModel(LDSSortFilterProxyModel):
    def __init__(self, parent=None):
        super(LDSSFPAvailableModel, self).__init__(parent)
        self.parent = parent
        self.ftext = ''
        self.regexfilter = None
        self.direction = True # True is 'normal' direction

    def setActiveFilter(self,text):
        self.ftext = str(text)
        self.invalidateFilter()
    
    def filterAcceptsRow(self,row,parent):
        '''Override for row filter function, filter from filteredit box'''
        for i in range(0,self.sourceModel().columnCount()):
            field = self.sourceModel().data(self.sourceModel().index(row, i, parent),Qt.DisplayRole)
            #print 'field="',field,'"    search_string="',self.ftext,'"'
            if re.search(self.ftext,field,re.IGNORECASE):
                return self.direction
        return not self.direction     
        
    
def main():
    #when called from the CL need to init main UI
    from lds.gui.LDSGUI import LDSMain
    #func to call config wizz
    app = QApplication(sys.argv)

    ldsc = LayerConfigSelector(LDSMain())
    ldsc.show()
    sys.exit(app.exec_()) 
    
    
    
if __name__ == '__main__':
    main()