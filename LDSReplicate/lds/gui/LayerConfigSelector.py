'''
v.0.0.1

LDSReplicate -  ldsrepl

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 13/02/2013

@author: jramsay
'''

from PyQt4.QtGui import (QApplication, QLabel, 
                         QVBoxLayout, QHBoxLayout, QGridLayout,QAbstractItemView,
                         QSizePolicy,QSortFilterProxyModel,
                         QMainWindow, QFrame, QStandardItemModel, 
                         QLineEdit,QToolTip, QFont, QHeaderView, 
                         QPushButton, QTableView,QMessageBox)
from PyQt4.QtCore import (Qt, QCoreApplication, QAbstractTableModel, QVariant)



import os
import re
import sys
import copy
import json

from lds.LDSDataStore import LDSDataStore
from lds.LDSUtilities import LDSUtilities, ConfigInitialiser
from lds.VersionUtilities import AppVersion


ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()

HCOLS = 2

#Notes:
#MS and PG settings entered in these dialogs are saved to config only
#When a new FGDB directory is set in the file dialog using the NewFolder button a new directory is created and a reference added to the user config
#When a new SLITE file is created by entering its name in the SL file dialog, it isnt created but a reference to it is put in the user config file
       
class LayerConfigSelector(QMainWindow):
    
    testdata = [('v:x845', '12 Mile Territorial Sea Limit Basepoints', ['New Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries','TESTSELECT']), 
                ('v:x846', '12 Mile Territorial Sea Outer Limit', ['New Zealand', 'Hydrographic &  Maritime', 'Maritime Boundaries']), 
                ('v:x842', '200 Mile Exclusive Economic Zone Outer Limits', ['New  Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries']), 
                ('v:x844', '24 Mile Contiguous Zone  Basepoints', ['New Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries']), 
                ('v:x843', '24 Mile  Contiguous Zone Outer Limits', ['New Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries']), 
                ('v:x1198', 'ASP: Check Combination', ['New Zealand', 'Roads and Addresses', 'Street and Places Index','TESTSELECT']), 
                ('v:x1199', 'ASP: GED Codes', ['New Zealand', 'Roads and Addresses', 'Street and Places Index']), 
                ('v:x1202', 'ASP:  MED Codes', ['New Zealand', 'Roads and Addresses', 'Street and Places Index','TESTSELECT'])] 

    def __init__(self,tp,uconf,group,dest='PostgreSQL',parent=None):
        '''Main entry point for the Layer selection dialog'''
        super(LayerConfigSelector, self).__init__(parent)
        
        self.tp = tp
        self.uconf = uconf
        self.group = group
        self.dest = dest


        #initialise layer data using existing source otherwise capabilities
        #get SRC here since we need it later for the reserved words anyway
        src = tp.initSource()
        dst = tp.initDestination(dest)
        dst.transferIETernal(tp.getConfInternal())    
        dst.setLayerConf(tp.getNewLayerConf(dst))
        if not dst.getLayerConf().exists():
            tp.initLayerConfig(src.getCapabilities(),dst)
        
        self.complete = dst.getLayerConf().getLConfAs3Array()
        self.reserved = set()

        #read the capabilities doc (as json) for reserved words
        for i in [l[3] for l in json.loads(tp.parseCapabilitiesDoc(src.getCapabilities(),'json'))]:
            self.reserved |= set(i) 
               
        #read the translated primary keys file
        self.inclayers = ['v:x'+x[0] for x in ConfigInitialiser.readCSV()]
        
        #Build models splitting by keyword if necessary 
        av_sl = self.splitData(str(group))
        
        self.available_model = LayerTableModel('L::available',self)
        self.available_model.initData(av_sl[0],self.inclayers)
        
        self.selection_model = LayerTableModel('R::selection',self)
        self.selection_model.initData(av_sl[1],self.inclayers)
        
        self.page = LayerSelectionPage(self)
        self.setCentralWidget(self.page)


        self.setWindowTitle("LDS Layer Selection")
        self.resize(725,480)

    def splitData(self,keyword):
        '''splits up the data according to a selection keyword'''
        alist = []
        slist = []
        for dp in self.complete:
            if keyword in dp[2]:
                slist.append(dp)
            else:
                alist.append(dp)
        return alist,slist
    
    def signalModels(self,prepost):
        if prepost=='PRE':        
            self.available_model.layoutAboutToBeChanged.emit()
            self.selection_model.layoutAboutToBeChanged.emit()
        elif prepost=='POST':
            self.available_model.layoutChanged.emit()
            self.selection_model.layoutChanged.emit() 
        
        
    
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
    colparams = ((0,65,'Name'), (1,250,'Title'), (2,350,'Keywords'))
    XFER_BW = 40
    def __init__(self, parent=None):
        super(LayerSelectionPage, self).__init__(parent)
        self.parent = parent

        QToolTip.setFont(QFont('SansSerif', 10))
        
        #label
        filterlabel = QLabel('Filter')
        availablelabel = QLabel('Available Layers')
        selectionlabel = QLabel('Layer Selections')
        keywordlabel = QLabel('Keyword')
        
        #button
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
        
        
        selectbutton = QPushButton('Select')
        selectbutton.setToolTip('Process selected rows')
        selectbutton.clicked.connect(self.doSelectClickAction)
        
        cancelbutton = QPushButton('Cancel')
        cancelbutton.setToolTip('Cancel Layer Selection')       
        cancelbutton.clicked.connect(QCoreApplication.instance().quit) 
        
        resetbutton = QPushButton('Reset')
        resetbutton.setToolTip('Reset all selections (Initialise any existing keyword selections)')       
        resetbutton.clicked.connect(self.doResetClickAction) 
        
        self.available_sfpm = LDSSFPAvailableModel(self)
        self.selection_sfpm = LDSSFPSelectionModel(self)
        
        self.available_sfpm.setSourceModel(self.parent.available_model)
        self.selection_sfpm.setSourceModel(self.parent.selection_model)
        
        #textedits
        filteredit = QLineEdit('')
        filteredit.setToolTip('Filter results table (filter operates across all fields)')       
        filteredit.textChanged.connect(self.available_sfpm.setActiveFilter);
        
        self.keywordedit = QLineEdit(self.parent.group)
        self.keywordedit.setToolTip('Select unique identifier to be saved in layer config (keyword)')
        #keywordedit.textChanged.connect(self.selection_sfpm.setActiveFilter);
        
        #notes.      
        #1. use selection filter to select stored keywords or to save new keywords
        #keywordedit.textChanged.connect(self.selection_sfpm.setActiveFilter);  

        
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
        
        #interesting, must set model after selection attributes but before hheaders else row selections/headers don't work properly
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
        vbox0 = QVBoxLayout()
        vbox0.addWidget(chooseallbutton)
        vbox0.addWidget(choosebutton)
        vbox0.addWidget(rejectbutton)
        vbox0.addWidget(rejectallbutton)
        
        vbox1 = QVBoxLayout()
        vbox1.addWidget(availablelabel)
        vbox1.addWidget(self.available)
        vbox1.addWidget(filterlabel)
        vbox1.addWidget(filteredit)
        
        vbox2 = QVBoxLayout()
        vbox2.addWidget(selectionlabel)
        vbox2.addWidget(self.selection)
        vbox2.addWidget(keywordlabel)
        vbox2.addWidget(self.keywordedit)
        
        hbox0 = QHBoxLayout()
        hbox0.addLayout(vbox1)
        hbox0.addLayout(vbox0)
        hbox0.addLayout(vbox2)

        
        
        hbox2 = QHBoxLayout()
        hbox2.addWidget(resetbutton)
        hbox2.addStretch(1)
        hbox2.addWidget(selectbutton)
        hbox2.addWidget(cancelbutton)
        
        vbox3 = QVBoxLayout()
        vbox3.addLayout(hbox0)
        vbox3.addLayout(hbox2)
        try:
            self.setLayout(vbox3)
        except Exception as e:
            print e
        
    def doSelectClickAction(self):
        '''Main selection action, takes selection and adds to conf layer (via tp)'''
        ktext = str(self.keywordedit.text())
        if ktext in self.parent.reserved:
            QMessageBox.about(self, "Reserved Keyword","'{0}' is a reserved keyword, please select again".format(ktext))
            return
        self.parent.tp.editLayerConf(self.parent.selection_model.mdata, self.parent.dest, ktext)
        self.parent.close()
            
    
    def doChooseAllClickAction(self):
#        si = self.available.selectedIndexes()
#        self.transferSelectedRows(si,self.available_sfpm,self.selection_sfpm)  
        self.parent.signalModels('PRE')
        self.parent.selection_model.mdata += self.parent.available_model.mdata
        self.parent.available_model.initData([])
        self.parent.signalModels('POST')
    
    def doChooseClickAction(self):
        '''Takes available selected and moves to selection'''
        select = self.available.selectionModel()
        if select.hasSelection():
            self.transferSelectedRows(select.selectedRows(),self.available_sfpm,self.selection_sfpm)
        else:
            ldslog.warn('L2R > Transfer action without selection')
            
    def transferSelectedRows(self,indices,from_model,to_model):
        tlist = []
        for proxyindex in indices:
            transfer = from_model.getData(proxyindex)
            tlist.append((proxyindex,transfer),)

        to_model.addData([t[1] for t in tlist])
        from_model.delData([t[0] for t in tlist])

            
    def doRejectClickAction(self):
        '''Takes available selected and moves to selection'''
        select = self.selection.selectionModel()
        if select.hasSelection():
            self.transferSelectedRows(select.selectedRows(),self.selection_sfpm,self.available_sfpm)
        else:
            ldslog.warn('R2L < Transfer action without selection')
            
    
    def doRejectAllClickAction(self):
        self.parent.signalModels('PRE')
        self.parent.available_model.mdata += self.parent.selection_model.mdata
        self.parent.selection_model.initData([])
        self.parent.signalModels('POST')
        
    def doResetClickAction(self):
        '''Reset the available pane and if there is anything in the keyword box use this to init the selection pane'''
        av_sl = self.parent.splitData(str(self.keywordedit.text()))
        self.parent.signalModels('PRE')
        self.parent.available_model.initData(av_sl[0])
        self.parent.selection_model.initData(av_sl[1])
        self.parent.signalModels('POST')

            

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
    #func to call config wizz
    app = QApplication(sys.argv)
    print 'this isnt going to work without a valid TP'
    ldsc = LayerConfigSelector()
    ldsc.show()
    sys.exit(app.exec_()) 
    
    
    
if __name__ == '__main__':
    main()
    
##active = [self.parent.selection_model.mdata[self.available_sfpm.mapToSource(pi).row()] for pi in select.selectedRows()]
##active = []
#tlist = []
#for proxyindex in select.selectedRows():
#    transfer = self.selection_sfpm.getData(proxyindex)
#    tlist.append((proxyindex,transfer),)
#
#self.available_sfpm.addData([t[1] for t in tlist])
#self.selection_sfpm.delData([t[0] for t in tlist])
#    
#    
#    ##sourceindex = self.selection_sfpm.mapToSource(proxyindex)
#    ##sourcedata = self.parent.selection_model.mdata[sourceindex.row()]
#    
#    ##self.parent.available_model.addDataRow(sourceindex,sourcedata)
#    ##self.parent.selection_model.delDataRow(sourceindex,sourcedata)
#    
#    #active.append(sourcedata)
#    
##self.parent.available_model.addData(sourceindex,active)
##self.parent.available_model.layoutChanged.emit()
##self.selection_sfpm.layoutChanged.emit()
##self.parent.selection_model.delData(sourceindex,active)
##self.parent.selection_model.layoutChanged.emit()
##self.available_sfpm.layoutChanged.emit()