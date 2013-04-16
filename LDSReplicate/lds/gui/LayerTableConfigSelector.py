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
                         QPushButton, QTableView)
from PyQt4.QtCore import (Qt, QCoreApplication, QAbstractTableModel, QVariant)



import os
import re
import sys


from lds.ReadConfig import GUIPrefsReader
from lds.LDSUtilities import LDSUtilities
from lds.VersionUtilities import AppVersion


ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()


#Notes:
#MS and PG settings entered in these dialogs are saved to config only
#When a new FGDB directory is set in the file dialog using the NewFolder button a new directory is created and a reference added to the user config
#When a new SLITE file is created by entering its name in the SL file dialog, it isnt created but a reference to it is put in the user config file
       
class LayerTableConfigSelector(QMainWindow):
    
    testdata = [('v:x845', '12 Mile Territorial Sea Limit Basepoints', ['New Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries','testkey']), 
                ('v:x846', '12 Mile Territorial Sea Outer Limit', ['New Zealand', 'Hydrographic &  Maritime', 'Maritime Boundaries']), 
                ('v:x842', '200 Mile Exclusive Economic Zone Outer Limits', ['New  Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries']), 
                ('v:x844', '24 Mile Contiguous Zone  Basepoints', ['New Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries']), 
                ('v:x843', '24 Mile  Contiguous Zone Outer Limits', ['New Zealand', 'Hydrographic & Maritime', 'Maritime Boundaries']), 
                ('v:x1198', 'ASP: Check Combination', ['New Zealand', 'Roads and Addresses', 'Street and Places Index','testkey']), 
                ('v:x1199', 'ASP: GED Codes', ['New Zealand', 'Roads and Addresses', 'Street and Places Index']), 
                ('v:x1202', 'ASP:  MED Codes', ['New Zealand', 'Roads and Addresses', 'Street and Places Index','testkey'])] 

    def __init__(self,user_config=None):
        super(LayerTableConfigSelector, self).__init__()
        #TODO. 
        #1. keywords filter
        #2. conf writer/reader
        #3. 
        
        
        #lds = LDSDataStore(None,'ldsincr.lnx.conf') 
        #capabilities = lds.getCapabilities()
        #self.data = LDSDataStore.fetchLayerInfo(capabilities)

        self.data = self.testdata
        self.model = LayerTableModel(self)
        
        self.page = LayerSelectionPage(self)
        self.setCentralWidget(self.page)


        self.setWindowTitle("LDS Layer Selection")
        self.resize(725,480)

        
    
class LayerTableModel(QAbstractTableModel):
    #NB. There dont need to be any row/col inserts but will need to add keyword (selecting a layer  = adding user-custom tag)
    #Data table is in the form
    #Name   |Title       |Keywords
    #-------+------------+--------
    #v:xNNNN|Topo Layer X|Topo,NZ,custom

    
    def __init__(self, parent=None):    
        super(LayerTableModel, self).__init__()
        self.parent = parent
        self.userkey = 'myfile'#user conf name
        self.layerdata = []
        self.setData(self.parent.data)
        
        
    #abstract subclass funcs
    def rowCount(self, parent=None):
        return len(self.layerdata)
    
    def columnCount(self, parent=None):
        return len(self.layerdata[0])
    
    def data(self,index=None,role=None): 
        print role
        if (role != Qt.DisplayRole):
            print 'not'
            return QVariant()
        ri = index.row()
        ci = index.column()
        if ci==2:
            return '; '.join(self.layerdata[ri][2])#cant use comma for some reason
        return self.layerdata[ri][ci]
    
    #???
    def headerData(self,x=None,y=None,z=None):
        return ('ID','Title','Keywords')
    
    #editable datamodel subclass funcs
    def setData(self,layerdata):
        for row in layerdata:
            self.layerdata.append(row)
        
    def flags(self,index=None):
        return Qt.ItemIsSelectable | Qt.ItemIsEnabled
    
    
        
class LayerSelectionPage(QFrame):
    #TODO. Filtering, (visible) row selection, multi selection
    colparams = ((0,100,'Name'), (1,250,'Title'), (2,350,'Keywords'))
    
    def __init__(self, parent=None):
        super(LayerSelectionPage, self).__init__()
        self.parent = parent

        QToolTip.setFont(QFont('SansSerif', 10))
        
        #label
        filterlabel = QLabel('Filter')
        selectionlabel = QLabel('Layer Selection')
        
        #button
        selectbutton = QPushButton('Select')
        selectbutton.setToolTip('Process selected rows')
        selectbutton.clicked.connect(self.doSelectClickAction)
        
        cancelbutton = QPushButton('Cancel')
        cancelbutton.setToolTip('Cancel Layer Selection')       
        cancelbutton.clicked.connect(QCoreApplication.instance().quit) 
        
        resetbutton = QPushButton('Reset')
        
        self.sfpmodel = LDSSortFilterProxyModel(self)
        self.sfpmodel.setSourceModel(self.parent.model)
        
        #textedits
        filteredit = QLineEdit('')
        filteredit.setToolTip('Filter results table (filter operates across all fields)')       
        filteredit.textChanged.connect(self.sfpmodel.setActiveFilter);

        
        #header
        headmodel = QStandardItemModel()
        headmodel.setHorizontalHeaderLabels([i[2] for i in self.colparams])
        headview = QHeaderView(Qt.Horizontal)
        headview.setModel(headmodel)
        headview.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)        

        #table
        self.ltv = QTableView()
        self.ltv.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.ltv.setSelectionMode(QAbstractItemView.MultiSelection)
        
        #interesting, must set model after selection attributes but before hheaders else row selections/headers don't work properly
        self.ltv.setModel(self.sfpmodel)
        
        self.ltv.setSortingEnabled(True)
        self.ltv.setHorizontalHeader(headview)
        
        

        for cp in self.colparams:
            self.ltv.setColumnWidth(cp[0],cp[1])

        self.ltv.verticalHeader().setVisible(False)
        self.ltv.horizontalHeader().setVisible(True)
        
        
        grid = QGridLayout()
        grid.addWidget(self.ltv,1,1)

        #layout       
        vbox = QVBoxLayout()
        vbox.addWidget(filterlabel)
        vbox.addWidget(filteredit)
        vbox.addWidget(selectionlabel)
        vbox.addLayout(grid)
        
        
        hbox = QHBoxLayout()
        hbox.addWidget(resetbutton)
        hbox.addStretch(1)
        hbox.addWidget(selectbutton)
        hbox.addWidget(cancelbutton)
        
        vbox.addLayout(hbox)
        
        self.setLayout(vbox)
        
    def doSelectClickAction(self):
        select = self.ltv.selectionModel()
        if select.hasSelection():
            print [self.parent.data[self.sfpmodel.mapToSource(mi).row()] for mi in select.selectedRows()]
        else:
            print 'No Selection'

class LDSSortFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super(LDSSortFilterProxyModel, self).__init__(parent)
        self.parent = parent
        self.ftext = ''
        self.regexfilter = None
        
    def setActiveFilter(self,text):
        self.ftext = str(text)
        #self.regexfilter = QRegExp(text, Qt.CaseInsensitive, QRegExp.FixedString)
        #self.setFilterRegExp(regexfilter)#simple filter for single column
        self.invalidateFilter()
        

        
    def filterAcceptsRow(self,row,parent):
        '''Override for row filter function'''
        for i in range(0,self.sourceModel().columnCount()):
            field = str(self.sourceModel().data(self.sourceModel().index(row, i, parent)))
            if re.search(self.ftext,field,re.IGNORECASE):
                return True
        return False        
    
    
def main():
    #func to call config wizz
    app = QApplication(sys.argv)
    ldsc = LayerTableConfigSelector()
    ldsc.show()
    sys.exit(app.exec_()) 
    
    
    
if __name__ == '__main__':
    main()