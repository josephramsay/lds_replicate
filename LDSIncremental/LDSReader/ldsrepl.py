'''
v.0.0.1

LDSIncremental -  ldsrepl

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 13/02/2013

@author: jramsay
'''

from PyQt4 import QtGui, QtCore

import sys

from TransferProcessor import TransferProcessor
from LDSDataStore import LDSDataStore

class LDSRepl(QtGui.QWidget):
  
    def __init__(self):
        super(LDSRepl, self).__init__()
        
        self.initUI()
    
    
    def getLayerList(self):
        self.src = LDSDataStore("http://wfs.data.linz.govt.nz/0aadbb5c801d470f8f075592f097f70c/wfs?service=WFS&request=GetCapabilities",None) 
        capabilities = self.src.getCapabilities()
        return LDSDataStore.fetchLayerNames(capabilities)
    
    def initUI(self):
        QtGui.QToolTip.setFont(QtGui.QFont('SansSerif', 10))
        
        #labels
        destLabel = QtGui.QLabel('Destination')
        layerLabel = QtGui.QLabel('Layer')
        groupLabel = QtGui.QLabel('Group')
        epsgLabel = QtGui.QLabel('EPSG')
        fromDateLabel = QtGui.QLabel('From Date')
        toDateLabel = QtGui.QLabel('To Date')
        
        initLabel = QtGui.QLabel('Initialise')
        cleanLabel = QtGui.QLabel('Clean')
        internalLabel = QtGui.QLabel('Internal')
        confLabel = QtGui.QLabel('User Config')

        #edit boxes
        self.layerEdit = QtGui.QLineEdit()
        self.groupEdit = QtGui.QLineEdit()
        self.epsgEdit = QtGui.QLineEdit()
        self.confEdit = QtGui.QLineEdit()
        
        #menus
        self.destmenulist = ('','MSSQL','PostgreSQL','SpatiaLite','FileGDB') 
        self.destMenu = QtGui.QComboBox(self)
        self.destMenu.addItems(self.destmenulist)
        
       
        
        #date selection
        self.fromDateEdit = QtGui.QDateEdit()
        self.fromDateEdit.setDate(QtCore.QDate(2000,01,01)) 
        self.fromDateEdit.setCalendarPopup(True)
        self.fromDateEdit.setEnabled(False)
        
        self.toDateEdit = QtGui.QDateEdit()
        self.toDateEdit.setDate(QtCore.QDate(2013,01,01)) 
        self.toDateEdit.setCalendarPopup(True)
        self.toDateEdit.setEnabled(False)
        
        #check boxes
        self.fromDateEnable = QtGui.QCheckBox()
        self.fromDateEnable.setCheckState(False)
        self.fromDateEnable.clicked.connect(self.doFromDateEnable)

        
        self.toDateEnable = QtGui.QCheckBox()
        self.toDateEnable.setCheckState(False) 
        self.toDateEnable.clicked.connect(self.doToDateEnable)
        
        self.internalTrigger = QtGui.QCheckBox()
        self.internalTrigger.setCheckState(False)
        
        self.initTrigger = QtGui.QCheckBox()
        self.initTrigger.setCheckState(False)
        
        self.cleanTrigger = QtGui.QCheckBox()
        self.cleanTrigger.setCheckState(False)
        
        
        #buttons
        okButton = QtGui.QPushButton("OK")
        okButton.setToolTip('Execute selected replication')
        okButton.clicked.connect(self.doOkClickAction)
        
        cancelButton = QtGui.QPushButton("Cancel")
        cancelButton.setToolTip('Cancel LDS Replicate')       
        cancelButton.clicked.connect(QtCore.QCoreApplication.instance().quit) 

        #grid
        grid = QtGui.QGridLayout()
        grid.setSpacing(10)
        
        
        #placement section ------------------------------------
        
        grid.addWidget(destLabel, 1, 0)
        grid.addWidget(self.destMenu, 1, 2)

        grid.addWidget(layerLabel, 2, 0)
        grid.addWidget(self.layerEdit, 2, 2)
        
        grid.addWidget(confLabel, 3, 0)
        grid.addWidget(self.confEdit, 3, 2)
        
        grid.addWidget(groupLabel, 4, 0)
        grid.addWidget(self.groupEdit, 4, 2)
        
        grid.addWidget(epsgLabel, 5, 0)
        grid.addWidget(self.epsgEdit, 5, 2)

        grid.addWidget(fromDateLabel, 6, 0)
        grid.addWidget(self.fromDateEnable, 6, 1)
        grid.addWidget(self.fromDateEdit, 6, 2)
        
        grid.addWidget(toDateLabel, 7, 0)
        grid.addWidget(self.toDateEnable, 7, 1)
        grid.addWidget(self.toDateEdit, 7, 2)

        grid.addWidget(internalLabel, 8, 0)
        grid.addWidget(initLabel, 8, 1)
        grid.addWidget(cleanLabel, 8, 2)
        
        grid.addWidget(self.internalTrigger, 9, 0)
        grid.addWidget(self.initTrigger, 9, 1)
        grid.addWidget(self.cleanTrigger, 9, 2)
        
        grid.addWidget(okButton, 10, 1)
        grid.addWidget(cancelButton, 10, 2)
        

        hbox = QtGui.QHBoxLayout()
        hbox.addStretch(1)
        hbox.addWidget(okButton)
        hbox.addWidget(cancelButton)

        vbox = QtGui.QVBoxLayout()
        vbox.addStretch(1)
        vbox.addLayout(hbox)

        
        self.setLayout(grid)  
        
        self.setGeometry(300, 300, 350, 250)
        self.setWindowTitle('LDS Replicate')    
        self.show()
       
        
        
    def centre(self):
        
        qr = self.frameGeometry()
        cp = QtGui.QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        
    def closeEvent(self, event):
        
        reply = QtGui.QMessageBox.question(self, 'Message', "Are you sure to quit?", QtGui.QMessageBox.Yes | QtGui.QMessageBox.No, QtGui.QMessageBox.No)

        if reply == QtGui.QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()       
        
    def doFromDateEnable(self):
        self.fromDateEdit.setEnabled(self.fromDateEnable.isChecked())
          
    def doToDateEnable(self):
        self.toDateEdit.setEnabled(self.toDateEnable.isChecked())  
          
    def doOkClickAction(self):
        print 'dest',self.destmenulist[self.destMenu.currentIndex()]
        
        print 'layer',self.layerEdit.text()
        print 'conf',self.confEdit.text()
        print 'group',self.groupEdit.text()
        print 'epsg',self.epsgEdit.text()
        
        print 'fd',self.fromDateEdit.date().toString('yyyy-MM-dd')
        print 'td',self.toDateEdit.date().toString('yyyy-MM-dd')
        
        print 'fe',self.fromDateEnable.isChecked()
        print 'te',self.toDateEnable.isChecked()
        
        print 'internal',self.internalTrigger.isChecked()
        print 'init',self.initTrigger.isChecked()
        print 'clean',self.cleanTrigger.isChecked()
        
        destination = self.destmenulist[self.destMenu.currentIndex()]
        layer = self.layerEdit.text()
        group = self.groupEdit.text()
        epsg = self.epsgEdit.text()
        fd = self.fromDateEdit.date().toString('yyyy-MM-dd')
        td = self.toDateEdit.date().toString('yyyy-MM-dd')
        conf = self.confEdit.text()
        internal = self.internalTrigger.isChecked(),

        tp = TransferProcessor(layer, 
                               None if group is None else group, 
                               None if epsg is None else epsg, 
                               None if fd is None else fd, 
                               None if td is None else td,
                               None, None, None, 
                               None if conf is None else conf , 
                               internal, None)
        
        destination = self.destmenulist[self.destMenu.currentIndex()]
        
        if destination == 'PostgreSQL':
            proc = tp.processLDS2PG
        elif destination == 'MSSQL':
            proc = tp.processLDS2MSSQL
        elif destination == 'SpatiaLite':
            proc = tp.processLDS2SpatiaLite
        elif destination == 'FileGDB':
            proc = tp.processLDS2FileGDB
        else:
            proc = None
        proc()
        
        
def main():
  
    app = QtGui.QApplication(sys.argv)
    lds = LDSRepl()
    sys.exit(app.exec_())
    
    
if __name__ == '__main__':
    main()