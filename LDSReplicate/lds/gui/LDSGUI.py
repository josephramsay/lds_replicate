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

from PyQt4.QtGui import (QApplication, QWizard, QWizardPage, QLabel,
                         QVBoxLayout, QHBoxLayout, QGridLayout,
                         QRegExpValidator, QCheckBox, QMessageBox, 
                         QMainWindow, QAction, QIcon, qApp, QFrame,
                         QLineEdit,QToolTip, QFont, QComboBox, QDateEdit, 
                         QPushButton, QDesktopWidget, QFileDialog, QTextEdit)
from PyQt4.QtCore import (QRegExp, QDate, QCoreApplication, QDir)

import os
import re
import sys
import logging

from lds.TransferProcessor import TransferProcessor
from lds.LDSDataStore import LDSDataStore
from lds.WFSDataStore import WFSDataStore
from lds.ReadConfig import GUIPrefsReader
from lds.LDSUtilities import LDSUtilities
from lds.VersionUtilities import AppVersion

from lds.PostgreSQLDataStore import PostgreSQLDataStore as PG
from lds.MSSQLSpatialDataStore import MSSQLSpatialDataStore as MS
from lds.FileGDBDataStore import FileGDBDataStore as FG
from lds.SpatiaLiteDataStore import SpatiaLiteDataStore as SL


ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()

class LDSRepl(QMainWindow):
    '''This file (GUI functionality) has not been tested in any meaningful way and is likely to break on unexpected input'''
  
    def __init__(self):
        super(LDSRepl, self).__init__()
        
        self.setGeometry(300, 300, 350, 250)
        self.setWindowTitle('LDS Data Replicator')
        
        self.controls = LDSControls(self)
        self.setCentralWidget(self.controls)
        
        self.statusbar = self.statusBar()
        self.statusbar.showMessage('Ready')
        
        openAction = QAction(QIcon('open.png'), '&Open', self)        
        openAction.setShortcut('Ctrl+O')
        openAction.setStatusTip('Open Prefs Editor')
        openAction.triggered.connect(self.launchEditor)
        
        exitAction = QAction(QIcon('exit.png'), '&Exit', self)        
        exitAction.setShortcut('Ctrl+Q')
        exitAction.setStatusTip('Exit Application')
        exitAction.triggered.connect(qApp.quit)
        
        menubar = self.menuBar()

        fileMenu = menubar.addMenu('&File')
        fileMenu.addAction(openAction)
        fileMenu.addSeparator()
        fileMenu.addAction(exitAction)

        helpMenu = menubar.addMenu('&Help')

    def launchEditor(self, checked=None):
        prefs = LDSPrefsEditor()
        prefs.setWindowTitle('LDS Preferences Editor')
        prefs.show() 
    
class LDSControls(QFrame):
    
    def __init__(self,parent):
        super(LDSControls, self).__init__()
        self.parent = parent
        self.gpr = GUIPrefsReader()
        self.initUI()
        
    def initUI(self):
        
        # 0      1       2       3       4      5    6    7
        #'dest','layer','uconf','group','epsg','fd','td','int'
        defaults = ('','','','','','','','True')
        rlist = map(lambda x,y: y if x is None or len(x)==0 else x,self.gpr.read(),defaults)
        
        
        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        destLabel = QLabel('Destination')
        layerLabel = QLabel('Layer')
        groupLabel = QLabel('Group')
        epsgLabel = QLabel('EPSG')
        fromDateLabel = QLabel('From Date')
        toDateLabel = QLabel('To Date')
        
        initLabel = QLabel('Initialise')
        cleanLabel = QLabel('Clean')
        internalLabel = QLabel('Internal')
        confLabel = QLabel('User Config')

        #edit boxes
        self.layerEdit = QLineEdit(rlist[1])
        self.layerEdit.setToolTip('Enter the layer you want to replicate using either v:x format or layer name')   
        self.groupEdit = QLineEdit(rlist[3])
        self.groupEdit.setToolTip('Enter a layer keyword or use your own custom keyword to select a group of layers')   
        self.epsgEdit = QLineEdit(rlist[4])
        self.epsgEdit.setToolTip('Setting an EPSG number here determines the output SR of the layer')   
        self.confEdit = QLineEdit(rlist[2])
        self.confEdit.setToolTip('Enter your user config file here')   
        
        #menus
        self.destmenulist = ('',PG.DRIVER_NAME,MS.DRIVER_NAME,FG.DRIVER_NAME,SL.DRIVER_NAME) 
        self.destMenu = QComboBox(self)
        self.destMenu.setToolTip('Choose the desired output type')   
        self.destMenu.addItems(self.destmenulist)
        self.destMenu.setCurrentIndex(self.destmenulist.index(rlist[0]))
        
       
        
        #date selection
        self.fromDateEdit = QDateEdit()
        if LDSUtilities.mightAsWellBeNone(rlist[5]) is not None:
            self.fromDateEdit.setDate(QDate(int(rlist[5][0:4]),int(rlist[5][5:7]),int(rlist[5][8:10]))) 
        self.fromDateEdit.setCalendarPopup(True)
        self.fromDateEdit.setEnabled(False)
        
        self.toDateEdit = QDateEdit()
        if LDSUtilities.mightAsWellBeNone(rlist[6]) is not None:
            self.toDateEdit.setDate(QDate(int(rlist[6][0:4]),int(rlist[6][5:7]),int(rlist[6][8:10]))) 
        self.toDateEdit.setCalendarPopup(True)
        self.toDateEdit.setEnabled(False)
        
        #check boxes
        self.fromDateEnable = QCheckBox()
        self.fromDateEnable.setCheckState(False)
        self.fromDateEnable.clicked.connect(self.doFromDateEnable)

        
        self.toDateEnable = QCheckBox()
        self.toDateEnable.setCheckState(False) 
        self.toDateEnable.clicked.connect(self.doToDateEnable)
        
        self.internalTrigger = QCheckBox()
        self.internalTrigger.setToolTip('Sets where layer config settings are stored, external/internal')   
        self.internalTrigger.setCheckState(rlist[7]=='True')
        
        self.initTrigger = QCheckBox()
        self.initTrigger.setToolTip('Re writes the layer config settings (you need to do this on first run)')   
        self.initTrigger.setCheckState(False)
        
        self.cleanTrigger = QCheckBox()
        self.cleanTrigger.setToolTip('Instead of replicating, this deletes the layer chosen above')   
        self.cleanTrigger.setCheckState(False)
        
        
        #buttons
        okButton = QPushButton("OK")
        okButton.setToolTip('Execute selected replication')
        okButton.clicked.connect(self.doOkClickAction)
        
        cancelButton = QPushButton("Cancel")
        cancelButton.setToolTip('Cancel LDS Replicate')       
        cancelButton.clicked.connect(QCoreApplication.instance().quit) 

        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        
        #placement section ------------------------------------
        
        #-------------+----------------
        #   dst label |   dst dropdown
        # layer label | layer dropdown
        # ...
        #-------------+--+------+------
        #           opt1 | opt2 | opt3
        #----------------+----+-+------
        #                  ok | cancel
        #---------------------+--------

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

        vbox1 = QVBoxLayout()
        vbox1.addStretch(1)
        vbox1.addWidget(internalLabel)
        vbox1.addWidget(self.internalTrigger)
        
        vbox2 = QVBoxLayout()
        vbox2.addStretch(1)
        vbox2.addWidget(initLabel)
        vbox2.addWidget(self.initTrigger)
        
        vbox3 = QVBoxLayout()
        vbox3.addStretch(1)
        vbox3.addWidget(cleanLabel)
        vbox3.addWidget(self.cleanTrigger)
        
        hbox3 = QHBoxLayout()
        hbox3.addStretch(1)
        hbox3.addLayout(vbox1)
        hbox3.addLayout(vbox2)
        hbox3.addLayout(vbox3)
        
        hbox4 = QHBoxLayout()
        hbox4.addStretch(1)
        hbox4.addWidget(okButton)
        hbox4.addWidget(cancelButton)
        

        vbox = QVBoxLayout()
        #vbox.addStretch(1)
        vbox.addLayout(grid)
        vbox.addLayout(hbox3)
        vbox.addLayout(hbox4)
        
        
        self.setLayout(vbox)  
        
        #self.setGeometry(300, 300, 350, 250)
        #self.setWindowTitle('LDS Replicate')
       
        
        
    def centre(self):
        
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        
    def closeEvent(self, event):
        
        reply = QMessageBox.question(self, 'Message', "Are you sure to quit?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()       
        
    def doFromDateEnable(self):
        self.fromDateEdit.setEnabled(self.fromDateEnable.isChecked())
          
    def doToDateEnable(self):
        self.toDateEdit.setEnabled(self.toDateEnable.isChecked())  
          
    def doOkClickAction(self):
        
        destination = str(self.destmenulist[self.destMenu.currentIndex()])
        layer = str(self.layerEdit.text())
        uconf = str(self.confEdit.text())
        group = str(self.groupEdit.text())
        epsg = str(self.epsgEdit.text())
        fe = self.fromDateEnable.isChecked()
        te = self.toDateEnable.isChecked()
        fd = None if fe is False else str(self.fromDateEdit.date().toString('yyyy-MM-dd'))
        td = None if te is False else str(self.toDateEdit.date().toString('yyyy-MM-dd'))
        internal = self.internalTrigger.isChecked()
        init = self.initTrigger.isChecked()
        clean = self.cleanTrigger.isChecked()
        
        self.parent.statusbar.showMessage('Replicating '+layer)

        #'dest','layer','uconf','group','epsg','fd','td','int'
        self.gpr.write((destination,layer,uconf,group,epsg,fd,td,internal))
        
        ldslog.info('dest='+destination+', layer'+layer+', conf='+uconf+', group='+group+', epsg='+epsg)
        ldslog.info('fd='+str(fd)+', td='+str(td)+', fe='+str(fe)+', te='+str(te))
        ldslog.info('int='+str(internal)+', init='+str(init)+', clean='+str(clean))


        tp = TransferProcessor(layer, 
                               None if group is None else group, 
                               None if epsg is None else epsg, 
                               None if fd is None else fd, 
                               None if td is None else td,
                               None, None, None, 
                               None if uconf is None else uconf, 
                               internal, None)
        
        #NB init and clean are funcs because they appear as args, not opts in the CL
        if init:
            tp.setInitConfig()
        if clean:
            tp.setCleanConfig()
            
        proc = {'PostgreSQL':tp.processLDS2PG,
                'MSSQL':tp.processLDS2MSSQL,
                'SpatiaLite':tp.processLDS2SpatiaLite,
                'FileGDB':tp.processLDS2FileGDB
                }.get(destination)
        proc()
        
        self.parent.statusbar.showMessage('Replication of '+layer+' complete')
        
        
#--------------------------------------------------------------------------------------------------

class LDSPrefsEditor(QMainWindow):
    
    def __init__(self):
        super(LDSPrefsEditor, self).__init__()
        
        self.setWindowTitle('LDS Preferences Editor')
        
        self.editor = LDSPrefsFrame(self)
        self.setCentralWidget(self.editor)

        
        openAction = QAction(QIcon('open.png'), '&Open', self)        
        openAction.setShortcut('Ctrl+O')
        openAction.setStatusTip('Open File')
        openAction.triggered.connect(self.openFile)
        
        saveAction = QAction(QIcon('save.png'), '&Save', self)        
        saveAction.setShortcut('Ctrl+S')
        saveAction.setStatusTip('Save Changes')
        saveAction.triggered.connect(self.saveFile)
        
        saveAsAction = QAction(QIcon('save.png'), '&Save As', self)        
        saveAsAction.setShortcut('Ctrl+A')
        saveAsAction.setStatusTip('Save Changes')
        saveAsAction.triggered.connect(self.saveAsFile)
        
        exitAction = QAction(QIcon('exit.png'), '&Exit', self)        
        exitAction.setShortcut('Ctrl+Q')
        exitAction.setStatusTip('Exit Application')
        exitAction.triggered.connect(self.close)
        
        self.statusbar = self.statusBar()
        self.statusbar.showMessage('Ready')
        
        menubar = self.menuBar()

        fileMenu = menubar.addMenu('&File')
        fileMenu.addAction(openAction)
        fileMenu.addSeparator()
        fileMenu.addAction(saveAction)
        fileMenu.addAction(saveAsAction)
        fileMenu.addSeparator()
        fileMenu.addAction(exitAction) 
        
        self.initUI()
        
    def initUI(self):
        self.setGeometry(350,350,800,600)
        self.show() 
        
    def saveAsFile(self):
        filename = QFileDialog.getSaveFileName(self, 'Save File As', os.path.join(os.getcwd(),'../conf/'))#os.getenv('HOME'))
        f = open(filename, 'w')
        filedata = self.editor.textedit.toPlainText()
        f.write(filedata)
        f.close()
        
    def saveFile(self):
        f = open(self.filename, 'w')
        filedata = self.editor.textedit.toPlainText()
        f.write(filedata)
        f.close()
        
    def openFile(self):
        f=QDir.Filter(1)
        
        filedialog = QFileDialog()
        filedialog.setFilter(f)
        self.filename = filedialog.getOpenFileName(self, 'Open File', os.path.join(os.getcwd(),'../conf/'))#os.getenv('HOME'))
        f = open(self.filename, 'r')
        filedata = f.read()
        self.editor.textedit.setText(filedata)
        self.statusbar.showMessage('Editing '+self.filename)
        f.close()
        
class LDSPrefsFrame(QFrame):
    
    def __init__(self,parent):
        super(LDSPrefsFrame, self).__init__()
        self.parent = parent
        self.gpr = GUIPrefsReader()
        self.initUI()
        
    def initUI(self):

        #edit boxes
        self.textedit = QTextEdit() 
        
        vbox = QVBoxLayout()
        vbox.addWidget(self.textedit)
        
        self.setLayout(vbox)  

    
def main():
  
    app = QApplication(sys.argv)
    lds = LDSRepl()
    lds.show()
    sys.exit(app.exec_())
    
    
if __name__ == '__main__':
    main()