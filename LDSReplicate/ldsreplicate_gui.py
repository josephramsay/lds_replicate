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
from lds.ReadConfig import GUIPrefsReader
from lds.LDSUtilities import LDSUtilities
from lds.VersionUtilities import AppVersion

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
        #defs = ('','','ldsincr.conf','','','2000-01-01','2013-01-01','True')
        defs = ('','','ldsincr.conf','','','','','True')
        rlist = map(lambda x,y: y if x is None or len(x)==0 else x,self.gpr.read(),defs)
        
        
        
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
        self.groupEdit = QLineEdit(rlist[3])
        self.epsgEdit = QLineEdit(rlist[4])
        self.confEdit = QLineEdit(rlist[2])
        
        #menus
        self.destmenulist = ('','MSSQL','PostgreSQL','SpatiaLite','FileGDB') 
        self.destMenu = QComboBox(self)
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
        self.internalTrigger.setCheckState(rlist[7]=='True')
        
        self.initTrigger = QCheckBox()
        self.initTrigger.setCheckState(False)
        
        self.cleanTrigger = QCheckBox()
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

  
#---> QWizard Section <----------------------------------------------------------------------------
       
class LDSConfigWizard(QWizard):
    def __init__(self, parent=None):
        super(LDSConfigWizard, self).__init__(parent)
        
        self.plist = {'lds':(0,'LDS',LDSConfigPage),
                 'pg':(1,'PostgreSQL',PostgreSQLConfigPage),
                 'ms':(2,'MSSQLSpatial',MSSQLSpatialConfigPage),
                 'fg':(3,'FileGDB',FileGDBConfigPage),
                 'sl':(4,'SpatiaLite',SpatiaLiteConfigPage),
                 'proxy':(5,'Proxy',ProxyConfigPage),
                 'final':(6,'Final',ConfirmationPage)}
        
        for key in self.plist.keys():
            index = self.plist.get(key)[0]
            page = self.plist.get(key)[2]
            self.setPage(index, page(self,key))


        self.setWindowTitle("QVariant Test")
        self.resize(640,480)

        
class LDSConfigPage(QWizardPage):
    def __init__(self, parent=None,key=None):
        super(LDSConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = 'lds'
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')

        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        fileLabel = QLabel('User Config File')
        keyLabel = QLabel('LDS API Key')
        destLabel = QLabel('Output Type')
        proxyLabel = QLabel('Configure Proxy')
        encryptionLabel = QLabel('Enable Password Protection')
        
        #edit boxes
        self.fileEdit = QLineEdit('')
        self.keyEdit = QLineEdit('')
        
        #dropdown
        self.destSelect = QComboBox()
        self.destSelect.addItem('')
        self.destSelect.addItem(self.parent.plist.get('pg')[1], self.parent.plist.get('pg')[0])
        self.destSelect.addItem(self.parent.plist.get('ms')[1], self.parent.plist.get('ms')[0])
        self.destSelect.addItem(self.parent.plist.get('fg')[1], self.parent.plist.get('fg')[0])
        self.destSelect.addItem(self.parent.plist.get('sl')[1], self.parent.plist.get('sl')[0])
        
        
        self.keyEdit.setValidator(QRegExpValidator(QRegExp("[a-zA-Z0-9]{32}"), self))
        
        #checkbox
        self.proxyEnable = QCheckBox()
        self.encryptionEnable = QCheckBox()

        
        self.registerField(self.key+"file",self.fileEdit)
        self.registerField(self.key+"apikey",self.keyEdit)
        self.registerField(self.key+"dest",self.destSelect,"currentIndex")
        self.registerField(self.key+"proxy",self.proxyEnable)
        self.registerField(self.key+"encryption",self.encryptionEnable)
        
        #buttons
        cfileButton = QPushButton("...")
        cfileButton.setToolTip('Select Config File')
        cfileButton.setBaseSize(100, 100)
        cfileButton.clicked.connect(self.selectConfFile)


        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        grid.addWidget(fileLabel, 1, 0)
        grid.addWidget(self.fileEdit, 1, 2)
        grid.addWidget(cfileButton, 1, 3)        
        
        grid.addWidget(keyLabel, 2, 0)
        grid.addWidget(self.keyEdit, 2, 2)
        
        grid.addWidget(destLabel, 3, 0)
        grid.addWidget(self.destSelect, 3, 2)
        
        grid.addWidget(proxyLabel, 4, 0)
        grid.addWidget(self.proxyEnable, 4, 2)
        
        grid.addWidget(encryptionLabel, 5, 0)
        grid.addWidget(self.encryptionEnable, 5, 2)

        #layout       
        self.setLayout(grid)
        
    def selectConfFile(self):
        self.fileEdit.setText(QFileDialog.getOpenFileName())

    def nextId(self):
        if self.field(self.key+"proxy").toBool():
            return self.parent.plist.get('proxy')[0]
        return int(self.field(self.key+"dest").toString())
        
class ProxyConfigPage(QWizardPage):
    def __init__(self, parent=None,key=None):
        super(ProxyConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')

        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        hostLabel = QLabel('Proxy Host')
        portLabel = QLabel('Proxy Port')
        authLabel = QLabel('Authentication')
        usrLabel = QLabel('Username')
        pwdLabel = QLabel('Password')
        
        #edit boxes
        self.hostEdit = QLineEdit('')
        self.portEdit = QLineEdit('')
        
        #dropdown
        self.authSelect = QComboBox()
        self.authSelect.addItem('')
        self.authSelect.addItem('BASIC',1)
        self.authSelect.addItem('NTLM',2)

        
        self.usrEdit = QLineEdit('')
        self.pwdEdit = QLineEdit('')
        self.pwdEdit.setEchoMode(QLineEdit.Password)
        
        self.portEdit.setValidator(QRegExpValidator(QRegExp("\d{1,5}"), self))
        
        self.registerField(self.key+"host",self.hostEdit)
        self.registerField(self.key+"port",self.portEdit)
        self.registerField(self.key+"auth",self.authSelect,"currentIndex")
        self.registerField(self.key+"usr",self.usrEdit)
        self.registerField(self.key+"pwd",self.pwdEdit)

        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        #layout
        grid.addWidget(hostLabel, 1, 0)
        grid.addWidget(self.hostEdit, 1, 2)
        
        grid.addWidget(portLabel, 2, 0)
        grid.addWidget(self.portEdit, 2, 2)
        
        grid.addWidget(authLabel, 3, 0)
        grid.addWidget(self.authSelect, 3, 2)
        
        grid.addWidget(usrLabel, 4, 0)
        grid.addWidget(self.usrEdit, 4, 2)
        
        grid.addWidget(pwdLabel, 5, 0)
        grid.addWidget(self.pwdEdit, 5, 2)
        
        
        #layout                
        self.setLayout(grid)  
        
    def selectConfFile(self):
        self.fileEdit.setText(QFileDialog.getOpenFileName())

    def nextId(self):
        #now go to selected dest configger
        return int(self.field("ldsdest").toString())
            
class PostgreSQLConfigPage(QWizardPage):
    def __init__(self,parent=None,key=None):
        super(PostgreSQLConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')

        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        
        #labels
        hostLabel = QLabel('PostgreSQL Host')
        portLabel = QLabel('PostgreSQL Port')
        dbnameLabel = QLabel('PostgreSQL DB Name')
        schemaLabel = QLabel('PostgreSQL DB Schema')
        usrLabel = QLabel('Username')
        pwdLabel = QLabel('Password')
        
        #edit boxes
        self.hostEdit = QLineEdit('')
        self.portEdit = QLineEdit('')
        self.dbnameEdit = QLineEdit('')
        self.schemaEdit = QLineEdit('')
        self.usrEdit = QLineEdit('')
        self.pwdEdit = QLineEdit('')
        self.pwdEdit.setEchoMode(QLineEdit.Password)
        
        self.portEdit.setValidator(QRegExpValidator(QRegExp("\d{1,5}"), self))
        
        self.registerField(self.key+"host",self.hostEdit)
        self.registerField(self.key+"port",self.portEdit)
        self.registerField(self.key+"dbname",self.dbnameEdit)
        self.registerField(self.key+"schema",self.schemaEdit)
        self.registerField(self.key+"usr",self.usrEdit)
        self.registerField(self.key+"pwd",self.pwdEdit)

        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        #layout
        grid.addWidget(hostLabel, 1, 0)
        grid.addWidget(self.hostEdit, 1, 2)
        
        grid.addWidget(portLabel, 2, 0)
        grid.addWidget(self.portEdit, 2, 2)
        
        grid.addWidget(dbnameLabel, 3, 0)
        grid.addWidget(self.dbnameEdit, 3, 2)
        
        grid.addWidget(schemaLabel, 4, 0)
        grid.addWidget(self.schemaEdit, 4, 2)
        
        grid.addWidget(usrLabel, 5, 0)
        grid.addWidget(self.usrEdit, 5, 2)
        
        grid.addWidget(pwdLabel, 6, 0)
        grid.addWidget(self.pwdEdit, 6, 2)
        
        
        #layout                
        self.setLayout(grid)  
        

    def nextId(self):
        return self.parent.plist.get('final')[0]
        

        
        
class MSSQLSpatialConfigPage(QWizardPage):
    def __init__(self,parent=None,key=None):
        super(MSSQLSpatialConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')

        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        serverLabel = QLabel('MSSQLSpatial Server')
        dbnameLabel = QLabel('MSSQLSpatial DB Name')
        schemaLabel = QLabel('MSSQLSpatial DB Schema')
        trustLabel = QLabel('Trust')
        usrLabel = QLabel('Username')
        pwdLabel = QLabel('Password')
        
        #edit boxes
        self.serverEdit = QLineEdit('')
        self.dbnameEdit = QLineEdit('')
        self.schemaEdit = QLineEdit('')
        self.trustEdit = QLineEdit('')#make this a CB?
        self.usrEdit = QLineEdit('')
        self.pwdEdit = QLineEdit('')
        self.pwdEdit.setEchoMode(QLineEdit.Password)

        self.trustEdit.setValidator(QRegExpValidator(QRegExp("yes|no", re.IGNORECASE), self))
        
        self.registerField("msserver",self.serverEdit)
        self.registerField(self.key+"dbname",self.dbnameEdit)
        self.registerField(self.key+"schema",self.schemaEdit)
        self.registerField(self.key+"trust",self.trustEdit)
        self.registerField(self.key+"usr",self.usrEdit)
        self.registerField(self.key+"pwd",self.pwdEdit)

        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        #layout
        grid.addWidget(serverLabel, 1, 0)
        grid.addWidget(self.serverEdit, 1, 2)
        
        grid.addWidget(dbnameLabel, 2, 0)
        grid.addWidget(self.dbnameEdit, 2, 2)
        
        grid.addWidget(schemaLabel, 3, 0)
        grid.addWidget(self.schemaEdit, 3, 2)
        
        grid.addWidget(trustLabel, 4, 0)
        grid.addWidget(self.trustEdit, 4, 2)
        
        grid.addWidget(usrLabel, 5, 0)
        grid.addWidget(self.usrEdit, 5, 2)
        
        grid.addWidget(pwdLabel, 6, 0)
        grid.addWidget(self.pwdEdit, 6, 2)

        self.setLayout(grid)
          
    def nextId(self):
        return self.parent.plist.get('final')[0]
        
class FileGDBConfigPage(QWizardPage):
    def __init__(self,parent=None,key=None):
        super(FileGDBConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')

        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        fileLabel = QLabel('FileGDB DB File')
        
        #edit boxes
        self.fileEdit = QLineEdit('')#file selection dialog?
        
        self.fileEdit.setValidator(QRegExpValidator(QRegExp("*.gdb$", re.IGNORECASE), self))
        
        self.registerField(self.key+"file",self.fileEdit)
        
        #buttons
        fileButton = QPushButton("...")
        fileButton.setToolTip('Select FileGDB File')
        fileButton.clicked.connect(self.selectFileGDBFile)
        
        
        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        #layout                
        grid.addWidget(fileLabel,1,0)
        grid.addWidget(self.fileEdit,2,0)
        grid.addWidget(fileButton,2,3)
 
        
        self.setLayout(grid)  

    def selectFileGDBFile(self):
        self.fileEdit.setText(QFileDialog.getOpenFileName())
        
    def nextId(self):
        return self.parent.plist.get('final')[0]
        
        
class SpatiaLiteConfigPage(QWizardPage):
    def __init__(self,parent=None,key=None):
        super(SpatiaLiteConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')

        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        fileLabel = QLabel('SpatiaLite DB File')
        
        #edit boxes
        self.fileEdit = QLineEdit('')
        
        self.fileEdit.setValidator(QRegExpValidator(QRegExp("*.db$", re.IGNORECASE), self))
        
        self.registerField(self.key+"file",self.fileEdit)
        
        #buttons
        fileButton = QPushButton("...")
        fileButton.setToolTip('Select SpatiaLite File')
        fileButton.clicked.connect(self.selectSpatiaLiteFile)
        

        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        #layout
        grid.addWidget(fileLabel, 1, 0)
        grid.addWidget(self.fileEdit, 2, 0)
        grid.addWidget(fileButton, 2, 3)        
        
        #layout       
        vbox = QVBoxLayout()
        vbox.addLayout(grid)     
        
        self.setLayout(vbox)  

    def selectSpatiaLiteFile(self):
        self.fileEdit.setText(QFileDialog.getOpenFileName())
        
    def nextId(self):
        return self.parent.plist.get('final')[0]

class ConfirmationPage(QWizardPage):
    def __init__(self,parent=None,key=None):
        super(ConfirmationPage, self).__init__(parent)
        self.parent = parent
        
        QToolTip.setFont(QFont('SansSerif', 10))
        
    def initializePage(self):
        '''Override initpage'''
        super(ConfirmationPage, self).initializePage()

        vbox = QVBoxLayout()
       
        hbox1 = QHBoxLayout()
        hbox1.addWidget(QLabel('LDS File Name'))     
        hbox1.addWidget(QLabel(self.field("ldsfile").toString()))   
            
        vbox.addLayout(hbox1)
        
        hbox2 = QHBoxLayout()
        hbox2.addWidget(QLabel('LDS API Key'))     
        hbox2.addWidget(QLabel(self.field("ldsapikey").toString()))   
        
        vbox.addLayout(hbox2)
    
        if self.field("ldsproxy").toBool():
            
            hbox1 = QHBoxLayout()
            hbox1.addWidget(QLabel('Proxy Server'))     
            hbox1.addWidget(QLabel(self.field("proxyhost").toString()))   
            
            vbox.addLayout(hbox1)
        
            hbox2 = QHBoxLayout()
            hbox2.addWidget(QLabel('Proxy Port'))     
            hbox2.addWidget(QLabel(self.field("proxyport").toString()))   
        
            vbox.addLayout(hbox2)
        
        dest = int(self.field("ldsdest").toString())

        for f in {1:self.getPGFields,2:self.getMSFields,3:self.getFGFields,4:self.getSLFields}.get(dest)():
            name = QLabel(f[1])
            value = QLabel(f[2].toString())
            
            hbox = QHBoxLayout()
            hbox.addWidget(name)     
            hbox.addWidget(value)   
            
            vbox.addLayout(hbox)       
               
        self.setLayout(vbox)
        
    def getPGFields(self):
        flist = []
        flist += (('host','PostgreSQL Host',self.field("pghost")),)
        flist += (('port','PostgreSQL Port',self.field('pgport')),)
        flist += (('dbname','PostgreSQL DB Name',self.field('pgdbname')),)
        flist += (('schema','PostgreSQL Schema',self.field('pgschema')),)
        flist += (('user','PostgreSQL User Name',self.field('pgusr')),)
        flist += (('pass','PostgreSQL Password',self.field('pgpwd')),)
        return flist   
    
    def getMSFields(self):
        flist = []
        flist += ('server','MSSQLSpatial Server String',self.field('msserver'))
        flist += ('dbname','MSSQLSpatial DB Name',self.field('msdbname'))
        flist += ('schema','MSSQLSpatial Schema',self.field('msschema'))
        flist += ('trust','MSSQLSpatial Trust (Yes/No)',self.field('mstrust'))
        flist += ('user','MSSQLSpatial User Name',self.field('msusr'))
        flist += ('pass','MSSQLSpatial Password',self.field('mspwd'))
        return flist  
    
    def getFGFields(self):
        flist = []
        flist += ('file','FileGDB DB File Name',self.field('fgfile'))
        return flist  
    
    def getSLFields(self):
        flist = []
        flist += ('file','SpatiaLite File Name',self.field('slfile'))
        return flist
    
       
    def validatePage(self):
        from lds.ReadConfig import MainFileReader as MFR
        from lds.ConfigWrapper import ConfigWrapper
        from lds.LDSUtilities import Encrypt
        rv = super(ConfirmationPage, self).validatePage()
        
        
        encrypt = self.field("ldsencryption").toBool()
        
        buildarray = ()
        
        buildarray += ((MFR.LDSN,'key',self.field("ldsapikey").toString()),)

        if self.field("ldsproxy").toBool():
            
            buildarray += ((MFR.PROXY,'host',self.field("proxyhost").toString()),)
            buildarray += ((MFR.PROXY,'port',self.field("proxyport").toString()),)
            buildarray += ((MFR.PROXY,'auth',self.field("proxyauth").toString()),)
            buildarray += ((MFR.PROXY,'user',self.field("proxyusr").toString()),)
            pwd = self.field("proxypwd").toString()
            if encrypt:
                pwd = Encrypt.ENC_PREFIX+Encrypt.secure(pwd)
            buildarray += ((MFR.PROXY,'pass',pwd),)

        
        dest = int(self.field("ldsdest").toString())
        
        for f in {1:self.getPGFields,2:self.getMSFields,3:self.getFGFields,4:self.getSLFields}.get(dest)():
            field = f[0]
            section = f[1][:f[1].find(' ')]
            value = str(f[2].toString())
            if field == 'pass' and encrypt:
                value = Encrypt.ENC_PREFIX+Encrypt.secure(value)
            buildarray += ((section,field,value),)
            
        ConfigWrapper.buildNewUserConfig(self.field("ldsfile").toString(), buildarray)
        
        return rv
   
        
        
def conf():
    #func to call config wizz
    app = QApplication(sys.argv)
    ldsc = LDSConfigWizard()
    ldsc.show()
    sys.exit(app.exec_()) 
    
def main():
  
    app = QApplication(sys.argv)
    #lds = LDSRepl()
    #lds.show()
    
#    for pwd in ('notlong','secretpassword','superdupermassivesecret','small','myspecialsecretx','myspecialsecret','reallyreallylongpasswordnobodywouldhonestlyuse'):
#        print len(pwd)
#        from lds.LDSUtilities import Encrypt
#        sec = Encrypt.secure(pwd)
#        pln = Encrypt.unSecure(sec)
#        print pwd,sec,pln
        
    ldsc = LDSConfigWizard()
    ldsc.show()
    sys.exit(app.exec_())
    
    
if __name__ == '__main__':
    main()