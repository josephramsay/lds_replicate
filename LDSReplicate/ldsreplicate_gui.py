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

from PyQt4 import QtGui, QtCore

import os
import sys
import logging

from lds.TransferProcessor import TransferProcessor
from lds.LDSDataStore import LDSDataStore
from lds.ReadConfig import GUIPrefsReader
from lds.LDSUtilities import LDSUtilities
from lds.VersionUtilities import AppVersion

ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()

class LDSRepl(QtGui.QMainWindow):
    '''This file (GUI functionality) has not been tested in any meaningful way and is likely to break on unexpected input'''
  
    def __init__(self):
        super(LDSRepl, self).__init__()
        
        self.setGeometry(300, 300, 350, 250)
        self.setWindowTitle('LDS Data Replicator')
        
        self.controls = LDSControls(self)
        self.setCentralWidget(self.controls)
        
        self.statusbar = self.statusBar()
        self.statusbar.showMessage('Ready')
        
        openAction = QtGui.QAction(QtGui.QIcon('open.png'), '&Open', self)        
        openAction.setShortcut('Ctrl+O')
        openAction.setStatusTip('Open Prefs Editor')
        openAction.triggered.connect(self.launchEditor)
        
        exitAction = QtGui.QAction(QtGui.QIcon('exit.png'), '&Exit', self)        
        exitAction.setShortcut('Ctrl+Q')
        exitAction.setStatusTip('Exit Application')
        exitAction.triggered.connect(QtGui.qApp.quit)
        
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
    
class LDSControls(QtGui.QFrame):
    
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
        self.layerEdit = QtGui.QLineEdit(rlist[1])   
        self.groupEdit = QtGui.QLineEdit(rlist[3])
        self.epsgEdit = QtGui.QLineEdit(rlist[4])
        self.confEdit = QtGui.QLineEdit(rlist[2])
        
        #menus
        self.destmenulist = ('','MSSQL','PostgreSQL','SpatiaLite','FileGDB') 
        self.destMenu = QtGui.QComboBox(self)
        self.destMenu.addItems(self.destmenulist)
        self.destMenu.setCurrentIndex(self.destmenulist.index(rlist[0]))
        
       
        
        #date selection
        self.fromDateEdit = QtGui.QDateEdit()
        if LDSUtilities.mightAsWellBeNone(rlist[5]) is not None:
            self.fromDateEdit.setDate(QtCore.QDate(int(rlist[5][0:4]),int(rlist[5][5:7]),int(rlist[5][8:10]))) 
        self.fromDateEdit.setCalendarPopup(True)
        self.fromDateEdit.setEnabled(False)
        
        self.toDateEdit = QtGui.QDateEdit()
        if LDSUtilities.mightAsWellBeNone(rlist[6]) is not None:
            self.toDateEdit.setDate(QtCore.QDate(int(rlist[6][0:4]),int(rlist[6][5:7]),int(rlist[6][8:10]))) 
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
        self.internalTrigger.setCheckState(rlist[7]=='True')
        
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

        vbox1 = QtGui.QVBoxLayout()
        vbox1.addStretch(1)
        vbox1.addWidget(internalLabel)
        vbox1.addWidget(self.internalTrigger)
        
        vbox2 = QtGui.QVBoxLayout()
        vbox2.addStretch(1)
        vbox2.addWidget(initLabel)
        vbox2.addWidget(self.initTrigger)
        
        vbox3 = QtGui.QVBoxLayout()
        vbox3.addStretch(1)
        vbox3.addWidget(cleanLabel)
        vbox3.addWidget(self.cleanTrigger)
        
        hbox3 = QtGui.QHBoxLayout()
        hbox3.addStretch(1)
        hbox3.addLayout(vbox1)
        hbox3.addLayout(vbox2)
        hbox3.addLayout(vbox3)
        
        hbox4 = QtGui.QHBoxLayout()
        hbox4.addStretch(1)
        hbox4.addWidget(okButton)
        hbox4.addWidget(cancelButton)
        

        vbox = QtGui.QVBoxLayout()
        #vbox.addStretch(1)
        vbox.addLayout(grid)
        vbox.addLayout(hbox3)
        vbox.addLayout(hbox4)
        
        
        self.setLayout(vbox)  
        
        #self.setGeometry(300, 300, 350, 250)
        #self.setWindowTitle('LDS Replicate')
       
        
        
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

class LDSPrefsEditor(QtGui.QMainWindow):
    
    def __init__(self):
        super(LDSPrefsEditor, self).__init__()
        
        self.setWindowTitle('LDS Preferences Editor')
        
        self.editor = LDSPrefsFrame(self)
        self.setCentralWidget(self.editor)

        
        openAction = QtGui.QAction(QtGui.QIcon('open.png'), '&Open', self)        
        openAction.setShortcut('Ctrl+O')
        openAction.setStatusTip('Open File')
        openAction.triggered.connect(self.openFile)
        
        saveAction = QtGui.QAction(QtGui.QIcon('save.png'), '&Save', self)        
        saveAction.setShortcut('Ctrl+S')
        saveAction.setStatusTip('Save Changes')
        saveAction.triggered.connect(self.saveFile)
        
        saveAsAction = QtGui.QAction(QtGui.QIcon('save.png'), '&Save As', self)        
        saveAsAction.setShortcut('Ctrl+A')
        saveAsAction.setStatusTip('Save Changes')
        saveAsAction.triggered.connect(self.saveAsFile)
        
        exitAction = QtGui.QAction(QtGui.QIcon('exit.png'), '&Exit', self)        
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
        filename = QtGui.QFileDialog.getSaveFileName(self, 'Save File As', os.path.join(os.getcwd(),'../conf/'))#os.getenv('HOME'))
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
        f=QtCore.QDir.Filter(1)
        
        filedialog = QtGui.QFileDialog()
        filedialog.setFilter(f)
        self.filename = filedialog.getOpenFileName(self, 'Open File', os.path.join(os.getcwd(),'../conf/'))#os.getenv('HOME'))
        f = open(self.filename, 'r')
        filedata = f.read()
        self.editor.textedit.setText(filedata)
        self.statusbar.showMessage('Editing '+self.filename)
        f.close()
        
class LDSPrefsFrame(QtGui.QFrame):
    
    def __init__(self,parent):
        super(LDSPrefsFrame, self).__init__()
        self.parent = parent
        self.gpr = GUIPrefsReader()
        self.initUI()
        
    def initUI(self):

        #edit boxes
        self.textedit = QtGui.QTextEdit() 
        
        vbox = QtGui.QVBoxLayout()
        vbox.addWidget(self.textedit)
        
        self.setLayout(vbox)  


        
#---> QWizard Section <----------------------------------------------------------------------------
       
class LDSConfigWizard(QtGui.QWizard):
    def __init__(self, parent=None):
        super(LDSConfigWizard, self).__init__(parent)
        
        self.plist = {'lds':(0,'LDS',LDSConfigPage),
                 'pg':(1,'PostgreSQL',PostgreSQLConfigPage),
                 'ms':(2,'MSSQLSpatial',MSSQLSpatialConfigPage),
                 'fg':(3,'FileGDB',FileGDBConfigPage),
                 'sl':(4,'SpatiaLite',SpatiaLiteConfigPage),
                 'final':(5,'Final',ConfirmationPage)}
        
        for key in self.plist.keys():
            self.setPage(self.plist.get(key)[0],self.plist.get(key)[2](self))

        self.setWindowTitle("QVariant Test")
        self.resize(640,480)

        
class LDSConfigPage(QtGui.QWizardPage):
    def __init__(self, parent=None):
        super(LDSConfigPage, self).__init__(parent)
        
        self.parent = parent 
        
        self.setTitle('LDS Configuration Options')

        QtGui.QToolTip.setFont(QtGui.QFont('SansSerif', 10))
        
        #labels
        cfileLabel = QtGui.QLabel('User Config File')
        keyLabel = QtGui.QLabel('LDS API Key')
        destLabel = QtGui.QLabel('Output Type')
        
        #edit boxes
        self.cfileEdit = QtGui.QLineEdit('')
        self.keyEdit = QtGui.QLineEdit('')
        self.destSelect = QtGui.QComboBox()
        self.destSelect.addItem('')
        self.destSelect.addItem(self.parent.plist.get('pg')[1], self.parent.plist.get('pg')[0])
        self.destSelect.addItem(self.parent.plist.get('ms')[1], self.parent.plist.get('ms')[0])
        self.destSelect.addItem(self.parent.plist.get('fg')[1], self.parent.plist.get('fg')[0])
        self.destSelect.addItem(self.parent.plist.get('sl')[1], self.parent.plist.get('sl')[0])
        
        self.registerField("cfile",self.cfileEdit,"currentStringData")
        self.registerField("apikey",self.keyEdit,"currentStringData")
        self.registerField('destselect',self.destSelect,"currentIndex")
        
        #buttons
        cfileButton = QtGui.QPushButton("...")
        cfileButton.setToolTip('Select Config File')
        cfileButton.setBaseSize(100, 100)
        cfileButton.clicked.connect(self.selectConfFile)


        #grid
        grid = QtGui.QGridLayout()
        grid.setSpacing(10)
        
        grid.addWidget(cfileLabel, 1, 0)
        grid.addWidget(self.cfileEdit, 1, 2)
        grid.addWidget(cfileButton, 1, 3)        
        
        grid.addWidget(keyLabel, 2, 0)
        grid.addWidget(self.keyEdit, 2, 2)
        
        grid.addWidget(destLabel, 3, 0)
        grid.addWidget(self.destSelect, 3, 2)

        #layout       
        self.setLayout(grid)
        
    def selectConfFile(self):
        self.cfileEdit.setText(QtGui.QFileDialog.getOpenFileName())

    def nextId(self):
        return int(self.field('destselect').toString())
        
        
        #self.parent.setCentralWidget(self.parent.pgconf)
class PostgreSQLConfigPage(QtGui.QWizardPage):
    def __init__(self,parent=None):
        super(PostgreSQLConfigPage, self).__init__(parent)
        
        self.parent = parent 
        
        QtGui.QToolTip.setFont(QtGui.QFont('SansSerif', 10))
        
        
        #labels
        hostLabel = QtGui.QLabel('PostgreSQL Host')
        portLabel = QtGui.QLabel('PostgreSQL Port')
        dbnameLabel = QtGui.QLabel('PostgreSQL DB Name')
        schemaLabel = QtGui.QLabel('PostgreSQL DB Schema')
        usrLabel = QtGui.QLabel('Username')
        pwdLabel = QtGui.QLabel('Password')
        
        #edit boxes
        self.hostEdit = QtGui.QLineEdit('')
        self.portEdit = QtGui.QLineEdit('')
        self.dbnameEdit = QtGui.QLineEdit('')
        self.schemaEdit = QtGui.QLineEdit('')
        self.usrEdit = QtGui.QLineEdit('')
        self.pwdEdit = QtGui.QLineEdit('')
        

        #grid
        grid = QtGui.QGridLayout()
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
        

        
        
class MSSQLSpatialConfigPage(QtGui.QWizardPage):
    def __init__(self,parent=None):
        super(MSSQLSpatialConfigPage, self).__init__(parent)
        
        self.parent = parent 
        
        QtGui.QToolTip.setFont(QtGui.QFont('SansSerif', 10))
        
        #labels
        serverLabel = QtGui.QLabel('MSSQLSpatial Server')
        dbnameLabel = QtGui.QLabel('MSSQLSpatial DB Name')
        schemaLabel = QtGui.QLabel('MSSQLSpatial DB Schema')
        trustLabel = QtGui.QLabel('Trust')
        usrLabel = QtGui.QLabel('Username')
        pwdLabel = QtGui.QLabel('Password')
        
        #edit boxes
        self.serverEdit = QtGui.QLineEdit('')
        self.dbnameEdit = QtGui.QLineEdit('')
        self.schemaEdit = QtGui.QLineEdit('')
        self.trustEdit = QtGui.QLineEdit('')#make this a CB?
        self.usrEdit = QtGui.QLineEdit('')
        self.pwdEdit = QtGui.QLineEdit('')
        

        #grid
        grid = QtGui.QGridLayout()
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
        
class FileGDBConfigPage(QtGui.QWizardPage):
    def __init__(self,parent=None):
        super(FileGDBConfigPage, self).__init__(parent)
        
        self.parent = parent 
        
        QtGui.QToolTip.setFont(QtGui.QFont('SansSerif', 10))
        
        #labels
        fileLabel = QtGui.QLabel('FileGDB DB File')
        
        #edit boxes
        self.fileEdit = QtGui.QLineEdit('')#file selection dialog?
        
        #buttons
        fileButton = QtGui.QPushButton("...")
        fileButton.setToolTip('Select FileGDB File')
        fileButton.clicked.connect(self.selectFileGDBFile)
        
        
        #grid
        grid = QtGui.QGridLayout()
        grid.setSpacing(10)
        
        #layout                
        grid.addWidget(fileLabel,1,0)
        grid.addWidget(self.fileEdit,2,0)
        grid.addWidget(fileButton,2,3)
 
        
        self.setLayout(grid)  

    def selectFileGDBFile(self):
        self.fileEdit.setText(QtGui.QFileDialog.getOpenFileName())
        
    def nextId(self):
        return self.parent.plist.get('final')[0]
        
        
class SpatiaLiteConfigPage(QtGui.QWizardPage):
    def __init__(self,parent=None):
        super(SpatiaLiteConfigPage, self).__init__(parent)
        
        self.parent = parent 
        
        QtGui.QToolTip.setFont(QtGui.QFont('SansSerif', 10))
        
        #labels
        fileLabel = QtGui.QLabel('SpatiaLite DB File')
        
        #edit boxes
        self.fileEdit = QtGui.QLineEdit('')
        
        #buttons
        fileButton = QtGui.QPushButton("...")
        fileButton.setToolTip('Select SpatiaLite File')
        fileButton.clicked.connect(self.selectSpatiaLiteFile)
        

        #grid
        grid = QtGui.QGridLayout()
        grid.setSpacing(10)
        
        #layout
        grid.addWidget(fileLabel, 1, 0)
        grid.addWidget(self.fileEdit, 2, 0)
        grid.addWidget(fileButton, 2, 3)        
        
        #layout       
        vbox = QtGui.QVBoxLayout()
        vbox.addLayout(grid)     
        
        self.setLayout(vbox)  

    def selectSpatiaLiteFile(self):
        self.fileEdit.setText(QtGui.QFileDialog.getOpenFileName())
        
    def nextId(self):
        return self.parent.plist.get('final')[0]

class ConfirmationPage(QtGui.QWizardPage):
    def __init__(self,parent=None):
        super(ConfirmationPage, self).__init__(parent)
        
        QtGui.QToolTip.setFont(QtGui.QFont('SansSerif', 10))
        
        #labels
        label = QtGui.QLabel('PostgreSQL Host')
        
        vbox = QtGui.QVBoxLayout()
        vbox.addWidget(label)     
        self.setLayout(vbox)  
        
def main():
  
    app = QtGui.QApplication(sys.argv)
    #lds = LDSRepl()
    #lds.show()
    
    ldsc = LDSConfigWizard()
    ldsc.show()
    sys.exit(app.exec_())
    
    
if __name__ == '__main__':
    main()