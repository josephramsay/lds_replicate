'''
v.0.0.9

LDSReplicate -  LDSGUI

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 13/02/2013

@author: jramsay
'''


from PyQt4.QtGui import (QApplication, QProgressBar, QLabel, QCursor,
                         QVBoxLayout, QHBoxLayout, QGridLayout, QMovie, QSizePolicy, 
                         QRegExpValidator, QCheckBox, QMessageBox, 
                         QMainWindow, QAction, QIcon, qApp, QFrame,
                         QLineEdit,QToolTip, QFont, QComboBox, QDateEdit, 
                         QPushButton, QDesktopWidget, QFileDialog, QTextEdit)
from PyQt4.QtCore import (QRegExp, QDate, QCoreApplication, QDir, Qt, QByteArray, 
                          QTimer, QEventLoop, QThread, QSize)


import os
import re
import sys
import subprocess
import gdal

from lds.TransferProcessor import TransferProcessor, LORG
from lds.ReadConfig import GUIPrefsReader, MainFileReader, LayerFileReader,LayerDSReader
from lds.LDSUtilities import LDSUtilities, ConfigInitialiser
from lds.VersionUtilities import AppVersion
from lds.gui.LayerConfigSelector import LayerConfigSelector
from lds.gui.ConfigConnector import ConfigConnector, ProcessRunner
from lds.ConfigWrapper import ConfigWrapper

from lds.DataStore import DataStore, MalformedConnectionString, DriverInitialisationException

ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()

class LDSMain(QMainWindow):
    '''This file (GUI functionality) has not been tested in any meaningful way and is likely to break on unexpected input'''
    
    HELPFILE = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../doc/README'))
    #destname,lgval,uc,epsg,fd,td
    DEF_RVALS = ('','','','2193','','')
    
    def __init__(self):
        super(LDSMain, self).__init__()
        
        self.layers = None
        self.groups = None
        self.gpr = None
        self.gvs = None
        
        initcc = True
        #if init=False no point reading gpr vals
        while initcc:
            try:
                self.initConfigConnector()
                initcc = False
            except MalformedConnectionString as mcse:
                ldslog.warn('Connection String malformed or missing. '+str(mcse))
                self.runWizardDialog(None,None)
            except DriverInitialisationException as die:
                ldslog.warn('Cannot Initialise selected driver. '+str(die))
                self.runWizardDialog(None,None)
                #self.initConfigConnector(self.DEF_RVALS)
                #initcc = False

        
        self.setGeometry(300, 300, 350, 250)
        self.setWindowTitle('LDS Data Replicator')
        
        self.statusbar = self.statusBar()
        self.statusbar.showMessage('Ready')
        
        self.controls = LDSControls(self)
        
        self.setCentralWidget(self.controls)
        
        openUCAction = QAction(QIcon('open.png'), 'Open &User Config', self)        
        openUCAction.setShortcut('Ctrl+U')
        openUCAction.setStatusTip('Open User Preferences Editor')
        openUCAction.triggered.connect(self.launchUCEditor)
        
        openLCAction = QAction(QIcon('open.png'), 'Open &Layer Config', self)        
        openLCAction.setShortcut('Ctrl+L')
        openLCAction.setStatusTip('Open Layer Config File (only applies to external LC storage)')
        openLCAction.triggered.connect(self.launchLCEditor)
        
        initUCAction = QAction(QIcon('uc.png'), 'Database &Setup Wizard', self)   
        initUCAction.setShortcut('Ctrl+S')
        initUCAction.setStatusTip('Open Database Setup Wizard')
        initUCAction.triggered.connect(self.runWizardAction)
        
        initLCAction = QAction(QIcon('lc.png'), 'Layer &Config Editor', self)   
        initLCAction.setShortcut('Ctrl+C')
        initLCAction.setStatusTip('Open Layer Config Editor')
        initLCAction.triggered.connect(self.runLayerConfigAction)
        
        exitAction = QAction(QIcon('exit.png'), '&Exit', self)        
        exitAction.setShortcut('Ctrl+E')
        exitAction.setStatusTip('Exit Application')
        exitAction.triggered.connect(qApp.quit)
        
        helpAction = QAction(QIcon('help.png'), '&Help', self)        
        helpAction.setShortcut('Ctrl+H')
        helpAction.setStatusTip('Open Help Document')
        helpAction.triggered.connect(self.launchHelpFile)
        
        self.menubar = self.menuBar()

        fileMenu = self.menubar.addMenu('&File')
        fileMenu.addAction(openUCAction)
        fileMenu.addAction(openLCAction)
        fileMenu.addSeparator()
        fileMenu.addAction(initUCAction)
        fileMenu.addAction(initLCAction)
        fileMenu.addSeparator()
        fileMenu.addAction(exitAction)

        helpMenu = self.menubar.addMenu('&Help')
        helpMenu.addAction(helpAction)

    def updateFromGPR(self):
        '''Read GPR file for changes or init'''
        if not self.gpr: 
            self.gpr = GUIPrefsReader()
        return [x if LDSUtilities.mightAsWellBeNone(x) else y for x,y in zip(self.gpr.read(),self.DEF_RVALS)]
        
    def initConfigConnector(self,gvs=None):
        self.gvs = gvs if gvs else self.updateFromGPR()
        self.confconn = ConfigConnector(self,self.gvs[2],self.gvs[1],self.gvs[0])
        
    def launchUCEditor(self, checked=None):
        fn = LDSUtilities.standardiseUserConfigName(str(self.controls.cflist[self.controls.confcombo.currentIndex()]))
        prefs = LDSPrefsEditor(fn,self)
        prefs.setWindowTitle('LDS Preferences Editor (User Config)')
        prefs.show()    
        
    def launchLCEditor(self, checked=None):
        fn = LDSUtilities.standardiseLayerConfigName(str(self.controls.destlist[self.controls.destcombo.currentIndex()]))
        prefs = LDSPrefsEditor(fn,self)
        prefs.setWindowTitle('LDS Preferences Editor (Layer Config)')
        prefs.show()
        
    def launchHelpFile(self):
        if os.name == 'nt':
            #windows
            from lds.WinUtilities import WinUtilities
            WinUtilities.callStartFile(self.HELPFILE)
        elif os.name == 'posix':
            #posix
            subprocess.Popen(['xdg-open', self.HELPFILE])

        
    def runWizardAction(self):
        '''Init and run the User Config setup wizz'''
        self.controls.setStatus(self.controls.STATUS.BUSY,'Opening User-Config Wizard')  
        #rather than readparams
        uconf = self.controls.confcombo.itemText(self.controls.confcombo.currentIndex())
        secname = self.controls.destcombo.itemText(self.controls.destcombo.currentIndex())
        self.statusbar.showMessage('Editing User-Config')
        self.runWizardDialog(uconf, secname)
        #update the gui with new dest data
        self.initConfigConnector()
        self.controls.updateGUIValues(self.gvs)

        
    def runWizardDialog(self,uconf,secname):
        '''User Config/Wizz dialog opener'''
        from lds.gui.MainConfigWizard import LDSConfigWizard
        ldscw = LDSConfigWizard(uconf,secname,self)
        ldscw.exec_()
        ldscw = None
        
    
    def runLayerConfigAction(self):
        '''Arg-less action to open a new layer config dialog'''        
        dest,lgval,uconf,_,_,_,_,_ = self.controls.readParameters()
        if not LDSUtilities.mightAsWellBeNone(dest):
            self.controls.setStatus(self.controls.STATUS.IDLE,'Cannot open Layer-Config without defined Destination')
            return
            
        if self.confconn is None:
            #if any parameters have changed, re-initialise
            self.confconn = ConfigConnector(uconf,lgval,dest)
        else:
            self.confconn.initConnections(uconf,lgval,dest)
            
        self.runLayerConfigDialog()
        
    def runLayerConfigDialog(self):
        '''Layer Config dialog opener'''
        ldscs = LayerConfigSelector(self)
        ldscs.show()
        
    def closeEvent(self, event):
        reply = QMessageBox.question(self, 'Message', "Are you sure to quit?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()
        
class LDSControls(QFrame):
        
    STATIC_IMG = ('error_static.png','linz_static.png','busy_static.png','clean_static.png')
    ANIM_IMG   = ('error.gif','linz.gif','layer.gif','clean.gif')
    
    IMG_SPEED  = 100
    IMG_WIDTH  = 64
    IMG_HEIGHT = 64
    
    MAX_WD = 450
    
    GD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../bin/gdal/gdal-data'))
    STATUS = LDSUtilities.enum('ERROR','IDLE','BUSY','CLEAN')
    
    def __init__(self,parent):
        super(LDSControls, self).__init__()
        self.parent = parent
        self.initConf()
        self.initEPSG()
        self.initUI()
        
    def initConf(self):
        '''Read files in conf dir ending in conf'''
        self.cflist = ConfigInitialiser.getConfFiles()
        #self.imgset = self.STATIC_IMG if ConfigWrapper().readDSProperty('Misc','indicator')=='static' else self.ANIM_IMG
        self.imgset = self.STATIC_IMG if self.parent.confconn.tp.src.mainconf.readDSProperty('Misc','indicator')=='static' else self.ANIM_IMG
        
    def initEPSG(self):
        '''Read GDAL EPSG files, splitting by NZ(RSR) and RestOfTheWorld'''
        gcs = ConfigInitialiser.readCSV(gdal.FindFile('gdal','gcs.csv'))
        pcs = ConfigInitialiser.readCSV(gdal.FindFile('gdal','pcs.csv'))

        #gcs = ConfigInitialiser.readCSV(os.path.join(self.GD_PATH,'gcs.csv'))
        #pcs = ConfigInitialiser.readCSV(os.path.join(self.GD_PATH,'pcs.csv'))
        self.nzlsr = [e[0]+' - '+e[3] for e in gcs if 'NZGD'     in e[1] or  'RSRGD'     in e[1]] \
                   + [e[0]+' - '+e[1] for e in pcs if 'NZGD'     in e[1] or  'RSRGD'     in e[1]]
        self.rowsr = [e[0]+' - '+e[3] for e in gcs if 'NZGD' not in e[1] and 'RSRGD' not in e[1]] \
                   + [e[0]+' - '+e[1] for e in pcs if 'NZGD' not in e[1] and 'RSRGD' not in e[1]]
                   
                   
    def initUI(self):
        
        # 0      1          2       3       4       5      6    7    8
        #'destname','lgselect','layer','uconf','group','epsg','fd','td','int'
        
        #self.rdest,rlgselect,self.rlayer,ruconf,self.rgroup,repsg,rfd,rtd,rint = readlist 
        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        destLabel = QLabel('Destination')
        lgLabel = QLabel('Group/Layer')
        epsgLabel = QLabel('EPSG')
        fromDateLabel = QLabel('From Date')
        toDateLabel = QLabel('To Date')
        confLabel = QLabel('User Config')
        
        self.view = QLabel() 
        self.view.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.view.setAlignment(Qt.AlignCenter)

        self.confcombo = QComboBox(self)
        self.confcombo.setToolTip('Enter your user config name (file) here')
        self.confcombo.addItems(self.cflist)
        self.confcombo.setEditable(False)
        #self.confcombo.currentIndexChanged.connect(self.doLGEditUpdate)
        
        #combos
        self.lgcombo = QComboBox(self)
        self.lgcombo.setMaximumWidth(self.MAX_WD)
        self.lgcombo.setDuplicatesEnabled(False)
        #self.lgcombo.setInsertPolicy(QComboBox.InsertAlphabetically)#?doesnt seem to work
        self.lgcombo.setToolTip('Select either Layer or Group entry')
        self.lgcombo.setEditable(False)
        #self.lgcombo.currentIndexChanged.connect(self.doLGEditUpdate)
        self.sepindex = None
        #self.updateLGValues()
        
        self.epsgcombo = QComboBox(self)
        self.epsgcombo.setMaximumWidth(self.MAX_WD)
        self.epsgcombo.setToolTip('Setting an EPSG number here determines the output SR of the layer')  
        self.epsgcombo.addItems(self.nzlsr)
        self.epsgcombo.insertSeparator(len(self.nzlsr))
        self.epsgcombo.addItems(self.rowsr)
        self.epsgcombo.setEditable(True)
        self.epsgcombo.setEnabled(False)
        
        self.destlist = self.getConfiguredDestinations()
        self.destcombo = QComboBox(self)
        self.destcombo.setToolTip('Choose the desired output type')   
        self.destcombo.setEditable(False)
        self.destcombo.addItems(self.destlist)

        #date selection
        self.fromdateedit = QDateEdit()
        self.fromdateedit.setCalendarPopup(True)
        self.fromdateedit.setEnabled(False)
        
        self.todateedit = QDateEdit()
        self.todateedit.setCalendarPopup(True)
        self.todateedit.setEnabled(False)
        
        #check boxes
        self.epsgenable = QCheckBox()
        self.epsgenable.setCheckState(False)
        self.epsgenable.clicked.connect(self.doEPSGEnable)       
        
        self.fromdateenable = QCheckBox()
        self.fromdateenable.setCheckState(False)
        self.fromdateenable.clicked.connect(self.doFromDateEnable)
        
        self.todateenable = QCheckBox()
        self.todateenable.setCheckState(False) 
        self.todateenable.clicked.connect(self.doToDateEnable)
        
        self.progressbar = QProgressBar()
        self.progressbar.setRange(0,100)
        self.progressbar.setVisible(True)
        self.progressbar.setMinimumWidth(self.MAX_WD)
        
        
        #buttons        
        self.initbutton = QPushButton("waiting")
        self.initbutton.setToolTip('Initialise the Layer Configuration')
        self.initbutton.clicked.connect(self.doInitClickAction)
        
        self.cleanbutton = QPushButton("Clean")
        self.cleanbutton.setToolTip('Clean the selected layer/group from local storage')
        self.cleanbutton.clicked.connect(self.doCleanClickAction)
        
        self.replicatebutton = QPushButton("Replicate")
        self.replicatebutton.setToolTip('Execute selected replication')
        self.replicatebutton.clicked.connect(self.doReplicateClickAction)
        
        self.cancelbutton = QPushButton("Close")
        self.cancelbutton.setToolTip('Close the LDS Replicate application')       
        self.cancelbutton.clicked.connect(self.parent.close)


        #set dialog values using GPR
        self.updateGUIValues(self.parent.gvs)
        
        #set onchange here otherwise we get circular initialisation
        self.destcombo.currentIndexChanged.connect(self.doDestChanged)
        self.confcombo.currentIndexChanged.connect(self.doConfChanged)

        self.setStatus(self.STATUS.IDLE)
        
        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        
        #placement section ------------------------------------
        #---------+---------+--------+---------+--------
        # dest LB           | dest DD
        # grp LB            | grp DD
        # conf LB           | conf DD
        # epsg L  | epsg CB | epsg DD
        # f dt L  | f dt CB | f dt DD
        # t td L  | t td CB | t td DD
        # icon    |       <- progress ->
        # layer B | <- . -> |repl B  | clean B | close B 
        #---------+---------+--------+---------+--------

        grid.addWidget(destLabel, 1, 0)
        grid.addWidget(self.destcombo, 1, 2)

        #grid.addWidget(layerLabel, 2, 0)
        grid.addWidget(lgLabel, 2, 0)
        grid.addWidget(self.lgcombo, 2, 2)
        
        grid.addWidget(confLabel, 3, 0)
        grid.addWidget(self.confcombo, 3, 2)
        
        #grid.addWidget(groupLabel, 4, 0)
        #grid.addWidget(self.groupEdit, 4, 2)
        
        grid.addWidget(epsgLabel, 5, 0)
        grid.addWidget(self.epsgenable, 5, 1)
        grid.addWidget(self.epsgcombo, 5, 2)

        grid.addWidget(fromDateLabel, 6, 0)
        grid.addWidget(self.fromdateenable, 6, 1)
        grid.addWidget(self.fromdateedit, 6, 2)
        
        grid.addWidget(toDateLabel, 7, 0)
        grid.addWidget(self.todateenable, 7, 1)
        grid.addWidget(self.todateedit, 7, 2)
        
        hbox3 = QHBoxLayout()
        hbox3.addWidget(self.view) 
        hbox3.addStretch(1)
        hbox3.addWidget(self.progressbar)

        #hbox3.addLayout(vbox2)
        #hbox3.addLayout(vbox3)
        
        hbox4 = QHBoxLayout()
        hbox4.addWidget(self.initbutton)
        hbox4.addStretch(1)
        hbox4.addWidget(self.replicatebutton)
        hbox4.addWidget(self.cleanbutton)
        hbox4.addWidget(self.cancelbutton)
        

        vbox = QVBoxLayout()
        #vbox.addStretch(1)
        vbox.addLayout(grid)
        vbox.addLayout(hbox3)
        vbox.addLayout(hbox4)
        
        self.setLayout(vbox)  
       
    #def setProgress(self,pct):
    #    self.progressbar.setValue(pct)
        
    def setStatus(self,status,message='',tooltip=None):
        '''Sets indicator icon and statusbar message'''
        self.parent.statusbar.showMessage(message)
        self.parent.statusbar.setToolTip(tooltip if tooltip else '')

        #progress
        loc = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../img/',self.imgset[status]))
        self.progressbar.setVisible(status in (self.STATUS.BUSY, self.STATUS.CLEAN))
        
        #icon
        anim = QMovie(loc, QByteArray(), self)
        anim.setScaledSize(QSize(self.IMG_WIDTH,self.IMG_HEIGHT))
        anim.setCacheMode(QMovie.CacheAll)
        anim.setSpeed(self.IMG_SPEED)
        self.view.clear()
        self.view.setMovie(anim)
        anim.start()

        self.view.repaint()
        QApplication.processEvents(QEventLoop.AllEvents)

    def mainWindowEnable(self,enable=True):
        cons = (self.lgcombo, self.confcombo, self.destcombo, 
                self.initbutton, self.replicatebutton, self.cleanbutton, self.cancelbutton,
                self.epsgenable,self.fromdateenable,self.todateenable,
                self.parent.menubar)
        for c in cons:
            c.setEnabled(enable)
            
        if enable:
            self.epsgcombo.setEnabled(self.epsgenable.checkState())
            self.fromdateedit.setEnabled(self.fromdateenable.checkState())
            self.todateedit.setEnabled(self.todateenable.checkState())
        else:
            self.epsgcombo.setEnabled(False)
            self.fromdateedit.setEnabled(False)
            self.todateedit.setEnabled(False)
            
            
        QApplication.restoreOverrideCursor() if enable else QApplication.setOverrideCursor(QCursor(Qt.WaitCursor)) 

                
            
    def refreshLGCombo(self):
        self.lgcombo.clear()
        self.lgcombo.addItems([i[2] for i in self.parent.confconn.lglist])
        #NOTE the separator consumes an index, if not clearing the combobox selectively remove the old sepindex (assumes g preceeds l)
        #if self.sepindex:
        #    self.lgcombo.removeItem(self.sepindex)
        self.sepindex = [i[0] for i in self.parent.confconn.lglist].count(LORG.GROUP)
        self.lgcombo.insertSeparator(self.sepindex)
        
    def updateLGValues(self,uconf,lgval,dest):
        '''Sets the values displayed in the Layer/Group combo'''
        #because we cant seem to sort combobox entries and want groups at the top, clear and re-add
        self.parent.confconn.initConnections(uconf,lgval,dest)
        self.refreshLGCombo()
        
    def centre(self):
        
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        
  
    def getInitLabel(self,rdest):
        '''Returns a name for the init/layer button.'''
        #for internal DS too much overhead intialising a new data connection so we default to "init"
        if LDSUtilities.mightAsWellBeNone(rdest) and LayerFileReader(rdest.lower()+TransferProcessor.LP_SUFFIX).exists():
                    return 'Layer Select'
        return 'Initalise'
    
    def gprParameters(self,rdest):
        '''Zip default and GPR values'''
        return [x if LDSUtilities.mightAsWellBeNone(x) else y for x,y in zip(self.parent.gpr.readsec(rdest),self.parent.DEF_RVALS[1:])]
    
    def doDestChanged(self):
        '''Read the destname parameter and fill dialog with matching GPR values'''
        rdest = str(self.destlist[self.destcombo.currentIndex()])
        rvals = self.gprParameters(rdest)
        self.updateGUIValues([rdest]+rvals)    
        
    def doConfChanged(self):
        '''Read the destname parameter and fill dialog with matching GPR values'''
        rdest = str(self.destlist[self.destcombo.currentIndex()])
        rlg,_,rep,rfd,rtd = self.gprParameters(rdest)
        ruc = str(self.cflist[self.confcombo.currentIndex()])
        self.updateGUIValues((rdest,rlg,ruc,rep,rfd,rtd))
        
    def updateGUIValues(self,readlist):
        '''Fill dialog values from provided list'''
        #Note. rlgval must be an object var since its used in doaction function
        rdest,self.rlgval,ruconf,repsg,rfd,rtd = readlist
        
        
        #Destination Menu
        selecteddest = LDSUtilities.standardiseDriverNames(rdest)
        if selecteddest not in self.destlist:
            self.destlist = self.getConfiguredDestinations()
            self.destcombo.addItem(selecteddest)
        destindex = self.destlist.index(selecteddest) if selecteddest else 0
        self.destcombo.setCurrentIndex(destindex)
        
        #InitButton
        #ilabel = self.getInitLabel(selecteddest)
        self.initbutton.setText('Layer Select')#ilabel)
        
        #Config File
        confindex = 0
        if LDSUtilities.mightAsWellBeNone(ruconf):
            confindex = self.cflist.index(ruconf.split('.')[0])
        self.confcombo.setCurrentIndex(confindex)
        #self.confEdit.setText(ruconf if LDSUtilities.mightAsWellBeNone(ruconf) else '')
        
        #Layer/Group Selection
        
        self.updateLGValues(ruconf,self.rlgval,rdest)
        lgindex = None
        if LDSUtilities.mightAsWellBeNone(self.rlgval):
            lgindex = self.parent.confconn.getLGIndex(self.rlgval,col=1)
            
        if lgindex:
            #advance by 1 for sep
            lgindex += 1 if lgindex>self.sepindex else 0 
        else:
            #using the separator index sets the combo to blank
            lgindex = self.sepindex
        self.lgcombo.setCurrentIndex(lgindex)
        #self.doLGEditUpdate()
        
        #EPSG
        if LDSUtilities.mightAsWellBeNone(repsg)!=None:
            epsgedit = self.epsgcombo.lineEdit()
            epsgedit.setText([e for e in self.nzlsr+self.rowsr if re.match('^\s*(\d+).*',e).group(1)==repsg][0])
        
        #To/From Dates
        if LDSUtilities.mightAsWellBeNone(rfd) is not None:
            self.fromdateedit.setDate(QDate(int(rfd[0:4]),int(rfd[5:7]),int(rfd[8:10])))
        else:
            early = DataStore.EARLIEST_INIT_DATE
            self.fromdateedit.setDate(QDate(int(early[0:4]),int(early[5:7]),int(early[8:10])))
            
        if LDSUtilities.mightAsWellBeNone(rtd) is not None:
            self.todateedit.setDate(QDate(int(rtd[0:4]),int(rtd[5:7]),int(rtd[8:10]))) 
        else:
            today = DataStore.getCurrent()
            self.todateedit.setDate(QDate(int(today[0:4]),int(today[5:7]),int(today[8:10])))
            
        #Internal/External CheckBox
#        if LDSUtilities.mightAsWellBeNone(rint) is not None:
#            self.internalTrigger.setChecked(rint.lower()==DataStore.CONF_INT)
#        else:
#            self.internalTrigger.setChecked(DataStore.DEFAULT_CONF==DataStore.CONF_INT)
        
        
    def getConfiguredDestinations(self):
        defml = ['',]+DataStore.DRIVER_NAMES.values()
        return [d for d in self.parent.gpr.getDestinations() if d in defml]
        
    def doEPSGEnable(self):
        self.epsgcombo.setEnabled(self.epsgenable.isChecked())
        
    def doFromDateEnable(self):
        self.fromdateedit.setEnabled(self.fromdateenable.isChecked())
          
    def doToDateEnable(self):
        self.todateedit.setEnabled(self.todateenable.isChecked())  
          
    def readParameters(self):
        '''Read values out of dialogs'''
        destination = LDSUtilities.mightAsWellBeNone(str(self.destlist[self.destcombo.currentIndex()]))
        lgindex = self.parent.confconn.getLGIndex(str(self.lgcombo.currentText()))
        #NB need to test for None explicitly since zero is a valid index
        lgval = self.parent.confconn.lglist[lgindex][1] if LDSUtilities.mightAsWellBeNone(lgindex) is not None else None       
        #uconf = LDSUtilities.standardiseUserConfigName(str(self.confcombo.lineEdit().text()))
        #uconf = str(self.confcombo.lineEdit().text())
        uconf = str(self.cflist[self.confcombo.currentIndex()])
        ee = self.epsgenable.isChecked()
        epsg = None if ee is False else re.match('^\s*(\d+).*',str(self.epsgcombo.lineEdit().text())).group(1)
        fe = self.fromdateenable.isChecked()
        te = self.todateenable.isChecked()
        fd = None if fe is False else str(self.fromdateedit.date().toString('yyyy-MM-dd'))
        td = None if te is False else str(self.todateedit.date().toString('yyyy-MM-dd'))
        
        return destination,lgval,uconf,epsg,fe,te,fd,td
    
    def doInitClickAction(self):
        '''Initialise the LC on LC-button-click, action'''
        try:
            try:
                self.setStatus(self.STATUS.BUSY,'Opening Layer-Config Editor')  
                self.progressbar.setValue(0)
                self.parent.runLayerConfigAction()
            finally:
                self.setStatus(self.STATUS.IDLE,'Ready')
        except Exception as e:
            self.setStatus(self.STATUS.ERROR,'Error in Layer-Config',str(e))
        
    def doCleanClickAction(self):
        '''Set clean anim and run clean'''
        lgo = str(self.lgcombo.currentText())
        
        try:
            self.setStatus(self.STATUS.CLEAN,'Running Clean '+lgo)
            self.progressbar.setValue(0)
            self.runReplicationScript(True)
        except Exception as e:
            self.setStatus(self.STATUS.ERROR,'Failed Clean of '+lgo,str(e))
        
    def doReplicateClickAction(self):
        '''Set busy anim and run repl'''
        lgo = str(self.lgcombo.currentText())
        try:
            self.setStatus(self.STATUS.BUSY,'Running Replicate '+lgo)
            self.progressbar.setValue(0)
            self.runReplicationScript(False)
        except Exception as e:
            self.setStatus(self.STATUS.ERROR,'Failed Replication of '+lgo,str(e))

    def runReplicationScript(self,clean=False):
        '''Run the layer/group repliction script'''
        destination,lgval,uconf,epsg,fe,te,fd,td = self.readParameters()

        uconf_path = LDSUtilities.standardiseUserConfigName(uconf)
        destination_path = LDSUtilities.standardiseLayerConfigName(destination)
        destination_driver = LDSUtilities.standardiseDriverNames(destination)
        
        if not os.path.exists(uconf_path):
            self.userConfMessage(uconf_path)
            return
        elif not MainFileReader(uconf_path).hasSection(destination_driver):
            self.userConfMessage(uconf_path,destination_driver)
            return
        
        if not self.parent.confconn.dst.getLayerConf().exists():
            self.layerConfMessage(destination_path)
            return
  
        #-----------------------------------------------------

        #'destname','layer','uconf','group','epsg','fd','td','int'
     
        self.parent.gpr.write((destination_driver,lgval,uconf,epsg,fd,td))        
        ldslog.info('dest={0}, lg={1}, conf={2}, epsg={3}'.format(destination_driver,lgval,uconf,epsg))
        ldslog.info('fd={0}, td={1}, fe={2}, te={3}'.format(str(fd),str(td),str(fe),str(te)))

        lgindex = self.parent.confconn.getLGIndex(lgval,col=1)
        #lorg = self.parent.confconn.lglist[lgindex][0]
        #----------don't need lorg in TP anymore but it is useful for sorting/counting groups
        #self.parent.confconn.tp.setLayerOrGroup(lorg)
        self.parent.confconn.tp.setLayerGroupValue(lgval)
        self.parent.confconn.tp.setFromDate(fd)
        self.parent.confconn.tp.setToDate(td)
        self.parent.confconn.tp.setUserConf(uconf)
        
        #because clean state persists in TP
        if clean:
            self.parent.confconn.tp.setCleanConfig()
        else:
            self.parent.confconn.tp.clearCleanConfig()
            
        #initialise the data source since uconf may have changed
        self.parent.confconn.tp.src = self.parent.confconn.tp.initSource()
        
        #Open ProcessRunner and run with TP(proc)/self(gui) instances
        #HACK temp add of dest_drv to PR call
        self.tpr = ProcessRunner(self,destination_driver)
        self.tpr.start()
        
    def quitProcessRunner(self):
        self.tpr.join()
        self.tpr.quit()
        self.trp = None

        
    def userConfMessage(self,uconf,secname=None):
        ucans = QMessageBox.warning(self, 'User Config Missing/Incomplete', 
                                'Specified User-Config file, '+str(uconf)+' does not exist' if secname is None else 'User-Config file does not contain '+str(secname)+' section', 
                                'Back','Initialise User Config')
        if not ucans:
            #Retry
            ldslog.warn('Retry specifying UC')
            #self.confcombo.setCurrentIndex(0)
            return
        #Init
        ldslog.warn('Reset User Config Wizard')
        self.parent.runWizardAction()


    def layerConfMessage(self,dest):
        lcans = QMessageBox.warning(self, 'Layer Config Missing', 
                                'Required Layer-Config file, '+str(dest)+' does not exist', 
                                'Back','Run Layer Select')
        if not lcans:
            #Retry
            ldslog.warn('Retry specifying LC')
            self.destcombo.setCurrentIndex(0)
            return
        #Init
        ldslog.warn('Reset Layer Config')
        self.doInitClickAction()
        
        
#--------------------------------------------------------------------------------------------------

class LDSPrefsEditor(QMainWindow):
    
    def __init__(self,filename,parent):
        super(LDSPrefsEditor, self).__init__()
        
        self.parent = parent
        self.filename = filename
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
        
        
        with open(self.filename, 'r') as f:
            filedata = f.read()
        self.editor.textedit.setText(filedata)
        self.statusbar.showMessage('Editing '+self.filename)
        
        self.initUI()
        
    def initUI(self):
        self.setGeometry(350,350,800,600)
        self.show() 
        
    def saveAsFile(self):
        filename = QFileDialog.getSaveFileName(self, 'Save File As', os.path.join(os.getcwd(),'../conf/'))#os.getenv('HOME'))
        filedata = self.editor.textedit.toPlainText()
        with open(filename, 'w') as f:
            f.write(filedata)
        
    def saveFile(self):
        filedata = self.editor.textedit.toPlainText()
        with open(self.filename, 'w') as f:
            f.write(filedata)

        
    def openFile(self):
        f=QDir.Filter(1)
        
        filedialog = QFileDialog()
        filedialog.setFilter(f)
        self.filename = filedialog.getOpenFileName(self, 'Open File', os.path.join(os.getcwd(),'../conf/'))#os.getenv('HOME'))
        with open(self.filename, 'r') as f:
            filedata = f.read()
        self.editor.textedit.setText(filedata)
        self.statusbar.showMessage('Editing '+self.filename)

        
class LDSPrefsFrame(QFrame):
    
    def __init__(self,parent):
        super(LDSPrefsFrame, self).__init__()
        self.parent = parent
        self.initUI()
        
    def initUI(self):

        #edit boxes
        self.textedit = QTextEdit() 
        
        vbox = QVBoxLayout()
        vbox.addWidget(self.textedit)
        
        self.setLayout(vbox)  

def main():

    app = QApplication(sys.argv)
    lds = LDSMain()
    lds.show()
    sys.exit(app.exec_())
    
    
if __name__ == '__main__':
    main()