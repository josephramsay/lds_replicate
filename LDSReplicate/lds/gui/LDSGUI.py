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
                         QCheckBox, QMessageBox, 
                         QMainWindow, QAction, QIcon, QFrame,
                         QToolTip, QFont, QComboBox, QDateEdit, 
                         QPushButton, QDesktopWidget, QFileDialog, QTextEdit)
from PyQt4.QtCore import (QDate, QDir, Qt, QByteArray,
                          QEventLoop, QSize)

try:  
    from PyQt4.QtCore import QString  
except ImportError:  
    # we are using Python3 or QGis so QString is not defined  
    QString = str 
    
import os
import re
import sys
import subprocess
import gdal

# print 'CWD from ldsgui :',os.getcwd()
# print'exists tp',os.path.exists(os.path.join(os.getcwd(),'lds/TransferProcessor.py'))

from lds.TransferProcessor import LORG 
from lds.ReadConfig import GUIPrefsReader, MainFileReader
from lds.LDSUtilities import LDSUtilities as LU, ConfigInitialiser
from lds.gui.LQTUtilities import LQTUtilities as LQ
from lds.VersionUtilities import AppVersion
from lds.ConfigConnector import ConfigConnector, ProcessRunner, EndpointConnectionException, ConnectionConfigurationException
from lds.gui.LayerConfigSelector import LayerConfigSelector
from lds.DataStore import DataStore, MalformedConnectionString, DriverInitialisationException, DatasourceCreateException, DatasourceOpenException

ldslog = LU.setupLogging()

__version__ = AppVersion.getVersion()

IMG_LOC = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../img/'))
LINZ,OPEN,LC,UC,EXIT,HELP = ['{}/{}.png'.format(IMG_LOC,png) for png in ('linz_static','open','lc','uc','exit','help')]

class LDSMain(QMainWindow):
    '''This file (GUI functionality) has not been tested in any meaningful way and is likely to break on unexpected input'''

    HELPFILE = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../doc/README'))
    #            destname,lgval,uc, epsg, fd,td
    DEF_RVALS = ('',      '',   '','2193','','')
    
    MAX_WIZARD_ATTEMPTS = 2
    
    DUMMY_CONF = '_deleteme'
    
    WGEO = (300, 300, 350, 250)
    
    def __init__(self,initlc=False):        ####INITLC FOR TRACING ONLY DELETE IN PRODUCTION
        super(LDSMain, self).__init__()
        
        self.layers = None
        self.groups = None
        self.gpr = None
        self.gvs = None
        
        initcc = True
        #if init=False no point reading gpr vals
        wcount = 0
        while initcc and wcount<self.MAX_WIZARD_ATTEMPTS:
            try:
                self.initConfigConnector(initlc=initlc)
                initcc = False
                break
            except MalformedConnectionString as mcse:
                ldslog.warn('Connection String malformed or missing. '+str(mcse))
                self.runWizardDialog(None,None)
                wcount += 1
            except DriverInitialisationException as die:
                ldslog.warn('Cannot Initialise selected driver. '+str(die))
                self.runWizardDialog(None,None)
                wcount += 1
                #self.initConfigConnector(self.DEF_RVALS)
                #initcc = False
            except Exception as e:
                ldslog.error('Unhandled Exception in Config Setup. '+str(e))
                wcount += 1
        else:
            raise e

        self.setGeometry(*self.WGEO)
        self.setWindowTitle('LDS Data Replicator')
        self.setWindowIcon(QIcon(LINZ))
        
        self.statusbar = self.statusBar()
        self.statusbar.showMessage('Ready')
        
        self.controls = LDSControls(self)
        
        self.setCentralWidget(self.controls)
        
        openUCAction = QAction(QIcon(OPEN), 'Open &User Config', self)        
        openUCAction.setShortcut('Ctrl+U')
        openUCAction.setStatusTip('Open User Preferences Editor')
        openUCAction.triggered.connect(self.launchUCEditor)
        
        openLCAction = QAction(QIcon(OPEN), 'Open &Layer Config', self)        
        openLCAction.setShortcut('Ctrl+L')
        openLCAction.setStatusTip('Open Layer Config File (only applies to external LC storage)')
        openLCAction.triggered.connect(self.launchLCEditor)
        #self.enableLCEdit(openLCAction)
        
        initUCAction = QAction(QIcon(UC), 'Database &Setup Wizard', self)   
        initUCAction.setShortcut('Ctrl+S')
        initUCAction.setStatusTip('Open Database Setup Wizard')
        initUCAction.triggered.connect(self.runWizardAction)
        
        initLCAction = QAction(QIcon(LC), 'Layer &Config Editor', self)   
        initLCAction.setShortcut('Ctrl+C')
        initLCAction.setStatusTip('Open Layer Config Editor')
        initLCAction.triggered.connect(self.runLayerConfigAction)
        
        exitAction = QAction(QIcon(EXIT), '&Exit', self)        
        exitAction.setShortcut('Ctrl+E')
        exitAction.setStatusTip('Exit Application')
        exitAction.triggered.connect(self.close)#qApp.quit)
        
        helpAction = QAction(QIcon(HELP), '&Help', self)        
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
        return [x if LU.assessNone(x) else y for x,y in zip(self.gpr.read(),self.DEF_RVALS)]
        
    def initConfigConnector(self,gvs=None,initlc=None):
        '''Try to initialise the configconnector object'''
        self.gvs = gvs if gvs else self.updateFromGPR()
        try:
            uc = self.gvs[2]
            dst = self.gvs[0]
            self.confconn = ConfigConnector(self,uc,self.gvs[1],dst,initlc)
        except RuntimeError as rer:
            msg = 'Runtime error creating {} using "{}.conf" config file. {}'.format(self.gvs[0],uc,rer)
            self.errorEvent(msg)
        except UnicodeEncodeError as uee:
            msg = 'Unicode Encode Error, encode/sanitise input and try again. {}'.format(uee)
            self.errorEvent(msg)
            raise
        except UnicodeDecodeError as ude:
            msg = 'Unicode Decode Error, stored unicode value not being presented correctly. {}'.format(ude)
            #self.errorEvent(msg)
            raise
        except DatasourceCreateException as dce:
            msg = 'Cannot DS CREATE {} connection using "{}.conf" config file. Switching selected config and retrying. Please edit config or set new datasource; {}'.format(dst,uc,dce)
            self.switchDSSelection(dst)
            self.errorEvent(msg)
        except DatasourceOpenException as doe:
            msg = 'Cannot DS OPEN {} connection using "{}.conf" config file. Switching selected config and retrying. Please edit config or set new datasource; {}'.format(dst,uc,doe)
            self.switchDSSelection(dst)
            self.errorEvent(msg)
        except EndpointConnectionException as ece:
            msg = 'Cannot open {} connection using "{}.conf" config file. Internal error; {}'.format(dst,uc,ece)
            self.alertEvent(msg)    
            self.runWizardDialog(uc if uc else self.DUMMY_CONF,dst)
            os.remove(os.path.abspath(os.path.join(os.path.dirname(__file__),'../conf/',self.DUMMY_CONF+'.conf')))
        except ConnectionConfigurationException as cce:
            msg = 'Cannot open {} connection; {}'.format(dst,cce)
            self.alertEvent(msg)   
            self.runWizardDialog(uc if uc else self.DUMMY_CONF,dst)
            os.remove(os.path.abspath(os.path.join(os.path.dirname(__file__),'../conf/',self.DUMMY_CONF+'.conf')))
                

    def switchDSSelection(self,disconnected):
        #check next best configured DS configs
        trythis = [DataStore.DRIVER_NAMES[i] for i in ('fg','sl','pg','ms') 
                   if DataStore.DRIVER_NAMES[i] in self.gpr.getDestinations() 
                   and DataStore.DRIVER_NAMES[i] != disconnected][0]
        self.gpr.writesecline('prefs', 'dest', trythis)
        
    def enableLCEdit(self,control):
        '''Enable User Config read control for external configs'''
        dep = self.confconn.reg.openEndPoint(self.confconn.destname,self.confconn.uconf)
        control.setEnabled(dep.confwrap.readDSProperty(self.confconn.destname,'config')=='external')
        self.confconn.reg.closeEndPoint(self.confconn.destname)
        dep = None
        
    def launchUCEditor(self, checked=None):
        '''User Config Editor launch'''
        fn = LU.standardiseUserConfigName(str(self.controls.cflist[self.controls.confcombo.currentIndex()]))
        prefs = LDSPrefsEditor(fn,self)
        prefs.setWindowTitle('LDS Preferences Editor (User Config)')
        prefs.show()    
        
    def launchLCEditor(self, checked=None):
        '''Layer Config Editor launch'''
        fn = LU.standardiseLayerConfigName(str(self.controls.destlist[self.controls.destcombo.currentIndex()]))
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
        
        if not LU.assessNone(dest):
            self.controls.setStatus(self.controls.STATUS.IDLE,'Cannot open Layer-Config without defined Destination')
            return
            
        if self.confconn is None:
            #build a new confconn
            self.confconn = ConfigConnector(uconf,lgval,dest)
        else:
            #if any parameters have changed, re-initialise
            self.confconn.initConnections(uconf,lgval,dest)
            
        self.runLayerConfigDialog()
        
    def runLayerConfigDialog(self):
        '''Layer Config dialog opener'''
        ldscs = LayerConfigSelector(self)
        ldscs.show()
        
    def closeEvent(self, event, bypass=False):
        '''Close confirmation dialog'''
        reply = QMessageBox.question(self, 'Message', "Are you sure you want to quit?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()
            
    def errorEvent(self,msg):
        '''Display error message and close'''
        ldslog.error(msg)
        popup = QMessageBox.critical(self, 'Error',msg, QMessageBox.Close)
        self.close()
        
    def alertEvent(self,msg):
        '''Display message and continue'''
        ldslog.error(msg)
        popup = QMessageBox.warning(self, 'Attention',msg, QMessageBox.Ok)
        
class LDSControls(QFrame):
        
    STATIC_IMG = ('error_static.png','linz_static.png','busy_static.png','clean_static.png')
    ANIM_IMG   = ('error.gif','linz.gif','layer.gif','clean.gif')
    
    IMG_SPEED  = 100
    IMG_WIDTH  = 64
    IMG_HEIGHT = 64
    
    MAX_WD = 450
    
    GD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../bin/gdal/gdal-data'))
    STATUS = LU.enum('ERROR','IDLE','BUSY','CLEAN')
    
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
        #self.imgset = self.STATIC_IMG if self.parent.confconn.tp.src.confwrap.readDSProperty('Misc','indicator')=='static' else self.ANIM_IMG
        sep = self.parent.confconn.reg.openEndPoint(self.parent.confconn.SRCNAME,self.parent.confconn.uconf)
        self.imgset = self.STATIC_IMG if sep.confwrap.readDSProperty('Misc','indicator')=='static' else self.ANIM_IMG
        self.parent.confconn.reg.closeEndPoint(self.parent.confconn.SRCNAME)
        
    def initEPSG(self):
        '''Read GDAL EPSG files, splitting by NZ(RSR) and RestOfTheWorld'''

        gcsf = gdal.FindFile('gdal','gcs.csv') 
        if not gcsf:
            gcsf = os.path.join(self.GD_PATH,'gcs.csv')
        pcsf = gdal.FindFile('gdal','pcs.csv') 
        if not pcsf: 
            pcsf = os.path.join(self.GD_PATH,'pcs.csv')
        gcs = ConfigInitialiser.readCSV(gcsf)
        pcs = ConfigInitialiser.readCSV(pcsf)

        self.nzlsr = [(e[0],e[0]+' - '+e[3]) for e in gcs if 'NZGD'     in e[1] or  'RSRGD'     in e[1]] \
                   + [(e[0],e[0]+' - '+e[1]) for e in pcs if 'NZGD'     in e[1] or  'RSRGD'     in e[1]]
        self.rowsr = [(e[0],e[0]+' - '+e[3]) for e in gcs if 'NZGD' not in e[1] and 'RSRGD' not in e[1]] \
                   + [(e[0],e[0]+' - '+e[1]) for e in pcs if 'NZGD' not in e[1] and 'RSRGD' not in e[1]]
                   
                   
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
        self.sepindex = None
        #self.updateLGValues()
        
        self.epsgcombo = QComboBox(self)
        self.epsgcombo.setMaximumWidth(self.MAX_WD)
        self.epsgcombo.setToolTip('Setting an EPSG number here determines the output SR of the layer')  
        self.epsgcombo.addItems([i[1] for i in self.nzlsr])
        self.epsgcombo.insertSeparator(len(self.nzlsr))
        self.epsgcombo.addItems([i[1] for i in self.rowsr])
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
        self.lgcombo.currentIndexChanged.connect(self.doLGComboChanged)

        self.setStatus(self.STATUS.IDLE)
        
        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        
        #placement section ------------------------------------
        #---------+---------+--------+---------+--------
        # dest LB |         | dest DD
        # grp LB  |         | grp DD
        # conf LB |         | conf DD
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
        loc = os.path.abspath(os.path.join(IMG_LOC,self.imgset[status]))
        #loc = os.path.abspath(os.path.join(os.path.dirname(__file__),self.parent.IMG_LOC,self.imgset[status]))
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
        '''Re index LG combobox since a refreshLG call (new dest?) will usually mean new groups'''
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
        #TRACE#        
        #pdb.set_trace()
        sf = None
        try:
            self.parent.confconn.initConnections(uconf,lgval,dest)
        except Exception as e:
            sf=1
            ldslog.error('Error Updating UC Values. '+str(e))
            
        if sf:
            self.setStatus(self.STATUS.ERROR,'Error Updating UC Values', str(e))
        else:
            self.setStatus(self.STATUS.IDLE)
            
        self.refreshLGCombo()
        
    def centre(self):
        
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        
    
    def gprParameters(self,rdest):
        '''Zip default and GPR values'''
        return [x if LU.assessNone(x) else y for x,y in zip(self.parent.gpr.readsec(rdest),self.parent.DEF_RVALS[1:])]
    
    def getLCE(self,ln):
        '''Read layer parameters'''
        dep = self.parent.confconn.reg.openEndPoint(self.parent.confconn.destname,self.parent.confconn.uconf)
        #sep = self.parent.confconn.reg.openEndPoint('WFS',self.parent.confconn.uconf)
        self.parent.confconn.reg.setupLayerConfig(self.parent.confconn.tp,None,dep,initlc=False)
        lce = dep.getLayerConf().readLayerParameters(ln)
        #self.parent.confconn.reg.closeEndPoint('WFS')
        self.parent.confconn.reg.closeEndPoint(self.parent.confconn.destname)
        sep,dep = None,None
        return lce
    
    
    def doDestChanged(self):
        '''Read the destname parameter and fill dialog with matching GPR values'''
        rdest = str(self.destlist[self.destcombo.currentIndex()])
        rvals = self.gprParameters(rdest)
        self.updateGUIValues([rdest]+rvals)    
        
        
    def doConfChanged(self):
        '''Read the user conf parameter and fill dialog with matching GPR values'''
        rdest = str(self.destlist[self.destcombo.currentIndex()])
        rlg,_,rep,rfd,rtd = self.gprParameters(rdest)
        ruc = str(self.cflist[self.confcombo.currentIndex()])
        self.updateGUIValues((rdest,rlg,ruc,rep,rfd,rtd))
        
        
    def doLGComboChanged(self):
        '''Read the layer/group value and change epsg to layer or gpr match'''
        #get a matching LG entry and test whether its a layer or group
        #lgi = self.parent.confconn.getLayerGroupIndex(self.lgcombo.currentText().toUtf8().data())
        lgi = self.parent.confconn.getLayerGroupIndex(LQ.readWidgetText(self.lgcombo.currentText()))
        #lgi can be none if we init a new group, in which case we use the GPR value
        if lgi:
            lge = self.parent.confconn.lglist[lgi]
            lce = self.getLCE(lge[1]) if lge[0]==LORG.LAYER else None
        else:
            lce = None
        
        #look for filled layer conf epsg OR use prefs stored in gpr
        if lce and LU.assessNone(lce.epsg):
            epsgval = lce.epsg
        else:
            rdest = str(self.destlist[self.destcombo.currentIndex()])
            _,_,epsgval,_,_ = self.gprParameters(rdest)
        epsgindex = [i[0] for i in self.nzlsr+[(0,0)]+self.rowsr].index(epsgval)
        if self.epsgcombo.currentIndex() != epsgindex:
            self.epsgcombo.setCurrentIndex(int(epsgindex))

        
    def updateGUIValues(self,readlist):
        '''Fill dialog values from provided list'''
        #TODO. Remove circular references when setCurrentIndex() triggers do###Changed()
        #Read user input
        rdest,self.rlgval,ruconf,repsg,rfd,rtd = readlist
        
        #--------------------------------------------------------------------
        
        #Destination Menu
        selecteddest = LU.standardiseDriverNames(rdest)
        if selecteddest not in self.destlist:
            self.destlist = self.getConfiguredDestinations()
            self.destcombo.addItem(selecteddest)
        destindex = self.destlist.index(selecteddest) if selecteddest else 0
        
        if self.destcombo.currentIndex() != destindex:
            self.destcombo.setCurrentIndex(destindex)
        
        #InitButton
        self.initbutton.setText('Layer Select')
        
        #Config File
        confindex = 0
        if LU.assessNone(ruconf):
            ruconf = ruconf.split('.')[0]
            if ruconf not in self.cflist:
                self.cflist += [ruconf,]
                self.confcombo.addItem(ruconf)
            confindex = self.cflist.index(ruconf)
            
        if self.confcombo.currentIndex() != confindex:
            self.confcombo.setCurrentIndex(confindex)
        #self.confEdit.setText(ruconf if LU.assessNone(ruconf) else '')
        
        #Layer/Group Selection
        self.updateLGValues(ruconf,self.rlgval,rdest)
        lgindex = None
        if LU.assessNone(self.rlgval):
            #index of list value
            lgindex = self.parent.confconn.getLayerGroupIndex(self.rlgval,col=1)
            
        if LU.assessNone(lgindex):
            #advance by 1 for sep
            lgindex += 1 if lgindex>self.sepindex else 0 
        else:
            #using the separator index sets the combo to blank
            lgindex = self.sepindex
        if self.lgcombo.currentIndex() != lgindex:
            self.lgcombo.setCurrentIndex(lgindex)
        #self.doLGEditUpdate()
        
        #EPSG
        #                                user > layerconf
        #useepsg = LU.precedence(repsg, lce.epsg if lce else None, None)
        epsgindex = [i[0] for i in self.nzlsr+[(None,None)]+self.rowsr].index(repsg)
        if self.epsgcombo.currentIndex() != epsgindex:
            self.epsgcombo.setCurrentIndex(epsgindex)
            
        #epsgedit = self.epsgcombo.lineEdit()
        #epsgedit.setText([e[1] for e in self.nzlsr+self.rowsr if e[0]==repsg][0])
        
        #epsgedit.setText([e for e in self.nzlsr+self.rowsr if re.match('^\s*(\d+).*',e).group(1)==repsg][0])
        
        #To/From Dates
        if LU.assessNone(rfd):
            self.fromdateedit.setDate(QDate(int(rfd[0:4]),int(rfd[5:7]),int(rfd[8:10])))
        else:
            early = DataStore.EARLIEST_INIT_DATE
            self.fromdateedit.setDate(QDate(int(early[0:4]),int(early[5:7]),int(early[8:10])))
            
        if LU.assessNone(rtd):
            self.todateedit.setDate(QDate(int(rtd[0:4]),int(rtd[5:7]),int(rtd[8:10]))) 
        else:
            today = DataStore.getCurrent()
            self.todateedit.setDate(QDate(int(today[0:4]),int(today[5:7]),int(today[8:10])))
            
        #Internal/External CheckBox
#        if LU.assessNone(rint):
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
        destination = LU.assessNone(str(self.destlist[self.destcombo.currentIndex()]))
        #lgindex = self.parent.confconn.getLayerGroupIndex(self.lgcombo.currentText().toUtf8().data())
        lgindex = self.parent.confconn.getLayerGroupIndex(LQ.readWidgetText(self.lgcombo.currentText()))
        #NB need to test for None explicitly since zero is a valid index
        lgval = self.parent.confconn.lglist[lgindex][1] if LU.assessNone(lgindex) else None       
        #uconf = LU.standardiseUserConfigName(str(self.confcombo.lineEdit().text()))
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
            self.setStatus(self.STATUS.ERROR,'Error in Layer-Config',str(sys.exc_info()))#e))
        
    def doCleanClickAction(self):
        '''Set clean anim and run clean'''
        #lgo = self.lgcombo.currentText().toUtf8().data()
        lgo = LQ.readWidgetText(self.lgcombo.currentText())
        
        try:
            self.setStatus(self.STATUS.CLEAN,'Running Clean '+lgo)
            self.progressbar.setValue(0)
            self.runReplicationScript(True)
        except Exception as e:
            self.setStatus(self.STATUS.ERROR,'Failed Clean of '+lgo,str(sys.exc_info()))#e))
        
    def doReplicateClickAction(self):
        '''Set busy anim and run repl'''
        lgo = self.lgcombo.currentText()#.toUtf8().data()#only used for error messages
        try:
            self.setStatus(self.STATUS.BUSY,'Running Replicate '+lgo)
            self.progressbar.setValue(0)
            self.runReplicationScript(False)
            ldslog.debug('TRPfinish')
        except Exception as e:
            self.setStatus(self.STATUS.ERROR,'Failed Replication of '+lgo,str(sys.exc_info()))#e))

    def runReplicationScript(self,clean=False):
        '''Run the layer/group repliction script'''
        destination,lgval,uconf,epsg,fe,te,fd,td = self.readParameters()
        uconf_path = LU.standardiseUserConfigName(uconf)
        destination_path = LU.standardiseLayerConfigName(destination)
        destination_driver = LU.standardiseDriverNames(destination)

        if not os.path.exists(uconf_path):
            self.userConfMessage(uconf_path)
            return
        elif not MainFileReader(uconf_path).hasSection(destination_driver):
            self.userConfMessage(uconf_path,destination_driver)
            return
        #-----------------------------------------------------
        #'destname','layer','uconf','group','epsg','fd','td','int'
     
        self.parent.gpr.write((destination_driver,lgval,uconf,epsg,fd,td))        
        ldslog.info(u'dest={0}, lg={1}, conf={2}, epsg={3}'.format(destination_driver,lgval,uconf,epsg))
        ldslog.info('fd={0}, td={1}, fe={2}, te={3}'.format(fd,td,fe,te))
        lgindex = self.parent.confconn.getLayerGroupIndex(lgval,col=1)
        #lorg = self.parent.confconn.lglist[lgindex][0]
        #----------don't need lorg in TP anymore but it is useful for sorting/counting groups
        #self.parent.confconn.tp.setLayerOrGroup(lorg)
        self.parent.confconn.tp.setLayerGroupValue(lgval)
        if self.fromdateenable.isChecked(): self.parent.confconn.tp.setFromDate(fd)
        if self.todateenable.isChecked(): self.parent.confconn.tp.setToDate(td)
        self.parent.confconn.tp.setUserConf(uconf)
        if self.epsgenable: self.parent.confconn.tp.setEPSG(epsg)
        
        #because clean state persists in TP
        if clean:
            self.parent.confconn.tp.setCleanConfig()
        else:
            self.parent.confconn.tp.clearCleanConfig()
        #(re)initialise the data source since uconf may have changed
        #>>#self.parent.confconn.tp.src = self.parent.confconn.initSourceWrapper()
        #--------------------------
        ###ep = self.parent.confconn.reg.openEndPoint(self.parent.confconn.destname,self.parent.confconn.uconf)
        
        ###self.parent.confconn.reg.closeEndPoint(self.parent.confconn.destname)
        ###ep = None
        #Open ProcessRunner and run with TP(proc)/self(gui) instances
        #HACK temp add of dest_drv to PR call
        try:
            #TODO. Test for valid LC first
            self.tpr = ProcessRunner(self,destination_driver)
        except Exception as e:
            ldslog.error('Cannot create ProcessRunner {}. NB Possible missing Layer Config {}'.format(str(e),destination_path))
            self.layerConfMessage(destination_path)
            return
        #If PR has been successfully created we must vave a valid dst    
        ldslog.debug('TRPstart')
        self.tpr.start()
        
#     def quitProcessRunner(self):
#         self.tpr.join()
#         self.tpr.quit()
#         self.trp = None

        
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
            #self.destcombo.setCurrentIndex(0)
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