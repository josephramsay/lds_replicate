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
                         QVBoxLayout, QHBoxLayout, QGridLayout, QMovie, QSizePolicy, 
                         QRegExpValidator, QCheckBox, QMessageBox, 
                         QMainWindow, QAction, QIcon, qApp, QFrame,
                         QLineEdit,QToolTip, QFont, QComboBox, QDateEdit, 
                         QPushButton, QDesktopWidget, QFileDialog, QTextEdit)
from PyQt4.QtCore import (QRegExp, QDate, QCoreApplication, QDir, Qt, QByteArray, QTimer, QEventLoop)

import os
import re
import sys
import subprocess

from lds.TransferProcessor import TransferProcessor
from lds.ReadConfig import GUIPrefsReader, MainFileReader
from lds.LDSUtilities import LDSUtilities, ConfigInitialiser
from lds.VersionUtilities import AppVersion

from lds.DataStore import DataStore

ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()

class LDSRepl(QMainWindow):
    '''This file (GUI functionality) has not been tested in any meaningful way and is likely to break on unexpected input'''
  
    def __init__(self):
        super(LDSRepl, self).__init__()
        
        self.setGeometry(300, 300, 350, 250)
        self.setWindowTitle('LDS Data Replicator')
        
        self.statusbar = self.statusBar()
        self.statusbar.showMessage('Ready')
        
        self.controls = LDSControls(self)
        self.setCentralWidget(self.controls)

        openAction = QAction(QIcon('open.png'), '&Open', self)        
        openAction.setShortcut('Ctrl+O')
        openAction.setStatusTip('Open Prefs Editor')
        openAction.triggered.connect(self.launchEditor)
        
        initUCAction = QAction(QIcon('uc.png'), '&User Wizard', self)   
        initUCAction.setShortcut('Ctrl+U')
        initUCAction.setStatusTip('Open User Config Wizard')
        initUCAction.triggered.connect(self.runWizardAction)
        
        initLCAction = QAction(QIcon('lc.png'), '&Layer Config', self)   
        initLCAction.setShortcut('Ctrl+L')
        initLCAction.setStatusTip('Open Layer Config Editor')
        initLCAction.triggered.connect(self.runLayerConfigAction)
        
        exitAction = QAction(QIcon('exit.png'), '&Exit', self)        
        exitAction.setShortcut('Ctrl+Q')
        exitAction.setStatusTip('Exit Application')
        exitAction.triggered.connect(qApp.quit)
        
        helpAction = QAction(QIcon('help.png'), '&Help', self)        
        helpAction.setShortcut('Ctrl+H')
        helpAction.setStatusTip('Open Help Document')
        helpAction.triggered.connect(self.launchHelpFile)
        
        menubar = self.menuBar()

        fileMenu = menubar.addMenu('&File')
        fileMenu.addAction(openAction)
        fileMenu.addSeparator()
        fileMenu.addAction(initUCAction)
        fileMenu.addAction(initLCAction)
        fileMenu.addSeparator()
        fileMenu.addAction(exitAction)

        helpMenu = menubar.addMenu('&Help')
        helpMenu.addAction(helpAction)

    def launchEditor(self, checked=None):
        prefs = LDSPrefsEditor(self)
        prefs.setWindowTitle('LDS Preferences Editor')
        prefs.show()
        
    def launchHelpFile(self):
        helpfile = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../doc/README'))
        if os.name == 'nt':
            #windows
            os.startfile(helpfile)
        elif os.name == 'posix':
            #posix
            subprocess.Popen(['xdg-open', helpfile])

        
    def runWizardAction(self):
        from lds.gui.MainConfigWizard import LDSConfigWizard
        #rather than readparams
        uconf = self.controls.confEdit.text()
        secname = self.controls.destmenu.itemText(self.controls.destmenu.currentIndex())
        
        self.statusbar.showMessage('Editing User-Config')
        ldscw = LDSConfigWizard(uconf,secname,self)
        ldscw.exec_()
        
    def getTPParams(self):
        '''Init a new TP and return selected controls'''
        destination,lgopt,layer,uconf,group,epsg,fe,te,fd,td,internal = self.controls.readParameters()
        tp = TransferProcessor(layer, 
                               None if group is None else group, 
                               None if epsg is None else epsg, 
                               None if fd is None else fd, 
                               None if td is None else td,
                               None, None, None, 
                               None if uconf is None else uconf, 
                               internal)
        
        return tp,uconf,group,destination
    
    #def getLayerConf(self):
    #    tp,uconf,group,destination = self.getTPParams()
    #    dst = tp.initDestination(destination)
    #    lc = tp.getNewLayerConf(dst)
        
    def runLayerConfigAction(self):
        '''Open a new Layer dialog'''
        from lds.gui.LayerConfigSelector import LayerConfigSelector
        self.statusbar.showMessage('Editing Layer Config')                  
        tp,uconf,group,destination = self.getTPParams()
        #nedd a valid dest (to write <dest>.layer.properties) and uconf
        if LDSUtilities.mightAsWellBeNone(destination) is not None: # Also need? and LDSUtilities.mightAsWellBeNone(uconf) is not None?
            ldsc = LayerConfigSelector(tp,uconf,group,destination,self)
            ldsc.show()
        else:
            self.controls.setStatus(self.controls.STATUS.IDLE,'Cannot open Layer-Config without defined Destination')

        
    def closeEvent(self, event):
        reply = QMessageBox.question(self, 'Message', "Are you sure to quit?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()     
        
        
class LDSControls(QFrame):
        
    STATIC_IMG = ('linz_static.png','clean_static.png','busy_static.png')
    ANIM_IMG = ('linz.gif','clean.gif','busy.gif')
    IMG = STATIC_IMG
    
    GD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../bin/gdal/gdal-data'))
    STATUS = LDSUtilities.enum('IDLE','BUSY','CLEAN')
    LGOPTS = ('Layer','Group')
    
    DEF_RVALS = ('','Layer','','','','2193','','','False')
    #GD_PATH = os.path.abspath('/home/jramsay/temp/ldsreplicate_builddir/32/bin/gdal/gdal-data/')
    def __init__(self,parent):
        super(LDSControls, self).__init__()
        self.parent = parent
        self.gpr = GUIPrefsReader()
        self.initEPSG()
        self.initUI()
        
    def initEPSG(self):
        '''Read GDAL EPSG files, splitting by NZ(RSR) and RestOfTheWorld'''
        gcs = ConfigInitialiser.readCSV(os.path.join(self.GD_PATH,'gcs.csv'))
        pcs = ConfigInitialiser.readCSV(os.path.join(self.GD_PATH,'pcs.csv'))
        self.nzlsr = [e[0]+' - '+e[3] for e in gcs if 'NZGD'     in e[1] or  'RSRGD'     in e[1]] \
                   + [e[0]+' - '+e[1] for e in pcs if 'NZGD'     in e[1] or  'RSRGD'     in e[1]]
        self.rowsr = [e[0]+' - '+e[3] for e in gcs if 'NZGD' not in e[1] and 'RSRGD' not in e[1]] \
                   + [e[0]+' - '+e[1] for e in pcs if 'NZGD' not in e[1] and 'RSRGD' not in e[1]]
                   
                   
    def initUI(self):
        
        # 0      1          2       3       4       5      6    7    8
        #'dest','lgselect','layer','uconf','group','epsg','fd','td','int'
        
        #self.rdest,rlgselect,self.rlayer,ruconf,self.rgroup,repsg,rfd,rtd,rint = readlist 
        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        destLabel = QLabel('Destination')
        epsgLabel = QLabel('EPSG')
        fromDateLabel = QLabel('From Date')
        toDateLabel = QLabel('To Date')
        internalLabel = QLabel('Internal')
        confLabel = QLabel('User Config')
        
        self.view = QLabel() 
        self.view.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.view.setAlignment(Qt.AlignCenter)

        #edit boxes
        self.confEdit = QLineEdit()
        self.confEdit.setToolTip('Enter your user config file here')   
        
        self.lgcombo = QComboBox()
        self.lgcombo.setToolTip('Select either Layer or Group entry')  
        self.lgcombo.addItems(self.LGOPTS)
        self.lgcombo.setEditable(False)
        self.lgcombo.currentIndexChanged.connect(self.doLGEditUpdate)
        
        self.lgEdit = QLineEdit()
        
        self.epsgcombo = QComboBox()
        self.epsgcombo.setToolTip('Setting an EPSG number here determines the output SR of the layer')  
        self.epsgcombo.addItems(self.nzlsr)
        self.epsgcombo.insertSeparator(len(self.nzlsr))
        self.epsgcombo.addItems(self.rowsr)
        self.epsgcombo.setEditable(True)
        
        #menus
        self.destmenulist = ['',]+DataStore.DRIVER_NAMES.values()
        self.destmenu = QComboBox(self)
        self.destmenu.setToolTip('Choose the desired output type')   
        self.destmenu.addItems(self.destmenulist)

        #date selection
        self.fromDateEdit = QDateEdit()
        self.fromDateEdit.setCalendarPopup(True)
        self.fromDateEdit.setEnabled(False)
        
        self.toDateEdit = QDateEdit()
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
        
        
        #buttons
        initButton = QPushButton("Initialise")
        initButton.setToolTip('Initialise the Layer Configuration')
        initButton.clicked.connect(self.doInitClickAction)
        
        cleanButton = QPushButton("Clean")
        cleanButton.setToolTip('Clean the selected layer/group from local storage')
        cleanButton.clicked.connect(self.doCleanClickAction)
        
        replicateButton = QPushButton("Replicate")
        replicateButton.setToolTip('Execute selected replication')
        replicateButton.clicked.connect(self.doReplicateClickAction)
        
        cancelButton = QPushButton("Close")
        cancelButton.setToolTip('Close the LDS Replicate application')       
        cancelButton.clicked.connect(self.parent.close)


        #set dialog values
        readlist = map(lambda x,y: y if x is None or len(x)==0 else x,self.gpr.read(),self.DEF_RVALS)
        self.updateGUIValues(readlist)
        #set onchange here otherwise we get circular initialisation
        self.destmenu.currentIndexChanged.connect(self.doDestChanged)

        self.setStatus(self.STATUS.IDLE)
        
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
        grid.addWidget(self.destmenu, 1, 2)

        #grid.addWidget(layerLabel, 2, 0)
        grid.addWidget(self.lgcombo, 2, 0)
        grid.addWidget(self.lgEdit, 2, 2)
        
        grid.addWidget(confLabel, 3, 0)
        grid.addWidget(self.confEdit, 3, 2)
        
        #grid.addWidget(groupLabel, 4, 0)
        #grid.addWidget(self.groupEdit, 4, 2)
        
        grid.addWidget(epsgLabel, 5, 0)
        grid.addWidget(self.epsgcombo, 5, 2)

        grid.addWidget(fromDateLabel, 6, 0)
        grid.addWidget(self.fromDateEnable, 6, 1)
        grid.addWidget(self.fromDateEdit, 6, 2)
        
        grid.addWidget(toDateLabel, 7, 0)
        grid.addWidget(self.toDateEnable, 7, 1)
        grid.addWidget(self.toDateEdit, 7, 2)
        
        hbox3 = QHBoxLayout()
        hbox3.addWidget(self.view)
        hbox3.addStretch(1)
        hbox3.addWidget(internalLabel)
        hbox3.addWidget(self.internalTrigger)
        #hbox3.addLayout(vbox2)
        #hbox3.addLayout(vbox3)
        
        hbox4 = QHBoxLayout()
        hbox4.addWidget(initButton)
        hbox4.addStretch(1)
        hbox4.addWidget(replicateButton)
        hbox4.addWidget(cleanButton)
        hbox4.addWidget(cancelButton)
        

        vbox = QVBoxLayout()
        #vbox.addStretch(1)
        vbox.addLayout(grid)
        vbox.addLayout(hbox3)
        vbox.addLayout(hbox4)
        
        
        self.setLayout(vbox)  
       
        
    def setStatus(self,status,message=''):
        self.parent.statusbar.showMessage(message)
        
        if status is self.STATUS.BUSY:
            loc = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../img/',self.IMG[2]))
        elif status is self.STATUS.CLEAN:
            loc = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../img/',self.IMG[1]))
        elif status is self.STATUS.IDLE:
            loc = os.path.abspath(os.path.join(os.path.dirname(__file__),'../../img/',self.IMG[0]))
        else:
            ldslog.warn('Unknown Status')
            return
        
        anim = QMovie(loc, QByteArray(), self)
        anim.setCacheMode(QMovie.CacheAll)
        anim.setSpeed(50)
        self.view.clear()
        self.view.setMovie(anim)
        anim.start()

        self.view.repaint()
        QApplication.processEvents(QEventLoop.AllEvents)

        
    def centre(self):
        
        qr = self.frameGeometry()
        cp = QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())
        
  
    def doDestChanged(self):
        '''Read the dest parameter and fill dialog with matching GPR values'''
        rdest = str(self.destmenulist[self.destmenu.currentIndex()])
        rvals = map(lambda x,y: y if x is None or len(x)==0 else x,self.gpr.readsec(rdest),self.DEF_RVALS[1:])
        self.updateGUIValues([rdest]+rvals)
        
    def updateGUIValues(self,readlist):
        '''Fill dialog values from provided list'''
        #Note. rlayer and rgroup must be object vars since they're used in doaction function
        rdest,rlgselect,self.rlayer,ruconf,self.rgroup,repsg,rfd,rtd,rint = readlist 
        
        #Destination Meun
        selecteddest = LDSUtilities.standardiseDriverNames(rdest)
        destindex = self.destmenulist.index('' if selecteddest is None else selecteddest)
        self.destmenu.setCurrentIndex(destindex)
        
        #Config File
        self.confEdit.setText(ruconf)
        
        #Layer/Group Selection
        lgindex = self.LGOPTS.index(rlgselect)
        self.lgcombo.setCurrentIndex(lgindex)
        self.doLGEditUpdate()
        
        #EPSG
        if LDSUtilities.mightAsWellBeNone(repsg)!=None:
            epsgedit = self.epsgcombo.lineEdit()
            epsgedit.setText([e for e in self.nzlsr+self.rowsr if re.match('^\s*(\d+).*',e).group(1)==repsg][0])
        
        #To/From Dates
        if LDSUtilities.mightAsWellBeNone(rfd) is not None:
            self.fromDateEdit.setDate(QDate(int(rfd[0:4]),int(rfd[5:7]),int(rfd[8:10])))
        else:
            early = DataStore.EARLIEST_INIT_DATE
            self.fromDateEdit.setDate(QDate(int(early[0:4]),int(early[5:7]),int(early[8:10])))
            
        if LDSUtilities.mightAsWellBeNone(rtd) is not None:
            self.toDateEdit.setDate(QDate(int(rtd[0:4]),int(rtd[5:7]),int(rtd[8:10]))) 
        else:
            today = DataStore.getCurrent()
            self.toDateEdit.setDate(QDate(int(today[0:4]),int(today[5:7]),int(today[8:10])))
            
        #Internal/External CheckBox
        self.internalTrigger.setCheckState(rint.lower()=='internal')
        
        
    def doLGEditUpdate(self):
        lgopt = self.lgcombo.currentText()
        if lgopt == 'Layer':
            self.lgEdit.setText(self.rlayer)
            self.lgEdit.setToolTip('Enter the Layer you want to replicate using either v:x### format or a valid layer name') 
        elif lgopt == 'Group':
            self.lgEdit.setText(self.rgroup)
            self.lgEdit.setToolTip('Enter an LDS keyword or use your own custom keyword to select a group of layers')  
    
#    def doLayerDisable(self):
#        self.layerEdit.setText('')
#        
#    def doGroupDisable(self):
#        self.groupEdit.setText('')
        
    def doFromDateEnable(self):
        self.fromDateEdit.setEnabled(self.fromDateEnable.isChecked())
          
    def doToDateEnable(self):
        self.toDateEdit.setEnabled(self.toDateEnable.isChecked())  
          
    def readParameters(self):
        destination = str(self.destmenulist[self.destmenu.currentIndex()])
        lgopt = str(self.lgcombo.currentText())
        #FIXME this only needs to be one param not two (or is it more efficient to split them here?)
        if lgopt == 'Layer':
            layer = str(self.lgEdit.text())
            group = None
        elif lgopt == 'Group':
            layer = None
            group = str(self.lgEdit.text())
            
        uconf = str(self.confEdit.text())
        epsg = re.match('^\s*(\d+).*',str(self.epsgcombo.lineEdit().text())).group(1)
        fe = self.fromDateEnable.isChecked()
        te = self.toDateEnable.isChecked()
        fd = None if fe is False else str(self.fromDateEdit.date().toString('yyyy-MM-dd'))
        td = None if te is False else str(self.toDateEdit.date().toString('yyyy-MM-dd'))
        internal = 'internal' if self.internalTrigger.isChecked() else 'external'
        
        return destination,lgopt,layer,uconf,group,epsg,fe,te,fd,td,internal
    
    def doInitClickAction(self):
        '''Initialise the LC on LC-button-click, action'''
        self.setStatus(self.STATUS.BUSY,'Opening Layer-Config Editor')
        self.parent.runLayerConfigAction()
        #self.setStatus(self.STATUS.IDLE,'Editing Layer-Config')
        
    def doCleanClickAction(self):
        '''Set clean anim and run clean'''
        lgo = str(self.lgcombo.currentText())
        self.setStatus(self.STATUS.CLEAN,'Running Clean '+lgo)
        self.runReplicationScript(True)
        
        #self.setStatus(self.STATUS.IDLE,'Clean '+lgo+' Complete')
        
    def doReplicateClickAction(self):
        '''Set busy anim and run repl'''
        lgo = str(self.lgcombo.currentText())
        self.setStatus(self.STATUS.BUSY,'Running Replicate '+lgo)
        self.runReplicationScript(False)
        
        #self.setStatus(self.STATUS.IDLE,'Replicate '+lgo+' Complete')

    def runReplicationScript(self,clean=False):
        '''Run the layer/group repliction script'''
        destination,lgopt,layer,uconf,group,epsg,fe,te,fd,td,internal = self.readParameters()

        uconf = LDSUtilities.standardiseUserConfigName(uconf)
        destination_path = LDSUtilities.standardiseLayerConfigName(destination)
        destination_driver = LDSUtilities.standardiseDriverNames(destination)
        
        if not os.path.exists(uconf):
            self.userConfMessage(uconf)
            return
        elif not MainFileReader(uconf).hasSection(destination_driver):
            self.userConfMessage(uconf,destination_driver)
            return
        

        if not os.path.exists(destination_path):
            self.layerConfMessage(destination_path)
            return
  
        #-----------------------------------------------------

        #'dest','layer','uconf','group','epsg','fd','td','int'
     
        self.gpr.write((destination_driver,lgopt,layer,uconf,group,epsg,fd,td,internal))        
        ldslog.info('dest={0}, lg={1}, layer={2}, conf={3}, group={4}, epsg={5}'.format(destination_driver,lgopt,str(layer),uconf,str(group),epsg))
        ldslog.info('fd={0}, td={1}, fe={2}, te={3}, int={4}'.format(str(fd),str(td),str(fe),str(te),str(internal)))

        tp = TransferProcessor(layer, 
                               None if group is None else group, 
                               None if epsg is None else epsg, 
                               None if fd is None else fd, 
                               None if td is None else td,
                               None, None, None, 
                               None if uconf is None else uconf, 
                               internal)

        if clean:
            tp.setCleanConfig()
            
        tp.processLDS(tp.initDestination(destination_driver))
        
        self.setStatus(self.STATUS.IDLE,('Clean' if clean else 'Replicate')+' Complete')

        
    def userConfMessage(self,uconf,secname=None):
        ucans = QMessageBox.warning(self, 'User Config Missing/Incomplete', 
                                'Specified User-Config file, '+str(uconf)+' does not exist' if secname is None else 'User-Config file does not contain '+str(secname)+' section', 
                                'Back','Initialise User Config')
        if not ucans:
            #Retry
            ldslog.warn('Retry specifying UC')
            self.confEdit.setText('')
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
            self.destmenu.setCurrentIndex(0)
            return
        #Init
        ldslog.warn('Reset Layer Config')
        self.doInitClickAction()
        
        
#--------------------------------------------------------------------------------------------------

class LDSPrefsEditor(QMainWindow):
    
    def __init__(self,parent):
        super(LDSPrefsEditor, self).__init__()
        
        self.parent = parent
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