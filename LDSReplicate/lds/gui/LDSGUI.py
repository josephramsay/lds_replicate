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
        
        self.controls = LDSControls(self)
        self.setCentralWidget(self.controls)
        
        self.statusbar = self.statusBar()
        self.statusbar.showMessage('Ready')
        
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
        
        menubar = self.menuBar()

        fileMenu = menubar.addMenu('&File')
        fileMenu.addAction(openAction)
        fileMenu.addSeparator()
        fileMenu.addAction(initUCAction)
        fileMenu.addAction(initLCAction)
        fileMenu.addSeparator()
        fileMenu.addAction(exitAction)

        helpMenu = menubar.addMenu('&Help')

    def launchEditor(self, checked=None):
        prefs = LDSPrefsEditor()
        prefs.setWindowTitle('LDS Preferences Editor')
        prefs.show()
        
    def runWizardAction(self):
        from lds.gui.MainConfigWizard import LDSConfigWizard
        #rather than readparams
        uconf = self.controls.confEdit.text()
        secname = self.controls.destMenu.itemText(self.controls.destMenu.currentIndex())
        
        self.statusbar.showMessage('Initialising User Config File')
        ldscw = LDSConfigWizard(uconf,secname)
        ldscw.exec_()
        
    def getTPParams(self):
        '''Init a new TP and return selected controls'''
        destination,layer,uconf,group,epsg,fe,te,fd,td,internal,init,clean = self.controls.readParameters()
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
        
        ldsc = LayerConfigSelector(tp,uconf,group,destination,self)
        ldsc.show()
        
        
class LDSControls(QFrame):
    
    GD_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../bin/gdal/gdal-data'))
    #GD_PATH = os.path.abspath('/home/jramsay/temp/ldsreplicate_builddir/32/bin/gdal/gdal-data/')
    def __init__(self,parent):
        super(LDSControls, self).__init__()
        self.parent = parent
        self.gpr = GUIPrefsReader()
        #read GDAL EPSG files, splitting by NZ and RestOfTheWorld
        gcs = ConfigInitialiser.readCSV(os.path.join(self.GD_PATH,'gcs.csv'))
        pcs = ConfigInitialiser.readCSV(os.path.join(self.GD_PATH,'pcs.csv'))
        self.nzlsr = [e[0]+' - '+e[3] for e in gcs if 'NZGD'     in e[1] or  'RSRGD'     in e[1]] \
                   + [e[0]+' - '+e[1] for e in pcs if 'NZGD'     in e[1] or  'RSRGD'     in e[1]]
        self.rowsr = [e[0]+' - '+e[3] for e in gcs if 'NZGD' not in e[1] and 'RSRGD' not in e[1]] \
                   + [e[0]+' - '+e[1] for e in pcs if 'NZGD' not in e[1] and 'RSRGD' not in e[1]]
        self.initUI()
        
    def initUI(self):
        
        # 0      1       2       3       4      5    6    7
        #'dest','layer','uconf','group','epsg','fd','td','int'
        defaults = ('','','','','','','','False')
        rlist = map(lambda x,y: y if x is None or len(x)==0 else x,self.gpr.read(),defaults)
        
        
        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        destLabel = QLabel('Destination')
        layerLabel = QLabel('Layer')
        groupLabel = QLabel('Group')
        epsgLabel = QLabel('EPSG')
        fromDateLabel = QLabel('From Date')
        toDateLabel = QLabel('To Date')
        
        cleanLabel = QLabel('Clean')
        internalLabel = QLabel('Internal')
        confLabel = QLabel('User Config')

        #edit boxes
        self.layerEdit = QLineEdit(rlist[1])
        self.layerEdit.setToolTip('Enter the layer you want to replicate using either v:x format or layer name')  
        self.layerEdit.textEdited.connect(self.doGroupDisable)  
        
        self.groupEdit = QLineEdit(rlist[3])
        self.groupEdit.setToolTip('Enter a layer keyword or use your own custom keyword to select a group of layers')  
        self.groupEdit.textEdited.connect(self.doLayerDisable) 
        
        #self.epsgEdit = QLineEdit(rlist[4])
        #self.epsgEdit.setToolTip('Setting an EPSG number here determines the output SR of the layer')  
        self.confEdit = QLineEdit(rlist[2])
        self.confEdit.setToolTip('Enter your user config file here')   
        
        self.epsgcombo = QComboBox()
        self.epsgcombo.setToolTip('Setting an EPSG number here determines the output SR of the layer')  
        self.epsgcombo.addItems(self.nzlsr)
        self.epsgcombo.insertSeparator(len(self.nzlsr))
        self.epsgcombo.addItems(self.rowsr)
        self.epsgcombo.setEditable(True)
        
        if LDSUtilities.mightAsWellBeNone(rlist[4])!=None:
            epsgedit = self.epsgcombo.lineEdit()
            epsgedit.setText([e for e in self.nzlsr+self.rowsr if re.match('^\s*(\d+).*',e).group(1)==rlist[4]][0])
        
        #menus
        self.destmenulist = ['',]+DataStore.DRIVER_NAMES.values()
        selecteddest = LDSUtilities.standardiseDriverNames(rlist[0])
        destindex = self.destmenulist.index('' if selecteddest is None else selecteddest)
        self.destMenu = QComboBox(self)
        self.destMenu.setToolTip('Choose the desired output type')   
        self.destMenu.addItems(self.destmenulist)
        self.destMenu.setCurrentIndex(destindex)
        #does it really make sense to do this since (for internal) we need a connection before connection parameters have been set up
        #self.destmenu.currentIndexChanged.connect(self.doDestMenuChanged)
        
       
        
        #date selection
        self.fromDateEdit = QDateEdit()
        if LDSUtilities.mightAsWellBeNone(rlist[5]) is not None:
            self.fromDateEdit.setDate(QDate(int(rlist[5][0:4]),int(rlist[5][5:7]),int(rlist[5][8:10])))
        else:
            early = DataStore.EARLIEST_INIT_DATE
            self.fromDateEdit.setDate(QDate(int(early[0:4]),int(early[5:7]),int(early[8:10])))
        self.fromDateEdit.setCalendarPopup(True)
        self.fromDateEdit.setEnabled(False)
        
        self.toDateEdit = QDateEdit()
        if LDSUtilities.mightAsWellBeNone(rlist[6]) is not None:
            self.toDateEdit.setDate(QDate(int(rlist[6][0:4]),int(rlist[6][5:7]),int(rlist[6][8:10]))) 
        else:
            today = DataStore.getCurrent()
            self.toDateEdit.setDate(QDate(int(today[0:4]),int(today[5:7]),int(today[8:10])))
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
        grid.addWidget(self.epsgcombo, 5, 2)

        grid.addWidget(fromDateLabel, 6, 0)
        grid.addWidget(self.fromDateEnable, 6, 1)
        grid.addWidget(self.fromDateEdit, 6, 2)
        
        grid.addWidget(toDateLabel, 7, 0)
        grid.addWidget(self.toDateEnable, 7, 1)
        grid.addWidget(self.toDateEdit, 7, 2)

        vbox1 = QHBoxLayout()
        vbox1.addStretch(1)
        vbox1.addWidget(internalLabel)
        vbox1.addWidget(self.internalTrigger)
        
        #vbox2 = QVBoxLayout()
        #vbox2.addStretch(1)
        #vbox2.addWidget(initLabel)
        #vbox2.addWidget(self.initTrigger)
        
        #vbox3 = QVBoxLayout()
        #vbox3.addStretch(1)
        #vbox3.addWidget(cleanLabel)
        #vbox3.addWidget(self.cleanTrigger)
        
        hbox3 = QHBoxLayout()
        hbox3.addStretch(1)
        hbox3.addLayout(vbox1)
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
        
    def doLayerDisable(self):
        self.layerEdit.setText('')
        
    def doGroupDisable(self):
        self.groupEdit.setText('')
        
    def doFromDateEnable(self):
        self.fromDateEdit.setEnabled(self.fromDateEnable.isChecked())
          
    def doToDateEnable(self):
        self.toDateEdit.setEnabled(self.toDateEnable.isChecked())  
          
    def readParameters(self):
        destination = str(self.destmenulist[self.destMenu.currentIndex()])
        layer = str(self.layerEdit.text())
        uconf = str(self.confEdit.text())
        group = str(self.groupEdit.text())
        epsg = re.match('^\s*(\d+).*',str(self.epsgcombo.lineEdit().text())).group(1)
        fe = self.fromDateEnable.isChecked()
        te = self.toDateEnable.isChecked()
        fd = None if fe is False else str(self.fromDateEdit.date().toString('yyyy-MM-dd'))
        td = None if te is False else str(self.toDateEdit.date().toString('yyyy-MM-dd'))
        internal = 'internal' if self.internalTrigger.isChecked() else 'external'
        init = self.initTrigger.isChecked()
        clean = self.cleanTrigger.isChecked()
        
        return destination,layer,uconf,group,epsg,fe,te,fd,td,internal,init,clean
    
    def doInitClickAction(self):
        '''Initialise the LC on LC-button-click, action'''
        self.parent.runLayerConfigAction()
        
    def doCleanClickAction(self):
        '''Read the provided parameters inserting true for the clean param'''
        params = self.readParameters()[:11]+(True,)
        self.runReplicationScript(params,'Clean')
        
    def doReplicateClickAction(self):
        '''Read the provided parameters inserting false for the clean param'''
        params = self.readParameters()[:11]+(False,)
        self.runReplicationScript(params,'Replicate')

    def runReplicationScript(self,params,message='Replicate'):
        '''Run the layer/group repliction script'''
        destination,layer,uconf,group,epsg,fe,te,fd,td,internal,init,clean = params

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
                                
        self.parent.statusbar.showMessage('Running '+message+' '+layer)

        #'dest','layer','uconf','group','epsg','fd','td','int'
        self.gpr.write((destination_driver,layer,uconf,group,epsg,fd,td,internal))        
        ldslog.info('dest='+destination_driver+', layer'+layer+', conf='+uconf+', group='+group+', epsg='+epsg)
        ldslog.info('fd='+str(fd)+', td='+str(td)+', fe='+str(fe)+', te='+str(te))
        ldslog.info('int='+str(internal)+', init='+str(init)+', clean='+str(clean))

        tp = TransferProcessor(layer, 
                               None if group is None else group, 
                               None if epsg is None else epsg, 
                               None if fd is None else fd, 
                               None if td is None else td,
                               None, None, None, 
                               None if uconf is None else uconf, 
                               internal)
        
        #NB init and clean are funcs because they appear as args, not opts in the CL
        if init:
            tp.setInitConfig()
            #if you are initialising probably want to do a layer select?
            #self.openLayerConfigSelector_THISSHOULDTHROWANERROR(tp,uconf,group,destination)
        if clean:
            tp.setCleanConfig()
            
        tp.processLDS(tp.initDestination(destination_driver))

        l_g = group if LDSUtilities.mightAsWellBeNone(group) is not None else (layer if LDSUtilities.mightAsWellBeNone(layer) is not None else 'layers')
        self.parent.statusbar.showMessage('{0} of {1} complete'.format(message,l_g))
        
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
            self.destMenu.setCurrentIndex(0)
            return
        #Init
        ldslog.warn('Reset Layer Config')
        self.doInitClickAction()
        
        
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