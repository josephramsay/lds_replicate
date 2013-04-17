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

from PyQt4.QtGui import (QApplication)


import sys


from lds.LDSUtilities import LDSUtilities
from lds.VersionUtilities import AppVersion



ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()




def lconf():
    #func to call layer config selector
    from lds.gui.LayerTableConfigSelector import LayerTableConfigSelector
    app = QApplication(sys.argv)
    ldsc = LayerTableConfigSelector()
    ldsc.show()
    sys.exit(app.exec_())         
        
def conf():
    #func to call config wizz
    from lds.gui.MainConfigWizard import LDSConfigWizard
    app = QApplication(sys.argv)
    ldsc = LDSConfigWizard()
    ldsc.show()
    sys.exit(app.exec_()) 
    
def main():
    from  lds.gui.LDSGUI import LDSRepl
    app = QApplication(sys.argv)
    lds = LDSRepl()
    lds.show()
    sys.exit(app.exec_())
    
    
if __name__ == '__main__':
    main()
    
    

#class LDSRepl(QMainWindow):
#    '''This file (GUI functionality) has not been tested in any meaningful way and is likely to break on unexpected input'''
#  
#    def __init__(self):
#        super(LDSRepl, self).__init__()
#        
#        self.setGeometry(300, 300, 350, 250)
#        self.setWindowTitle('LDS Data Replicator')
#        
#        self.controls = LDSControls(self)
#        self.setCentralWidget(self.controls)
#        
#        self.statusbar = self.statusBar()
#        self.statusbar.showMessage('Ready')
#        
#        openAction = QAction(QIcon('open.png'), '&Open', self)        
#        openAction.setShortcut('Ctrl+O')
#        openAction.setStatusTip('Open Prefs Editor')
#        openAction.triggered.connect(self.launchEditor)
#        
#        exitAction = QAction(QIcon('exit.png'), '&Exit', self)        
#        exitAction.setShortcut('Ctrl+Q')
#        exitAction.setStatusTip('Exit Application')
#        exitAction.triggered.connect(qApp.quit)
#        
#        menubar = self.menuBar()
#
#        fileMenu = menubar.addMenu('&File')
#        fileMenu.addAction(openAction)
#        fileMenu.addSeparator()
#        fileMenu.addAction(exitAction)
#
#        helpMenu = menubar.addMenu('&Help')
#
#    def launchEditor(self, checked=None):
#        prefs = LDSPrefsEditor()
#        prefs.setWindowTitle('LDS Preferences Editor')
#        prefs.show() 
#    
#class LDSControls(QFrame):
#    
#    def __init__(self,parent):
#        super(LDSControls, self).__init__()
#        self.parent = parent
#        self.gpr = GUIPrefsReader()
#        self.initUI()
#        
#    def initUI(self):
#        
#        # 0      1       2       3       4      5    6    7
#        #'dest','layer','uconf','group','epsg','fd','td','int'
#        defaults = ('','','ldsincr.conf','','','','','True')
#        rlist = map(lambda x,y: y if x is None or len(x)==0 else x,self.gpr.read(),defaults)
#        
#        
#        
#        QToolTip.setFont(QFont('SansSerif', 10))
#        
#        #labels
#        destLabel = QLabel('Destination')
#        layerLabel = QLabel('Layer')
#        groupLabel = QLabel('Group')
#        epsgLabel = QLabel('EPSG')
#        fromDateLabel = QLabel('From Date')
#        toDateLabel = QLabel('To Date')
#        
#        initLabel = QLabel('Initialise')
#        cleanLabel = QLabel('Clean')
#        internalLabel = QLabel('Internal')
#        confLabel = QLabel('User Config')
#
#        #edit boxes
#        self.layerEdit = QLineEdit(rlist[1])
#        self.layerEdit.setToolTip('Enter the layer you want to replicate using either v:x format or layer name')   
#        self.groupEdit = QLineEdit(rlist[3])
#        self.groupEdit.setToolTip('Enter a layer keyword or use your own custom keyword to select a group of layers')   
#        self.epsgEdit = QLineEdit(rlist[4])
#        self.epsgEdit.setToolTip('Setting an EPSG number here determines the output SR of the layer')   
#        self.confEdit = QLineEdit(rlist[2])
#        self.confEdit.setToolTip('Enter your user config file here')   
#        
#        #menus
#        self.destmenulist = ('',PG.DRIVER_NAME,MS.DRIVER_NAME,FG.DRIVER_NAME,SL.DRIVER_NAME) 
#        self.destMenu = QComboBox(self)
#        self.destMenu.setToolTip('Choose the desired output type')   
#        self.destMenu.addItems(self.destmenulist)
#        self.destMenu.setCurrentIndex(self.destmenulist.index(rlist[0]))
#        
#       
#        
#        #date selection
#        self.fromDateEdit = QDateEdit()
#        if LDSUtilities.mightAsWellBeNone(rlist[5]) is not None:
#            self.fromDateEdit.setDate(QDate(int(rlist[5][0:4]),int(rlist[5][5:7]),int(rlist[5][8:10]))) 
#        self.fromDateEdit.setCalendarPopup(True)
#        self.fromDateEdit.setEnabled(False)
#        
#        self.toDateEdit = QDateEdit()
#        if LDSUtilities.mightAsWellBeNone(rlist[6]) is not None:
#            self.toDateEdit.setDate(QDate(int(rlist[6][0:4]),int(rlist[6][5:7]),int(rlist[6][8:10]))) 
#        self.toDateEdit.setCalendarPopup(True)
#        self.toDateEdit.setEnabled(False)
#        
#        #check boxes
#        self.fromDateEnable = QCheckBox()
#        self.fromDateEnable.setCheckState(False)
#        self.fromDateEnable.clicked.connect(self.doFromDateEnable)
#
#        
#        self.toDateEnable = QCheckBox()
#        self.toDateEnable.setCheckState(False) 
#        self.toDateEnable.clicked.connect(self.doToDateEnable)
#        
#        self.internalTrigger = QCheckBox()
#        self.internalTrigger.setToolTip('Sets where layer config settings are stored, external/internal')   
#        self.internalTrigger.setCheckState(rlist[7]=='True')
#        
#        self.initTrigger = QCheckBox()
#        self.initTrigger.setToolTip('Re writes the layer config settings (you need to do this on first run)')   
#        self.initTrigger.setCheckState(False)
#        
#        self.cleanTrigger = QCheckBox()
#        self.cleanTrigger.setToolTip('Instead of replicating, this deletes the layer chosen above')   
#        self.cleanTrigger.setCheckState(False)
#        
#        
#        #buttons
#        okButton = QPushButton("OK")
#        okButton.setToolTip('Execute selected replication')
#        okButton.clicked.connect(self.doOkClickAction)
#        
#        cancelButton = QPushButton("Cancel")
#        cancelButton.setToolTip('Cancel LDS Replicate')       
#        cancelButton.clicked.connect(QCoreApplication.instance().quit) 
#
#        #grid
#        grid = QGridLayout()
#        grid.setSpacing(10)
#        
#        
#        #placement section ------------------------------------
#        
#        #-------------+----------------
#        #   dst label |   dst dropdown
#        # layer label | layer dropdown
#        # ...
#        #-------------+--+------+------
#        #           opt1 | opt2 | opt3
#        #----------------+----+-+------
#        #                  ok | cancel
#        #---------------------+--------
#
#        grid.addWidget(destLabel, 1, 0)
#        grid.addWidget(self.destMenu, 1, 2)
#
#        grid.addWidget(layerLabel, 2, 0)
#        grid.addWidget(self.layerEdit, 2, 2)
#        
#        grid.addWidget(confLabel, 3, 0)
#        grid.addWidget(self.confEdit, 3, 2)
#        
#        grid.addWidget(groupLabel, 4, 0)
#        grid.addWidget(self.groupEdit, 4, 2)
#        
#        grid.addWidget(epsgLabel, 5, 0)
#        grid.addWidget(self.epsgEdit, 5, 2)
#
#        grid.addWidget(fromDateLabel, 6, 0)
#        grid.addWidget(self.fromDateEnable, 6, 1)
#        grid.addWidget(self.fromDateEdit, 6, 2)
#        
#        grid.addWidget(toDateLabel, 7, 0)
#        grid.addWidget(self.toDateEnable, 7, 1)
#        grid.addWidget(self.toDateEdit, 7, 2)
#
#        vbox1 = QVBoxLayout()
#        vbox1.addStretch(1)
#        vbox1.addWidget(internalLabel)
#        vbox1.addWidget(self.internalTrigger)
#        
#        vbox2 = QVBoxLayout()
#        vbox2.addStretch(1)
#        vbox2.addWidget(initLabel)
#        vbox2.addWidget(self.initTrigger)
#        
#        vbox3 = QVBoxLayout()
#        vbox3.addStretch(1)
#        vbox3.addWidget(cleanLabel)
#        vbox3.addWidget(self.cleanTrigger)
#        
#        hbox3 = QHBoxLayout()
#        hbox3.addStretch(1)
#        hbox3.addLayout(vbox1)
#        hbox3.addLayout(vbox2)
#        hbox3.addLayout(vbox3)
#        
#        hbox4 = QHBoxLayout()
#        hbox4.addStretch(1)
#        hbox4.addWidget(okButton)
#        hbox4.addWidget(cancelButton)
#        
#
#        vbox = QVBoxLayout()
#        #vbox.addStretch(1)
#        vbox.addLayout(grid)
#        vbox.addLayout(hbox3)
#        vbox.addLayout(hbox4)
#        
#        
#        self.setLayout(vbox)  
#        
#        #self.setGeometry(300, 300, 350, 250)
#        #self.setWindowTitle('LDS Replicate')
#       
#        
#        
#    def centre(self):
#        
#        qr = self.frameGeometry()
#        cp = QDesktopWidget().availableGeometry().center()
#        qr.moveCenter(cp)
#        self.move(qr.topLeft())
#        
#    def closeEvent(self, event):
#        
#        reply = QMessageBox.question(self, 'Message', "Are you sure to quit?", QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
#
#        if reply == QMessageBox.Yes:
#            event.accept()
#        else:
#            event.ignore()       
#        
#    def doFromDateEnable(self):
#        self.fromDateEdit.setEnabled(self.fromDateEnable.isChecked())
#          
#    def doToDateEnable(self):
#        self.toDateEdit.setEnabled(self.toDateEnable.isChecked())  
#          
#    def doOkClickAction(self):
#        
#        destination = str(self.destmenulist[self.destMenu.currentIndex()])
#        layer = str(self.layerEdit.text())
#        uconf = str(self.confEdit.text())
#        group = str(self.groupEdit.text())
#        epsg = str(self.epsgEdit.text())
#        fe = self.fromDateEnable.isChecked()
#        te = self.toDateEnable.isChecked()
#        fd = None if fe is False else str(self.fromDateEdit.date().toString('yyyy-MM-dd'))
#        td = None if te is False else str(self.toDateEdit.date().toString('yyyy-MM-dd'))
#        internal = self.internalTrigger.isChecked()
#        init = self.initTrigger.isChecked()
#        clean = self.cleanTrigger.isChecked()
#        
#        self.parent.statusbar.showMessage('Replicating '+layer)
#
#        #'dest','layer','uconf','group','epsg','fd','td','int'
#        self.gpr.write((destination,layer,uconf,group,epsg,fd,td,internal))
#        
#        ldslog.info('dest='+destination+', layer'+layer+', conf='+uconf+', group='+group+', epsg='+epsg)
#        ldslog.info('fd='+str(fd)+', td='+str(td)+', fe='+str(fe)+', te='+str(te))
#        ldslog.info('int='+str(internal)+', init='+str(init)+', clean='+str(clean))
#
#
#        tp = TransferProcessor(layer, 
#                               None if group is None else group, 
#                               None if epsg is None else epsg, 
#                               None if fd is None else fd, 
#                               None if td is None else td,
#                               None, None, None, 
#                               None if uconf is None else uconf, 
#                               internal, None)
#        
#        #NB init and clean are funcs because they appear as args, not opts in the CL
#        if init:
#            tp.setInitConfig()
#        if clean:
#            tp.setCleanConfig()
#            
#        proc = {'PostgreSQL':tp.processLDS2PG,
#                'MSSQL':tp.processLDS2MSSQL,
#                'SpatiaLite':tp.processLDS2SpatiaLite,
#                'FileGDB':tp.processLDS2FileGDB
#                }.get(destination)
#        proc()
#        
#        self.parent.statusbar.showMessage('Replication of '+layer+' complete')
#        
#        
##--------------------------------------------------------------------------------------------------
#
#class LDSPrefsEditor(QMainWindow):
#    
#    def __init__(self):
#        super(LDSPrefsEditor, self).__init__()
#        
#        self.setWindowTitle('LDS Preferences Editor')
#        
#        self.editor = LDSPrefsFrame(self)
#        self.setCentralWidget(self.editor)
#
#        
#        openAction = QAction(QIcon('open.png'), '&Open', self)        
#        openAction.setShortcut('Ctrl+O')
#        openAction.setStatusTip('Open File')
#        openAction.triggered.connect(self.openFile)
#        
#        saveAction = QAction(QIcon('save.png'), '&Save', self)        
#        saveAction.setShortcut('Ctrl+S')
#        saveAction.setStatusTip('Save Changes')
#        saveAction.triggered.connect(self.saveFile)
#        
#        saveAsAction = QAction(QIcon('save.png'), '&Save As', self)        
#        saveAsAction.setShortcut('Ctrl+A')
#        saveAsAction.setStatusTip('Save Changes')
#        saveAsAction.triggered.connect(self.saveAsFile)
#        
#        exitAction = QAction(QIcon('exit.png'), '&Exit', self)        
#        exitAction.setShortcut('Ctrl+Q')
#        exitAction.setStatusTip('Exit Application')
#        exitAction.triggered.connect(self.close)
#        
#        self.statusbar = self.statusBar()
#        self.statusbar.showMessage('Ready')
#        
#        menubar = self.menuBar()
#
#        fileMenu = menubar.addMenu('&File')
#        fileMenu.addAction(openAction)
#        fileMenu.addSeparator()
#        fileMenu.addAction(saveAction)
#        fileMenu.addAction(saveAsAction)
#        fileMenu.addSeparator()
#        fileMenu.addAction(exitAction) 
#        
#        self.initUI()
#        
#    def initUI(self):
#        self.setGeometry(350,350,800,600)
#        self.show() 
#        
#    def saveAsFile(self):
#        filename = QFileDialog.getSaveFileName(self, 'Save File As', os.path.join(os.getcwd(),'../conf/'))#os.getenv('HOME'))
#        f = open(filename, 'w')
#        filedata = self.editor.textedit.toPlainText()
#        f.write(filedata)
#        f.close()
#        
#    def saveFile(self):
#        f = open(self.filename, 'w')
#        filedata = self.editor.textedit.toPlainText()
#        f.write(filedata)
#        f.close()
#        
#    def openFile(self):
#        f=QDir.Filter(1)
#        
#        filedialog = QFileDialog()
#        filedialog.setFilter(f)
#        self.filename = filedialog.getOpenFileName(self, 'Open File', os.path.join(os.getcwd(),'../conf/'))#os.getenv('HOME'))
#        f = open(self.filename, 'r')
#        filedata = f.read()
#        self.editor.textedit.setText(filedata)
#        self.statusbar.showMessage('Editing '+self.filename)
#        f.close()
#        
#class LDSPrefsFrame(QFrame):
#    
#    def __init__(self,parent):
#        super(LDSPrefsFrame, self).__init__()
#        self.parent = parent
#        self.gpr = GUIPrefsReader()
#        self.initUI()
#        
#    def initUI(self):
#
#        #edit boxes
#        self.textedit = QTextEdit() 
#        
#        vbox = QVBoxLayout()
#        vbox.addWidget(self.textedit)
#        
#        self.setLayout(vbox)  
#
#  
##---> QWizard Section <----------------------------------------------------------------------------
#
##Notes:
##MS and PG settings entered in these dialogs are saved to config only
##When a new FGDB directory is set in the file dialog using the NewFolder button a new directory is created and a reference added to the user config
##When a new SLITE file is created by entering its name in the SL file dialog, it isnt created but a reference to it is put in the user config file
#       
#class LDSConfigWizard(QWizard):
#    
#    def __init__(self, parent=None):
#        super(LDSConfigWizard, self).__init__(parent)
#        
#        self.plist = {'lds':(0,'LDS',LDSConfigPage),
#                 'pg':(1,'PostgreSQL',PostgreSQLConfigPage),
#                 'ms':(2,'MSSQLSpatial',MSSQLSpatialConfigPage),
#                 'fg':(3,'FileGDB',FileGDBConfigPage),
#                 'sl':(4,'SpatiaLite',SpatiaLiteConfigPage),
#                 'proxy':(5,'Proxy',ProxyConfigPage),
#                 'final':(6,'Final',ConfirmationPage)}
#        
#        for key in self.plist.keys():
#            index = self.plist.get(key)[0]
#            page = self.plist.get(key)[2]
#            self.setPage(index, page(self,key))
#
#
#        self.setWindowTitle("LDS Configuration Setup Wizard")
#        self.resize(640,480)
#
#        
#class LDSConfigPage(QWizardPage):
#    def __init__(self, parent=None,key=None):
#        super(LDSConfigPage, self).__init__(parent)
#        
#        self.parent = parent 
#        self.key = 'lds'
#        
#        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
#        self.setSubTitle('Here you can enter a name for your custom configuration file, your LDS API key and required output. Also select whether you want to configure a proxy or enable password encryption')
#
#
#        QToolTip.setFont(QFont('SansSerif', 10))
#        
#        #labels
#        fileLabel = QLabel('User Config File')
#        keyLabel = QLabel('LDS API Key')
#        destLabel = QLabel('Output Type')
#        proxyLabel = QLabel('Configure Proxy')
#        encryptionLabel = QLabel('Enable Password Protection')
#        
#        
#        infoLinkLabel = QLabel('<a href="http://www.linz.govt.nz/about-linz/linz-data-service/features/how-to-use-web-services">LDS API Information Page</a>')
#        infoLinkLabel.setOpenExternalLinks(True);
#        keyLinkLabel = QLabel('<a href="http://data.linz.govt.nz/my/api/">LDS API Key</a>')
#        keyLinkLabel.setOpenExternalLinks(True);
#
#        
#        #edit boxes
#        self.fileEdit = QLineEdit('')
#        self.fileEdit.setToolTip('Name of user config file (without .conf suffix)')
#        self.keyEdit = QLineEdit('')
#        self.keyEdit.setToolTip('This is your LDS API key. If you have an account you can copy your key from here <a href="http://data.linz.govt.nz/my/api/">http://data.linz.govt.nz/my/api/</a>')
#        
#        #dropdown
#        self.destSelect = QComboBox()
#        self.destSelect.setToolTip('Choose from one of four possible output destinations')
#        self.destSelect.addItem('')
#        self.destSelect.addItem(self.parent.plist.get('pg')[1], self.parent.plist.get('pg')[0])
#        self.destSelect.addItem(self.parent.plist.get('ms')[1], self.parent.plist.get('ms')[0])
#        self.destSelect.addItem(self.parent.plist.get('fg')[1], self.parent.plist.get('fg')[0])
#        self.destSelect.addItem(self.parent.plist.get('sl')[1], self.parent.plist.get('sl')[0])
#        
#        
#        self.keyEdit.setValidator(QRegExpValidator(QRegExp("[a-fA-F0-9]{32}", re.IGNORECASE), self))
#        
#        #checkbox
#        self.proxyEnable = QCheckBox()
#        self.proxyEnable.setToolTip('Enable proxy selection dialog')
#        self.encryptionEnable = QCheckBox()
#        self.encryptionEnable.setToolTip('Encrypt any passwords saved to user config file')
#
#        
#        self.registerField(self.key+"file",self.fileEdit)
#        self.registerField(self.key+"apikey",self.keyEdit)
#        self.registerField(self.key+"dest",self.destSelect,"currentIndex")
#        self.registerField(self.key+"proxy",self.proxyEnable)
#        self.registerField(self.key+"encryption",self.encryptionEnable)
#        
#        #buttons
#        #cfileButton = QPushButton("...")
#        #cfileButton.setToolTip('Select Config File')
#        #cfileButton.setBaseSize(100, 100)
#        #cfileButton.clicked.connect(self.selectConfFile)
#
#
#        #grid
#        grid = QGridLayout()
#        grid.setSpacing(10)
#        
#        grid.addWidget(fileLabel, 1, 0)
#        grid.addWidget(self.fileEdit, 1, 2)
#        #grid.addWidget(cfileButton, 1, 3)        
#        
#        grid.addWidget(keyLabel, 2, 0)
#        grid.addWidget(self.keyEdit, 2, 2)
#        
#        grid.addWidget(destLabel, 3, 0)
#        grid.addWidget(self.destSelect, 3, 2)
#        
#        grid.addWidget(proxyLabel, 4, 0)
#        grid.addWidget(self.proxyEnable, 4, 2)
#        
#        grid.addWidget(encryptionLabel, 5, 0)
#        grid.addWidget(self.encryptionEnable, 5, 2)
#
#        #layout       
#        vbox = QVBoxLayout()
#        vbox.addLayout(grid)
#        vbox.addStretch(1)
#        vbox.addWidget(keyLinkLabel)
#        vbox.addWidget(infoLinkLabel)
#        
#        self.setLayout(vbox)
#        
#    #def selectConfFile(self):
#    #    self.fileEdit.setText(QFileDialog.getOpenFileName())
#
#    def nextId(self):
#        if self.field(self.key+"proxy").toBool():
#            return self.parent.plist.get('proxy')[0]
#        return int(self.field(self.key+"dest").toString())
#        
#class ProxyConfigPage(QWizardPage):
#    def __init__(self, parent=None,key=None):
#        super(ProxyConfigPage, self).__init__(parent)
#        
#        self.parent = parent 
#        self.key = key
#        
#        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
#        self.setSubTitle('Enter the hostname/ip-address, port number and authentication details of your HTTP proxy')
#
#        QToolTip.setFont(QFont('SansSerif', 10))
#        
#        #labels
#        hostLabel = QLabel('Proxy Host')
#        portLabel = QLabel('Proxy Port')
#        authLabel = QLabel('Authentication')
#        usrLabel = QLabel('Username')
#        pwdLabel = QLabel('Password')
#        
#        #edit boxes
#        self.hostEdit = QLineEdit('')
#        self.hostEdit.setToolTip('Enter Proxy host (IP Address or hostname)')
#        self.portEdit = QLineEdit('')
#        self.portEdit.setToolTip('Enter Proxy port')
#        
#        #dropdown
#        self.authSelect = QComboBox()
#        
#        index = 1
#        self.authSelect.addItem('')
#        self.authSelect.setToolTip('Select appropriate proxy authentication mechanism')
#        for method in WFSDataStore.PROXY_AUTH:
#            self.authSelect.addItem(method,index)
#            index += 1
#        
#        self.usrEdit = QLineEdit('')
#        self.usrEdit.setToolTip('Enter your proxy username (if required)')
#        self.pwdEdit = QLineEdit('')
#        self.usrEdit.setToolTip('Enter your proxy password (if required)')
#        self.pwdEdit.setEchoMode(QLineEdit.Password)
#        
#        self.portEdit.setValidator(QRegExpValidator(QRegExp("\d{1,5}"), self))
#        
#        self.registerField(self.key+"host",self.hostEdit)
#        self.registerField(self.key+"port",self.portEdit)
#        self.registerField(self.key+"auth",self.authSelect,"currentIndex")
#        self.registerField(self.key+"usr",self.usrEdit)
#        self.registerField(self.key+"pwd",self.pwdEdit)
#
#        #grid
#        grid = QGridLayout()
#        grid.setSpacing(10)
#        
#        #layout
#        grid.addWidget(hostLabel, 1, 0)
#        grid.addWidget(self.hostEdit, 1, 2)
#        
#        grid.addWidget(portLabel, 2, 0)
#        grid.addWidget(self.portEdit, 2, 2)
#        
#        grid.addWidget(authLabel, 3, 0)
#        grid.addWidget(self.authSelect, 3, 2)
#        
#        grid.addWidget(usrLabel, 4, 0)
#        grid.addWidget(self.usrEdit, 4, 2)
#        
#        grid.addWidget(pwdLabel, 5, 0)
#        grid.addWidget(self.pwdEdit, 5, 2)
#        
#        
#        #layout                
#        self.setLayout(grid)  
#        
#    def selectConfFile(self):
#        self.fileEdit.setText(QFileDialog.getOpenFileName())
#
#    def nextId(self):
#        #now go to selected dest configger
#        return int(self.field("ldsdest").toString())
#            
#class PostgreSQLConfigPage(QWizardPage):
#    def __init__(self,parent=None,key=None):
#        super(PostgreSQLConfigPage, self).__init__(parent)
#        
#        self.parent = parent 
#        self.key = key
#        
#        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
#        self.setSubTitle('Enter the hostname/ip-address, port number, name and schema of your PostgreSQL server instance.')
#
#        
#        QToolTip.setFont(QFont('SansSerif', 10))
#        
#        
#        #labels
#        hostLabel = QLabel('PostgreSQL Host')
#        portLabel = QLabel('PostgreSQL Port')
#        dbnameLabel = QLabel('PostgreSQL DB Name')
#        schemaLabel = QLabel('PostgreSQL DB Schema')
#        usrLabel = QLabel('Username')
#        pwdLabel = QLabel('Password')
#        
#        #edit boxes
#        self.hostEdit = QLineEdit('')
#        self.hostEdit.setToolTip('Enter the name of your PostgreSQL host/IP-address')
#        self.portEdit = QLineEdit('')
#        self.portEdit.setToolTip('Enter the PostgreSQL listen port')
#        self.dbnameEdit = QLineEdit('')
#        self.dbnameEdit.setToolTip('Enter the name of the PostgreSQL DB to connect with')
#        self.schemaEdit = QLineEdit('')
#        self.schemaEdit.setToolTip('Set the database schema here')
#        self.usrEdit = QLineEdit('')
#        self.usrEdit.setToolTip('Name of PostgreSQL account/user')
#        self.pwdEdit = QLineEdit('')
#        self.pwdEdit.setToolTip('Enter PostgreSQL account password')
#        self.pwdEdit.setEchoMode(QLineEdit.Password)
#        
#        self.portEdit.setValidator(QRegExpValidator(QRegExp("\d{1,5}"), self))
#        
#        self.registerField(self.key+"host",self.hostEdit)
#        self.registerField(self.key+"port",self.portEdit)
#        self.registerField(self.key+"dbname",self.dbnameEdit)
#        self.registerField(self.key+"schema",self.schemaEdit)
#        self.registerField(self.key+"usr",self.usrEdit)
#        self.registerField(self.key+"pwd",self.pwdEdit)
#
#        #grid
#        grid = QGridLayout()
#        grid.setSpacing(10)
#        
#        #layout
#        grid.addWidget(hostLabel, 1, 0)
#        grid.addWidget(self.hostEdit, 1, 2)
#        
#        grid.addWidget(portLabel, 2, 0)
#        grid.addWidget(self.portEdit, 2, 2)
#        
#        grid.addWidget(dbnameLabel, 3, 0)
#        grid.addWidget(self.dbnameEdit, 3, 2)
#        
#        grid.addWidget(schemaLabel, 4, 0)
#        grid.addWidget(self.schemaEdit, 4, 2)
#        
#        grid.addWidget(usrLabel, 5, 0)
#        grid.addWidget(self.usrEdit, 5, 2)
#        
#        grid.addWidget(pwdLabel, 6, 0)
#        grid.addWidget(self.pwdEdit, 6, 2)
#        
#        
#        #layout                
#        self.setLayout(grid)  
#        
#
#    def nextId(self):
#        return self.parent.plist.get('final')[0]
#        
#
#class MSSQLSpatialConfigPage(QWizardPage):
#    def __init__(self,parent=None,key=None):
#        super(MSSQLSpatialConfigPage, self).__init__(parent)
#        
#        self.parent = parent 
#        self.key = key
#        
#        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
#        self.setSubTitle('Enter the server string (host\instance) name and schema of your MSSQL server. Select "Trust" if using trusted authentication')
#
#        
#        QToolTip.setFont(QFont('SansSerif', 10))
#        
#        #labels
#        serverLabel = QLabel('MSSQLSpatial Server')
#        dbnameLabel = QLabel('MSSQLSpatial DB Name')
#        schemaLabel = QLabel('MSSQLSpatial DB Schema')
#        trustLabel = QLabel('Trust')
#        usrLabel = QLabel('Username')
#        pwdLabel = QLabel('Password')
#        
#        #edit boxes
#        self.serverEdit = QLineEdit('') 
#        self.serverEdit.setToolTip('Enter MSSQL Server string. Format typically <host-name>\<db-instance>')
#        self.dbnameEdit = QLineEdit('')
#        self.dbnameEdit.setToolTip('Enter the name of the MSSQL database')
#        self.schemaEdit = QLineEdit('')
#        self.schemaEdit.setToolTip('Enter schema name (this is not mandatory but a common default in MSSQL is "dbo")')
#        self.trustCheckBox = QCheckBox('')
#        self.trustCheckBox.setToolTip('Use MSSQL trusted client authentication')
#        self.usrEdit = QLineEdit('')
#        self.usrEdit.setToolTip('Enter MSSQL Username')
#        self.pwdEdit = QLineEdit('')
#        self.pwdEdit.setToolTip('Enter MSSQL Password')
#        self.pwdEdit.setEchoMode(QLineEdit.Password)
#
#        #self.trustEdit.setValidator(QRegExpValidator(QRegExp("yes|no", re.IGNORECASE), self))
#        
#        self.registerField("msserver",self.serverEdit)
#        self.registerField(self.key+"dbname",self.dbnameEdit)
#        self.registerField(self.key+"schema",self.schemaEdit)
#        self.registerField(self.key+"trust",self.trustCheckBox)
#        self.registerField(self.key+"usr",self.usrEdit)
#        self.registerField(self.key+"pwd",self.pwdEdit)
#
#        #grid
#        grid = QGridLayout()
#        grid.setSpacing(10)
#        
#        #layout
#        grid.addWidget(serverLabel, 1, 0)
#        grid.addWidget(self.serverEdit, 1, 2)
#        
#        grid.addWidget(dbnameLabel, 2, 0)
#        grid.addWidget(self.dbnameEdit, 2, 2)
#        
#        grid.addWidget(schemaLabel, 3, 0)
#        grid.addWidget(self.schemaEdit, 3, 2)
#        
#        grid.addWidget(trustLabel, 4, 0)
#        grid.addWidget(self.trustCheckBox, 4, 2)
#        
#        grid.addWidget(usrLabel, 5, 0)
#        grid.addWidget(self.usrEdit, 5, 2)
#        
#        grid.addWidget(pwdLabel, 6, 0)
#        grid.addWidget(self.pwdEdit, 6, 2)
#
#        self.setLayout(grid)
#          
#    def nextId(self):
#        return self.parent.plist.get('final')[0]
#        
#class FileGDBConfigPage(QWizardPage):
#    
#    def __init__(self,parent=None,key=None):
#        super(FileGDBConfigPage, self).__init__(parent)
#        
#        self.parent = parent 
#        self.key = key
#        
#        self.filter = ".*\.gdb$"
#        self.proceed = False
#        
#        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
#        self.setSubTitle('Enter your FileGDB directory name. (This must carry a .gdb suffix)')
#
#        
#        QToolTip.setFont(QFont('SansSerif', 10))
#        
#        #labels
#        fileLabel = QLabel('FileGDB DB directory')
#        
#        #edit boxes
#        self.fileEdit = QLineEdit('')#dir selection dialog? Can't prefilter file selection for directories
#        self.fileEdit.setToolTip('Enter FileGDB directory (must have .gdb suffix)')
#        
#        self.fileEdit.setValidator(QRegExpValidator(QRegExp(self.filter, re.IGNORECASE), self))
#        
#        self.registerField(self.key+"file",self.fileEdit)
#        
#        #buttons
#        fileButton = QPushButton("...")
#        fileButton.setToolTip('Select FileGDB File')
#        fileButton.clicked.connect(self.selectFileGDBFile)
#        
#        
#        #grid
#        grid = QGridLayout()
#        grid.setSpacing(10)
#        
#        #layout                
#        grid.addWidget(fileLabel,1,0)
#        grid.addWidget(self.fileEdit,2,0)
#        grid.addWidget(fileButton,2,3)
# 
#        
#        self.setLayout(grid)  
#
#    def selectFileGDBFile(self):
#        fdtext = QFileDialog.getExistingDirectory(self,'Select FileGDB Directory','~')
#        if re.match(self.filter,fdtext):
#            self.fileEdit.setText(fdtext)
#            self.proceed = True
#        else:
#            self.fileEdit.setText('')
#        
#    def nextId(self):
#        if self.proceed:
#            return self.parent.plist.get('final')[0]
#        return self.parent.plist.get('fg')[0]
#        
#        
#class SpatiaLiteConfigPage(QWizardPage):
#    def __init__(self,parent=None,key=None):
#        super(SpatiaLiteConfigPage, self).__init__(parent)
#        
#        self.parent = parent 
#        self.key = key
#        
#        self.proceed = False
#        self.filter = ".*\.db$|.*\.sqlite\d*$"
#        
#        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
#        self.setSubTitle('Enter your SpatiaLite data file name. (This should carry a .db or .sqlite suffix)')
#
#        
#        QToolTip.setFont(QFont('SansSerif', 10))
#        
#        #labels
#        fileLabel = QLabel('SpatiaLite DB File')
#        
#        #edit boxes
#        self.fileEdit = QLineEdit('')
#        self.fileEdit.setToolTip('Select or create SpatiaLite data file (recommended suffixes .db and .sqlite)')
#        
#        self.fileEdit.setValidator(QRegExpValidator(QRegExp(self.filter, re.IGNORECASE), self))
#        
#        self.registerField(self.key+"file",self.fileEdit)
#        
#        #buttons
#        fileButton = QPushButton("...")
#        fileButton.setToolTip('Select SpatiaLite File')
#        fileButton.clicked.connect(self.selectSpatiaLiteFile)
#        
#
#        #grid
#        grid = QGridLayout()
#        grid.setSpacing(10)
#        
#        #layout
#        grid.addWidget(fileLabel, 1, 0)
#        grid.addWidget(self.fileEdit, 2, 0)
#        grid.addWidget(fileButton, 2, 3)        
#        
#        #layout       
#        vbox = QVBoxLayout()
#        vbox.addLayout(grid)     
#        
#        self.setLayout(vbox)  
#
#    def selectSpatiaLiteFile(self):
#        fdtext = QFileDialog.getOpenFileName(self,'Select SpatiaLite File','~','SQlite (*.db *.sqlite *.sqlite3)')
#        if re.match(self.filter,fdtext):
#            self.fileEdit.setText(fdtext)
#            self.proceed = True
#        else:
#            self.fileEdit.setText('')
#        
#    def nextId(self):
#        if self.proceed:
#            return self.parent.plist.get('final')[0]
#        return self.parent.plist.get('sl')[0]
#
#class ConfirmationPage(QWizardPage):
#    def __init__(self,parent=None,key=None):
#        super(ConfirmationPage, self).__init__(parent)
#        self.parent = parent
#        
#        self.destlist = {1:('PostgreSQL',self.getPGFields),2:('MSSQLSpatial',self.getMSFields),3:('FileGDB',self.getFGFields),4:('SpatiaLite',self.getSLFields)}
#        
#        self.setTitle('Confirm Your Selection')
#        self.setSubTitle('The values recorded below are ready to be written to the configuration file. Click "Finish" to confirm')
#        
#        QToolTip.setFont(QFont('SansSerif', 10))
#        
#    def initializePage(self):
#        '''Override initpage'''
#        super(ConfirmationPage, self).initializePage()
#        
#        hfont = QFont()
#        #hfont.setPointSize(11)
#        hfont.setBold(True)
#        
#        vbox = QVBoxLayout()
#        sec1 = QLabel('LDS')
#        sec1.setFont(hfont)
#        vbox.addWidget(sec1)
#        
#        
#        self.ldsfile = str(self.field("ldsfile").toString())
#        hbox1 = QHBoxLayout()
#        hbox1.addWidget(QLabel('LDS File Name'))     
#        hbox1.addWidget(QLabel(self.ldsfile))   
#            
#        vbox.addLayout(hbox1)
#        
#        hbox2 = QHBoxLayout()
#        hbox2.addWidget(QLabel('LDS API Key'))
#        hbox2.addWidget(QLabel(self.field("ldsapikey").toString()))   
#        
#        vbox.addLayout(hbox2)
#    
#        if self.field("ldsproxy").toBool():
#            sec2 = QLabel('Proxy')
#            sec2.setFont(hfont)
#            vbox.addWidget(sec2)
#            
#            hbox3 = QHBoxLayout()
#            hbox3.addWidget(QLabel('Proxy Server'))     
#            hbox3.addWidget(QLabel(self.field("proxyhost").toString()))   
#            
#            vbox.addLayout(hbox3)
#        
#            hbox4 = QHBoxLayout()
#            hbox4.addWidget(QLabel('Proxy Port'))     
#            hbox4.addWidget(QLabel(self.field("proxyport").toString()))   
#        
#            vbox.addLayout(hbox4)
#            
#            hbox5 = QHBoxLayout()
#            hbox5.addWidget(QLabel('Proxy Auth'))     
#            hbox5.addWidget(QLabel(self.field("proxyauth").toString()))   
#        
#            vbox.addLayout(hbox5)
#            
#            hbox6 = QHBoxLayout()
#            hbox6.addWidget(QLabel('Proxy User'))     
#            hbox6.addWidget(QLabel(self.field("proxyusr").toString()))   
#        
#            vbox.addLayout(hbox6)
#        
#        self.selected = self.destlist.get(int(self.field("ldsdest").toString()))
#        sec3 = QLabel(self.selected[0])
#        sec3.setFont(hfont)
#        vbox.addWidget(sec3)
#        
#        self.selectedfields = self.selected[1]()
#        for f in self.selectedfields:
#            if f[0] is not 'pass':
#                name = QLabel(f[1])
#                value = QLabel(f[2])
#                
#                hbox = QHBoxLayout()
#                hbox.addWidget(name)     
#                hbox.addWidget(value)   
#                
#                vbox.addLayout(hbox)   
#               
#        self.setLayout(vbox)
#        
#    def getPGFields(self):
#        flist = []
#        flist += (('host','PostgreSQL Host',str(self.field("pghost").toString())),)
#        flist += (('port','PostgreSQL Port',str(self.field('pgport').toString())),)
#        flist += (('dbname','PostgreSQL DB Name',str(self.field('pgdbname').toString())),)
#        flist += (('schema','PostgreSQL Schema',str(self.field('pgschema').toString())),)
#        flist += (('user','PostgreSQL User Name',str(self.field('pgusr').toString())),)
#        flist += (('pass','PostgreSQL Password',str(self.field('pgpwd').toString())),)
#        return flist   
#    
#    def getMSFields(self):
#        flist = []
#        flist += (('server','MSSQLSpatial Server String',str(self.field('msserver').toString())),)
#        flist += (('dbname','MSSQLSpatial DB Name',str(self.field('msdbname').toString())),)
#        flist += (('schema','MSSQLSpatial Schema',str(self.field('msschema').toString())),)
#        flist += (('trust','MSSQLSpatial Trust','yes' if self.field('mstrust').toBool() else 'no'),)
#        flist += (('user','MSSQLSpatial User Name',str(self.field('msusr').toString())),)
#        flist += (('pass','MSSQLSpatial Password',str(self.field('mspwd').toString())),)
#        return flist  
#    
#    def getFGFields(self):
#        flist = []
#        flist += (('file','FileGDB DB File Name',str(self.field('fgfile').toString())),)
#        return flist  
#    
#    def getSLFields(self):
#        flist = []
#        flist += (('file','SpatiaLite File Name',str(self.field('slfile').toString())),)
#        return flist
#    
#       
#    def validatePage(self):
#        from lds.ReadConfig import MainFileReader as MFR
#        from lds.ConfigWrapper import ConfigWrapper
#        from lds.LDSUtilities import Encrypt
#        rv = super(ConfirmationPage, self).validatePage()
#        
#        
#        encrypt = self.field("ldsencryption").toBool()
#        
#        buildarray = ()
#        
#        buildarray += ((MFR.LDSN,'key',str(self.field("ldsapikey").toString())),)
#
#        if self.field("ldsproxy").toBool():
#            
#            buildarray += ((MFR.PROXY,'host',str(self.field("proxyhost").toString())),)
#            buildarray += ((MFR.PROXY,'port',str(self.field("proxyport").toString())),)
#            buildarray += ((MFR.PROXY,'auth',WFSDataStore.PROXY_AUTH[int(self.field("proxyauth").toString())]),)
#            buildarray += ((MFR.PROXY,'user',str(self.field("proxyusr").toString())),)
#            pwd = self.field("proxypwd").toString()
#            if encrypt:
#                pwd = Encrypt.ENC_PREFIX+Encrypt.secure(pwd)
#            buildarray += ((MFR.PROXY,'pass',pwd),)
#
#
#        section = self.selected[0]
#        for sf in self.selectedfields:
#            name = sf[0]
#            value = sf[2]
#            if name == 'pass' and encrypt:
#                value = Encrypt.ENC_PREFIX+Encrypt.secure(value)
#            buildarray += ((section,name,value),)
#            
#        #save values to user config file
#        ConfigWrapper.buildNewUserConfig(self.field("ldsfile").toString(), buildarray)
#        #save userconf and dest to gui prefs
#        gpr = GUIPrefsReader()
#        #zips with (dest,layer,uconf...
#        gpr.write( (section,'',self.ldsfile+('' if re.search('\.conf$',self.ldsfile) else '.conf')) )
#        
#        return rv
   
