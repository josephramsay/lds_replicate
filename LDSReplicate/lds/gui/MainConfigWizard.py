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

import re
import sys

from lds.DataStore import DSReaderException,DatasourcePrivilegeException
from lds.TransferProcessor import TransferProcessor
from lds.LDSDataStore import LDSDataStore
from lds.WFSDataStore import WFSDataStore
from lds.ReadConfig import GUIPrefsReader, MainFileReader
from lds.LDSUtilities import LDSUtilities
from lds.VersionUtilities import AppVersion

from lds.PostgreSQLDataStore import PostgreSQLDataStore as PG
from lds.MSSQLSpatialDataStore import MSSQLSpatialDataStore as MS
from lds.FileGDBDataStore import FileGDBDataStore as FG
from lds.SpatiaLiteDataStore import SpatiaLiteDataStore as SL


ldslog = LDSUtilities.setupLogging()

__version__ = AppVersion.getVersion()

#Notes:
#MS and PG settings entered in these dialogs are saved to config only
#When a new FGDB directory is set in the file dialog using the NewFolder button a new directory is created and a reference added to the user config
#When a new SLITE file is created by entering its name in the SL file dialog, it isnt created but a reference to it is put in the user config file
       
class LDSConfigWizard(QWizard):
    
    def __init__(self, uchint=None, sechint=None, parent=None):
        super(LDSConfigWizard, self).__init__(parent)
        self.parent = parent
        self.uchint = uchint
        self.sechint = sechint

        self.setMFR(self.uchint)
        
        self.plist = {'lds':(0,'LDS',LDSConfigPage),
                 'pg':(1,'PostgreSQL',PostgreSQLConfigPage),
                 'ms':(2,'MSSQLSpatial',MSSQLSpatialConfigPage),
                 'fg':(3,'FileGDB',FileGDBConfigPage),
                 'sl':(4,'SQLite',SpatiaLiteConfigPage),
                 'proxy':(5,'Proxy',ProxyConfigPage),
                 'final':(6,'Final',ConfirmationPage)}
        
        for key in self.plist.keys():
            index = self.plist.get(key)[0]
            page = self.plist.get(key)[2]
            self.setPage(index, page(self,key))

        self.setWindowTitle("LDS Configuration Setup Wizard")
        self.resize(640,480)
        

    def setMFR(self,uc):
        '''Inits a new MFR. If UC exists gets touched else a new 'blank' named file is opened. Its also read as a configparser obj'''        
        self.mfr = MainFileReader(uc)
    
    def getMFR(self):
        return self.mfr
    
    def done(self,event):
        '''Override of done to reset parent's status. Note, this gets called before validate page'''
        self.parent.controls.setStatus(self.parent.controls.STATUS.IDLE,'UC Done') 
        super(LDSConfigWizard,self).done(event) 
        
        
class LDSConfigPage(QWizardPage):
    def __init__(self, parent=None,key=None):
        super(LDSConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key#'lds'
        
        (ldsurl,ldskey,ldssvc,ldsver,ldsfmt,ldscql) = self.parent.mfr.readLDSConfig()
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
        self.setSubTitle('Here you can enter a name for your custom configuration file, your LDS API key and required output. Also select whether you want to configure a proxy or enable password encryption')

        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        fileLabel = QLabel('User Config File')
        keyLabel = QLabel('LDS API Key')
        destLabel = QLabel('Output Type')
        proxyLabel = QLabel('Configure Proxy')
        encryptionLabel = QLabel('Enable Password Protection')
        
        
        infoLinkLabel = QLabel('<a href="http://www.linz.govt.nz/about-linz/linz-data-service/features/how-to-use-web-services">LDS API Information Page</a>')
        infoLinkLabel.setOpenExternalLinks(True);
        keyLinkLabel = QLabel('<a href="http://data.linz.govt.nz/my/api/">LDS API Key</a>')
        keyLinkLabel.setOpenExternalLinks(True);

        #edit boxes
        self.fileEdit = QLineEdit(self.parent.uchint)
        self.fileEdit.setToolTip('Name of user config file (without .conf suffix)')
        self.keyEdit = QLineEdit(ldskey)
        self.keyEdit.setToolTip('This is your LDS API key. If you have an account you can copy your key from here <a href="http://data.linz.govt.nz/my/api/">http://data.linz.govt.nz/my/api/</a>')
               
        #dropdown
        self.destSelect = QComboBox()
        self.destSelect.setToolTip('Choose from one of four possible output destinations')
        self.destSelect.addItem('')
        for itemkey in ('pg','ms','fg','sl'):
            itemindex = self.parent.plist.get(itemkey)[0]
            itemdata = self.parent.plist.get(itemkey)[1]
            self.destSelect.addItem(itemdata, itemindex)
            if itemdata == self.parent.sechint:
                self.destSelect.setCurrentIndex(itemindex)
        
        self.keyEdit.setValidator(QRegExpValidator(QRegExp("[a-fA-F0-9]{32}", re.IGNORECASE), self))
        
        #checkbox
        self.proxyEnable = QCheckBox()
        self.proxyEnable.setToolTip('Enable proxy selection dialog')
        self.encryptionEnable = QCheckBox()
        self.encryptionEnable.setToolTip('Encrypt any passwords saved to user config file')

        
        self.registerField(self.key+"file",self.fileEdit)
        self.registerField(self.key+"apikey",self.keyEdit)
        self.registerField(self.key+"dest",self.destSelect,"currentIndex")
        self.registerField(self.key+"proxy",self.proxyEnable)
        self.registerField(self.key+"encryption",self.encryptionEnable)

        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        grid.addWidget(fileLabel, 1, 0)
        grid.addWidget(self.fileEdit, 1, 2)
        #grid.addWidget(cfileButton, 1, 3)        
        
        grid.addWidget(keyLabel, 2, 0)
        grid.addWidget(self.keyEdit, 2, 2)
        
        grid.addWidget(destLabel, 3, 0)
        grid.addWidget(self.destSelect, 3, 2)
        
        grid.addWidget(proxyLabel, 4, 0)
        grid.addWidget(self.proxyEnable, 4, 2)
        
        grid.addWidget(encryptionLabel, 5, 0)
        grid.addWidget(self.encryptionEnable, 5, 2)

        #layout       
        vbox = QVBoxLayout()
        vbox.addLayout(grid)
        vbox.addStretch(1)
        vbox.addWidget(keyLinkLabel)
        vbox.addWidget(infoLinkLabel)
        
        self.setLayout(vbox)
        
    #def selectConfFile(self):
    #    self.fileEdit.setText(QFileDialog.getOpenFileName())

    def nextId(self):
        if self.field(self.key+"proxy").toBool():
            return self.parent.plist.get('proxy')[0]
        return int(self.field(self.key+"dest").toString())
        
class ProxyConfigPage(QWizardPage):
    def __init__(self, parent=None,key=None):
        super(ProxyConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        (pxyhost,pxyport,pxyauth,pxyusr,pxypwd) = self.parent.mfr.readProxyConfig()
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
        self.setSubTitle('Enter the hostname/ip-address, port number and authentication details of your HTTP proxy')

        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        hostLabel = QLabel('Proxy Host')
        portLabel = QLabel('Proxy Port')
        authLabel = QLabel('Authentication')
        usrLabel = QLabel('Username')
        pwdLabel = QLabel('Password')
        
        #edit boxes
        self.hostEdit = QLineEdit(pxyhost)
        self.hostEdit.setToolTip('Enter Proxy host (IP Address or hostname)')
        self.portEdit = QLineEdit(pxyport)
        self.portEdit.setToolTip('Enter Proxy port')
        
        #dropdown
        self.authSelect = QComboBox()
        
        index = 1
        self.authSelect.addItem('')
        self.authSelect.setToolTip('Select appropriate proxy authentication mechanism')
        for method in WFSDataStore.PROXY_AUTH:
            self.authSelect.addItem(method,index)
            index += 1
        self.authSelect.setCurrentIndex(0 if LDSUtilities.mightAsWellBeNone(pxyauth) is None else WFSDataStore.PROXY_AUTH.index(pxyauth))
        
        self.usrEdit = QLineEdit(pxyusr)
        self.usrEdit.setToolTip('Enter your proxy username (if required)')
        self.pwdEdit = QLineEdit('')#pxypwd
        self.usrEdit.setToolTip('Enter your proxy password (if required)')
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
        
        (pghost,pgport,pgdbname,pgschema,pgusr,pgpwd,pgover,pgconfig,pgepsg,pgcql) = self.parent.mfr.readPostgreSQLConfig()
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
        self.setSubTitle('Enter the hostname/ip-address, port number, name and schema of your PostgreSQL server instance.')
     
        QToolTip.setFont(QFont('SansSerif', 10))
              
        #labels
        hostLabel = QLabel('PostgreSQL Host')
        portLabel = QLabel('PostgreSQL Port')
        dbnameLabel = QLabel('PostgreSQL DB Name')
        schemaLabel = QLabel('PostgreSQL DB Schema')
        usrLabel = QLabel('Username')
        pwdLabel = QLabel('Password')
        
        #edit boxes
        self.hostEdit = QLineEdit(pghost)
        self.hostEdit.setToolTip('Enter the name of your PostgreSQL host/IP-address')
        self.portEdit = QLineEdit('5432' if LDSUtilities.mightAsWellBeNone(pgport) is None else pgport)
        self.portEdit.setToolTip('Enter the PostgreSQL listen port')
        self.dbnameEdit = QLineEdit(pgdbname)
        self.dbnameEdit.setToolTip('Enter the name of the PostgreSQL DB to connect with')
        self.schemaEdit = QLineEdit(pgschema)
        self.schemaEdit.setToolTip('Set the database schema here')
        self.usrEdit = QLineEdit(pgusr)
        self.usrEdit.setToolTip('Name of PostgreSQL account/user')
        self.pwdEdit = QLineEdit('')#pgpwd
        self.pwdEdit.setToolTip('Enter PostgreSQL account password')
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
        if self.testConnection():
            return self.parent.plist.get('final')[0]
        return self.parent.plist.get('pg')[0]

    
    def testConnection(self):
        if not any(f for f in (self.hostEdit.isModified(),self.portEdit.isModified(),self.dbnameEdit.isModified(),
                               self.schemaEdit.isModified(),self.usrEdit.isModified(),self.pwdEdit.isModified())):
            return False
        cs = PG.buildConnStr(self.hostEdit.text(),self.portEdit.text(),self.dbnameEdit.text(),
                            self.schemaEdit.text(),self.usrEdit.text(),self.pwdEdit.text())
        pg = PG(cs)
        pg.applyConfigOptions()
        try:
            pg.ds = pg.initDS(pg.destinationURI(None),False)
            pg.checkGeoPrivileges(self.schemaEdit.text(),self.usrEdit.text())
        except DatasourcePrivilegeException as dpe:
            QMessageBox.warning(self, 'Connection Error', 'Cannot access Geo tables: '+str(dpe), 'OK')
            return False
        except DSReaderException as dse:
            QMessageBox.warning(self, 'Connection Error', 'Cannot connect to database using parameters provided: '+str(dse), 'OK')
            return False
        except RuntimeError as rte:
            QMessageBox.warning(self, 'RuntimeError', 'Error connecting to database: '+str(rte), 'OK')
            return False
        return True
        

class MSSQLSpatialConfigPage(QWizardPage):
    def __init__(self,parent=None,key=None):
        super(MSSQLSpatialConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        (msodbc,msserver,msdsn,mstrust,msdbname,msschema,msusr,mspwd,msconfig,msepsg,mscql) = self.parent.mfr.readMSSQLConfig()
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
        self.setSubTitle('Enter the server string (host\instance) name and schema of your MSSQL server. Select "Trust" if using trusted authentication')
       
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        serverLabel = QLabel('MSSQLSpatial Server')
        dbnameLabel = QLabel('MSSQLSpatial DB Name')
        schemaLabel = QLabel('MSSQLSpatial DB Schema')
        trustLabel = QLabel('Trust')
        usrLabel = QLabel('Username')
        pwdLabel = QLabel('Password')
        
        #edit boxes
        self.serverEdit = QLineEdit(msserver) 
        self.serverEdit.setToolTip('Enter MSSQL Server string. Format typically <host-name>\<db-instance>')
        self.dbnameEdit = QLineEdit(msdbname)
        self.dbnameEdit.setToolTip('Enter the name of the MSSQL database')
        self.schemaEdit = QLineEdit(msschema)
        self.schemaEdit.setToolTip('Enter schema name (this is not mandatory but a common default in MSSQL is "dbo")')
        self.trustCheckBox = QCheckBox('YES')
        self.trustCheckBox.setChecked(mstrust is not None and mstrust.lower()=='yes')
        self.trustCheckBox.setToolTip('Use MSSQL trusted client authentication')
        self.usrEdit = QLineEdit(msusr)
        self.usrEdit.setToolTip('Enter MSSQL Username')
        self.pwdEdit = QLineEdit('')#mspwd
        self.pwdEdit.setToolTip('Enter MSSQL Password')
        self.pwdEdit.setEchoMode(QLineEdit.Password)

        #self.trustEdit.setValidator(QRegExpValidator(QRegExp("yes|no", re.IGNORECASE), self))
        
        self.registerField(self.key+"server",self.serverEdit)
        self.registerField(self.key+"dbname",self.dbnameEdit)
        self.registerField(self.key+"schema",self.schemaEdit)
        self.registerField(self.key+"trust",self.trustCheckBox)
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
        grid.addWidget(self.trustCheckBox, 4, 2)
        
        grid.addWidget(usrLabel, 5, 0)
        grid.addWidget(self.usrEdit, 5, 2)
        
        grid.addWidget(pwdLabel, 6, 0)
        grid.addWidget(self.pwdEdit, 6, 2)

        self.setLayout(grid)
          
    
    def nextId(self):
        if self.testConnection():
            return self.parent.plist.get('final')[0]
        return self.parent.plist.get('ms')[0]

    
    def testConnection(self):
        if not all(f for f in (self.serverEdit.isModified(),self.dbnameEdit.isModified())):
            return False
        cs = MS.buildConnStr(self.serverEdit.text(),self.dbnameEdit.text(),self.schemaEdit.text(),
                            'yes' if self.trustCheckBox.isChecked() else 'no',self.usrEdit.text(),self.pwdEdit.text())
        ms = MS(cs)
        ms.applyConfigOptions()
        try:
            ms.initDS(ms.destinationURI(None),False)
            #ms.checkGeoPrivileges(self.usrEdit.text())
        except DSReaderException as dse:
            QMessageBox.warning(self, 'Connection Error', 'Cannot connect to MS data base using parameters provided: '+str(dse), 'OK')
            return False
        return True
        
class FileGDBConfigPage(QWizardPage):
    
    def __init__(self,parent=None,key=None):
        super(FileGDBConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        (fgfname,fgconfig,fgepsg,fgcql) = self.parent.mfr.readFileGDBConfig()
        
        self.filter = ".*\.gdb$"
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
        self.setSubTitle('Enter your FileGDB directory name. (This must carry a .gdb suffix)')

        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        fileLabel = QLabel('FileGDB DB directory')
        descLabel = QLabel('Enter the path to an existing FileGDB directory OR type in the name\nof a new FileGDB to create.\n\n(Do NOT create a new empty directory)')
        
        #edit boxes
        self.fileEdit = QLineEdit(fgfname)#dir selection dialog? Can't prefilter file selection for directories
        self.fileEdit.setToolTip('Browse to existing FileGDB OR Enter name for new FileGDB (must have .gdb suffix)')
        
        self.fileEdit.setValidator(QRegExpValidator(QRegExp(self.filter, re.IGNORECASE), self))
        
        self.registerField(self.key+"file",self.fileEdit)
        
        #buttons
        fileButton = QPushButton("...")
        fileButton.setToolTip('Select FileGDB Directory')
        fileButton.clicked.connect(self.selectFileGDBFile)
           
        #grid
        grid = QGridLayout()
        grid.setSpacing(10)
        
        #layout                
        grid.addWidget(fileLabel,1,0)
        grid.addWidget(self.fileEdit,2,0)
        grid.addWidget(fileButton,2,3)
        grid.addWidget(descLabel,3,0)
 
        
        self.setLayout(grid)  

    def selectFileGDBFile(self):
        fdtext = QFileDialog.getExistingDirectory(self,'Select FileGDB Directory','~',QFileDialog.ShowDirsOnly)
        #fdtext = QFileDialog.getSaveFileName(self,'Select FileGDB Directory OR Set New','~')
        if re.match(self.filter,fdtext):
            self.fileEdit.setText(fdtext)
        else:
            self.fileEdit.setText('')
        
    def nextId(self):
        if re.match(self.filter,str(self.fileEdit.text())):
            return self.parent.plist.get('final')[0]
        return self.parent.plist.get('fg')[0]
        
        
class SpatiaLiteConfigPage(QWizardPage):
    def __init__(self,parent=None,key=None):
        super(SpatiaLiteConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        (slfname,slconfig,slepsg,slcql) = self.parent.mfr.readSpatiaLiteConfig()
        
        self.filter = ".*\.db$|.*\.sqlite\d*$"
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
        self.setSubTitle('Browse to existing SpatiaLite DB OR enter new SpatiaLite DB file name. (This should carry a .db or .sqlite suffix)')
    
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        fileLabel = QLabel('SpatiaLite DB File')
        
        #edit boxes
        self.fileEdit = QLineEdit(slfname)
        self.fileEdit.setToolTip('Select or create SpatiaLite data file (recommended suffixes .db and .sqlite)')
        
        self.fileEdit.setValidator(QRegExpValidator(QRegExp(self.filter, re.IGNORECASE), self))
        
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
        fdtext = QFileDialog.getSaveFileName(self,'Select SpatiaLite File','~','SQlite (*.db *.sqlite *.sqlite3)')
        if re.match(self.filter,fdtext):
            self.fileEdit.setText(fdtext)
        else:
            self.fileEdit.setText('')
        
    def nextId(self):
        if re.match(self.filter,str(self.fileEdit.text())):
            return self.parent.plist.get('final')[0]
        return self.parent.plist.get('sl')[0]

class ConfirmationPage(QWizardPage):
        
    def __init__(self,parent=None,key=None):
        super(ConfirmationPage, self).__init__(parent)
        self.parent = parent
        
        #TODO. integrate properly with plist
        self.destlist = {self.parent.plist.get('pg')[0]:(self.parent.plist.get('pg')[1],self.getPGFields),
                         self.parent.plist.get('ms')[0]:(self.parent.plist.get('ms')[1],self.getMSFields),
                         self.parent.plist.get('fg')[0]:(self.parent.plist.get('fg')[1],self.getFGFields),
                         self.parent.plist.get('sl')[0]:(self.parent.plist.get('sl')[1],self.getSLFields)}
        
        self.setTitle('Confirm Your Selection')
        self.setSubTitle('The values recorded below are ready to be written to the configuration file. Click "Finish" to confirm')
        
        QToolTip.setFont(QFont('SansSerif', 10))
        
    def initializePage(self):
        '''Override initpage'''
        super(ConfirmationPage, self).initializePage()
        
        hfont = QFont()
        #hfont.setPointSize(11)
        hfont.setBold(True)
        
        vbox = QVBoxLayout()
        sec1 = QLabel('LDS')
        sec1.setFont(hfont)
        vbox.addWidget(sec1)
        
        
        self.ldsfile = str(self.field("ldsfile").toString())
        hbox1 = QHBoxLayout()
        hbox1.addWidget(QLabel('LDS File Name'))     
        hbox1.addWidget(QLabel(self.ldsfile))   
            
        vbox.addLayout(hbox1)
        
        hbox2 = QHBoxLayout()
        hbox2.addWidget(QLabel('LDS API Key'))
        hbox2.addWidget(QLabel(self.field("ldsapikey").toString()))   
        
        vbox.addLayout(hbox2)
    
        if self.field("ldsproxy").toBool():
            sec2 = QLabel('Proxy')
            sec2.setFont(hfont)
            vbox.addWidget(sec2)
            
            hbox3 = QHBoxLayout()
            hbox3.addWidget(QLabel('Proxy Server'))     
            hbox3.addWidget(QLabel(self.field("proxyhost").toString()))   
            
            vbox.addLayout(hbox3)
        
            hbox4 = QHBoxLayout()
            hbox4.addWidget(QLabel('Proxy Port'))     
            hbox4.addWidget(QLabel(self.field("proxyport").toString()))   
        
            vbox.addLayout(hbox4)
            
            hbox5 = QHBoxLayout()
            hbox5.addWidget(QLabel('Proxy Auth'))     
            hbox5.addWidget(QLabel(self.field("proxyauth").toString()))   
        
            vbox.addLayout(hbox5)
            
            hbox6 = QHBoxLayout()
            hbox6.addWidget(QLabel('Proxy User'))     
            hbox6.addWidget(QLabel(self.field("proxyusr").toString()))   
        
            vbox.addLayout(hbox6)
            
        #select dest from index of destmenu
        self.selected = self.destlist.get(int(self.field("ldsdest").toString()))
        sec3 = QLabel(self.selected[0])
        sec3.setFont(hfont)
        vbox.addWidget(sec3)
        
        self.selectedfields = self.selected[1]()
        for f in self.selectedfields:
            if f[0] is not 'pass':
                name = QLabel(f[1])
                value = QLabel(f[2])
                
                hbox = QHBoxLayout()
                hbox.addWidget(name)     
                hbox.addWidget(value)   
                
                vbox.addLayout(hbox)           
           
        self.setLayout(vbox)
        
    def getPGFields(self):
        flist = []
        flist += (('host','PostgreSQL Host',str(self.field("pghost").toString())),)
        flist += (('port','PostgreSQL Port',str(self.field('pgport').toString())),)
        flist += (('dbname','PostgreSQL DB Name',str(self.field('pgdbname').toString())),)
        flist += (('schema','PostgreSQL Schema',str(self.field('pgschema').toString())),)
        flist += (('user','PostgreSQL User Name',str(self.field('pgusr').toString())),)
        flist += (('pass','PostgreSQL Password',str(self.field('pgpwd').toString())),)
        return flist   
    
    def getMSFields(self):
        flist = []
        flist += (('server','MSSQLSpatial Server String',str(self.field('msserver').toString())),)
        flist += (('dbname','MSSQLSpatial DB Name',str(self.field('msdbname').toString())),)
        flist += (('schema','MSSQLSpatial Schema',str(self.field('msschema').toString())),)
        flist += (('trust','MSSQLSpatial Trust','yes' if self.field('mstrust').toBool() else 'no'),)
        flist += (('user','MSSQLSpatial User Name',str(self.field('msusr').toString())),)
        flist += (('pass','MSSQLSpatial Password',str(self.field('mspwd').toString())),)
        return flist  
    
    def getFGFields(self):
        flist = []
        flist += (('file','FileGDB DB File Name',str(self.field('fgfile').toString())),)
        return flist  
    
    def getSLFields(self):
        flist = []
        flist += (('file','SpatiaLite File Name',str(self.field('slfile').toString())),)
        return flist
    
       
    def validatePage(self):
        from lds.ReadConfig import MainFileReader as MFR
        from lds.ConfigWrapper import ConfigWrapper
        from lds.LDSUtilities import Encrypt
        rv = super(ConfirmationPage, self).validatePage()
        
        
        encrypt = self.field("ldsencryption").toBool()
        
        buildarray = ()
        
        buildarray += ((MFR.LDSN,'key',str(self.field("ldsapikey").toString())),)

        if self.field("ldsproxy").toBool():
            
            buildarray += ((MFR.PROXY,'host',str(self.field("proxyhost").toString())),)
            buildarray += ((MFR.PROXY,'port',str(self.field("proxyport").toString())),)
            buildarray += ((MFR.PROXY,'auth',WFSDataStore.PROXY_AUTH[int(self.field("proxyauth").toString())]),)
            buildarray += ((MFR.PROXY,'user',str(self.field("proxyusr").toString())),)
            pwd = self.field("proxypwd").toString()
            if encrypt:
                pwd = Encrypt.ENC_PREFIX+Encrypt.secure(pwd)
            buildarray += ((MFR.PROXY,'pass',pwd),)


        section = self.selected[0]
        for sf in self.selectedfields:
            name = sf[0]
            value = sf[2]
            if name == 'pass' and encrypt:
                value = Encrypt.ENC_PREFIX+Encrypt.secure(value)
            buildarray += ((section,name,value),)
            
        ucfile = LDSUtilities.standardiseUserConfigName(str(self.field("ldsfile").toString()))
        #save values to user config file 
        self.parent.setMFR(self.ldsfile)
        ConfigWrapper.writeUserConfigData(self.parent.getMFR(), buildarray)
        #save userconf and dest to gui prefs
        gpr = GUIPrefsReader()
        #zips with (dest,lgsel,layer,uconf...
        gpr.write(( section,'','',ucfile))
        
        #pass back name of UC file for GUI dialog
        if self.ldsfile is not None:
            self.parent.parent.controls.confEdit.setText(self.ldsfile)
            
        return rv
   
        
        
def main():
    #func to call config wizz
    app0 = QApplication(sys.argv)
    ldsc = LDSConfigWizard()
    ldsc.show()
    rv0 = app0.exec_()
    rv1 = 0
    if rv0==0:
        from  lds.gui.LDSGUI import LDSRepl
        app1 = QApplication(sys.argv)
        lds = LDSRepl()
        lds.show()
        rv1 = app1.exec_()
    
    sys.exit(rv1 + rv0)
    
    
    
if __name__ == '__main__':
    main()