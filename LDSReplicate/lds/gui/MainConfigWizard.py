'''
v.0.0.9

LDSReplicate -  MainConfigWizard

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Created on 13/02/2013

@author: jramsay
'''


from PyQt4.QtGui import (QApplication, QWizard, QWizardPage, QLabel, 
                         QVBoxLayout, QHBoxLayout, QGridLayout, QRadioButton,
                         QRegExpValidator, QCheckBox, QMessageBox, QGroupBox,
                         QLineEdit,QToolTip, QFont, QComboBox, QPushButton, QFileDialog)
from PyQt4.QtCore import (QRegExp,QCoreApplication)

import re
import sys
import os

from lds.DataStore import DSReaderException, DatasourceConnectException, DatasourceOpenException, DatasourcePrivilegeException
from lds.WFSDataStore import WFSDataStore
from lds.LDSDataStore import LDSDataStore
from lds.ReadConfig import GUIPrefsReader, MainFileReader
from lds.LDSUtilities import LDSUtilities
from lds.VersionUtilities import AppVersion

from lds.PostgreSQLDataStore import PostgreSQLDataStore
from lds.MSSQLSpatialDataStore import MSSQLSpatialDataStore
from lds.FileGDBDataStore import FileGDBDataStore
from lds.SpatiaLiteDataStore import SpatiaLiteDataStore


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
        #Assumes being called from main dialog. If being run as standalone there won't be a parent.controls so just quit
        try:
            super(LDSConfigWizard,self).done(event)
            if self.parent and hasattr(self.parent,'controls'):
                self.parent.controls.setStatus(self.parent.controls.STATUS.IDLE,'UC Done')
        except:
            sys.exit(1)
        
        
class LDSConfigPage(QWizardPage):
    def __init__(self, parent=None,key=None):
        super(LDSConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key#'lds'
        
        try:
            (ldsurl,ldskey,ldssvc,ldsver,ldsfmt,ldscql) = self.parent.mfr.readLDSConfig()
        except:
            (ldsurl,ldskey,ldssvc,ldsver,ldsfmt,ldscql) = (None,)*6
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
        self.setSubTitle('Here you can enter a name for your custom configuration file, your LDS API key and required output. Also select whether you want to configure a proxy or enable password encryption')

        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        fileLabel = QLabel('User Config File')
        keyLabel = QLabel('LDS API Key')
        destLabel = QLabel('Output Type')
        internalLabel = QLabel('Save Layer-Config in DB')
        self.warnLabel = QLabel('!!!')
        encryptionLabel = QLabel('Enable Password Protection')
        serviceLabel = QLabel('Service Type')
        versionLabel = QLabel('Service Version')
        
        
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
                
        self.serviceSelect = QComboBox()
        self.serviceSelect.setToolTip('Choose from WFS (or one day, WMS)')
        for itemkey in ('','WFS','WMS','WMTS'):
            self.serviceSelect.addItem(itemkey)
            self.serviceSelect.setCurrentIndex(0)        
            
        self.versionSelect = QComboBox()
        self.versionSelect.setToolTip('Choose service Version')
        for itemkey in ('','1.0.0','1.1.0','2.0.0'):
            self.versionSelect.addItem(itemkey)
            self.versionSelect.setCurrentIndex(0)
        
        
        self.keyEdit.setValidator(QRegExpValidator(QRegExp("[a-fA-F0-9]{32}", re.IGNORECASE), self))
        
        #checkbox
        self.internalEnable = QCheckBox()
        self.internalEnable.setToolTip('Enable saving layer-config (per layer config and progress settings) internally')
        self.internalEnable.toggle()
        self.internalEnable.setChecked(True)
        self.internalEnable.stateChanged.connect(self.setWarn)
        
        self.encryptionEnable = QCheckBox()
        self.encryptionEnable.setToolTip('Encrypt any passwords saved to user config file')

        
        self.registerField(self.key+"file",self.fileEdit)
        self.registerField(self.key+"apikey",self.keyEdit)
        self.registerField(self.key+"dest",self.destSelect,"currentIndex")
        self.registerField(self.key+"internal",self.internalEnable)
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
        
        grid.addWidget(internalLabel, 4, 0)
        grid.addWidget(self.internalEnable, 4, 2)
        #if self.internalEnable.checkState(): grid.addWidget(intwarnLabel, 4, 4)
        
        grid.addWidget(encryptionLabel, 5, 0)
        grid.addWidget(self.encryptionEnable, 5, 2)
        
        svgrid = QGridLayout()
        svgrid.addWidget(serviceLabel, 0, 0) 
        svgrid.addWidget(self.serviceSelect, 0, 2) 
        svgrid.addWidget(versionLabel, 1, 0) 
        svgrid.addWidget(self.versionSelect, 1, 2)
        
        hbox = QHBoxLayout()
        hbox.addStretch(1)
        hbox.addLayout(svgrid)

        #layout       
        vbox = QVBoxLayout()
        vbox.addLayout(grid)
        #vbox.addLayout(hbox)
        vbox.addStretch(1)
        vbox.addWidget(self.warnLabel)
        vbox.addWidget(keyLinkLabel)
        vbox.addWidget(infoLinkLabel)
        
        self.setLayout(vbox)
        
    #def selectConfFile(self):
    #    self.fileEdit.setText(QFileDialog.getOpenFileName())

    def nextId(self):
        #if self.field(self.key+"proxy").toBool():
        return self.parent.plist.get('proxy')[0]
        #return int(self.field(self.key+"dest").toString())
        
    def setWarn(self):
        if self.internalEnable.checkState(): 
            ldslog.warn('Warning, Internal config selected')
            self.warnLabel.setText('!!!')
            QApplication.processEvents()
        else:
            ldslog.warn('External config selected')
            self.warnLabel.setText('^_^')
            QApplication.processEvents()
    
class ProxyConfigPage(QWizardPage):
    def __init__(self, parent=None,key=None):
        super(ProxyConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        try:
            (pxytype,pxyhost,pxyport,pxyauth,pxyusr,pxypwd) = self.parent.mfr.readProxyConfig()
        except:
            (pxytype,pxyhost,pxyport,pxyauth,pxyusr,pxypwd) = (None,)*6
            
        #if we use enums for pxy types
        #pxytype = [a[0] for a in WFSDataStore.PROXY_TYPE.reverse.items() if a[1]==pxytype][0]

            
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
        self.setSubTitle('Enter the hostname/ip-address, port number and authentication details of your HTTP proxy')

        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        directlabel = QLabel('Direct Connection')
        systemlabel = QLabel('Use System Proxy settings')
        proxylabel = QLabel('Configure Proxy')
        
        hostLabel = QLabel('Proxy Host')
        portLabel = QLabel('Proxy Port')
        authLabel = QLabel('Authentication')
        usrLabel = QLabel('Username')
        pwdLabel = QLabel('Password')
        
        
        #radio buttons
        self.directradio = QRadioButton()
        self.systemradio = QRadioButton()
        self.usrdefradio = QRadioButton()
        
        
        #edit boxes
        self.hostEdit = QLineEdit(pxyhost)
        self.hostEdit.setToolTip('Enter Proxy host (IP Address or hostname)')
        self.portEdit = QLineEdit(pxyport)
        self.portEdit.setToolTip('Enter Proxy port')
        
        #dropdown
        self.authSelect = QComboBox()
        self.authSelect.addItem('')
        self.authSelect.setToolTip('Select appropriate proxy authentication mechanism')
        self.authSelect.addItems(WFSDataStore.PROXY_AUTH)
        self.authSelect.setCurrentIndex(0 if LDSUtilities.assessNone(pxyauth) is None else WFSDataStore.PROXY_AUTH.index(pxyauth))
        
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
        
        self.registerField(self.key+WFSDataStore.PROXY_TYPE[0],self.directradio)
        self.registerField(self.key+WFSDataStore.PROXY_TYPE[1],self.systemradio)
        self.registerField(self.key+WFSDataStore.PROXY_TYPE[2],self.usrdefradio)

        #grid
        grid1 = QGridLayout()
        grid1.setSpacing(10)
        
        grid2 = QGridLayout()
        grid2.setSpacing(10)
        
        #layout
        hbox = QHBoxLayout()
        grid1.addWidget(self.directradio,1,0)
        grid1.addWidget(directlabel,1,1)
        grid1.addWidget(self.systemradio,2,0)
        grid1.addWidget(systemlabel,2,1)
        grid1.addWidget(self.usrdefradio,3,0)
        grid1.addWidget(proxylabel,3,1)
        hbox.addLayout(grid1)
        hbox.addStretch(1)
        
        
        self.gbox = QGroupBox('Proxy Configuration')

        #dsu
        subs = False
        if pxytype == WFSDataStore.PROXY_TYPE[1]:
            #system
            self.systemradio.setChecked(True)
        elif pxytype == WFSDataStore.PROXY_TYPE[2]:
            #user_defined
            self.usrdefradio.setChecked(True)
            subs = True
        else:
            #direct (default)
            self.directradio.setChecked(True)
            
        self.setUserDefined(subs)
        
        self.directradio.clicked.connect(self.disableUserDefined)
        self.systemradio.clicked.connect(self.disableUserDefined)
        self.usrdefradio.clicked.connect(self.enableUserDefined)
        
        grid2.addWidget(hostLabel, 1, 0)
        grid2.addWidget(self.hostEdit, 1, 2)
        
        grid2.addWidget(portLabel, 2, 0)
        grid2.addWidget(self.portEdit, 2, 2)
        
        grid2.addWidget(authLabel, 3, 0)
        grid2.addWidget(self.authSelect, 3, 2)
        
        grid2.addWidget(usrLabel, 4, 0)
        grid2.addWidget(self.usrEdit, 4, 2)
        
        grid2.addWidget(pwdLabel, 5, 0)
        grid2.addWidget(self.pwdEdit, 5, 2)
             
        self.gbox.setLayout(grid2)
        
        #layout    
        vbox = QVBoxLayout()
        vbox.addLayout(hbox)
        vbox.insertWidget(1,self.gbox)
        self.setLayout(vbox)  
        
    def selectConfFile(self):
        self.fileEdit.setText(QFileDialog.getOpenFileName())

    def nextId(self):
        #now go to selected dest configger
        #return int(self.field("ldsdest").toString())
    
        if self.testConnection():
            return int(self.field("ldsdest").toString())
        return self.parent.plist.get('proxy')[0]
    
    def disableUserDefined(self):
        self.setUserDefined(False)
        
    def enableUserDefined(self):
        self.setUserDefined(True)
        
    def setUserDefined(self,udval):
        self.gbox.setEnabled(udval)
        self.hostEdit.setEnabled(udval)
        self.portEdit.setEnabled(udval)
        self.authSelect.setEnabled(udval)
        self.usrEdit.setEnabled(udval)
        self.pwdEdit.setEnabled(udval)
        
    def testConnection(self):
        if not self.usrdefradio.isChecked(): 
            return True
        if not any(f for f in (self.hostEdit.isModified(),self.portEdit.isModified(),
                               self.usrEdit.isModified(),self.pwdEdit.isModified())):
            return False
        proxydata = {'type':'USER','host':str(self.hostEdit.text()),'port':str(self.portEdit.text()),
                     'auth':str(WFSDataStore.PROXY_AUTH[self.authSelect.currentIndex()-1]),
                     'user':str(self.usrEdit.text()),'pass':str(self.pwdEdit.text())}
        wfsdata = {'key':'00112233445566778899aabbccddeeff'}#key not necessary but config tester checks format
        lds = LDSDataStore(None,{'Proxy':proxydata,'WFS':wfsdata}) 
        lds.applyConfigOptions()
        
        try:
            #use website likely to be up (that isn't LDS so error is distinct)
            lds.initDS('http://www.google.com/',False)
        except DatasourceConnectException as dce:
            QMessageBox.warning(self, 'Connection Error', 'Cannot connect to network using proxy parameters provided {}'.format(dce), 'OK')
            return False
        except DatasourceOpenException as dse:
            QMessageBox.info(self, 'Connection Warning', 'Connection parameters confirmed, Datasource initialisation untested. Continuing.\n{}'.format(dse), 'OK')
            return True
        except RuntimeError as rte:
            QMessageBox.warning(self, 'RuntimeError', 'Error connecting to network: '+str(rte), 'OK')
            return False
        return True
 
            
class PostgreSQLConfigPage(QWizardPage):
    def __init__(self,parent=None,key=None):
        super(PostgreSQLConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        
        try:
            (pghost,pgport,pgdbname,pgschema,pgusr,pgpwd,pgover,pgconfig,pgepsg,pgcql) = self.parent.mfr.readPostgreSQLConfig()
        except:
            (pghost,pgport,pgdbname,pgschema,pgusr,pgpwd,pgover,pgconfig,pgepsg,pgcql) = (None,)*10
        
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
        self.portEdit = QLineEdit('5432' if LDSUtilities.assessNone(pgport) is None else pgport)
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
        cs = PostgreSQLDataStore.buildConnStr(self.hostEdit.text(),self.portEdit.text(),self.dbnameEdit.text(),
                            self.schemaEdit.text(),self.usrEdit.text(),self.pwdEdit.text())
        pg = PostgreSQLDataStore(cs,None)
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

        try:
            (msodbc,msserver,msdsn,mstrust,msdbname,msschema,msusr,mspwd,msconfig,msepsg,mscql) = self.parent.mfr.readMSSQLConfig()
        except:
            (msodbc,msserver,msdsn,mstrust,msdbname,msschema,msusr,mspwd,msconfig,msepsg,mscql) = (None,)*11
        
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
        self.trustCheckBox.setChecked(mstrust and mstrust.lower()=='yes')
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
        if not any(f for f in (self.serverEdit.isModified(),self.dbnameEdit.isModified(),self.schemaEdit.isModified(),
                               self.usrEdit.isModified(),self.pwdEdit.isModified())):
            return False
        cs = MSSQLSpatialDataStore.buildConnStr(self.serverEdit.text(),self.dbnameEdit.text(),self.schemaEdit.text(),
                            'yes' if self.trustCheckBox.isChecked() else 'no',self.usrEdit.text(),self.pwdEdit.text())
        ms = MSSQLSpatialDataStore(cs,None)
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
        self.text_entered = False
        
        try:
            (fgfname,fgconfig,fgepsg,fgcql) = self.parent.mfr.readFileGDBConfig()
        except:
            (fgfname,fgconfig,fgepsg,fgcql) = (None,)*4
        
        self.filter = ".*\.gdb$"
        
        self.setTitle(self.parent.plist.get(self.key)[1]+' Configuration Options')
        self.setSubTitle('Enter your FileGDB directory name. (This must carry a .gdb suffix)')

        
        QToolTip.setFont(QFont('SansSerif', 10))
        
        #labels
        fileLabel = QLabel('FileGDB DB directory')
        descLabel = QLabel('Enter the path to an existing FileGDB directory OR type in the name\nof a new FileGDB to create.\n')
        
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
        self.text_entered = False
        if re.match(self.filter,fdtext):
            self.deleteEmptyDir(fdtext)
            self.fileEdit.setText(fdtext)
            self.text_entered = True
        else:
            self.fileEdit.setText('')
        
    def deleteEmptyDir(self,fdtext):
        '''If the user creates an empty directory we delete it but retain the name so fgdb can init properly'''
        if os.path.exists(fdtext) and len(os.listdir(fdtext))==0:
            os.rmdir(str(fdtext))
        
        
    def nextId(self):
        if self.testConnection():
            return self.parent.plist.get('final')[0]
        return self.parent.plist.get('fg')[0]
    
    def testConnection(self):
        if not self.fileEdit.isModified() and not self.text_entered:
            return False
        
        fn = str(self.fileEdit.text())
        fg = FileGDBDataStore(fn,None)
        fg.applyConfigOptions()
        try:
            fg.initDS(fg.destinationURI(None),True)
            #ms.checkGeoPrivileges(self.usrEdit.text())
        except DSReaderException as dse:
            QMessageBox.warning(self, 'Connection Error', 'Cannot connect to FileGDB file using parameters provided: '+str(dse), 'OK')
            return False
        return True
        
        
class SpatiaLiteConfigPage(QWizardPage):
    def __init__(self,parent=None,key=None):
        super(SpatiaLiteConfigPage, self).__init__(parent)
        
        self.parent = parent 
        self.key = key
        self.text_entered = False
        
        try:
            (slfname,slconfig,slepsg,slcql) = self.parent.mfr.readSpatiaLiteConfig()
        except:
            (slfname,slconfig,slepsg,slcql) = (None,)*4
        
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
        self.text_entered = False
        if re.match(self.filter,fdtext):
            self.fileEdit.setText(fdtext)
            self.text_entered = True
        else:
            self.fileEdit.setText('')
        
    def nextId(self):    
        if self.testConnection():
            return self.parent.plist.get('final')[0]
        return self.parent.plist.get('sl')[0]
    
    def testConnection(self):
        if not self.fileEdit.isModified() and not self.text_entered:
            return False
        
        sn = str(self.fileEdit.text())
        sl = SpatiaLiteDataStore(sn,None)
        sl.applyConfigOptions()
        try:
            sl.initDS(sl.destinationURI(None),True)
            #ms.checkGeoPrivileges(self.usrEdit.text())
        except DSReaderException as dse:
            QMessageBox.warning(self, 'Connection Error', 'Cannot connect to SpatiaLite file using parameters provided: '+str(dse), 'OK')
            return False
        return True
    
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
        
        hbox7 = QHBoxLayout()
        hbox7.addWidget(QLabel('In/Ex Config'))
        hbox7.addWidget(QLabel('Internal' if self.field("ldsinternal").toBool() else 'External'))   
        
        vbox.addLayout(hbox7)
    
        sec2 = QLabel('Proxy')
        sec2.setFont(hfont)
        vbox.addWidget(sec2)
        
        if self.field("proxy"+WFSDataStore.PROXY_TYPE[2]).toBool():
        #if self.field("proxyproxy").toBool():
            hboxp = QHBoxLayout()
            hboxp.addWidget(QLabel('Proxy Type'))     
            hboxp.addWidget(QLabel(WFSDataStore.PROXY_TYPE[2]))   
            
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
        elif self.field("proxy"+WFSDataStore.PROXY_TYPE[1]).toBool():
            hbox3 = QHBoxLayout()
            hbox3.addWidget(QLabel('Proxy Type'))     
            hbox3.addWidget(QLabel(WFSDataStore.PROXY_TYPE[1])) 
            vbox.addLayout(hbox3)
              
        elif self.field("proxy"+WFSDataStore.PROXY_TYPE[0]).toBool():
            hbox3 = QHBoxLayout()
            hbox3.addWidget(QLabel('Proxy Type'))     
            hbox3.addWidget(QLabel(WFSDataStore.PROXY_TYPE[0]))  
            vbox.addLayout(hbox3)
        else:
            ldslog.error('No Proxy defined') 
            
            
        #select dest from index of destcombo ie PostgreSQL, FileGDB etc
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
        inex = 'internal' if self.field("ldsinternal").toBool() else 'external'
        
        buildarray = ()
        
        buildarray += ((MFR.LDSN,'key',str(self.field("ldsapikey").toString())),)
        
        #proxy type = user defined
        if self.field("proxy"+WFSDataStore.PROXY_TYPE[2]).toBool():
            buildarray += ((MFR.PROXY,'type',WFSDataStore.PROXY_TYPE[2]),)
            buildarray += ((MFR.PROXY,'host',str(self.field("proxyhost").toString())),)
            buildarray += ((MFR.PROXY,'port',str(self.field("proxyport").toString())),)
            buildarray += ((MFR.PROXY,'auth',WFSDataStore.PROXY_AUTH[int(self.field("proxyauth").toString())-1]),)
            buildarray += ((MFR.PROXY,'user',str(self.field("proxyusr").toString())),)
            pwd = self.field("proxypwd").toString()
            if encrypt:
                pwd = Encrypt.ENC_PREFIX+Encrypt.secure(pwd)
            buildarray += ((MFR.PROXY,'pass',pwd),)
            
        #proxy type = system
        elif self.field("proxy"+WFSDataStore.PROXY_TYPE[1]).toBool():
            buildarray += ((MFR.PROXY,'type',WFSDataStore.PROXY_TYPE[1]),)            
            buildarray += ((MFR.PROXY,'host',''),)
            buildarray += ((MFR.PROXY,'port',''),)
            buildarray += ((MFR.PROXY,'auth',''),)
            buildarray += ((MFR.PROXY,'user',''),)
            buildarray += ((MFR.PROXY,'pass',''),)
#            if os.name == 'nt':
#                #windows
#                from lds.WinUtilities import Registry as WR
#                (_,host,port) = WR.readProxyValues()
#                buildarray += ((MFR.PROXY,'host',host),)
#                buildarray += ((MFR.PROXY,'port',port),)
#            else:
#                #unix etc
#                hp = os.environ('http_proxy')
#                rm = re.search('http://([a-zA-Z0-9_\.\-]+):(\d+)',hp)
#                buildarray += ((MFR.PROXY,'host',rm.group(1)),)
#                buildarray += ((MFR.PROXY,'port',rm.group(2)),)               
            
        #proxy type = direct
        elif self.field("proxy"+WFSDataStore.PROXY_TYPE[0]).toBool():
            buildarray += ((MFR.PROXY,'type',WFSDataStore.PROXY_TYPE[0]),)            
            buildarray += ((MFR.PROXY,'host',''),)
            buildarray += ((MFR.PROXY,'port',''),)
            buildarray += ((MFR.PROXY,'auth',''),)
            buildarray += ((MFR.PROXY,'user',''),)
            buildarray += ((MFR.PROXY,'pass',''),)

        section = self.selected[0]
        for sf in self.selectedfields:
            name = sf[0]
            value = sf[2]
            if name == 'pass' and encrypt:
                value = Encrypt.ENC_PREFIX+Encrypt.secure(value)
            buildarray += ((section,name,value),)

        buildarray += ((section,'config',inex),)
        ucfile = str(self.field("ldsfile").toString())
        #ucfile = LDSUtilities.standardiseUserConfigName(str(self.field("ldsfile").toString()))
        
        #save values to user config file 
        self.parent.setMFR(self.ldsfile)
        ConfigWrapper.writeUserConfigData(self.parent.getMFR(), buildarray)
        #save userconf and dest to gui prefs
        gpr = GUIPrefsReader()
        #zips with (dest,lgsel,layer,uconf...
        gpr.write(( section,'',ucfile))
        
        #pass back name of UC file for GUI dialog
        if self.ldsfile and self.parent.parent and hasattr(self.parent.parent,'controls'):
            self.parent.parent.controls.confcombo.setEditText(self.ldsfile)
            
        return rv

        
def main():
    #func to call config wizz
    app0 = QApplication(sys.argv)
    ldsc = LDSConfigWizard()
    ldsc.show()

    sys.exit(app0.exec_())
    
    
    
if __name__ == '__main__':
    main()