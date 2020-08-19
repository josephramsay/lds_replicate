# -*- coding: utf-8 -*-
'''
v.0.0.9

LDSReplicate -  ldsreplicate_plugin

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Plugin wrapper class for QGis

Created on 9/08/2015

@author: jramsay
'''


import os
import sys

from PyQt4.QtCore import QSettings, QTranslator, qVersion, QCoreApplication
from PyQt4.QtGui import QAction, QIcon
# Initialize Qt resources from file resources.py
###import resources_qrc

from lds.WinUtilities import WinUtilities, Registry
WIN_ARCH = WinUtilities.getArchitecture()        
INST_DIR = Registry.readInstDir('Path')+'\\{}'
#list gets explicitly reverse ordered based on occurance so items at the top go to the top of the sys.path
ADD_PATH = [(i,INST_DIR.format(p)) for i,p in enumerate([
    'bin',
    'bin\\lxml',
    'bin\\configparser',
    'bin\\Crypto',
    'bin\\gdal\\plugins',     
    'bin\\gdal\\python\\osgeo', 
    'bin\\gdal\\plugins-optional',
    'bin\\gdal\\plugins-external' ,
    'bin\\gdal\\python',
    ])]

class LDSReplicatePlugin:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        '''cons'''
        # Save reference to the QGIS interface
        self.iface = iface

        self.sysPathAppend(ADD_PATH) 
        self.actions = []
        self.menu = self.tr(u'&LDSR')
        self.toolbar = self.iface.addToolBar(u'LDSR')
        return


    # noinspection PyMethodMayBeStatic
    def tr(self, message):
        ''''Get the translation for a string using Qt translation API.'''
        # noinspection PyTypeChecker,PyArgumentList,PyCallByClass
        return QCoreApplication.translate('LDSReplicate', message)


    def add_action(self,icon_path,text,callback,enabled_flag=True,add_to_menu=True,add_to_toolbar=True,status_tip=None,whats_this=None,parent=None):
        '''Adds an item to the toolbar.'''

        icon = QIcon(icon_path)
        action = QAction(icon, text, parent)
        action.triggered.connect(callback)
        action.setEnabled(enabled_flag)

        if status_tip is not None:
            action.setStatusTip(status_tip)

        if whats_this is not None:
            action.setWhatsThis(whats_this)

        if add_to_toolbar:
            self.toolbar.addAction(action)

        if add_to_menu:
            self.iface.addPluginToMenu(self.menu, action)

        self.actions.append(action)

        return action

    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""
        import gdal
        #from osgeo import gdal
        return
        
        from lds.gui.LDSGUI import LINZ, UC, LC
        
        self.add_action(LINZ,
            text=self.tr(u'LDS Replicate'),
            callback=self.run,
            parent=self.iface.mainWindow())
        
        self.add_action(LC,
            text=self.tr(u'LayerConfig Setup'),
            callback=self.lconf,
            parent=self.iface.mainWindow())
        
        self.add_action(UC,
            text=self.tr(u'UserConfig Setup'),
            callback=self.uconf,
            parent=self.iface.mainWindow())


    def unload(self):
        """Removes the plugin menu item and icon from QGIS GUI."""
        for action in self.actions:
            self.iface.removePluginMenu(self.tr(u'&LDSReplicate'),action)
            self.iface.removeToolBarIcon(action)
        # remove the toolbar
        del self.toolbar


    def run(self):
        """Run method that performs all the real work"""
        from lds.gui import LDSGUI
        lg = LDSGUI.LDSMain()
        lg.show()
        
    def lconf(self):
        from lds.gui.LayerConfigSelector import LayerConfigSelector
        lcs = LayerConfigSelector(LDSMain(initlc=True))
        lcs.show()
        
    def uconf(self):
        from lds.gui.MainConfigWizard import LDSConfigWizard
        lcw = LDSConfigWizard()
        lcw.show()
        
        
    @staticmethod
    def sysPathAppend(plist):
        '''Append library paths to sys.path if missing'''
        for pth in [os.path.realpath(p[1]) for p in sorted(plist,reverse=True)]:
                if pth not in sys.path:
                    #print p,'->',sys.path
                    sys.path.insert(1, pth)

