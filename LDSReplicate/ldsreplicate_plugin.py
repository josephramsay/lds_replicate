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

from PyQt4.QtCore import QSettings, QTranslator, qVersion, QCoreApplication
from PyQt4.QtGui import QAction, QIcon
# Initialize Qt resources from file resources.py
###import resources_qrc

from lds.gui import LDSGUI
from lds.gui.LDSGUI import LDSMain, LINZ, UC, LC
from lds.gui.MainConfigWizard import LDSConfigWizard
from lds.gui.LayerConfigSelector import LayerConfigSelector

class LDSReplicatePlugin:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        '''cons'''
        # Save reference to the QGIS interface
        self.iface = iface
        
        self.actions = []
        self.menu = self.tr(u'&LDSR')
        self.toolbar = self.iface.addToolBar(u'LDSR')

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
        lg = LDSGUI.LDSMain()
        lg.show()
        
    def lconf(self):
        lcs = LayerConfigSelector(LDSMain(initlc=True))
        lcs.show()
        
    def uconf(self):
        lcw = LDSConfigWizard()
        lcw.show()

