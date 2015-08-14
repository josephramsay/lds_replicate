# -*- coding: utf-8 -*-
'''
v.0.0.9

LDSReplicate -  LDSUtilities

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Simple LDS utilities class with QT

Created on 9/08/2015

@author: jramsay
'''


import re
import logging

#ldslog = LDSUtilities.setupLogging()
mainlog = 'DEBUG'
ldslog = logging.getLogger(mainlog)

try:  
    from PyQt4.QtCore import QString  
except ImportError:  
    # we are using Python3 or QGis so QString is not defined  
    QString = str 

class LQTUtilities(object):    
    @staticmethod
    def readWidgetText(wtxt):
        #combo self.lgcombo.currentText().toUtf8().data()
        #lineedit self.keywordcombo.lineEdit().text().toUtf8().data()
        return wtxt.toUtf8().data() if isinstance(wtxt,QString) else wtxt

