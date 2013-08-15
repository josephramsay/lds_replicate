'''
v.0.0.9

LDSReplicate -  WinUtilities

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Windows only utilities, to be included in Windows build only 

Created on 9/08/2012

@author: jramsay
'''
# for windows lxml binary from here http://www.lfd.uci.edu/~gohlke/pythonlibs/#lxml

import re
import os
import logging

import _winreg
from _winreg import *


ldslog = LDSUtilities.setupLogging()

class WinUtilities(object):
    '''Windows specific functions.'''
     
    @staticmethod
    def callStartFile(file):
        os.startfile(file)
        
        
class Registry(object):
    '''Windows Registry functions''' 
    
    INTERNET_SETTINGS = _winreg.OpenKey(_winreg.HKEY_CURRENT_USER, r'Software\Microsoft\Windows\CurrentVersion\Internet Settings', 0, _winreg.KEY_ALL_ACCESS)
         
    @staticmethod
    def readProxyValues():
        enable = Registry.getRegistryKey('ProxyEnable')
        hp = Registry.getRegistryKey('ProxyServer')
        host,port = hp.split(':')
        return (enable,host,port)    
        
    @staticmethod
    def writeProxyValues(host,port):
        hp = str(host)+":"+str(port)
        Registry.setRegistryKey('ProxyEnable',1)
        Registry.setRegistryKey('ProxyServer',hp)
        
    
    
    @classmethod
    def setRegistryKey(cls, name, value):
        _, reg_type = _winreg.QueryValueEx(cls.INTERNET_SETTINGS, name)
        _winreg.SetValueEx(cls.INTERNET_SETTINGS, name, 0, reg_type, value)
        
    @classmethod
    def getRegistryKey(cls, name):
        val ,_ = _winreg.QueryValueEx(cls.INTERNET_SETTINGS, name)
        return val



    

