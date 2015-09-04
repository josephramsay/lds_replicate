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

import os
import re
import sys
import platform

import _winreg
from _winreg import *

class WinUtilities(object):
    '''Windows utility/info functions.'''
     
    @staticmethod
    def callStartFile(file):
        os.startfile(file)
        
    @staticmethod
    def getArchitecture():
        a = int(re.match('(\d+)',platform.architecture()[0]).group(1))
        b = 64 if sys.maxsize>1e11 else 32
        return (a+b)/2
        
        
class Registry(object):
    '''Windows Registry functions''' 
    INTERNET_SETTINGS = _winreg.OpenKey(_winreg.HKEY_CURRENT_USER, r'Software\Microsoft\Windows\CurrentVersion\Internet Settings', 0, _winreg.KEY_ALL_ACCESS)
        
    @staticmethod
    def readProxyValues():
        enable = Registry._getRegistryKey('ProxyEnable')
        hp = Registry._getRegistryKey('ProxyServer')
        host,port = hp.split(':')
        return (enable,host,port)    
        
    @staticmethod
    def writeProxyValues(host,port):
        hp = str(host)+":"+str(port)
        Registry._setRegistryKey('ProxyEnable',1)
        Registry._setRegistryKey('ProxyServer',hp)
        
    @staticmethod
    def readInstDir(name):
        return Registry._readAppVal(name)
        
    #---------------------------------------------------------
    
    @classmethod
    def _readAppVal(cls,name):
        '''Used to find name in reg i.e. install path to LDSR app'''
        ipath = 0
        val = None
        arch = WinUtilities.getArchitecture() 
        if arch == 32: ipath = r'SOFTWARE\LDS Replicate'
        elif arch == 64: ipath = r'SOFTWARE\Wow6432Node\LDS Replicate'
        try:    
            key = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, ipath, 0, _winreg.KEY_READ)
            val,_ = _winreg.QueryValueEx(key, name)
        except: pass
        return val

    @classmethod
    def _setRegistryKey(cls, name, value):
        _, reg_type = _winreg.QueryValueEx(cls.INTERNET_SETTINGS, name)
        _winreg.SetValueEx(cls.INTERNET_SETTINGS, name, 0, reg_type, value)
        
    @classmethod
    def _getRegistryKey(cls, name):
        val ,_ = _winreg.QueryValueEx(cls.INTERNET_SETTINGS, name)
        return val



    

