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
    from lds.gui.LayerConfigSelector import LayerConfigSelector
    app = QApplication(sys.argv)
    ldsc = LayerConfigSelector()
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
    #main application
    from  lds.gui.LDSGUI import LDSRepl
    app = QApplication(sys.argv)
    lds = LDSRepl()
    lds.show()
    sys.exit(app.exec_())
    
    
if __name__ == '__main__':
    main()
    
    
   