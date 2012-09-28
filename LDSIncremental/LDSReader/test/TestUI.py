'''
Created on 17/08/2012

@author: jramsay
'''
import unittest
import os


class TestUI(unittest.TestCase):
    '''basic test of command line arguments and whether they work as expected'''

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test1CmdLine_Help(self):
        os.system('python main.py -h')
        
    def test2CmdLine_AutoLayer(self):
        os.system('python main.py -l v:x787 pg')
        
    def test3CmdLine_DefLayer(self):
        os.system('python main.py -l v:x787 -f 2010-01-01 -t 2012-07-01 pg')
        
    def test4CmdLine_AllFD(self):
        os.system('python main.py -l v:x787 -f ALL pg')
    


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()