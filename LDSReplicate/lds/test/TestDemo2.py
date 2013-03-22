'''
v.0.0.1

LDSReplicate -  TestDemo

Copyright 2011 Crown copyright (c)
Land Information New Zealand and the New Zealand Government.
All rights reserved

This program is released under the terms of the new BSD license. See the 
LICENSE file for more information.

Tests for demo cases

Created on 17/09/2012

@author: jramsay
'''

import unittest
import os
import sys
import logging

ldslog = logging.getLogger('LDS')
ldslog.setLevel(logging.DEBUG)


path = os.path.normpath(os.path.join(os.path.dirname(__file__), "../../log/"))
if not os.path.exists(path):
    os.mkdir(path)
df = os.path.join(path,"debug.log")

#df = '../debug.log'
fh = logging.FileHandler(df,'w')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(module)s - %(message)s')
fh.setFormatter(formatter)
ldslog.addHandler(fh)


#class TestUI(LDSIncrTestCase):
class TestDemo2(unittest.TestCase):
    '''Basic tests of ldsreplicate.py using command line arguments to see whether they work as expected'''
    
    #select ST_ASText(ST_SimplifyPreserveTopology(ST_Buffer(ST_Transform(shape, 4167),0.05),0.05)) from public.regional_councils_2012 where regc12 = '08'
    #CLIP = 'POLYGON((174.739222 -39.810771,174.926194 -39.548552,174.625801 -39.137798,174.721585 -39.031124,174.742907 -38.796715,174.941195 -38.617184,175.625963 -38.439547,175.686786 -38.565822,175.58734 -38.751797,175.609528 -38.868575,175.731932 -38.976883,175.640608 -39.240826,175.796584 -39.246325,176.020399 -39.02932,176.120962 -39.094031,176.244005 -39.502942,176.17109 -39.965283,176.415972 -40.12476,176.454172 -40.344817,176.912877 -40.44767,176.92454 -40.562462,176.569138 -40.860852,176.209807 -40.72145,175.878108 -40.827484,175.490379 -40.754243,175.184611 -40.808433,174.80462 -40.646209,174.915349 -40.338567,174.835197 -40.172632,174.669511 -40.084037,174.739222 -39.810771
    #CLIP = 'POLYGON((174.7392 -39.8108,174.9262 -39.5486,174.6258 -39.1378,174.7216 -39.0311,174.7429 -38.7967,174.9412 -38.6172,175.626 -38.4395,175.6868 -38.5658,175.5873 -38.7518,175.6095 -38.8686,175.7319 -38.9769,175.6406 -39.2408,175.7966 -39.2463,176.0204 -39.0293,176.121 -39.094,176.244 -39.5029,176.1711 -39.9653,176.416 -40.1248,176.4542 -40.3448,176.9129 -40.4477,176.9245 -40.5625,176.5691 -40.8609,176.2098 -40.7214,175.8781 -40.8275,175.4904 -40.7542,175.1846 -40.8084,174.8046 -40.6462,174.9153 -40.3386,174.8352 -40.1726,174.6695 -40.084,174.7392 -39.8108))'        
    #POLYGON((174.739222311851 -39.8107711630911,174.926194085388 -39.548552006442,174.625801314617 -39.1377975005512,174.721584697209 -39.0311236834587,174.742906940682 -38.7967145894666,174.941195210777 -38.6171835749901,175.625963241006 -38.439547493451,175.686786296678 -38.5658224831574,175.587340410075 -38.7517971166413,175.609528418532 -38.868575331127,175.731932409022 -38.9768831268206,175.640608497725 -39.2408263907197,175.796583558305 -39.2463254439309,176.020399269563 -39.029319709085,176.120961931721 -39.094030919849,176.244004822582 -39.5029416530752,176.171089655395 -39.9652826592673,176.41597154341 -40.1247604993454,176.454172406029 -40.3448170543318,176.912876724971 -40.4476698522701,176.924539945874 -40.5624618096635,176.569137675298 -40.8608519304872,176.209807438558 -40.7214498996482,175.878108207742 -40.8274842019004,175.49037896591 -40.7542430140045,175.184610729962 -40.8084332358987,174.804620042923 -40.646208533341,174.915349011546 -40.3385667586135,174.835196784541 -40.1726323475811,174.669511303723 -40.0840374182088,174.739222311851 -39.8107711630911))'


    def setUp(self):
        self.MS="MSSQL:server=LZ104588-VM\SQLExpress;database=LDSINCR;UID=mssqluser;PWD=mssqlpass;Driver=SQL Server Native Client 11.0;Schema=dbo"
        #super(TestUI,self).setUp()
        self.APP = '../../ldsreplicate.py'
        if 'inux' in sys.platform:
            self.DB = 'pg'
            self.UC = "../conf/ldsincr.lnx.conf"
            self.CLIP = "POLYGON((174.739222311851 -39.8107711630911,174.926194085388 -49.548552006442,174.625801314617 -39.1377975005512,174.721584697209 -39.0311236834587,174.742906940682 -38.7967145894666,174.941195210777 -38.6171835749901,175.625963241006 -38.439547493451,175.686786296678 -38.5658224831574,175.587340410075 -38.7517971166413,175.609528418532 -38.868575331127,175.731932409022 -38.9768831268206,175.640608497725 -39.2408263907197,175.796583558305 -39.2463254439309,176.020399269563 -39.029319709085,176.120961931721 -39.094030919849,176.244004822582 -39.5029416530752,176.171089655395 -39.9652826592673,176.41597154341 -40.1247604993454,176.454172406029 -40.3448170543318,176.912876724971 -40.4476698522701,176.924539945874 -40.5624618096635,176.569137675298 -40.8608519304872,176.209807438558 -40.7214498996482,175.878108207742 -40.8274842019004,175.49037896591 -40.7542430140045,175.184610729962 -40.8084332358987,174.804620042923 -40.646208533341,174.915349011546 -40.3385667586135,174.835196784541 -40.1726323475811,174.669511303723 -40.0840374182088,174.739222311851 -39.8107711630911))"
            self.BB = "'within(shape,geomFromWkt("+self.CLIP+"))'"
        elif 'win32' in sys.platform:
            self.DB = 'ms'
            self.UC = "../conf/ldsincr.win.conf"
            self.CLIP = "POLYGON((174.739222311851 -39.8107711630911,174.926194085388 -39.548552006442,174.625801314617 -39.1377975005512,174.721584697209 -39.0311236834587,174.742906940682 -38.7967145894666,174.941195210777 -38.6171835749901,175.625963241006 -38.439547493451,175.686786296678 -38.5658224831574,175.587340410075 -38.7517971166413,175.609528418532 -38.868575331127,175.731932409022 -38.9768831268206,175.640608497725 -39.2408263907197,175.796583558305 -39.2463254439309,176.020399269563 -39.029319709085,176.120961931721 -39.094030919849,176.244004822582 -39.5029416530752,176.171089655395 -39.9652826592673,176.41597154341 -40.1247604993454,176.454172406029 -40.3448170543318,176.912876724971 -40.4476698522701,176.924539945874 -40.5624618096635,176.569137675298 -40.8608519304872,176.209807438558 -40.7214498996482,175.878108207742 -40.8274842019004,175.49037896591 -40.7542430140045,175.184610729962 -40.8084332358987,174.804620042923 -40.646208533341,174.915349011546 -40.3385667586135,174.835196784541 -40.1726323475811,174.669511303723 -40.0840374182088,174.739222311851 -39.8107711630911))"
            self.BB = "\"within(shape,geomFromWkt("+self.CLIP+"))\""
        else:
            sys.exit(1)


            
            
    def tearDown(self):
        #super(TestUI,self).tearDown()
        pass


#    def test01Parcels(self):
#        '''parcels'''
#        #772 = nz_primary_parcels
#        l = 772
#        self.prepLayer(l, self.DB)
#        st = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" -e 2193 -c "+self.BB+" init "+self.DB
#        print st
#        #self.assertEquals(os.system(st),0)
#        
#        
#    def test02Roads(self):
#        '''Road subsections'''
#        #793 = nz_road_centre_line_subsections_electoral
#        l = 793
#        self.prepLayer(l, self.DB)
#        st = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" -e 2193 -c "+self.BB+" "+self.DB
#        print st
#        self.assertEquals(os.system(st),0)
#        
#        
#    def test03StreetAddr(self):
#        '''test boundingbox command and sr conversion'''
#        #779 = nz_street_address_electoral
#        l = 779
#        self.prepLayer(l, self.DB)
#        st = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" -e 2193 -c "+self.BB+" "+self.DB
#        print st
#        self.assertEquals(os.system(st),0)
#        
#        
#    def test04GeodeticMarks(self):
#        '''geodetic marks'''
#        #787 = nz_geodetic_marks
#        l = 787
#        self.prepLayer(l, self.DB)
#        st = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" -e 2193 -c "+self.BB+" "+self.DB
#        print st
#        self.assertEquals(os.system(st),0)
        
              
    def test05ASPTitleParcels(self):
        '''geodetic marks'''
        #1569 = nz_title_parcel_association_list
        l = 1569
        self.prepLayer(l, self.DB)
        st = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" "+self.DB
        print st
        self.assertEquals(os.system(st),0)     
        
        
    def test06ASPTitles(self):
        '''geodetic marks'''
        #1567 = nz_property_titles_list
        l = 1567
        self.prepLayer(l, self.DB)
        st = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" "+self.DB
        print st
        self.assertEquals(os.system(st),0)    
        
        
    def test07ASPTitlesOwners(self):
        '''Titles Owners - private table'''
        #1564 = nz_property_titles_owners_list
        l = 1564
        self.prepLayer(l, self.DB)
        st = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" "+self.DB
        print st
        self.assertEquals(os.system(st),0)
                
                
    def test08ASPTitleEstates(self):
        '''Title Estates'''
        #1566 = nz_property_title_estates_list
        l = 1566
        self.prepLayer(l, self.DB)
        st = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" "+self.DB
        print st
        self.assertEquals(os.system(st),0)
        
#    def test09LandDistrict(self):
#        '''geodetic marks'''
#        #785 = nz_land_districts
#        l = 785
#        self.prepLayer(l, self.DB)
#        st = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" -e 2193 -c "+self.BB+" "+self.DB
#        print st
#        self.assertEquals(os.system(st),0)
#              

    def prepLayer(self,l,o):
        '''Common layer clean function'''
        stc = "python "+self.APP+" -u "+self.UC+" -l v:x"+str(l)+" clean "+self.DB
        print stc
        os.system(stc)
        
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testLDSRead']
    unittest.main()