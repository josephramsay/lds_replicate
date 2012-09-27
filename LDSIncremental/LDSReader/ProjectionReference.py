'''
Created on 17/08/2012

@author: jramsay
'''
import osr
import logging

ldslog =  logging.getLogger('LDS')

class Projection(object):
    '''
    classdocs
    '''


    def __init__(self):
 
        '''EPSG Projection 2193 - NZGD2000 / New Zealand Transverse Mercator 2000'''
        self.EPSG2193_ogc = 'PROJCS["NZGD2000 / New Zealand Transverse Mercator 2000",GEOGCS["NZGD2000",DATUM["New_Zealand_Geodetic_Datum_2000",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6167"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4167"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",173],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",1600000],PARAMETER["false_northing",10000000],AUTHORITY["EPSG","2193"],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'
        self.EPSG2193_esri = 'PROJCS["NZGD2000 / New Zealand Transverse Mercator 2000",GEOGCS["NZGD2000",DATUM["D_NZGD_2000",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",173],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",1600000],PARAMETER["false_northing",10000000],UNIT["Meter",1]]'
        
        '''EPSG Projection 4167 - NZGD2000'''
        self.EPSG4167_ogc = 'GEOGCS["NZGD2000",DATUM["New_Zealand_Geodetic_Datum_2000",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6167"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4167"]]'
        self.EPSG4167_esri = 'GEOGCS["NZGD2000",DATUM["D_NZGD_2000",SPHEROID["GRS_1980",6378137,298.257222101]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'
        
        '''EPSG Projection 27200 - NZGD49 / New Zealand Map Grid'''
        self.EPSG27200_ogc = 'PROJCS["NZGD49 / New Zealand Map Grid",GEOGCS["NZGD49",DATUM["New_Zealand_Geodetic_Datum_1949",SPHEROID["International 1924",6378388,297,AUTHORITY["EPSG","7022"]],TOWGS84[59.47,-5.04,187.44,0.47,-0.1,1.024,-4.5993],AUTHORITY["EPSG","6272"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4272"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["New_Zealand_Map_Grid"],PARAMETER["latitude_of_origin",-41],PARAMETER["central_meridian",173],PARAMETER["false_easting",2510000],PARAMETER["false_northing",6023150],AUTHORITY["EPSG","27200"],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'
        self.EPSG27200_esri = 'PROJCS["NZGD49 / New Zealand Map Grid",GEOGCS["NZGD49",DATUM["D_New_Zealand_1949",SPHEROID["International_1924",6378388,297]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["New_Zealand_Map_Grid"],PARAMETER["latitude_of_origin",-41],PARAMETER["central_meridian",173],PARAMETER["false_easting",2510000],PARAMETER["false_northing",6023150],UNIT["Meter",1]]'
        
    def getDefaultProjection(self):
        return self.getProjection(2193)
    
    @classmethod
    def getProjection(self,pid):
        return {
            2193:self.EPSG2193_esri,
            4167:self.EPSG4167_esri,
            27200:self.EPSG27200_esri
        }.get(pid,self.getDefaultProjection())
        
    @classmethod
    def getDefaultSpatialRef(self):
        srs = osr.SpatialReference()
        srs.SetGeogCS("GCS_NZGD_2000","D_NZGD_2000","GRS_1980",6378137.0,298.257222101,"Greenwich",0.0,"Degree",0.0174532925199433)
        srs.SetAuthority("GEOGCS","EPSG",4167)
        return srs
    
    @classmethod
    def modifyMorphedSpatialReference(self,sref):
        geogcs = sref.ExportToWkt()
        if geogcs[8:16]=='NZGD2000':
            geogcs = geogcs.replace('NZGD2000','GCS_NZGD_2000')
            sref.ImportFromWkt(geogcs)
        return sref
        
    
    @classmethod
    def validateEPSG(self,epsg):
        try:
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(int(epsg))
            #srs.SetFromUserInput(epsg)#consider allowing WKT
        except:
            ldslog.warning("Invalid EPSG,"+str(epsg)+". Reprojection disabled")
            print "Invalid EPSG,",epsg,". Reprojection disabled"
            srs = None
        return srs
    
        
        
class Geometry(object):
    
    def __init__(self):
        '''
        Constructor
        '''
        self.DEF = 'SHAPE'
        
    @classmethod
    def getGeoTransform(self):
        return self.DEF