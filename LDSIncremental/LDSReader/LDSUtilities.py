'''
Created on 28/08/2012

@author: jramsay
'''


import re

class LDSUtilities(object):
    '''Does the LDS related stuff not specifically part of the datastore''' 
    
    def __init__(self):
        pass
    
    
    @classmethod
    def splitLayerName(self,layername):
        '''layer name typically in the format v:x###'''
        '''TODO. test for valid layername'''
        return "/"+layername.split(":")[0]+"/"+layername.split(":")[1]
    
    @classmethod
    def cropChangeset(self,layername):
        return layername.replace("-changeset","")
    
    @classmethod
    def checkDateFormat(self,xdate):        
        return type(xdate) is str and re.search('^ALL$|^\d{4}\-\d{2}-\d{2}$',xdate)

        
    @classmethod
    def checkLayerName(self,lname):
        '''make sure layer name conforms to v:x format or user is asking for ALL layers'''
        return type(lname) is str and re.search('^ALL$|^v:x\d+$',lname) 
        
    @classmethod
    def checkCQL(self,c):
        '''since CQL commands are freeform strings we need to try and validate at least the most basic errors. this is pretty basic and just looks for valid predicates
        <predicate> ::= <comparison predicate> | <text predicate> | <null predicate> | <temporal predicate> | <classification predicate> | <existence_predicate> | <between predicate> | <include exclude predicate>       
        BNF http://docs.geotools.org/latest/userguide/library/cql/internal.html
        NB. Needs more work...?'''
        v = 0
        
        #comp pred
        if re.match('.*(?:!=|=|<|>|<=|>=)',c):
            v+=1
        #text pred
        if re.match('.*(?:not\s*)?like.*',c,re.IGNORECASE):
            v+=2
        #null pred
        if re.match('.*is\s*(?:not\s*)?null.*',c,re.IGNORECASE):
            v+=4
        #time pred
        if re.match('.*(?:before|during|after)',c,re.IGNORECASE):
            v+=8
        #clsf pred, not defined
        #exst pred
        if re.match('.*(?:does-not-)?exist',c,re.IGNORECASE):
            v+=32
        #btwn pred
        if re.match('.*(?:not\s*)?between',c,re.IGNORECASE):
            v+=64
        #incl pred
        if re.match('.*(?:include|exclude)',c,re.IGNORECASE):
            v+=128
        #geo predicates just for good measure, returns v=16 overriding classification pred
        if re.match('.*(?:equals|disjoint|intersects|touches|crosses|within|contains|overlaps|bbox|dwithin|beyond|relate)',c,re.IGNORECASE):
            v+=16
            
        if v>0:
            '''return ("&cql_filter="+c,v)'''
            return "&cql_filter="+c
        else:
            return ""
    
    
    
    def parseLayerList(self,ds):
        layers = ()
        for li in range(0,ds.GetLayerCount()):
            layer = ds.GetLayerByIndex(li)

            print "layer",layer.GetName()#,layer.GetLayerDefn()
            layers += (layer.GetName(),)

        return layers

    
    def parseFeatureList(self,ds,layername):
        head = ()
        body = ()
        
        #lep = self.constructEndpoint(layername)
        #lds = self.connect(lep)
        #geo = ds.GetGeomType()
        #sref = ds.GetSpatialRef()
        '''get the first row and use it to build a headers list = ((colname,coltype),)'''
        layer = ds.GetLayerByName(layername)
        feat = layer.GetNextFeature()
        for fc in range(0,feat.GetFieldCount()):
            fdef = feat.GetFieldDefnRef(fc)

            head += ((fdef.GetName(),fdef.GetType()),) 
        print "head",layer.GetName(),head
        
        '''get data by column header'''
        while feat is not None:
            '''put the geo obj in the first col'''
            data = (feat.GetGeometryRef().ExportToWkt(),)
            for col in head:
                data += (feat.GetField(col[0]),)
                
            print "data",data
            body += (data,) 
            
            feat = layer.GetNextFeature()
        return (head,body)
            
                
    def parseFieldList(self,ds,layername):
        head = ()
        layer = ds.GetLayerByName(layername)
        feat = layer.GetNextFeature()
        for fc in range(0,feat.GetFieldCount()):
            fdef = feat.GetFieldDefnRef(fc)

            head += ((fdef.GetName(),fdef.GetType()),) 
            
        print "head",layer.GetName(),head
        
