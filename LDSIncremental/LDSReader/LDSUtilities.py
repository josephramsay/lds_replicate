'''
Simple LDS specific utilities class

Created on 28/08/2012

@author: jramsay
'''


import re
import logging

ldslog = logging.getLogger('LDS')

class LDSUtilities(object):
    '''Does the LDS related stuff not specifically part of the datastore''' 
    
    def __init__(self):
        pass
    
    
    @classmethod
    def splitLayerName(self,layername):
        '''Splits a layer name typically in the format v:x### into /v/x### for URI inclusion'''
        return "/"+layername.split(":")[0]+"/"+layername.split(":")[1]
    
    @classmethod
    def cropChangeset(self,layername):
        '''Removes changeset identifier from layer name'''
        return layername.rstrip("-changeset")
    
    @classmethod
    def checkDateFormat(self,xdate):
        '''Checks a date parameter conforms to yyyy-mm-dd format or is the "ALL" keyword'''        
        return type(xdate) is str and re.search('^ALL$|^\d{4}\-\d{2}-\d{2}$',xdate)

        
    @classmethod
    def checkLayerName(self,lname):
        '''Makes sure a layer name conforms to v:x format or user is asking for ALL layers'''
        return type(lname) is str and re.search('^ALL$|^v:x\d+$',lname) 
        
    @classmethod
    def checkCQL(self,cql):
        '''Since CQL commands are freeform strings we need to try and validate at least the most basic errors. This is very simple
        RE matcher that just looks for valid predicates.
        
        <predicate> ::= <comparison predicate> | <text predicate> | <null predicate> | <temporal predicate> | <classification predicate> | <existence_predicate> | <between predicate> | <include exclude predicate>
               
        BNF http://docs.geotools.org/latest/userguide/library/cql/internal.html'''
        v = 0
        
        #comp pred
        if re.match('.*(?:!=|=|<|>|<=|>=)',cql):
            v+=1
        #text pred
        if re.match('.*(?:not\s*)?like.*',cql,re.IGNORECASE):
            v+=2
        #null pred
        if re.match('.*is\s*(?:not\s*)?null.*',cql,re.IGNORECASE):
            v+=4
        #time pred
        if re.match('.*(?:before|during|after)',cql,re.IGNORECASE):
            v+=8
        #clsf pred, not defined
        #exst pred
        if re.match('.*(?:does-not-)?exist',cql,re.IGNORECASE):
            v+=32
        #btwn pred
        if re.match('.*(?:not\s*)?between',cql,re.IGNORECASE):
            v+=64
        #incl pred
        if re.match('.*(?:include|exclude)',cql,re.IGNORECASE):
            v+=128
        #geo predicates just for good measure, returns v=16 overriding classification pred
        if re.match('.*(?:equals|disjoint|intersects|touches|crosses|within|contains|overlaps|bbox|dwithin|beyond|relate)',cql,re.IGNORECASE):
            v+=16
            
        ldslog.debug("CQL check:"+cql+":"+str(v))
        if v>0:
            return "&cql_filter="+cql
        else:
            return ""
    
    
#    #no longer used
#    
#    def parseLayerList(self,ds):
#        layers = ()
#        for li in range(0,ds.GetLayerCount()):
#            layer = ds.GetLayerByIndex(li)
#
#            print "layer",layer.GetName()#,layer.GetLayerDefn()
#            layers += (layer.GetName(),)
#
#        return layers
#
#    
#    def parseFeatureList(self,ds,layername):
#        head = ()
#        body = ()
#        
#        #lep = self.constructEndpoint(layername)
#        #lds = self.connect(lep)
#        #geo = ds.GetGeomType()
#        #sref = ds.GetSpatialRef()
#        '''get the first row and use it to build a headers list = ((colname,coltype),)'''
#        layer = ds.GetLayerByName(layername)
#        feat = layer.GetNextFeature()
#        for fc in range(0,feat.GetFieldCount()):
#            fdef = feat.GetFieldDefnRef(fc)
#
#            head += ((fdef.GetName(),fdef.GetType()),) 
#        print "head",layer.GetName(),head
#        
#        '''get data by column header'''
#        while feat is not None:
#            '''put the geo obj in the first col'''
#            data = (feat.GetGeometryRef().ExportToWkt(),)
#            for col in head:
#                data += (feat.GetField(col[0]),)
#                
#            print "data",data
#            body += (data,) 
#            
#            feat = layer.GetNextFeature()
#        return (head,body)
#            
#                
#    def parseFieldList(self,ds,layername):
#        head = ()
#        layer = ds.GetLayerByName(layername)
#        feat = layer.GetNextFeature()
#        for fc in range(0,feat.GetFieldCount()):
#            fdef = feat.GetFieldDefnRef(fc)
#
#            head += ((fdef.GetName(),fdef.GetType()),) 
#            
#        print "head",layer.GetName(),head
        
